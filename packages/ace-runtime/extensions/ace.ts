/**
 * ACE 0.1 extension for Pi: receive external events into the session you are chatting in, and
 * publish events to peers.
 *
 * Configuration is read from `.ace.json` in the session working directory (RFC §10 runtime
 * configuration), or from `ACE_CONFIG` when that points somewhere else:
 *
 * ```json
 * {
 *   "sender": "agent-a",
 *   "defaultActivation": "next_turn",
 *   "inputs":  [ { "name": "from-b", "transport": "redis-streams", "stream": "ace:to-a", "group": "ace-pi" } ],
 *   "outputs": [ { "name": "to-b",   "transport": "redis-streams", "stream": "ace:to-b" } ]
 * }
 * ```
 *
 * Start Pi with the extension:
 *
 * ```bash
 * pi --extension /path/to/ace-runtime/extensions/ace.ts
 * ```
 *
 * Receiving: while a turn runs, `next_turn` events are queued after it (`followUp`) and `immediate`
 * events at its next boundary (`steer`); while Pi is idle the event starts a turn. `manual` events are
 * retained in memory — inspect and activate them with `/ace`, `/ace pending`, `/ace activate <sender> <id>`.
 *
 * Publishing: the `ace_publish` tool appends an ACE message to a configured output, which is how two
 * Pi agents talk to each other (agent A consumes `ace:to-a` and publishes to `ace:to-b`; agent B the
 * other way round). The address lives in configuration, never in the message (RFC §4.1).
 *
 * `ACE_CONFIG` points at a different configuration file; every MQ setting stays in that file.
 * `ACE_LOG=1` also logs runtime lines in modes without a UI.
 */

import { randomUUID } from "node:crypto";
import { StringEnum } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import {
	ACE_CONFIG_FILENAME,
	type AceLogger,
	type AcePublisher,
	AceRuntime,
	createPublishers,
	createTransports,
	type InputConfig,
	type OutputConfig,
	PiExtensionAdapter,
	type ResolvedAceConfig,
	resolveAceConfig,
	validateAceMessage,
} from "../src/index.ts";

function describeError(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

/** Report to the user: notification in UI modes, stderr in print/JSON modes. */
function report(ctx: ExtensionContext, message: string, type: "info" | "warning" | "error" = "info"): void {
	if (ctx.hasUI) {
		ctx.ui.notify(message, type);
		return;
	}
	console.error(message);
}

/** Runtime logs: last action on the status line, problems as notifications. */
function createLogger(ctx: ExtensionContext): AceLogger {
	const logToStderr = process.env.ACE_LOG === "1";
	return {
		info: (line) => {
			if (ctx.hasUI) {
				ctx.ui.setStatus("ace", line.replace(/^\[ACE\] /, "ace: "));
				return;
			}
			if (logToStderr) console.error(line);
		},
		warn: (line) => report(ctx, line, "warning"),
		error: (line) => report(ctx, line, "error"),
	};
}

function truncate(text: string, limit = 60): string {
	return text.length <= limit ? text : `${text.slice(0, limit - 1)}…`;
}

function describeTarget(config: InputConfig | OutputConfig): string {
	const address = config.stream ?? config.subject ?? config.topic ?? "(no address)";
	return `${config.name} (${config.transport} ${String(address)})`;
}

/**
 * Resolve which configured output a publish call targets.
 *
 * `target` names an entry of `outputs`; it is optional when exactly one output is configured.
 */
function selectPublisher(
	publishers: Readonly<Record<string, AcePublisher>>,
	target: string | undefined,
): { name: string; publisher: AcePublisher } {
	const names = Object.keys(publishers);
	if (names.length === 0) {
		throw new Error(`no outputs configured; add an "outputs" entry to ${ACE_CONFIG_FILENAME}`);
	}
	if (target === undefined) {
		if (names.length > 1) throw new Error(`several outputs configured (${names.join(", ")}); pass target`);
		const name = names[0] as string;
		return { name, publisher: publishers[name] as AcePublisher };
	}
	const publisher = publishers[target];
	if (!publisher) throw new Error(`unknown output "${target}" (configured: ${names.join(", ")})`);
	return { name: target, publisher };
}

export default function aceExtension(pi: ExtensionAPI): void {
	let sessionContext: ExtensionContext | undefined;
	let runtime: AceRuntime | undefined;
	let publishers: Record<string, AcePublisher> = {};
	let resolvedConfig: ResolvedAceConfig | undefined;
	let transportErrorReported = false;

	const adapter = new PiExtensionAdapter({ pi, isIdle: () => sessionContext?.isIdle() ?? true });

	pi.on("session_start", async (_event, ctx) => {
		sessionContext = ctx;
		if (runtime) return;

		let resolved: ResolvedAceConfig;
		try {
			resolved = resolveAceConfig({ cwd: ctx.cwd });
		} catch (error) {
			report(ctx, `[ace] not started: ${describeError(error)}`, "warning");
			return;
		}

		const logger = createLogger(ctx);
		publishers = createPublishers(resolved.outputs, {
			onError: (error) => report(ctx, `[ace] publish transport error: ${describeError(error)}`, "error"),
		});
		runtime = new AceRuntime({
			engine: adapter,
			inputs: resolved.inputs,
			transports: createTransports(resolved.inputs, {
				onError: (error) => {
					// A broker that dies mid-session would otherwise repeat the same error.
					if (transportErrorReported) return;
					transportErrorReported = true;
					report(ctx, `[ace] transport error: ${describeError(error)}`, "error");
				},
			}),
			...(resolved.defaultActivation ? { defaultActivation: resolved.defaultActivation } : {}),
			logger,
		});

		try {
			await runtime.start();
			resolvedConfig = resolved;
			const outputs =
				resolved.outputs.length > 0 ? `; publishing to ${resolved.outputs.map(describeTarget).join(", ")}` : "";
			report(
				ctx,
				`[ace] listening (${resolved.source}): ${resolved.inputs.map(describeTarget).join(", ")}${outputs}`,
			);
		} catch (error) {
			runtime = undefined;
			resolvedConfig = undefined;
			report(
				ctx,
				`[ace] could not start: ${describeError(error)} (check the broker in .ace.json, then restart Pi)`,
				"error",
			);
		}
	});

	pi.on("session_shutdown", async () => {
		const active = runtime;
		const activePublishers = Object.values(publishers);
		runtime = undefined;
		publishers = {};
		resolvedConfig = undefined;
		sessionContext = undefined;
		await active?.stop();
		for (const publisher of activePublishers) await publisher.close();
	});

	pi.registerTool({
		name: "ace_publish",
		label: "ACE Publish",
		description:
			"Publish an ACE 0.1 event to a configured peer agent or service (the `outputs` of .ace.json). " +
			"The recipient's agent receives the body as an external event and acts on it on its own. " +
			"The body is opaque to ACE: write plain text the peer's agent should understand.",
		promptGuidelines: [
			"Use ace_publish to notify another agent or service; keep the body self-contained.",
			"Events you publish are visible to every agent consuming the target stream, including yourself if you consume it.",
		],
		parameters: Type.Object({
			body: Type.String({ description: "Event body; the peer's agent reads this" }),
			activation: Type.Optional(
				StringEnum(["default", "next_turn", "immediate", "manual"] as const, {
					description: "How urgently the peer should process it; omit unless you know the peer's setup",
				}),
			),
			target: Type.Optional(
				Type.String({ description: "Configured output name; required only when several exist" }),
			),
			id: Type.Optional(Type.String({ description: "Message id for correlation; generated when omitted" })),
		}),

		async execute(_toolCallId, params) {
			const { name, publisher } = selectPublisher(publishers, params.target);
			const sender = resolvedConfig?.sender ?? `pi-${process.pid}`;
			const message = validateAceMessage({
				aceVersion: "0.1",
				id: params.id ?? `evt_${randomUUID()}`,
				sender,
				activation: params.activation ?? "default",
				body: params.body,
			});

			await publisher.publish(message);

			return {
				content: [
					{
						type: "text",
						text: `Published ${message.id} from ${sender} to output "${name}" (activation: ${message.activation}).`,
					},
				],
				details: {
					id: message.id,
					sender: message.sender,
					activation: message.activation,
					target: name,
					bodyLength: message.body.length,
				},
			};
		},
	});

	pi.registerCommand("ace", {
		description: "ACE event runtime: status, pending manual events, activation",
		// No argument completions on purpose: an open completion popup swallows the first Enter
		// in the TUI, which would run the command before its arguments are finished.
		handler: async (args, ctx) => {
			const [subcommand, ...rest] = args.trim().split(/\s+/);

			if (!runtime) {
				report(
					ctx,
					`[ace] not running: add ${ACE_CONFIG_FILENAME} to ${ctx.cwd} (or set ACE_STREAM) and restart Pi`,
					"warning",
				);
				return;
			}

			if (subcommand === "activate") {
				const [sender, id] = rest;
				if (!sender || !id) {
					report(ctx, "[ace] usage: /ace activate <sender> <id>", "warning");
					return;
				}
				try {
					await runtime.activatePendingEvent(sender, id);
					report(ctx, `[ace] activated ${sender}/${id}`);
				} catch (error) {
					report(ctx, `[ace] ${describeError(error)}`, "error");
				}
				return;
			}

			const pending = runtime.pendingEvents;
			if (subcommand === "pending") {
				if (pending.length === 0) {
					report(ctx, "[ace] no pending manual events");
					return;
				}
				const lines = pending.map(
					(event) => `${event.message.sender}/${event.message.id}: ${truncate(event.message.body)}`,
				);
				report(ctx, `[ace] pending manual events (${pending.length}):\n${lines.join("\n")}`);
				return;
			}

			const state = adapter.isRunning() ? "running" : "idle";
			const outputs = Object.keys(publishers);
			report(
				ctx,
				`[ace] ${resolvedConfig?.source ?? "started"} as ${resolvedConfig?.sender ?? "?"}, agent ${state}, ` +
					`${pending.length} pending manual event(s), ${outputs.length > 0 ? `outputs: ${outputs.join(", ")}` : "no outputs"}`,
			);
		},
	});
}
