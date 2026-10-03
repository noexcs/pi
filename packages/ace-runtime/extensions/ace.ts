/**
 * ACE 0.1 extension for Pi: inject external events into the session you are chatting in.
 *
 * Configuration is read from `.ace.json` in the session working directory (RFC §10 runtime
 * configuration), or from `ACE_CONFIG` when that points somewhere else:
 *
 * ```json
 * {
 *   "defaultActivation": "next_turn",
 *   "inputs": [
 *     {
 *       "name": "build-events",
 *       "transport": "redis-streams",
 *       "stream": "ace:events",
 *       "group": "ace-pi",
 *       "url": "redis://127.0.0.1:6379",
 *       "activation": "next_turn"
 *     }
 *   ]
 * }
 * ```
 *
 * Start Pi with the extension:
 *
 * ```bash
 * pi --extension /path/to/ace-runtime/extensions/ace.ts
 * ```
 *
 * and publish from anywhere:
 *
 * ```bash
 * redis-cli XADD ace:events '*' message \
 *   '{"aceVersion":"0.1","id":"e1","sender":"ci","activation":"next_turn","body":"Build failed."}'
 * ```
 *
 * While a turn runs, `next_turn` events are queued after it (`followUp`) and `immediate` events at its
 * next boundary (`steer`); while Pi is idle the event starts a turn. `manual` events are retained in
 * memory — inspect and activate them with `/ace`, `/ace pending`, `/ace activate <sender> <id>`.
 *
 * Without a config file, a single input is taken from `ACE_STREAM` (plus `ACE_GROUP`, `ACE_REDIS_URL`,
 * `ACE_CONSUMER`, `ACE_FIELD`); `ACE_LOG=1` also logs runtime lines in modes without a UI.
 */

import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
	ACE_CONFIG_FILENAME,
	type AceLogger,
	AceRuntime,
	createTransports,
	type InputConfig,
	PiExtensionAdapter,
	type ResolvedAceInputs,
	resolveAceInputs,
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

function describeInput(input: InputConfig): string {
	const address = input.stream ?? input.subject ?? input.topic ?? "(no address)";
	return `${input.name} (${input.transport} ${String(address)})`;
}

export default function aceExtension(pi: ExtensionAPI): void {
	let sessionContext: ExtensionContext | undefined;
	let runtime: AceRuntime | undefined;
	let origin: string | undefined;

	const adapter = new PiExtensionAdapter({ pi, isIdle: () => sessionContext?.isIdle() ?? true });

	pi.on("session_start", async (_event, ctx) => {
		sessionContext = ctx;
		if (runtime) return;

		let resolved: ResolvedAceInputs;
		try {
			resolved = resolveAceInputs({ cwd: ctx.cwd });
		} catch (error) {
			report(ctx, `[ace] not started: ${describeError(error)}`, "warning");
			return;
		}

		const logger = createLogger(ctx);
		runtime = new AceRuntime({
			engine: adapter,
			inputs: resolved.inputs,
			transports: createTransports(resolved.inputs, {
				onError: (error) => report(ctx, `[ace] transport error: ${describeError(error)}`, "error"),
			}),
			...(resolved.defaultActivation ? { defaultActivation: resolved.defaultActivation } : {}),
			logger,
		});

		try {
			await runtime.start();
			origin = resolved.source;
			report(ctx, `[ace] listening (${resolved.source}): ${resolved.inputs.map(describeInput).join(", ")}`);
		} catch (error) {
			runtime = undefined;
			origin = undefined;
			report(ctx, `[ace] could not start: ${describeError(error)}`, "error");
		}
	});

	pi.on("session_shutdown", async () => {
		const active = runtime;
		runtime = undefined;
		origin = undefined;
		sessionContext = undefined;
		await active?.stop();
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
			report(ctx, `[ace] ${origin ?? "started"}, agent ${state}, ${pending.length} pending manual event(s)`);
		},
	});
}
