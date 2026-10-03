import type { AgentEngine } from "../agent/agent-engine.ts";
import type { AceLogger } from "../logger.ts";
import type { ConcreteActivation } from "../protocol/ace-message.ts";
import { AceValidationError, decodeAceMessage } from "../protocol/validator.ts";
import type { Transport } from "../transport/transport.ts";
import { DEFAULT_RUNTIME_ACTIVATION, resolveActivation } from "./activation-resolver.ts";
import { type DispatchResult, EventDispatcher } from "./event-dispatcher.ts";
import { AceConfigError, type InputConfig, validateInputConfig } from "./input-config.ts";
import { type PendingAceEvent, PendingEventStore } from "./pending-event-store.ts";

export interface AceRuntimeOptions {
	/** Agent engine that receives ACE events (RFC-facing §15). */
	engine: AgentEngine;
	/** Where messages come from and which activation the receiver forces (RFC §4.1, §8). */
	inputs: readonly InputConfig[];
	/** Transport instances by name; `InputConfig.transport` selects one. */
	transports: Readonly<Record<string, Transport>>;
	/** Fallback activation; ACE 0.1 requires `next_turn` when unset (RFC §8). */
	defaultActivation?: ConcreteActivation;
	logger?: AceLogger;
}

/** Result of handling one raw inbound message. */
export interface AceHandleResult extends DispatchResult {
	readonly inputName: string;
}

/**
 * ACE 0.1 runtime: receive → validate → resolve activation → dispatch (RFC §9,
 * design doc §20).
 *
 * The runtime owns no MQ metadata, no agent loop, and no transport internals; it
 * is the boundary between a transport and an agent engine.
 */
export class AceRuntime {
	private readonly engine: AgentEngine;
	private readonly inputs: readonly InputConfig[];
	private readonly transports: Readonly<Record<string, Transport>>;
	private readonly defaultActivation: ConcreteActivation;
	private readonly logger: AceLogger;
	private readonly pendingEventStore = new PendingEventStore();
	private readonly dispatcher: EventDispatcher;
	private started = false;

	constructor(options: AceRuntimeOptions) {
		this.inputs = options.inputs.map(validateInputConfig);
		const seenTransports = new Set<string>();
		for (const input of this.inputs) {
			if (seenTransports.has(input.transport)) {
				throw new AceConfigError(
					`transport "${input.transport}" is used by more than one input; ACE events would be delivered twice`,
				);
			}
			seenTransports.add(input.transport);
			if (!options.transports[input.transport]) {
				throw new AceConfigError(`input "${input.name}" references unknown transport "${input.transport}"`);
			}
		}

		this.engine = options.engine;
		this.transports = options.transports;
		this.defaultActivation = options.defaultActivation ?? DEFAULT_RUNTIME_ACTIVATION;
		this.logger = options.logger ?? {};
		this.dispatcher = new EventDispatcher(this.engine, this.pendingEventStore, this.logger);
	}

	/** Connect every input's transport (RFC §33). */
	async start(): Promise<void> {
		if (this.started) throw new AceConfigError("ACE runtime is already started");
		this.started = true;
		try {
			for (const input of this.inputs) {
				await this.transports[input.transport].start((raw) => this.deliver(raw, input));
			}
		} catch (error) {
			// Do not claim to be started when a transport refused to connect.
			this.started = false;
			throw error;
		}
		this.logger.info?.(
			`[ACE] runtime started inputs=${this.inputs.map((input) => input.name).join(",")} defaultActivation=${this.defaultActivation}`,
		);
	}

	/** Disconnect transports and wait for the agent engine to settle (RFC §33). */
	async stop(): Promise<void> {
		if (!this.started) return;
		this.started = false;
		for (const input of this.inputs) {
			await this.transports[input.transport].stop();
		}
		await this.engine.waitForIdle();
		this.logger.info?.("[ACE] runtime stopped");
	}

	/**
	 * Full ACE path for one raw message.
	 *
	 * Throws {@link AceValidationError} for non-conforming messages. The
	 * transport-facing path ({@link start}) logs and drops those instead, and
	 * lets other errors propagate so the transport can retry or dead-letter
	 * (design doc §30).
	 */
	async handleRawMessage(raw: unknown, input: InputConfig): Promise<AceHandleResult> {
		const message = decodeAceMessage(raw);
		const activation = resolveActivation(message, input, this.defaultActivation);
		this.logger.info?.(`[ACE] received id=${message.id} sender=${message.sender} input=${input.name}`);
		const result = await this.dispatcher.dispatch(message, input.name, activation);
		return { ...result, inputName: input.name };
	}

	/** Handle a raw message addressed to a configured input by name. */
	async handleMessage(raw: unknown, inputName: string): Promise<AceHandleResult> {
		const input = this.inputs.find((candidate) => candidate.name === inputName);
		if (!input) throw new AceConfigError(`unknown input "${inputName}"`);
		return this.handleRawMessage(raw, input);
	}

	/** Events retained for `manual` activation (RFC §7.3, §12). */
	get pendingEvents(): readonly PendingAceEvent[] {
		return this.pendingEventStore.list();
	}

	/**
	 * Explicitly activate a retained `manual` event.
	 *
	 * ACE 0.1 leaves the trigger to the runtime or user (§7.3); this is that
	 * runtime control hook. Identity is `(sender, id)` (RFC §5.2).
	 */
	async activatePendingEvent(sender: string, id: string): Promise<void> {
		const event = this.pendingEventStore.take(sender, id);
		if (!event) {
			throw new Error(`No pending ACE event for sender="${sender}" id="${id}"`);
		}
		this.logger.info?.(`[ACE] activating id=${id} sender=${sender} input=${event.inputName}`);
		await this.engine.inject(event.message, "next_turn");
	}

	private async deliver(raw: unknown, input: InputConfig): Promise<void> {
		try {
			await this.handleRawMessage(raw, input);
		} catch (error) {
			if (error instanceof AceValidationError) {
				const fields = error.issues.map((issue) => issue.path || "<message>").join(",");
				this.logger.warn?.(`[ACE] rejected input=${input.name} fields=${fields}`);
				return;
			}
			throw error;
		}
	}
}
