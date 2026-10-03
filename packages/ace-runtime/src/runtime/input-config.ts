import type { Activation } from "../protocol/ace-message.ts";
import { isActivation } from "../protocol/ace-message.ts";
import { describeValue, isPlainObject } from "../utils.ts";

/**
 * Runtime input configuration (RFC §4.1, §8, §9).
 *
 * Not an ACE protocol object: `transport` and any transport-specific keys
 * (Kafka `topic`, NATS `subject`, …) are interpreted by the transport adapter,
 * never by ACE.
 */
export interface InputConfig {
	name: string;
	transport: string;
	activation?: Activation;
	[key: string]: unknown;
}

/** Thrown when runtime configuration is unusable (RFC-facing §10). */
export class AceConfigError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "AceConfigError";
	}
}

/** Validate a single input configuration entry (RFC §9). */
export function validateInputConfig(value: unknown): InputConfig {
	if (!isPlainObject(value)) {
		throw new AceConfigError(`input config must be an object, received ${describeValue(value)}`);
	}

	const { name, transport, activation } = value;
	if (typeof name !== "string" || name.length === 0) {
		throw new AceConfigError("input config requires a non-empty name");
	}
	if (typeof transport !== "string" || transport.length === 0) {
		throw new AceConfigError(`input "${name}" requires a non-empty transport`);
	}
	if (activation !== undefined && !isActivation(activation)) {
		throw new AceConfigError(`input "${name}" has invalid activation ${describeValue(activation)}`);
	}

	return { ...value, name, transport, activation };
}
