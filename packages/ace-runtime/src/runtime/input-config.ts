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

/** Anything with a name that carries transport settings: an input or an output. */
export interface NamedTransportConfig {
	name: string;
	transport: string;
	[key: string]: unknown;
}

/** Read a required non-empty string setting; `subject` names the owner in the error. */
export function requiredStringField(config: NamedTransportConfig, key: string, subject: string): string {
	const value = config[key];
	if (typeof value !== "string" || value.length === 0) {
		throw new AceConfigError(`${subject} needs a non-empty "${key}", received ${describeValue(value)}`);
	}
	return value;
}

/** Read an optional non-empty string setting, falling back to a default. */
export function optionalStringField(
	config: NamedTransportConfig,
	key: string,
	fallback: string,
	subject: string,
): string {
	const value = config[key];
	if (value === undefined) return fallback;
	if (typeof value !== "string" || value.length === 0) {
		throw new AceConfigError(`${subject} has invalid "${key}": ${describeValue(value)}`);
	}
	return value;
}

/** Read an optional positive integer setting, falling back to a default. */
export function positiveIntegerField(
	config: NamedTransportConfig,
	key: string,
	fallback: number,
	subject: string,
): number {
	const value = config[key];
	if (value === undefined) return fallback;
	if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
		throw new AceConfigError(`${subject} has invalid "${key}": ${describeValue(value)} (needs an integer >= 1)`);
	}
	return value;
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
