import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { ConcreteActivation } from "../protocol/ace-message.ts";
import { isConcreteActivation } from "../protocol/ace-message.ts";
import { RedisStreamsTransport, redisStreamsConfigFrom } from "../transport/redis-streams-transport.ts";
import type { Transport } from "../transport/transport.ts";
import { describeValue, isPlainObject } from "../utils.ts";
import { AceConfigError, type InputConfig, validateInputConfig } from "./input-config.ts";

/** Name of the runtime configuration file read from the session working directory. */
export const ACE_CONFIG_FILENAME = ".ace.json";

/** Transport kinds this runtime can build from configuration (RFC §4.1 lists the others). */
export const SUPPORTED_TRANSPORTS: readonly string[] = ["redis-streams"];

/**
 * Runtime configuration (RFC §10) as stored in {@link ACE_CONFIG_FILENAME}.
 *
 * Not an ACE protocol object: transports and their addresses are deployment settings, and an
 * ACE message never carries them (RFC §4).
 */
export interface AceConfigFile {
	/** Fallback activation for inputs and messages that delegate with `default` (RFC §8). */
	defaultActivation?: ConcreteActivation;
	inputs: InputConfig[];
}

export interface LoadedAceConfig {
	/** Path the configuration was read from, for logs and `/ace` output. */
	source: string;
	config: AceConfigFile;
}

/** Validate a parsed `.ace.json` document. */
export function parseAceConfig(value: unknown, source: string): AceConfigFile {
	if (!isPlainObject(value)) {
		throw new AceConfigError(`${source} must contain a JSON object, received ${describeValue(value)}`);
	}

	const { defaultActivation, inputs } = value;
	if (defaultActivation !== undefined && !isConcreteActivation(defaultActivation)) {
		throw new AceConfigError(
			`${source}: defaultActivation must be immediate|next_turn|manual, received ${describeValue(defaultActivation)}`,
		);
	}
	if (!Array.isArray(inputs) || inputs.length === 0) {
		throw new AceConfigError(`${source}: inputs must be a non-empty array`);
	}

	const parsed = inputs.map(validateInputConfig);
	for (const input of parsed) validateTransportSettings(input);
	return { defaultActivation, inputs: parsed };
}

/** Reject unknown transport kinds and invalid transport settings while reading the file. */
function validateTransportSettings(input: InputConfig): void {
	switch (input.transport) {
		case "redis-streams":
			redisStreamsConfigFrom(input);
			return;
		default:
			throw new AceConfigError(
				`input "${input.name}" uses unsupported transport ${describeValue(input.transport)} (available: ${SUPPORTED_TRANSPORTS.join(", ")})`,
			);
	}
}

/**
 * Load `.ace.json` from `ACE_CONFIG` or `<cwd>/.ace.json`.
 *
 * Returns `undefined` when neither exists, so a caller can fall back to another source.
 */
export function loadAceConfig(options: {
	cwd: string;
	env?: Readonly<Record<string, string | undefined>>;
}): LoadedAceConfig | undefined {
	const env = options.env ?? process.env;
	const source = env.ACE_CONFIG ?? join(options.cwd, ACE_CONFIG_FILENAME);
	if (!existsSync(source)) return undefined;

	let parsed: unknown;
	try {
		parsed = JSON.parse(readFileSync(source, "utf8"));
	} catch (error) {
		throw new AceConfigError(
			`${source} is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	return { source, config: parseAceConfig(parsed, source) };
}

/**
 * Build one input from environment variables, for runs without `.ace.json`.
 *
 * Returns `undefined` when `ACE_STREAM` is unset.
 */
export function inputFromEnvironment(
	env: Readonly<Record<string, string | undefined>> = process.env,
): InputConfig | undefined {
	if (!env.ACE_STREAM) return undefined;
	return {
		name: env.ACE_INPUT ?? "ace-events",
		transport: "redis-streams",
		stream: env.ACE_STREAM,
		group: env.ACE_GROUP ?? `ace-pi-${process.pid}`,
		...(env.ACE_REDIS_URL ? { url: env.ACE_REDIS_URL } : {}),
		...(env.ACE_CONSUMER ? { consumer: env.ACE_CONSUMER } : {}),
		...(env.ACE_FIELD ? { field: env.ACE_FIELD } : {}),
		activation: "default",
	};
}

/** Inputs, their activation default, and where they came from. */
export interface ResolvedAceInputs {
	inputs: InputConfig[];
	defaultActivation?: ConcreteActivation;
	/** Config path or `"environment"`, for logs and `/ace` output. */
	source: string;
}

/**
 * Resolve the inputs a host should run: `.ace.json` (or `ACE_CONFIG`) first, then a single input
 * from `ACE_STREAM`.
 *
 * Throws {@link AceConfigError} when neither source configures an input, when the file is invalid,
 * or when it names an unsupported transport.
 */
export function resolveAceInputs(options: {
	cwd: string;
	env?: Readonly<Record<string, string | undefined>>;
}): ResolvedAceInputs {
	const env = options.env ?? process.env;
	const loaded = loadAceConfig({ cwd: options.cwd, env });
	if (loaded) {
		return {
			inputs: loaded.config.inputs,
			defaultActivation: loaded.config.defaultActivation,
			source: loaded.source,
		};
	}

	const fromEnvironment = inputFromEnvironment(env);
	if (!fromEnvironment) {
		throw new AceConfigError(
			`no ${ACE_CONFIG_FILENAME} in ${options.cwd} and ACE_STREAM is unset; nothing to consume`,
		);
	}
	return { inputs: [fromEnvironment], source: "environment" };
}

/**
 * Create one transport per configured input, keyed by input name (the key
 * {@link AceRuntime} expects).
 */
export function createTransports(
	inputs: readonly InputConfig[],
	options: { onError: (error: unknown) => void },
): Record<string, Transport> {
	const transports: Record<string, Transport> = {};
	for (const input of inputs) {
		transports[input.name] = createTransport(input, options.onError);
	}
	return transports;
}

function createTransport(input: InputConfig, onError: (error: unknown) => void): Transport {
	switch (input.transport) {
		case "redis-streams":
			return new RedisStreamsTransport(input, { onError });
		default:
			throw new AceConfigError(
				`input "${input.name}" uses unsupported transport "${input.transport}" (available: ${SUPPORTED_TRANSPORTS.join(", ")})`,
			);
	}
}
