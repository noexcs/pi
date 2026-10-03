import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { ConcreteActivation } from "../protocol/ace-message.ts";
import { isConcreteActivation } from "../protocol/ace-message.ts";
import type { AcePublisher } from "../transport/redis-streams-publisher.ts";
import { RedisStreamsPublisher } from "../transport/redis-streams-publisher.ts";
import {
	REDIS_STREAMS_DEFAULTS,
	RedisStreamsTransport,
	redisStreamsConfigFrom,
} from "../transport/redis-streams-transport.ts";
import type { Transport } from "../transport/transport.ts";
import { describeValue, isPlainObject } from "../utils.ts";
import {
	AceConfigError,
	type InputConfig,
	optionalStringField,
	requiredStringField,
	validateInputConfig,
} from "./input-config.ts";

/**
 * Name of the runtime configuration file read from the session working directory.
 *
 * Every MQ setting (addresses, streams, groups, targets, identity) lives here; the code holds only
 * the generic mechanisms and the defaults a transport falls back to.
 */
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
	/** Sender identifier this session publishes under (RFC §5.3); required once `outputs` exist. */
	sender?: string;
	inputs: InputConfig[];
	/** Publishing targets (RFC §19); the address stays here, never in the message (§4.1). */
	outputs?: OutputConfig[];
}

/** A publishing target: the keys of an input (`name`, `transport`, address, …) minus consumer-only ones. */
export interface OutputConfig {
	name: string;
	transport: string;
	[key: string]: unknown;
}

export interface LoadedAceConfig {
	/** Path the configuration was read from, for logs and `/ace` output. */
	source: string;
	config: AceConfigFile;
}

/** Inputs, publishing targets, identity, the activation default, and where they came from. */
export interface ResolvedAceConfig {
	inputs: InputConfig[];
	outputs: OutputConfig[];
	defaultActivation?: ConcreteActivation;
	/** `pi-<pid>` when the configuration does not name one. */
	sender: string;
	source: string;
}

/** Validate a parsed `.ace.json` document. */
export function parseAceConfig(value: unknown, source: string): AceConfigFile {
	if (!isPlainObject(value)) {
		throw new AceConfigError(`${source} must contain a JSON object, received ${describeValue(value)}`);
	}

	const { defaultActivation, inputs, sender } = value;
	if (defaultActivation !== undefined && !isConcreteActivation(defaultActivation)) {
		throw new AceConfigError(
			`${source}: defaultActivation must be immediate|next_turn|manual, received ${describeValue(defaultActivation)}`,
		);
	}
	if (!Array.isArray(inputs) || inputs.length === 0) {
		throw new AceConfigError(`${source}: inputs must be a non-empty array`);
	}
	if (sender !== undefined && (typeof sender !== "string" || sender.length === 0)) {
		throw new AceConfigError(`${source}: sender must be a non-empty string, received ${describeValue(sender)}`);
	}

	const parsedInputs = inputs.map(validateInputConfig);
	for (const input of parsedInputs) validateTransportSettings(input);

	const parsedOutputs = parseOutputs(value.outputs, source);
	if (parsedOutputs && sender === undefined) {
		throw new AceConfigError(`${source}: sender is required when outputs are configured (peers identify you by it)`);
	}

	return {
		defaultActivation,
		...(sender === undefined ? {} : { sender }),
		inputs: parsedInputs,
		...(parsedOutputs ? { outputs: parsedOutputs } : {}),
	};
}

function parseOutputs(value: unknown, source: string): OutputConfig[] | undefined {
	if (value === undefined) return undefined;
	if (!Array.isArray(value) || value.length === 0) {
		throw new AceConfigError(`${source}: outputs must be a non-empty array when present`);
	}

	const outputs = value.map((entry) => validateOutputConfig(entry));
	const names = new Set<string>();
	for (const output of outputs) {
		if (names.has(output.name))
			throw new AceConfigError(`${source}: output name "${output.name}" is configured twice`);
		names.add(output.name);
		validateOutputSettings(output);
	}
	return outputs;
}

/** Validate one publishing target: same structure as an input, without consumer settings. */
export function validateOutputConfig(value: unknown): OutputConfig {
	if (!isPlainObject(value)) {
		throw new AceConfigError(`output config must be an object, received ${describeValue(value)}`);
	}

	const { name, transport } = value;
	if (typeof name !== "string" || name.length === 0) {
		throw new AceConfigError("output config requires a non-empty name");
	}
	if (typeof transport !== "string" || transport.length === 0) {
		throw new AceConfigError(`output "${name}" requires a non-empty transport`);
	}
	return { ...value, name, transport };
}

function validateOutputSettings(output: OutputConfig): void {
	const subject = `output "${output.name}"`;
	switch (output.transport) {
		case "redis-streams":
			requiredStringField(output, "stream", subject);
			optionalStringField(output, "url", REDIS_STREAMS_DEFAULTS.url, subject);
			optionalStringField(output, "field", REDIS_STREAMS_DEFAULTS.field, subject);
			return;
		default:
			throw new AceConfigError(
				`output "${output.name}" uses unsupported transport ${describeValue(output.transport)} (available: ${SUPPORTED_TRANSPORTS.join(", ")})`,
			);
	}
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
 * Returns `undefined` when neither exists; {@link resolveAceConfig} turns that into an error.
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
 * Resolve everything a host needs to run ACE in a session.
 *
 * MQ configuration comes from `.ace.json` only — `ACE_CONFIG` selects a different file path, but
 * there is no environment-variable fallback for addresses, streams, or groups.
 */
export function resolveAceConfig(options: {
	cwd: string;
	env?: Readonly<Record<string, string | undefined>>;
}): ResolvedAceConfig {
	const loaded = loadAceConfig(options);
	if (!loaded) {
		throw new AceConfigError(
			`no ${ACE_CONFIG_FILENAME} in ${options.cwd}: create one (inputs to consume, optional outputs to publish to)`,
		);
	}

	return {
		inputs: loaded.config.inputs,
		outputs: loaded.config.outputs ?? [],
		defaultActivation: loaded.config.defaultActivation,
		sender: loaded.config.sender ?? `pi-${process.pid}`,
		source: loaded.source,
	};
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

/**
 * Create one publisher per configured output, keyed by output name (the key the publishing tool
 * looks up).
 */
export function createPublishers(
	outputs: readonly OutputConfig[],
	options: { onError: (error: unknown) => void },
): Record<string, AcePublisher> {
	const publishers: Record<string, AcePublisher> = {};
	for (const output of outputs) {
		publishers[output.name] = createPublisher(output, options.onError);
	}
	return publishers;
}

function createPublisher(output: OutputConfig, onError: (error: unknown) => void): AcePublisher {
	const subject = `output "${output.name}"`;
	switch (output.transport) {
		case "redis-streams":
			return new RedisStreamsPublisher({
				url: optionalStringField(output, "url", REDIS_STREAMS_DEFAULTS.url, subject),
				stream: requiredStringField(output, "stream", subject),
				field: optionalStringField(output, "field", REDIS_STREAMS_DEFAULTS.field, subject),
				onError,
			});
		default:
			throw new AceConfigError(
				`output "${output.name}" uses unsupported transport "${output.transport}" (available: ${SUPPORTED_TRANSPORTS.join(", ")})`,
			);
	}
}
