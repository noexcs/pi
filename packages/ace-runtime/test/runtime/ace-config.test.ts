import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
	ACE_CONFIG_FILENAME,
	createPublishers,
	createTransports,
	loadAceConfig,
	parseAceConfig,
	resolveAceConfig,
} from "../../src/runtime/ace-config.ts";
import { AceConfigError } from "../../src/runtime/input-config.ts";

const buildInput = {
	name: "build-events",
	transport: "redis-streams",
	stream: "ace:events",
	group: "ace-pi",
};

const peerOutput = { name: "to-b", transport: "redis-streams", stream: "ace:to-b" };

const directories: string[] = [];

function temporaryDirectory(): string {
	const directory = mkdtempSync(join(tmpdir(), "ace-config-"));
	directories.push(directory);
	return directory;
}

function writeConfig(directory: string, config: unknown): string {
	const path = join(directory, ACE_CONFIG_FILENAME);
	writeFileSync(path, JSON.stringify(config));
	return path;
}

afterEach(() => {
	while (directories.length > 0) rmSync(directories.pop() as string, { recursive: true, force: true });
});

describe("parseAceConfig", () => {
	it("accepts inputs and a default activation", () => {
		expect(parseAceConfig({ defaultActivation: "immediate", inputs: [buildInput] }, ".ace.json")).toEqual({
			defaultActivation: "immediate",
			inputs: [buildInput],
		});
	});

	it("accepts outputs together with a sender", () => {
		expect(parseAceConfig({ sender: "agent-a", inputs: [buildInput], outputs: [peerOutput] }, ".ace.json")).toEqual({
			sender: "agent-a",
			inputs: [buildInput],
			outputs: [peerOutput],
		});
	});

	it.each([
		["a non-object document", ["not", "an", "object"]],
		["a delegated defaultActivation", { defaultActivation: "default", inputs: [buildInput] }],
		["missing inputs", { defaultActivation: "next_turn" }],
		["empty inputs", { inputs: [] }],
		["an unsupported input transport", { inputs: [{ ...buildInput, transport: "kafka" }] }],
		["an input without a stream", { inputs: [{ name: "builds", transport: "redis-streams", group: "g" }] }],
		["an empty sender", { sender: "", inputs: [buildInput] }],
		["outputs without a sender", { inputs: [buildInput], outputs: [peerOutput] }],
		["empty outputs", { sender: "a", inputs: [buildInput], outputs: [] }],
		["a duplicated output name", { sender: "a", inputs: [buildInput], outputs: [peerOutput, peerOutput] }],
		[
			"an unsupported output transport",
			{ sender: "a", inputs: [buildInput], outputs: [{ ...peerOutput, transport: "nats" }] },
		],
		[
			"an output without a stream",
			{ sender: "a", inputs: [buildInput], outputs: [{ name: "to-b", transport: "redis-streams" }] },
		],
	])("rejects %s", (_name, document) => {
		expect(() => parseAceConfig(document, ".ace.json")).toThrow(AceConfigError);
	});

	it("names the file in the error", () => {
		expect(() => parseAceConfig({}, "/tmp/project/.ace.json")).toThrow(/\/tmp\/project\/\.ace\.json/);
	});
});

describe("loadAceConfig", () => {
	it("reads .ace.json from the working directory", () => {
		const cwd = temporaryDirectory();
		writeConfig(cwd, { inputs: [buildInput] });

		const loaded = loadAceConfig({ cwd });

		expect(loaded?.source).toBe(join(cwd, ACE_CONFIG_FILENAME));
		expect(loaded?.config.inputs).toEqual([buildInput]);
	});

	it("prefers ACE_CONFIG over the working directory", () => {
		const cwd = temporaryDirectory();
		const elsewhere = join(temporaryDirectory(), "custom.json");
		writeConfig(cwd, { inputs: [buildInput] });
		writeFileSync(elsewhere, JSON.stringify({ inputs: [{ ...buildInput, stream: "ace:custom" }] }));

		expect(loadAceConfig({ cwd, env: { ACE_CONFIG: elsewhere } })?.config.inputs[0]?.stream).toBe("ace:custom");
	});

	it("returns undefined without a config file", () => {
		expect(loadAceConfig({ cwd: temporaryDirectory(), env: {} })).toBeUndefined();
	});

	it("reports invalid JSON with the path", () => {
		const cwd = temporaryDirectory();
		writeFileSync(join(cwd, ACE_CONFIG_FILENAME), "{ not json");

		expect(() => loadAceConfig({ cwd })).toThrow(/not valid JSON/);
	});
});

describe("resolveAceConfig", () => {
	it("resolves inputs, outputs and the sender from the file", () => {
		const cwd = temporaryDirectory();
		const source = writeConfig(cwd, { sender: "agent-a", inputs: [buildInput], outputs: [peerOutput] });

		const resolved = resolveAceConfig({ cwd, env: {} });

		expect(resolved).toMatchObject({ source, sender: "agent-a", inputs: [buildInput], outputs: [peerOutput] });
	});

	it("defaults the sender to a per-process identity", () => {
		const cwd = temporaryDirectory();
		writeConfig(cwd, { inputs: [buildInput] });

		const resolved = resolveAceConfig({ cwd, env: {} });

		expect(resolved.sender).toMatch(/^pi-\d+$/);
		expect(resolved.outputs).toEqual([]);
	});

	it("requires the configuration file: MQ settings never come from the environment", () => {
		const cwd = temporaryDirectory();

		expect(() =>
			resolveAceConfig({ cwd, env: { ACE_STREAM: "ace:env", ACE_REDIS_URL: "redis://elsewhere" } }),
		).toThrow(/no \.ace\.json in/);
	});
});

describe("createTransports / createPublishers", () => {
	it("keys one transport per input name", () => {
		const transports = createTransports(
			[
				{ ...buildInput, name: "builds" },
				{ ...buildInput, name: "alerts", stream: "ace:alerts" },
			],
			{ onError: () => {} },
		);

		expect(Object.keys(transports).sort()).toEqual(["alerts", "builds"]);
		expect(transports.builds).not.toBe(transports.alerts);
	});

	it("keys one publisher per output name", () => {
		const publishers = createPublishers(
			[
				{ ...peerOutput, name: "to-b" },
				{ ...peerOutput, name: "to-c", stream: "ace:to-c" },
			],
			{ onError: () => {} },
		);

		expect(Object.keys(publishers).sort()).toEqual(["to-b", "to-c"]);
		expect(publishers["to-b"]).not.toBe(publishers["to-c"]);
	});

	it("rejects an unsupported input transport kind", () => {
		expect(() =>
			createTransports([{ name: "alerts", transport: "kafka", topic: "ace" }], { onError: () => {} }),
		).toThrow(/unsupported transport "kafka"/);
	});

	it("rejects an unsupported output transport kind", () => {
		expect(() =>
			createPublishers([{ name: "to-b", transport: "nats", subject: "ace" }], { onError: () => {} }),
		).toThrow(/unsupported transport "nats"/);
	});
});
