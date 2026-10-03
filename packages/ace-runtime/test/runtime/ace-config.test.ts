import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
	ACE_CONFIG_FILENAME,
	createTransports,
	inputFromEnvironment,
	loadAceConfig,
	parseAceConfig,
	resolveAceInputs,
} from "../../src/runtime/ace-config.ts";
import { AceConfigError } from "../../src/runtime/input-config.ts";

const buildInput = {
	name: "build-events",
	transport: "redis-streams",
	stream: "ace:events",
	group: "ace-pi",
};

const directories: string[] = [];

function temporaryDirectory(): string {
	const directory = mkdtempSync(join(tmpdir(), "ace-config-"));
	directories.push(directory);
	return directory;
}

afterEach(() => {
	while (directories.length > 0) rmSync(directories.pop() as string, { recursive: true, force: true });
});

describe("parseAceConfig", () => {
	it("accepts a config with inputs and a default activation", () => {
		expect(parseAceConfig({ defaultActivation: "immediate", inputs: [buildInput] }, ".ace.json")).toEqual({
			defaultActivation: "immediate",
			inputs: [buildInput],
		});
	});

	it("accepts a config without a default activation", () => {
		expect(parseAceConfig({ inputs: [buildInput] }, ".ace.json").defaultActivation).toBeUndefined();
	});

	it.each([
		["a non-object document", ["not", "an", "object"]],
		["a delegated defaultActivation", { defaultActivation: "default", inputs: [buildInput] }],
		["missing inputs", { defaultActivation: "next_turn" }],
		["empty inputs", { inputs: [] }],
		["an unsupported transport", { inputs: [{ ...buildInput, transport: "kafka" }] }],
		["an input without a stream", { inputs: [{ name: "builds", transport: "redis-streams", group: "g" }] }],
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
		writeFileSync(join(cwd, ACE_CONFIG_FILENAME), JSON.stringify({ inputs: [buildInput] }));

		const loaded = loadAceConfig({ cwd });

		expect(loaded?.source).toBe(join(cwd, ACE_CONFIG_FILENAME));
		expect(loaded?.config.inputs).toEqual([buildInput]);
	});

	it("prefers ACE_CONFIG over the working directory", () => {
		const cwd = temporaryDirectory();
		const elsewhere = join(temporaryDirectory(), "custom.json");
		writeFileSync(join(cwd, ACE_CONFIG_FILENAME), JSON.stringify({ inputs: [buildInput] }));
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

describe("inputFromEnvironment", () => {
	it("builds one input from ACE_* variables", () => {
		const input = inputFromEnvironment({
			ACE_STREAM: "ace:env",
			ACE_GROUP: "g1",
			ACE_REDIS_URL: "redis://broker:6380",
			ACE_CONSUMER: "c1",
			ACE_FIELD: "ace",
		});

		expect(input).toMatchObject({
			name: "ace-events",
			transport: "redis-streams",
			stream: "ace:env",
			group: "g1",
			url: "redis://broker:6380",
			consumer: "c1",
			field: "ace",
			activation: "default",
		});
	});

	it("defaults the group to a per-process consumer group", () => {
		expect(inputFromEnvironment({ ACE_STREAM: "ace:env" })?.group).toMatch(/^ace-pi-\d+$/);
	});

	it("returns undefined without ACE_STREAM", () => {
		expect(inputFromEnvironment({ ACE_REDIS_URL: "redis://broker" })).toBeUndefined();
	});
});

describe("resolveAceInputs", () => {
	it("prefers .ace.json over the environment", () => {
		const cwd = temporaryDirectory();
		writeFileSync(join(cwd, ACE_CONFIG_FILENAME), JSON.stringify({ inputs: [buildInput] }));

		const resolved = resolveAceInputs({ cwd, env: { ACE_STREAM: "ace:env" } });

		expect(resolved.source).toBe(join(cwd, ACE_CONFIG_FILENAME));
		expect(resolved.inputs).toHaveLength(1);
		expect(resolved.inputs[0]?.stream).toBe("ace:events");
	});

	it("falls back to a single environment input", () => {
		const resolved = resolveAceInputs({ cwd: temporaryDirectory(), env: { ACE_STREAM: "ace:env" } });

		expect(resolved.source).toBe("environment");
		expect(resolved.inputs[0]?.stream).toBe("ace:env");
	});

	it("throws when nothing is configured", () => {
		expect(() => resolveAceInputs({ cwd: temporaryDirectory(), env: {} })).toThrow(/nothing to consume/);
	});
});

describe("createTransports", () => {
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

	it("rejects an unsupported transport kind", () => {
		expect(() =>
			createTransports([{ name: "alerts", transport: "kafka", topic: "ace" }], { onError: () => {} }),
		).toThrow(/unsupported transport "kafka"/);
	});
});
