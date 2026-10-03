import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { parseAceConfig } from "../../src/runtime/ace-config.ts";
import { type JsonSchemaNode, schemaErrors } from "../support/json-schema.ts";

// Keeps the .ace.json schema document and the hand-written config validator from drifting apart.
const schema = JSON.parse(
	readFileSync(new URL("../../schema/ace-config.schema.json", import.meta.url), "utf8"),
) as JsonSchemaNode;

const buildInput = { name: "build-events", transport: "redis-streams", stream: "ace:events", group: "ace-pi" };
const peerOutput = { name: "to-b", transport: "redis-streams", stream: "ace:to-b" };

describe("ACE runtime configuration JSON Schema", () => {
	it.each<[string, unknown]>([
		["inputs only", { inputs: [buildInput] }],
		["a default activation", { defaultActivation: "immediate", inputs: [buildInput] }],
		["outputs with a sender", { sender: "agent-a", inputs: [buildInput], outputs: [peerOutput] }],
		["an input with an input-level activation override", { inputs: [{ ...buildInput, activation: "immediate" }] }],
		["unknown transport-specific keys", { inputs: [{ ...buildInput, futureKey: 1 }] }],
		["non-string sender", { sender: 7, inputs: [buildInput] }],
		["a non-object document", []],
		["missing inputs", { defaultActivation: "next_turn" }],
		["empty inputs", { inputs: [] }],
		["a delegated defaultActivation", { defaultActivation: "default", inputs: [buildInput] }],
		["an input missing name and transport", { inputs: [{ stream: "s", group: "g" }] }],
		["an input with an empty name", { inputs: [{ ...buildInput, name: "" }] }],
		["an unsupported transport kind", { inputs: [{ name: "x", transport: "kafka", topic: "t" }] }],
		["an input without a stream", { inputs: [{ name: "x", transport: "redis-streams", group: "g" }] }],
		["an input without a group", { inputs: [{ name: "x", transport: "redis-streams", stream: "s" }] }],
		["a non-integer count", { inputs: [{ ...buildInput, count: "8" }] }],
		["a zero count", { inputs: [{ ...buildInput, count: 0 }] }],
		["an empty sender", { sender: "", inputs: [buildInput] }],
		["outputs without a sender", { inputs: [buildInput], outputs: [peerOutput] }],
		["an empty outputs array", { sender: "agent-a", inputs: [buildInput], outputs: [] }],
		[
			"an output without a stream",
			{ sender: "agent-a", inputs: [buildInput], outputs: [{ name: "o", transport: "redis-streams" }] },
		],
	])("agrees with the config validator for %s", (_name, document) => {
		const schemaAccepts = schemaErrors(document, schema).length === 0;
		let validatorAccepts = true;
		try {
			parseAceConfig(document, ".ace.json");
		} catch {
			validatorAccepts = false;
		}
		expect(validatorAccepts).toBe(schemaAccepts);
	});

	// Semantic rules that JSON Schema cannot express: the validator owns them alone.
	it.each<[string, unknown]>([
		["duplicated output names", { sender: "agent-a", inputs: [buildInput], outputs: [peerOutput, peerOutput] }],
		["duplicated input names", { inputs: [buildInput, buildInput] }],
	])("rejects %s in code although the schema cannot see it", (_name, document) => {
		expect(schemaErrors(document, schema)).toEqual([]);
		expect(() => parseAceConfig(document, ".ace.json")).toThrow(/configured twice/);
	});
});
