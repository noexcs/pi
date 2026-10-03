import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
	AceValidationError,
	decodeAceMessage,
	parseAceMessage,
	validateAceMessage,
} from "../../src/protocol/validator.ts";

const validMessage = {
	aceVersion: "0.1",
	id: "evt_123",
	sender: "build-service",
	activation: "next_turn",
	body: "Build failed.",
};

describe("validateAceMessage", () => {
	it("accepts a minimal ACE 0.1 message (RFC §11)", () => {
		expect(validateAceMessage(validMessage)).toEqual(validMessage);
	});

	it("keeps unknown fields instead of rejecting them (RFC §15)", () => {
		const message = validateAceMessage({ ...validMessage, futureField: "value" });
		expect(message.futureField).toBe("value");
	});

	it.each([
		["missing aceVersion", { id: "1", sender: "s", activation: "next_turn", body: "b" }, "aceVersion"],
		["wrong aceVersion", { ...validMessage, aceVersion: "0.2" }, "aceVersion"],
		["missing id", { aceVersion: "0.1", sender: "s", activation: "next_turn", body: "b" }, "id"],
		["empty id", { ...validMessage, id: "" }, "id"],
		["non-string id", { ...validMessage, id: 7 }, "id"],
		["missing sender", { aceVersion: "0.1", id: "1", activation: "next_turn", body: "b" }, "sender"],
		["empty sender", { ...validMessage, sender: "" }, "sender"],
		["missing activation", { aceVersion: "0.1", id: "1", sender: "s", body: "b" }, "activation"],
		["invalid activation", { ...validMessage, activation: "unknown" }, "activation"],
		["missing body", { aceVersion: "0.1", id: "1", sender: "s", activation: "next_turn" }, "body"],
		["non-string body", { ...validMessage, body: { type: "build_failed" } }, "body"],
	])("rejects %s (RFC §13)", (_name, value, path) => {
		expect(() => validateAceMessage(value)).toThrow(AceValidationError);
		try {
			validateAceMessage(value);
		} catch (error) {
			expect((error as AceValidationError).issues.map((issue) => issue.path)).toContain(path);
		}
	});

	it("reports every violation at once", () => {
		try {
			validateAceMessage({ aceVersion: "0.1" });
			expect.unreachable();
		} catch (error) {
			expect((error as AceValidationError).issues).toHaveLength(4);
		}
	});

	it.each([[null], ["string"], [42], [[]]])("rejects a non-object message: %s", (value) => {
		expect(() => validateAceMessage(value)).toThrow(AceValidationError);
	});

	it("never leaks the body into the error message", () => {
		try {
			validateAceMessage({ aceVersion: "0.1", id: "", sender: "s", activation: "next_turn", body: "secret-token" });
			expect.unreachable();
		} catch (error) {
			expect((error as Error).message).not.toContain("secret-token");
		}
	});
});

describe("parseAceMessage / decodeAceMessage", () => {
	it("parses a JSON string", () => {
		expect(parseAceMessage(JSON.stringify(validMessage))).toEqual(validMessage);
	});

	it("rejects malformed JSON", () => {
		expect(() => parseAceMessage("{not json")).toThrow(AceValidationError);
	});

	it("decodes UTF-8 bytes", () => {
		expect(decodeAceMessage(new TextEncoder().encode(JSON.stringify(validMessage)))).toEqual(validMessage);
	});

	it("passes already decoded objects through", () => {
		expect(decodeAceMessage(validMessage)).toEqual(validMessage);
	});
});

// The RFC ships a JSON Schema; these tests keep the hand-written validator and the
// schema document from drifting apart.
type JsonSchema = {
	type?: string;
	required?: string[];
	const?: unknown;
	enum?: readonly unknown[];
	minLength?: number;
	properties?: Record<string, JsonSchema>;
	additionalProperties?: boolean;
};

const schema = JSON.parse(
	readFileSync(new URL("../../schema/ace-message-0.1.schema.json", import.meta.url), "utf8"),
) as JsonSchema;

function schemaIssues(value: unknown, node: JsonSchema = schema, path = ""): string[] {
	if (node.type === "object") {
		if (typeof value !== "object" || value === null || Array.isArray(value)) {
			return [`${path}: not an object`];
		}
		const record = value as Record<string, unknown>;
		const issues = (node.required ?? []).filter((key) => !(key in record)).map((key) => `${path}${key}: missing`);
		for (const [key, child] of Object.entries(node.properties ?? {})) {
			if (key in record) issues.push(...schemaIssues(record[key], child, `${path}${key}.`));
		}
		return issues;
	}
	if (node.type === "string") {
		if (typeof value !== "string") return [`${path}: not a string`];
		if (node.const !== undefined && value !== node.const) return [`${path}: const mismatch`];
		if (node.enum && !node.enum.includes(value)) return [`${path}: not in enum`];
		if (node.minLength !== undefined && value.length < node.minLength) return [`${path}: shorter than minLength`];
	}
	return [];
}

describe("ACE 0.1 JSON Schema", () => {
	it.each([
		["minimal message", validMessage],
		["unknown fields", { ...validMessage, futureField: "value" }],
		["empty body", { ...validMessage, body: "" }],
		["missing aceVersion", { id: "1", sender: "s", activation: "next_turn", body: "b" }],
		["wrong aceVersion", { ...validMessage, aceVersion: "0.2" }],
		["missing id", { aceVersion: "0.1", sender: "s", activation: "next_turn", body: "b" }],
		["empty id", { ...validMessage, id: "" }],
		["empty sender", { ...validMessage, sender: "" }],
		["missing activation", { aceVersion: "0.1", id: "1", sender: "s", body: "b" }],
		["invalid activation", { ...validMessage, activation: "unknown" }],
		["non-string body", { ...validMessage, body: 3 }],
		["non-object", "not a message"],
		["array", []],
	])("agrees with the validator for %s", (_name, value) => {
		const schemaAccepts = schemaIssues(value).length === 0;
		let validatorAccepts = true;
		try {
			validateAceMessage(value);
		} catch {
			validatorAccepts = false;
		}
		expect(validatorAccepts).toBe(schemaAccepts);
	});
});
