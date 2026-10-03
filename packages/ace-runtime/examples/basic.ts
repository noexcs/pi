/**
 * ACE 0.1 end-to-end example (design doc §26).
 *
 * ```text
 * External producer ─ InMemoryTransport ─ ACE Runtime ─ PiAdapter ─ Pi session ─ LLM
 * ```
 *
 * Pick a model from your Pi configuration:
 *
 * ```bash
 * ACE_MODEL=tailscale-zcs/Qwen3.8-27B npm run example:basic --workspace=ace-runtime
 * ```
 *
 * Without `ACE_MODEL` the session uses the model from Pi settings.
 */

import { createAgentSession, ModelRuntime, SessionManager } from "@earendil-works/pi-coding-agent";
import { AceRuntime, consoleAceLogger, InMemoryTransport, type InputConfig, PiAdapter } from "../src/index.ts";

const modelRuntime = await ModelRuntime.create({ allowModelNetwork: false });
const [providerId, modelId] = (process.env.ACE_MODEL ?? "").split("/");
const model = providerId && modelId ? modelRuntime.getModel(providerId, modelId) : undefined;
if (process.env.ACE_MODEL && !model) {
	throw new Error(`Unknown model "${process.env.ACE_MODEL}" (expected "<provider>/<model>")`);
}

const { session } = await createAgentSession({
	modelRuntime,
	model,
	sessionManager: SessionManager.inMemory(),
});

const transport = new InMemoryTransport();
const adapter = new PiAdapter({
	session,
	onRunError: (error) => console.error("[ACE] agent run failed:", error),
});
const input: InputConfig = { name: "build-events", transport: "memory", activation: "default" };
const runtime = new AceRuntime({
	engine: adapter,
	inputs: [input],
	transports: { memory: transport },
	logger: consoleAceLogger,
});

session.subscribe((event) => {
	if (event.type === "message_update" && event.assistantMessageEvent.type === "text_delta") {
		process.stdout.write(event.assistantMessageEvent.delta);
	}
});

try {
	await runtime.start();

	console.log("[ACE] publishing a build failure event\n");
	await transport.publish({
		aceVersion: "0.1",
		id: "evt_123",
		sender: "build-service",
		activation: "next_turn",
		body: "Build failed for project foo.",
	});

	await session.waitForIdle();
	console.log("\n[ACE] done");
} finally {
	await runtime.stop();
	session.dispose();
}
