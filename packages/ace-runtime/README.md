# ace-runtime

ACE (Agent Context Event Protocol) 0.1 runtime on top of [Pi](../../packages/coding-agent). It makes external
events — CI results, alerts, other agents — an *active* input to a running agent instead of something the agent
has to poll for.

Protocol semantics come from the `ACE-RFC-Draft-0.1.md` draft and engineering decisions from `ace-v0.1.md`.
Those two documents live in the workspace that hosts this implementation; they are not part of this fork.

```text
External World
      │
      ▼
┌──────────────┐   raw message    ┌───────────────────────────────┐   AceMessage   ┌────────────┐
│  Transport   │ ───────────────► │          ACE Runtime          │ ─────────────► │ PiAdapter  │
│ Kafka/NATS/  │                  │ decode → validate → resolve   │                │            │
│ memory       │ ◄─────────────── │ activation → dispatch         │                └─────┬──────┘
└──────────────┘  ack/retry/…     └───────────────────────────────┘                      │
                                                │                                 ┌──────▼──────┐
                                     manual ────┘ stored events                   │ Pi session  │
                                                                                 │ context →   │
                                                                                 │ agent turn  │
                                                                                 │ → LLM       │
                                                                                 └─────────────┘
```

The four layers stay separate: **ACE protocol ≠ ACE runtime ≠ transport ≠ agent engine**. Nothing here maps MQ
metadata to ACE fields, and nothing here teaches Pi about ACE: Pi only sees context text.

## Usage

```typescript
import { createAgentSession, SessionManager } from "@earendil-works/pi-coding-agent";
import { AceRuntime, InMemoryTransport, PiAdapter, type InputConfig } from "ace-runtime";

const { session } = await createAgentSession({ sessionManager: SessionManager.inMemory() });
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
});

await runtime.start();
await transport.publish({
	aceVersion: "0.1",
	id: "evt_123",
	sender: "build-service",
	activation: "next_turn",
	body: "Build failed for project foo.",
});
await session.waitForIdle();
await runtime.stop();
```

A runnable version is [`examples/basic.ts`](examples/basic.ts):

```bash
ACE_MODEL=tailscale-zcs/Qwen3.8-27B npm run example:basic --workspace=ace-runtime
```

## Layout

| Path | Role |
|---|---|
| `src/protocol/` | ACE 0.1 envelope, activation values, validator, decoder, [JSON Schema](schema/ace-message-0.1.schema.json) |
| `src/runtime/` | input configuration, activation resolution, dispatcher, manual-event store, `AceRuntime` |
| `src/transport/` | `Transport` boundary and `InMemoryTransport` |
| `src/agent/` | `AgentEngine` interface and `PiAdapter` |
| `src/logger.ts` | log lines that never carry a message body |
| `test/` | protocol, runtime, adapter unit tests and real-Pi-session integration tests |
| `schema/` | normative ACE 0.1 JSON Schema |

## Protocol summary

An ACE 0.1 message is exactly five fields; unknown fields are allowed and ignored (RFC §15).

| Field | Meaning |
|---|---|
| `aceVersion` | protocol version; this runtime accepts `"0.1"` only |
| `id` | message identity, unique per sender; `(sender, id)` identifies a message (RFC §5.2) |
| `sender` | sender identifier; not required to be an agent |
| `activation` | `immediate` \| `next_turn` \| `manual` \| `default` |
| `body` | opaque string; ACE never interprets it (RFC §6) |

Effective activation (RFC §8) — the receiver can always override the sender:

```text
input.activation != default  → input.activation
message.activation != default → message.activation
otherwise                     → runtime default (next_turn)
```

`default` is a delegation value, never an executed action, so a runtime default is typed as
`immediate | next_turn | manual`.

## Activation semantics on Pi

| Effective activation | Agent idle | Agent running |
|---|---|---|
| `next_turn` | `prompt()`: body enters context, turn starts | `followUp()`: queued, processed after the current run's pending work |
| `immediate` | `prompt()`: body enters context, turn starts | `steer()`: queued at the earliest public processing point |
| `manual` | retained in memory, no turn | retained in memory, no turn |

`immediate` preempts at Pi's next **turn boundary** instead of aborting the running turn, so no partial output
or in-flight tool call is discarded. Mid-turn cancellation is deliberately out of the MVP (design doc §28/§29).

Pi only drains its steering and follow-up queues from a *live* agent loop. An event queued after that loop's last
poll would sit there until some unrelated run drained it, so `PiAdapter` records every queued event, watches for
the conversation message Pi emits when it injects it, and starts a new run for anything still undelivered once
the session settles. Events therefore reach the model exactly once, in order.

## Context injection

`body` enters the Pi context as a user message in this form (design doc §18):

```text
[ACE Event]
sender: build-service
id: evt_123

Build failed for project foo.
```

The header is an adapter choice, **not** part of ACE: the protocol only requires `body` to be visible to later
reasoning. The fixed prefix also keeps an ACE body from being mistaken for a Pi slash command or prompt template.
Pass `renderEvent` to `PiAdapter` to change the format.

## Errors and logging

| Situation | Behavior |
|---|---|
| Non-conforming message | rejected; through a transport it is logged and dropped, `handleRawMessage` throws `AceValidationError` |
| Transport or injection failure | propagated, so the transport can retry or dead-letter (RFC §17, design doc §30) |
| Agent turn failure | reported through `PiAdapter`'s `onRunError`, since Pi records it on the assistant message rather than rejecting `prompt()` |

Log lines carry `id`, `sender`, `input`, and `activation` only — never the body.

## MVP limitations

- `manual` events live in process memory; a restart loses them (design doc §12). No persistence, no query API,
  no inbox API, no deduplication store.
- `InMemoryTransport` only: Kafka/NATS adapters, a CLI, agent registry, dynamic targets, bindings, result events
  and acknowledgement APIs are out of scope (design doc §27, §37).
- `AceRuntime` rejects two inputs sharing one transport, because every message would then be dispatched twice.

## Implementation notes

- `AgentEngine` is `inject(message, mode)` + `isRunning()` + `waitForIdle()`. The design doc's separate
  `startTurn()` is folded into `inject` because Pi starts a turn atomically with the injected message when the
  agent is idle; splitting them only adds a race window.
- The validator is hand-written; `test/protocol/validator.test.ts` checks it against `schema/` so the two cannot
  drift.
- The Pi engine is the public `@earendil-works/pi-coding-agent` SDK. Pi core is untouched.

## Development

```bash
npm test --workspace=ace-runtime    # unit + integration tests (in-process faux model, no network)
npm run build --workspace=ace-runtime
```
