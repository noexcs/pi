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
│ InMemory /   │                  │ decode → validate → resolve   │                │            │
│ Redis Streams│ ◄─────────────── │ activation → dispatch         │                └─────┬──────┘
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

Set `ACE_EVENT` to publish your own message instead of the demo event (`manual` events are then activated
explicitly, so all four activation values are observable from the command line):

```bash
ACE_EVENT='{"aceVersion":"0.1","id":"e1","sender":"ci","activation":"immediate","body":"Deploy failed."}' \
  ACE_MODEL=tailscale-zcs/Qwen3.8-27B npm run example:basic --workspace=ace-runtime
```

## Inject events into a live Pi session

The extension in [`extensions/ace.ts`](extensions/ace.ts) runs **inside** Pi and injects external events into the
session you are chatting in — no separate runtime process, no second session.

```bash
# 1. describe where events come from (session working directory)
cat > .ace.json <<'JSON'
{
  "defaultActivation": "next_turn",
  "inputs": [
    {
      "name": "build-events",
      "transport": "redis-streams",
      "stream": "ace:events",
      "group": "ace-pi",
      "url": "redis://127.0.0.1:6379",
      "activation": "next_turn"
    }
  ]
}
JSON

# 2. start Pi with the extension
pi --extension /path/to/ace-runtime/extensions/ace.ts

# 3. publish from anywhere; the event lands in the running conversation
redis-cli XADD ace:events '*' message \
  '{"aceVersion":"0.1","id":"e1","sender":"ci","activation":"next_turn","body":"Build failed."}'
```

Load it permanently by copying or symlinking the file into `~/.pi/agent/extensions/` (or a project
`.pi/extensions/`). `.ace.json` is read once per session: restart Pi or `/reload` after editing it. Set
`ACE_CONFIG` to read it from another path.

### `.ace.json`

| Field | Meaning |
|---|---|
| `defaultActivation` | `immediate` \| `next_turn` \| `manual`; the RFC §8 fallback when neither input nor message decides |
| `inputs[].name` | Input name; also the key its transport is registered under |
| `inputs[].transport` | Transport kind; `redis-streams` is the only one implemented (RFC §4.1 names the others) |
| `inputs[].activation` | Receiver override for this input (RFC §8); `default` delegates to the message |
| remaining keys | Transport settings: `stream`, `group`, `url`, `consumer`, `field`, `count`, `blockMs` |

Without `.ace.json` the extension falls back to `ACE_STREAM` (+ `ACE_GROUP`, `ACE_REDIS_URL`, `ACE_CONSUMER`,
`ACE_FIELD`); `ACE_LOG=1` also logs runtime lines in modes without a UI.

### What injection looks like

| Effective activation | Pi idle | Pi running |
|---|---|---|
| `next_turn` | event starts a turn | queued with `followUp`, processed after the current run's pending work |
| `immediate` | event starts a turn | queued with `steer`, processed at the current turn's next boundary |
| `manual` | retained in memory, no turn | retained in memory, no turn |

Pi resolves idle-vs-streaming itself for `sendUserMessage`, so the extension passes the delivery mode and lets Pi
queue the event; the last action also shows on the status line (`ace: injecting id=… sender=… agent=running`).

### `/ace` commands

| Command | Effect |
|---|---|
| `/ace` | origin of the configuration, agent state, number of retained `manual` events |
| `/ace pending` | list retained `manual` events (`sender/id: body`) |
| `/ace activate <sender> <id>` | inject a retained event as `next_turn` |

## Transports

`Transport` is the only seam between a broker and ACE: `start(handler)` / `stop()`. Broker metadata
(topic, subject, stream, group, entry ID, offset, consumer) stays inside the adapter and never becomes an
ACE field (RFC §4). Two adapters ship today.

### `InMemoryTransport`

In-process, for tests and examples: `start(handler)`, `stop()`, `publish(raw)`. Nothing is durable, nothing is
acknowledged.

### `RedisStreamsTransport`

Consumes from a Redis Stream consumer group (RFC §4, §17). Its settings come from the input config:

```typescript
const input: InputConfig = {
	name: "build-events",
	transport: "redis-streams",
	stream: "ace:build-events",
	group: "coding-agent",
	// optional: url, consumer, field, count, blockMs
	activation: "default",
};
const transport = new RedisStreamsTransport(input, {
	onError: (error) => console.error("[ACE] redis streams error:", error),
});
const runtime = new AceRuntime({ engine: adapter, inputs: [input], transports: { "redis-streams": transport } });
```

| Key | Default | Meaning |
|---|---|---|
| `stream` | required | Stream the consumer group reads |
| `group` | required | Consumer group; created at the stream tail (`$`) if missing |
| `url` | `redis://127.0.0.1:6379` | Broker URL |
| `consumer` | `ace-<pid>` | Consumer name inside the group |
| `field` | `message` | Stream entry field carrying the ACE message JSON |
| `count` | `16` | Entries per `XREADGROUP` |
| `blockMs` | `1000` | `XREADGROUP` block window; also bounds how fast `stop()` returns |

Acknowledgement policy:

| Situation | Result |
|---|---|
| Handler resolves (event accepted, including `manual` events stored) | entry is `XACK`ed |
| Invalid ACE message | logged by the runtime, then `XACK`ed — a poison message never blocks the stream |
| Handler rejects (injection or transport failure) | entry stays in the group's PEL |

Producers publish the ACE envelope as JSON in the payload field:

```bash
redis-cli XADD ace:build-events '*' \
  message '{"aceVersion":"0.1","id":"evt_1","sender":"ci","activation":"next_turn","body":"Build failed."}'
```

`examples/redis-streams.ts` runs this consumer against a real broker:

```bash
redis-server --port 6399 --daemonize yes --save '' --dir /tmp/ace-redis
ACE_MODEL=tailscale-zcs/Qwen3.8-27B ACE_REDIS_URL=redis://127.0.0.1:6399 ACE_EXIT_AFTER=1 \
  npm run example:redis --workspace=ace-runtime
```

Tests never need a broker: the adapter is split into a narrow `RedisStreamsClient` interface, a `redis`-backed
client, and the transport, so the test suite drives a fake client.

## Layout

| Path | Role |
|---|---|
| `src/protocol/` | ACE 0.1 envelope, activation values, validator, decoder, [JSON Schema](schema/ace-message-0.1.schema.json) |
| `src/runtime/` | input configuration, activation resolution, dispatcher, manual-event store, `AceRuntime` |
| `src/transport/` | `Transport` boundary, `InMemoryTransport`, `RedisStreamsTransport` (+ client interface / node-redis adapter) |
| `src/agent/` | `AgentEngine` interface and `PiAdapter` |
| `src/logger.ts` | log lines that never carry a message body |
| `extensions/` | `ace.ts`: Pi extension that injects events into the session it runs in |
| `test/` | protocol, runtime, adapter and transport unit tests, plus real-Pi-session integration tests |
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

Both engines (the SDK adapter and the in-session extension) map activation the same way; the SDK one calls Pi
directly, the extension passes Pi a delivery mode and lets the session decide.

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
- Transports: `InMemoryTransport` and `RedisStreamsTransport`. Kafka/NATS adapters, a CLI, agent registry,
  dynamic targets, bindings, result events and acknowledgement APIs are out of scope (design doc §27, §37).
- A Redis entry whose handler failed stays in the group's pending entries list; there is no reclaim worker
  (`XAUTOCLAIM`) yet, and a failed read ends consumption until the transport is recreated.
- `AceRuntime` registers transports by input name and rejects a transport instance shared by two inputs, because
  every message would then be dispatched twice. Two inputs may use the same transport kind with different settings.
- The extension engine's `waitForIdle()` resolves immediately: a session shutdown must not block the interactive UI
  on a live turn.

## Implementation notes

- `AgentEngine` is `inject(message, mode)` + `isRunning()` + `waitForIdle()`. The design doc's separate
  `startTurn()` is folded into `inject` because Pi starts a turn atomically with the injected message when the
  agent is idle; splitting them only adds a race window.
- The validator is hand-written; `test/protocol/validator.test.ts` checks it against `schema/` so the two cannot
  drift.
- `RedisStreamsTransport` depends on the `redis` package only inside `redis-streams-node-client.ts`; the transport
  talks to the narrow `RedisStreamsClient` interface, which is what tests substitute.
- `.ace.json` is validated when it is read (kind, activation, transport settings), so a broken configuration fails
  at session start with a message instead of mid-stream. `/ace` deliberately registers no argument completions:
  an open completion popup swallows the first Enter in the TUI.
- The Pi engine is the public `@earendil-works/pi-coding-agent` SDK. Pi core is untouched.

## Development

```bash
npm test --workspace=ace-runtime           # unit + integration tests (faux model, fake Redis client, no network)
npm run build --workspace=ace-runtime
npm run example:basic --workspace=ace-runtime   # in-memory transport, needs ACE_MODEL
npm run example:redis --workspace=ace-runtime   # Redis Streams consumer, needs ACE_MODEL + a broker
```
