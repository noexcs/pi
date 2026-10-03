import { createClient } from "redis";
import type { RedisStreamsClient } from "./redis-streams-client.ts";

function isGroupAlreadyExistsError(error: unknown): boolean {
	return error instanceof Error && error.message.includes("BUSYGROUP");
}

/** Resolve the field that carries the ACE message out of a stream entry. */
function payloadOf(entry: { message: Record<string, string> }, field: string): string | undefined {
	const value = entry.message[field];
	return typeof value === "string" ? value : undefined;
}

/**
 * Adapt the `redis` package to {@link RedisStreamsClient}.
 *
 * Connection-level failures arrive asynchronously, detached from any command, so they are
 * reported through `onError`.
 */
export function createRedisStreamsClient(
	url: string,
	field: string,
	onError: (error: unknown) => void,
): RedisStreamsClient {
	const client = createClient({ url });
	client.on("error", onError);

	return {
		async connect() {
			await client.connect();
		},

		async ensureGroup(stream, group) {
			try {
				// Start at the tail: ACE consumes events from now on; replay stays an
				// infrastructure capability we do not use yet (RFC §17).
				await client.xGroupCreate(stream, group, "$", { MKSTREAM: true });
			} catch (error) {
				if (!isGroupAlreadyExistsError(error)) throw error;
			}
		},

		async read(stream, group, consumer, count, blockMs) {
			const reply = await client.xReadGroup(group, consumer, [{ key: stream, id: ">" }], {
				COUNT: count,
				BLOCK: blockMs,
			});
			const messages: Array<{ id: string; message: Record<string, string> }> = reply?.[0]?.messages ?? [];
			return messages.map((entry) => ({
				id: entry.id,
				payload: payloadOf(entry, field),
			}));
		},

		async ack(stream, group, id) {
			await client.xAck(stream, group, id);
		},

		async close() {
			if (client.isOpen) await client.quit();
		},
	};
}
