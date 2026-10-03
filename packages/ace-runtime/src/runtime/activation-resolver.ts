import type { AceMessage, ConcreteActivation } from "../protocol/ace-message.ts";
import type { InputConfig } from "./input-config.ts";

/** Effective activation when neither the input nor the message picks one (RFC §8). */
export const DEFAULT_RUNTIME_ACTIVATION: ConcreteActivation = "next_turn";

/**
 * Effective activation precedence (RFC §8):
 *
 * ```text
 * input.activation != default  -> input.activation
 * message.activation != default -> message.activation
 * otherwise                     -> runtime.defaultActivation
 * ```
 *
 * The receiver's input configuration can therefore override the sender's
 * preference.
 */
export function resolveActivation(
	message: AceMessage,
	input?: InputConfig,
	runtimeDefaultActivation: ConcreteActivation = DEFAULT_RUNTIME_ACTIVATION,
): ConcreteActivation {
	const inputActivation = input?.activation;
	if (inputActivation !== undefined && inputActivation !== "default") {
		return inputActivation;
	}
	if (message.activation !== "default") {
		return message.activation;
	}
	return runtimeDefaultActivation;
}
