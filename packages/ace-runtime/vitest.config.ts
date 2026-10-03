import { fileURLToPath } from "node:url";
import { defineConfig, mergeConfig } from "vitest/config";
import baseConfig from "../../vitest.base.ts";

const codingAgentIndex = fileURLToPath(new URL("../coding-agent/src/index.ts", import.meta.url));

export default mergeConfig(
	baseConfig,
	defineConfig({
		test: {
			globals: true,
			environment: "node",
			testTimeout: 30000,
			reporters: process.env.GITHUB_ACTIONS ? ["dot", "github-actions"] : ["dot"],
			silent: "passed-only",
		},
		resolve: {
			// `@earendil-works/pi-coding-agent` exposes no "source" condition, so tests
			// run against its TypeScript sources like the other workspace packages.
			alias: [{ find: /^@earendil-works\/pi-coding-agent$/, replacement: codingAgentIndex }],
		},
	}),
);
