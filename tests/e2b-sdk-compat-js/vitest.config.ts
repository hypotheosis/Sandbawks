import { defineConfig } from "vitest/config";

// Mirrors the Python harness's runtime envelope:
//
// * Sequential execution — every test creates a real sandbox against
//   the gateway, and the gateway / kind cluster cannot service
//   parallel sandbox lifecycle traffic during smoke. `fileParallelism:
//   false` + `pool: "forks"` with a single fork keeps the suite
//   linear, matching `pytest`'s default behavior (no `-n auto`).
// * Generous test timeout — the slowest tests (`Sandbox.create` on a
//   contended kind cluster, the SSE reader's full read budget) need
//   tens of seconds. The Python harness uses pytest's default plus
//   per-test waits; this is the equivalent ceiling.
// * Reporters — `default` for human-readable CI logs and `junit` for
//   the artifact upload step in `.github/workflows/kind-smoke.yaml`.
//   `JUNIT_OUTPUT_FILE` lets the CI step pin an absolute path so the
//   file lands where `upload-artifact` expects it.
export default defineConfig({
    test: {
        include: ["tests/**/*.test.ts"],
        fileParallelism: false,
        pool: "forks",
        poolOptions: {
            forks: {
                singleFork: true,
            },
        },
        testTimeout: 60_000,
        hookTimeout: 60_000,
        reporters: process.env.JUNIT_OUTPUT_FILE
            ? ["default", ["junit", { outputFile: process.env.JUNIT_OUTPUT_FILE }]]
            : ["default"],
    },
});
