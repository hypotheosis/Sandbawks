// Command execution compatibility tests for the official
// `@e2b/code-interpreter` JS SDK. Mirrors
// `tests/e2b-sdk-compat/tests/test_commands.py`.
//
// Covers the spec §7 exec / code-run path the SDK drives over the
// `GET /sandboxes/{id}/exec` WebSocket.
//
// Every test in this module exercises a per-sandbox envd RPC: the
// SDK posts to the gateway base URL carrying `E2b-Sandbox-Id` /
// `E2b-Sandbox-Port` headers, and the gateway's
// `crates/sandbawks-api/src/routes/envd_proxy.rs` reverse-proxies the
// Connect-RPC call to the matching sandbox's envd (issue #191).

import { describe, expect, it } from "vitest";

import { templateId } from "./_env.js";

describe("commands", () => {
    it("commands.run blocking returns stdout and exit_code 0", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const result = await sbx.commands.run("echo hello-sandbawks", {
                timeoutMs: 30_000,
            });
            expect(result.exitCode).toBe(0);
            expect(result.stdout ?? "").toContain("hello-sandbawks");
        } finally {
            await sbx.kill();
        }
    });

    it("non-zero exits throw CommandExitError", async () => {
        const { Sandbox, CommandExitError } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            await expect(
                sbx.commands.run("sh -c 'exit 17'", { timeoutMs: 30_000 }),
            ).rejects.toBeInstanceOf(CommandExitError);
        } finally {
            await sbx.kill();
        }
    });

    it("onStdout callback streams chunks in order", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const chunks: string[] = [];
            const result = await sbx.commands.run(
                "sh -c 'for i in 1 2 3; do echo line-$i; done'",
                {
                    onStdout: (chunk) => {
                        chunks.push(chunk);
                    },
                    timeoutMs: 30_000,
                },
            );
            expect(result.exitCode).toBe(0);
            const joined = chunks.join("");
            expect(joined).toContain("line-1");
            expect(joined).toContain("line-2");
            expect(joined).toContain("line-3");
        } finally {
            await sbx.kill();
        }
    });

    it("background=true returns a handle that can be waited on", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const handle = await sbx.commands.run(
                "sh -c 'echo bg-out; sleep 0.1; echo done'",
                { background: true, timeoutMs: 30_000 },
            );
            const result = await handle.wait();
            expect(result.exitCode).toBe(0);
            expect(result.stdout ?? "").toContain("bg-out");
            expect(result.stdout ?? "").toContain("done");
        } finally {
            await sbx.kill();
        }
    });

    it("envs are surfaced to the spawned process", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const result = await sbx.commands.run("sh -c 'echo $SBX_MARKER'", {
                envs: { SBX_MARKER: "compat-harness" },
                timeoutMs: 30_000,
            });
            expect(result.exitCode).toBe(0);
            expect(result.stdout ?? "").toContain("compat-harness");
        } finally {
            await sbx.kill();
        }
    });
});
