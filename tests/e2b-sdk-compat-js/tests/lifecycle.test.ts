// Sandbox lifecycle compatibility tests for the official
// `@e2b/code-interpreter` JS SDK. Mirrors
// `tests/e2b-sdk-compat/tests/test_lifecycle.py`.
//
// Covers the spec §7 lifecycle endpoints the SDK drives:
//
// * `Sandbox.create`          → `POST /v2/sandboxes` (e2b >= 2.61)
// * `Sandbox.list`            → `GET /v2/sandboxes`
// * `Sandbox.isRunning`       → `GET /health` (gateway-synthesized per-sandbox probe)
// * `Sandbox.getInfo`         → `GET /sandboxes/{id}`
// * `Sandbox.setTimeout`      → `POST /sandboxes/{id}/timeout`
// * `Sandbox.pause`           → `POST /sandboxes/{id}/pause`
// * `Sandbox.connect`         → `POST /v2/sandboxes/{id}/connect` (e2b >= 2.61)
// * `Sandbox.kill`            → `POST /sandboxes/{id}/kill`
//
// `isRunning` / `getInfo` / `setTimeout` exercise the routes landed
// by #189. Note: `isRunning` hits `GET /health` (gateway-synthesized)
// rather than `GET /sandboxes/{id}` as #189 originally assumed —
// matching the Python harness's behavior.
//
// The compound flow `pause → resume → exec` (spec §13 acceptance #2)
// lives in `"pause → resume → exec roundtrip (compound)"`. The
// per-sandbox envd RPC proxy (`routes::envd_proxy`) landed in #191.

import { describe, expect, it } from "vitest";

import { templateId } from "./_env.js";

import { Sandbox } from "@e2b/code-interpreter";

describe("lifecycle", () => {
    it("create then kill", async () => {
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            expect(sbx.sandboxId).toBeTruthy();
        } finally {
            await sbx.kill();
        }
    });

    it("list includes a running sandbox", async () => {
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const paginator = Sandbox.list();
            const page = await paginator.nextItems();
            const ids = new Set(page.map((item) => item.sandboxId));
            expect(ids.has(sbx.sandboxId)).toBe(true);
        } finally {
            await sbx.kill();
        }
    });

    it("isRunning returns true for a running sandbox", async () => {
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const running = await sbx.isRunning();
            expect(running).toBe(true);
        } finally {
            await sbx.kill();
        }
    });

    // `Sandbox.getInfo` is a static method: `Sandbox.getInfo(sandboxId)`.
    // `SandboxInfo` exposes sandboxId / templateId / envdVersion / endAt.
    // The JS SDK does not surface a `state` field on SandboxInfo, so we
    // check the identity fields that the SDK parses from the response.
    it("getInfo returns sandbox details", async () => {
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const info = await Sandbox.getInfo(sbx.sandboxId);
            expect(info.sandboxId).toBe(sbx.sandboxId);
            expect(info.templateId).toBeTruthy();
            expect(info.envdVersion).toBeTruthy();
        } finally {
            await sbx.kill();
        }
    });

    // `sbx.setTimeout(timeoutMs)` is an instance method that takes
    // milliseconds (unlike the Python SDK which takes seconds).
    // We create with 120 s and bump to 600 s; `endAt` must advance.
    it("setTimeout extends the sandbox deadline", async () => {
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const before = (await Sandbox.getInfo(sbx.sandboxId)).endAt;
            await sbx.setTimeout(600_000);
            const after = (await Sandbox.getInfo(sbx.sandboxId)).endAt;
            expect(after > before).toBe(true);
        } finally {
            await sbx.kill();
        }
    });

    // Spec §13 acceptance #2 — pause → resume → exec preserves state.
    //
    // Writes a marker file, pauses the sandbox, reconnects via the
    // SDK's `connect`, and asserts the marker is still there *and* a
    // fresh `commands.run` succeeds on the resumed sandbox.
    //
    // `files.write` / `commands.run` route through the per-sandbox
    // envd RPC proxy in `crates/sandbawks-api/src/routes/envd_proxy.rs`
    // (issue #191). The cluster-side spec §13 acceptance #2 is also
    // exercised end-to-end by
    // `deploy/kind/fixtures/pause-resume-smoke.sh` (Plan 05 T14).
    it(
        "pause → resume → exec roundtrip (compound)",
        async () => {
            const sbx = await Sandbox.create(templateId, { timeoutMs: 300_000 });
            try {
                await sbx.files.write("/tmp/roundtrip.txt", "hello-from-pre-pause");
                const sandboxId = sbx.sandboxId;
                await sbx.pause();

                const resumed = await Sandbox.connect(sandboxId);
                const contents = await resumed.files.read("/tmp/roundtrip.txt");
                expect(contents).toBe("hello-from-pre-pause");

                const result = await resumed.commands.run("echo resumed", {
                    timeoutMs: 30_000,
                });
                expect(result.stdout ?? "").toContain("resumed");
            } finally {
                try {
                    await sbx.kill();
                } catch {
                    // best-effort teardown
                }
            }
        },
    );
});
