// Filesystem compatibility tests for the official
// `@e2b/code-interpreter` JS SDK. Mirrors
// `tests/e2b-sdk-compat/tests/test_files.py`.
//
// Covers the SDK `files.*` surface, which drives both the gRPC
// metadata pass-through (list/stat/remove/move/makedir/watch) and the
// signed-URL upload/download path (read/write) per spec §7. The
// compound flow `write → list → read → remove` (spec §13 acceptance
// #2) lives in `writeListReadRemoveRoundtrip`.
//
// Every test in this module exercises a per-sandbox envd RPC — the
// SDK proxies through the gateway with `E2b-Sandbox-Id` /
// `E2b-Sandbox-Port` headers, and the gateway's
// `crates/sandbawks-api/src/routes/envd_proxy.rs` reverse-proxies the
// Connect-RPC call to the matching sandbox's envd (issue #191).

import { describe, expect, it } from "vitest";

import { templateId } from "./_env.js";

describe("files", () => {
    it("makeDir then list shows the new directory", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            await sbx.files.makeDir("/tmp/compat-mkdir");
            const entries = await sbx.files.list("/tmp");
            const names = new Set(entries.map((e) => e.name));
            expect(names.has("compat-mkdir")).toBe(true);
        } finally {
            await sbx.kill();
        }
    });

    it("write text then read text round-trips", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            await sbx.files.write("/tmp/compat-text.txt", "round-trip-payload");
            const got = await sbx.files.read("/tmp/compat-text.txt");
            expect(got).toBe("round-trip-payload");
        } finally {
            await sbx.kill();
        }
    });

    it("write bytes then read bytes round-trips", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            // 1 KiB, every byte value × 4.
            const base = new Uint8Array(256);
            for (let i = 0; i < 256; i += 1) {
                base[i] = i;
            }
            const payload = new Uint8Array(base.length * 4);
            for (let i = 0; i < 4; i += 1) {
                payload.set(base, i * base.length);
            }
            await sbx.files.write("/tmp/compat-bin.dat", payload);
            const got = await sbx.files.read("/tmp/compat-bin.dat", {
                format: "bytes",
            });
            expect(Array.from(got as Uint8Array)).toEqual(Array.from(payload));
        } finally {
            await sbx.kill();
        }
    });

    it("exists flips across a remove", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            await sbx.files.write("/tmp/compat-exists.txt", "marker");
            expect(await sbx.files.exists("/tmp/compat-exists.txt")).toBe(true);
            await sbx.files.remove("/tmp/compat-exists.txt");
            expect(await sbx.files.exists("/tmp/compat-exists.txt")).toBe(false);
        } finally {
            await sbx.kill();
        }
    });

    it("getInfo reports the byte size of a freshly-written file", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const payload = new Uint8Array(1024).fill(0x78); // 'x'
            await sbx.files.write("/tmp/compat-info.bin", payload);
            const info = await sbx.files.getInfo("/tmp/compat-info.bin");
            expect(info.size).toBe(1024);
        } finally {
            await sbx.kill();
        }
    });

    it("rename moves a file across paths", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            await sbx.files.write("/tmp/compat-src.txt", "moveme");
            await sbx.files.rename("/tmp/compat-src.txt", "/tmp/compat-dst.txt");
            expect(await sbx.files.exists("/tmp/compat-src.txt")).toBe(false);
            expect(await sbx.files.read("/tmp/compat-dst.txt")).toBe("moveme");
        } finally {
            await sbx.kill();
        }
    });

    // Spec §13 acceptance #2 — write → list → read → remove.
    it("write → list → read → remove (compound)", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const block = new TextEncoder().encode("compound-flow-payload\n");
            const payload = new Uint8Array(block.length * 64);
            for (let i = 0; i < 64; i += 1) {
                payload.set(block, i * block.length);
            }
            const path = "/tmp/compat-roundtrip.bin";

            await sbx.files.write(path, payload);

            const entries = await sbx.files.list("/tmp");
            expect(entries.some((e) => e.name === "compat-roundtrip.bin")).toBe(true);

            const got = (await sbx.files.read(path, { format: "bytes" })) as Uint8Array;
            expect(Array.from(got)).toEqual(Array.from(payload));

            await sbx.files.remove(path);
            expect(await sbx.files.exists(path)).toBe(false);
        } finally {
            await sbx.kill();
        }
    });
});
