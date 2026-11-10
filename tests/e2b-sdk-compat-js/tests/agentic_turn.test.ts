// Multi-turn agentic execution smoke tests for the official
// `@e2b/code-interpreter` JS SDK. Mirrors
// `tests/e2b-sdk-compat/tests/test_agentic_turn.py`.
//
// Validates the sandbox as a stateful code-execution environment across
// sequential turns — the pattern LLM agents use when driving E2B.
//
// Each test drives a realistic multi-step agent session where the output
// of one command becomes the input of the next. The compound test
// additionally exercises a pause/resume boundary between turns, covering
// the full gVisor + S3 snapshot path.

import { describe, expect, it } from "vitest";

import { templateId } from "./_env.js";

describe("agentic", () => {
    it("multi-turn exec: each turn uses stdout from the prior", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            // Turn 1: produce initial data — simulates the LLM generating a token.
            const r1 = await sbx.commands.run("echo step1-payload", {
                timeoutMs: 30_000,
            });
            expect(r1.exitCode).toBe(0);
            const turn1 = (r1.stdout ?? "").trim();
            expect(turn1).toBe("step1-payload");

            // Turn 2: use turn 1's output as argument — simulates the LLM
            // reading the result and issuing a follow-up command.
            const r2 = await sbx.commands.run(`echo step2-from-${turn1}`, {
                timeoutMs: 30_000,
            });
            expect(r2.exitCode).toBe(0);
            const turn2 = (r2.stdout ?? "").trim();
            expect(turn2).toBe("step2-from-step1-payload");

            // Turn 3: compose the chain — simulates a third reasoning step.
            const r3 = await sbx.commands.run(`echo step3-from-${turn2}`, {
                timeoutMs: 30_000,
            });
            expect(r3.exitCode).toBe(0);
            expect(r3.stdout ?? "").toContain("step3-from-step2-from-step1-payload");
        } finally {
            await sbx.kill();
        }
    });

    it("file write then exec (write-run pattern)", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            // Models the LLM write-then-run pattern: agent generates code,
            // writes it, then executes and captures the result.
            const script = "#!/bin/sh\necho agent-wrote-this\n";
            await sbx.files.write("/tmp/agent_script.sh", script);

            const result = await sbx.commands.run("sh /tmp/agent_script.sh", {
                timeoutMs: 30_000,
            });
            expect(result.exitCode).toBe(0);
            expect(result.stdout ?? "").toContain("agent-wrote-this");
        } finally {
            await sbx.kill();
        }
    });

    // Compound — write state, run, pause, Sandbox.connect, run with prior output.
    //
    // Critical gate: an agent session spanning a pause/resume boundary must
    // preserve filesystem state (gVisor + S3 snapshot) and allow commands
    // that depend on pre-pause output to run on the resumed sandbox.
    //
    // Flow:
    // 1. Turn 1 — execute a command and capture its stdout.
    // 2. Persist that output to the filesystem.
    // 3. Pause the sandbox (triggers the gVisor + S3 snapshot path).
    // 4. Reconnect via Sandbox.connect.
    // 5. Turn 3 — read the persisted output and use it in a new command.
    it(
        "pause → resume preserves agentic state (compound)",
        async () => {
            const { Sandbox } = await import("@e2b/code-interpreter");
            const sbx = await Sandbox.create(templateId, { timeoutMs: 300_000 });
            try {
                // Turn 1: run a command and capture its output.
                const r1 = await sbx.commands.run("echo pre-pause-data", {
                    timeoutMs: 30_000,
                });
                expect(r1.exitCode).toBe(0);
                const turn1Output = (r1.stdout ?? "").trim();
                expect(turn1Output).toBe("pre-pause-data");

                // Persist the turn-1 result so it survives the snapshot, then pause.
                await sbx.files.write("/tmp/agent_state.txt", turn1Output);
                const sandboxId = sbx.sandboxId;
                await sbx.pause();

                // Resume and verify cross-turn state continuity.
                const resumed = await Sandbox.connect(sandboxId);
                const stored = await resumed.files.read("/tmp/agent_state.txt");
                expect(stored).toBe(turn1Output);

                // Turn 3: use the persisted output as a command argument, simulating
                // the agent reading the prior result and acting on it post-resume.
                const r3 = await resumed.commands.run(
                    `echo post-resume-${stored}`,
                    { timeoutMs: 30_000 },
                );
                expect(r3.exitCode).toBe(0);
                expect(r3.stdout ?? "").toContain(`post-resume-${turn1Output}`);
            } finally {
                try {
                    await sbx.kill();
                } catch {
                    // best-effort teardown
                }
            }
        },
    );

    it("background process visible across turns", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            // Turn 1: spawn a background process — simulates the agent starting
            // a long-running tool (e.g., a server or data pipeline).
            const handle = await sbx.commands.run("sh -c 'sleep 30'", {
                background: true,
                timeoutMs: 30_000,
            });
            expect(handle.pid).toBeTruthy();

            // Turn 2: verify it's visible via commands.list — simulates the
            // agent checking tool status before issuing the next instruction.
            const processes = await sbx.commands.list();
            const pids = new Set(processes.map((p) => p.pid));
            expect(pids.has(handle.pid)).toBe(true);

            // Turn 3: clean up — simulates the agent finishing its task.
            await sbx.commands.kill(handle.pid);
        } finally {
            await sbx.kill();
        }
    });
});
