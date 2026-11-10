// Clean-room E2B JS SDK smoke test against a LIVE sandbawks gateway.
//
// Purpose: isolate whether the "shell commands time out" symptom seen in
// zidane is the SDK/gateway path itself, or something in zidane. This
// script drives the official E2B SDK directly — no zidane — using the same
// production HOSTNAME routing (`{port}-{id}.{domain}`) that zidane uses,
// and runs the exact operations that hung (`echo hello`, `hostname`,
// `uname -a`) plus a file round-trip and a streaming command.
//
// It deliberately does NOT set `E2B_SANDBOX_URL` (the header-mode override
// the kind harness uses). With only `E2B_DOMAIN` set, the SDK builds
// `https://{port}-{sandboxId}.{domain}` for every envd call — exercising
// the wildcard Ingress + hostname-selection path end to end.
//
// Run:
//   cd tests/e2b-node-smoke
//   bun install        # or: npm install
//   E2B_API_KEY=sbk_... E2B_DOMAIN=e2b.example.com E2B_TEMPLATE=compound-sandbox \
//     bun run smoke.ts # or: npm run smoke
//
// Env:
//   E2B_API_KEY   (required) team API key the gateway authenticates.
//   E2B_DOMAIN    (required) wildcard base domain of the deployment.
//   E2B_TEMPLATE  template id to create from   (default: base)
//   CMD_TIMEOUT_MS    per-command exec timeout (default: 15000)
//   REQUEST_TIMEOUT_MS per-HTTP-request timeout (default: 20000)
//   KEEP          set to 1 to leave the sandbox running (skip kill)

import { Sandbox } from "@e2b/code-interpreter";

const DOMAIN = process.env.E2B_DOMAIN;
const API_KEY = process.env.E2B_API_KEY;
const TEMPLATE = process.env.E2B_TEMPLATE ?? "base";
const CMD_TIMEOUT_MS = Number(process.env.CMD_TIMEOUT_MS ?? 15000);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? 20000);

if (!API_KEY) {
    console.error("error: E2B_API_KEY is required (a team key, sbk_...).");
    process.exit(2);
}
if (!DOMAIN) {
    console.error("error: E2B_DOMAIN is required (the deployment's wildcard base domain).");
    process.exit(2);
}

let passed = 0;
let failed = 0;

/** Run one labelled step, time it, and record pass/fail without aborting. */
async function step<T>(label: string, fn: () => Promise<T>): Promise<T | undefined> {
    const t0 = Date.now();
    try {
        const result = await fn();
        const ms = Date.now() - t0;
        console.log(`  PASS  ${label}  (${ms}ms)`);
        passed++;
        return result;
    } catch (err) {
        const ms = Date.now() - t0;
        const msg = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
        console.log(`  FAIL  ${label}  (${ms}ms)  -> ${msg}`);
        failed++;
        return undefined;
    }
}

/** Run a shell command and print its result (the path zidane's `shell` uses). */
async function runCmd(sbx: Sandbox, cmd: string): Promise<void> {
    await step(`commands.run: ${cmd}`, async () => {
        const r = await sbx.commands.run(cmd, {
            timeoutMs: CMD_TIMEOUT_MS,
            requestTimeoutMs: REQUEST_TIMEOUT_MS,
        });
        const out = (r.stdout || r.stderr || "").trim();
        console.log(`        exit=${r.exitCode} out=${JSON.stringify(out.slice(0, 200))}`);
        return r;
    });
}

async function main(): Promise<void> {
    console.log("== sandbawks E2B JS SDK smoke (hostname mode) ==");
    console.log(`domain=${DOMAIN} template=${TEMPLATE} cmdTimeout=${CMD_TIMEOUT_MS}ms reqTimeout=${REQUEST_TIMEOUT_MS}ms`);
    console.log(`SDK control-plane base: https://api.${DOMAIN}  (matches *.${DOMAIN} ingress)`);
    console.log();

    const sbx = await step("Sandbox.create", () =>
        Sandbox.create(TEMPLATE, {
            apiKey: API_KEY,
            domain: DOMAIN,
            timeoutMs: 60_000,
            requestTimeoutMs: REQUEST_TIMEOUT_MS,
        }),
    );
    if (!sbx) {
        console.log("\nCannot continue without a sandbox. Control-plane create failed above.");
        summarize();
        return;
    }

    // The exact host the SDK will dial for envd RPCs — correlate this with
    // `kubectl get sandbox <id> -n sandbawks -o wide` to find the node.
    console.log(`  sandboxId = ${sbx.sandboxId}`);
    try {
        console.log(`  envd host = ${sbx.getHost(49983)}  (SDK target for commands/files)`);
    } catch {
        /* getHost may not exist on every SDK minor; non-fatal */
    }
    console.log();

    // The operations that hung in zidane, in the same order.
    await runCmd(sbx, "echo hello");
    await runCmd(sbx, "hostname");
    await runCmd(sbx, "uname -a");

    // Filesystem unary path (zidane's read_file/write_file/list_files).
    await step("files.write + files.read round-trip", async () => {
        const path = "/tmp/sandbawks-smoke.txt";
        const payload = `smoke-${sbx.sandboxId}`;
        await sbx.files.write(path, payload);
        const back = await sbx.files.read(path);
        if (back.trim() !== payload) {
            throw new Error(`readback mismatch: wrote ${JSON.stringify(payload)}, read ${JSON.stringify(back)}`);
        }
    });

    // Streaming path with incremental output over time — the connect-rpc
    // Start stream zidane's long shell commands depend on. onStdout fires
    // per chunk, so we can see whether output arrives live or all-at-once.
    await step("commands.run streaming (incremental stdout)", async () => {
        const stamps: number[] = [];
        const t0 = Date.now();
        const r = await sbx.commands.run("echo a; sleep 2; echo b; sleep 2; echo c", {
            timeoutMs: CMD_TIMEOUT_MS,
            requestTimeoutMs: REQUEST_TIMEOUT_MS,
            onStdout: () => stamps.push(Date.now() - t0),
        });
        console.log(`        exit=${r.exitCode} chunk_arrival_ms=[${stamps.join(", ")}]`);
        if (stamps.length > 1 && stamps[stamps.length - 1] - stamps[0] < 500) {
            console.log("        WARNING: all stdout arrived together -> response was buffered upstream");
        }
        return r;
    });

    if (process.env.KEEP === "1") {
        console.log(`\n(KEEP=1) leaving ${sbx.sandboxId} running for further inspection.`);
    } else {
        await step("Sandbox.kill", () => sbx.kill());
    }

    summarize();
}

function summarize(): void {
    console.log(`\npassed=${passed} failed=${failed}`);
    process.exit(failed === 0 ? 0 : 1);
}

main().catch((err) => {
    console.error("unexpected fatal error:", err);
    process.exit(1);
});
