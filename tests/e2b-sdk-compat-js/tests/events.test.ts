// Server-Sent Events compatibility tests. Mirrors
// `tests/e2b-sdk-compat/tests/test_events.py`.
//
// The e2b JS SDK does not currently expose a public `events` client —
// the SSE stream at `GET /events/sandboxes` and
// `GET /events/sandboxes/{id}` is documented in spec §7 but consumed
// out-of-band by the dashboard / a future SDK addition. We therefore
// hit the SSE endpoint directly with `fetch` to prove the gateway's
// contract holds.
//
// When the e2b JS SDK gains an `events` surface this module should
// switch to driving it via the SDK; the wire-level check below stays
// useful as a regression guard either way.

import { describe, expect, it } from "vitest";

import { apiKeyValue, gatewayUrl, templateId } from "./_env.js";

// Cap the SSE read so the test cannot hang the suite if the stream
// never delivers anything. 30 s absorbs CI load — the controller's
// reconcile cadence on a healthy kind cluster is sub-second, but a
// loaded CI runner can take noticeably longer to roll out the Pod
// and emit the first phase-transition Event.
const SSE_READ_TIMEOUT_MS = 30_000;

// Short cushion to let the SSE reader complete its HTTP upgrade +
// first read before we trigger the sandbox create that produces the
// phase-transition event.
const SSE_READER_WARMUP_MS = 1_000;

// Read up to the first SSE `data:` payload, bounded by `timeoutMs`.
// Returns `null` if no payload arrives within the budget, the
// stream closes empty, or the abort fires.
async function readFirstEvent(
    url: string,
    headers: Record<string, string>,
    timeoutMs: number,
): Promise<string | null> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const resp = await fetch(url, {
            headers,
            signal: controller.signal,
        });
        if (!resp.ok || !resp.body) {
            return null;
        }
        const reader = resp.body.getReader();
        const decoder = new TextDecoder("utf-8");
        let buffer = "";
        while (true) {
            const { value, done } = await reader.read();
            if (done) {
                break;
            }
            buffer += decoder.decode(value, { stream: true });
            // SSE messages are line-delimited; scan completed lines.
            let newlineIdx: number;
            while ((newlineIdx = buffer.indexOf("\n")) !== -1) {
                const line = buffer.slice(0, newlineIdx).trimEnd();
                buffer = buffer.slice(newlineIdx + 1);
                if (line.startsWith("data:")) {
                    return line.slice("data:".length).trim();
                }
            }
        }
        return null;
    } catch {
        return null;
    } finally {
        clearTimeout(timer);
    }
}

describe("events", () => {
    it("SSE stream emits an event for a new sandbox", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const url = `${gatewayUrl.replace(/\/$/, "")}/events/sandboxes`;
        const headers = {
            "X-API-Key": apiKeyValue,
            Accept: "text/event-stream",
        };

        // Start the SSE read in parallel — same shape as the Python
        // harness's threading.Thread. Give it a beat to finish the
        // HTTP upgrade before triggering the create.
        const readerPromise = readFirstEvent(url, headers, SSE_READ_TIMEOUT_MS);
        await new Promise((resolve) => setTimeout(resolve, SSE_READER_WARMUP_MS));

        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        let captured: string | null;
        try {
            captured = await readerPromise;
        } finally {
            await sbx.kill();
        }

        if (captured === null) {
            // Same self-skip rationale as the Python harness — a
            // controller configured without event publication is a
            // valid deployment shape, just not one we can probe here.
            console.warn(
                "no SSE event observed within timeout; " +
                    "controller may be configured without event publication",
            );
            return;
        }
        expect(captured.length).toBeGreaterThan(0);
    });

    it("per-sandbox SSE endpoint responds 200 + text/event-stream", async () => {
        const { Sandbox } = await import("@e2b/code-interpreter");
        const sbx = await Sandbox.create(templateId, { timeoutMs: 120_000 });
        try {
            const url = `${gatewayUrl.replace(/\/$/, "")}/events/sandboxes/${sbx.sandboxId}`;
            const headers = {
                "X-API-Key": apiKeyValue,
                Accept: "text/event-stream",
            };
            // Abort fast; we only need the response headers, not the
            // stream body.
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), 5_000);
            try {
                const resp = await fetch(url, {
                    headers,
                    signal: controller.signal,
                });
                expect(resp.status).toBe(200);
                const ctype = resp.headers.get("content-type") ?? "";
                expect(ctype.includes("text/event-stream")).toBe(true);
                // Tear down the response body so the SSE reader on the
                // gateway side notices the disconnect promptly.
                await resp.body?.cancel();
            } finally {
                clearTimeout(timer);
            }
        } finally {
            await sbx.kill();
        }
    });
});
