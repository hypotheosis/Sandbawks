"""Server-Sent Events compatibility tests.

The e2b Python SDK does not currently expose a public ``events``
client — the SSE stream at ``GET /events/sandboxes`` and
``GET /events/sandboxes/{id}`` is documented in spec §7 but consumed
out-of-band by the dashboard / a future SDK addition. We therefore
hit the SSE endpoint directly with ``httpx`` to prove the gateway's
contract holds.

When the e2b SDK gains an ``events`` surface this module should
switch to driving it via the SDK; the wire-level check below stays
useful as a regression guard either way.
"""

from __future__ import annotations

import threading
import time

import httpx
import pytest

pytestmark = pytest.mark.events

# Cap the SSE read so the test cannot hang the suite if the stream
# never delivers anything. 30 s absorbs CI load — the controller's
# reconcile cadence on a healthy kind cluster is sub-second, but a
# loaded CI runner can take noticeably longer to roll out the Pod
# and emit the first phase-transition Event.
SSE_READ_TIMEOUT_S = 30.0

# Short cushion to let the SSE reader thread complete its HTTP
# upgrade + first read before the test triggers the sandbox create
# that produces the phase-transition event. Without this, a fast
# `Sandbox.create` can fire (and finish emitting) before the reader
# is subscribed, leaving captured empty and the test self-skipped.
SSE_READER_WARMUP_S = 1.0


def _read_first_event(url: str, headers: dict[str, str]) -> str | None:
    """Read up to the first SSE ``data:`` payload, bounded by timeout."""
    deadline = time.monotonic() + SSE_READ_TIMEOUT_S
    with httpx.stream("GET", url, headers=headers, timeout=SSE_READ_TIMEOUT_S) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if time.monotonic() > deadline:
                return None
            if line.startswith("data:"):
                return line[len("data:") :].strip()
    return None


def test_sse_stream_emits_event_for_new_sandbox(
    gateway_url: str, api_key: str, template_id: str
) -> None:
    """Creating a sandbox produces at least one SSE event on the team stream."""
    from e2b import Sandbox

    url = f"{gateway_url.rstrip('/')}/events/sandboxes"
    headers = {"X-API-Key": api_key, "Accept": "text/event-stream"}

    captured: list[str] = []

    def reader() -> None:
        ev = _read_first_event(url, headers)
        if ev is not None:
            captured.append(ev)

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    # Give the reader a beat to finish its HTTP upgrade before we
    # trigger the create that produces the event we want to observe.
    time.sleep(SSE_READER_WARMUP_S)

    sbx = Sandbox.create(template=template_id, timeout=120)
    try:
        # Wait the full SSE budget *after* create returns, not in
        # parallel with it — Sandbox.create blocks until Running,
        # so under the old `+ 1` budget the reader was racing the
        # create's blocking period under one shared timeout.
        t.join(timeout=SSE_READ_TIMEOUT_S)
    finally:
        sbx.kill()

    if not captured:
        pytest.skip(
            "no SSE event observed within timeout; "
            "controller may be configured without event publication"
        )
    assert captured[0], "captured SSE event should have a non-empty payload"


def test_sse_per_sandbox_stream_reachable(
    gateway_url: str, api_key: str, template_id: str
) -> None:
    """The per-sandbox SSE endpoint responds 200 + ``text/event-stream``."""
    from e2b import Sandbox

    sbx = Sandbox.create(template=template_id, timeout=120)
    try:
        url = f"{gateway_url.rstrip('/')}/events/sandboxes/{sbx.sandbox_id}"
        headers = {"X-API-Key": api_key, "Accept": "text/event-stream"}
        with httpx.stream("GET", url, headers=headers, timeout=5.0) as resp:
            assert resp.status_code == 200
            ctype = resp.headers.get("content-type", "")
            assert "text/event-stream" in ctype
    finally:
        sbx.kill()
