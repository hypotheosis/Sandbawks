"""Sandbox lifecycle compatibility tests.

Covers the spec §7 lifecycle endpoints the e2b SDK drives:

* ``Sandbox.create``          → ``POST /v2/sandboxes`` (e2b >= 2.61)
* ``Sandbox.list``            → ``GET /v2/sandboxes``
* ``Sandbox.get_info``        → ``GET /sandboxes/{id}``
* ``Sandbox.set_timeout``     → ``POST /sandboxes/{id}/timeout``
* ``Sandbox.is_running``      → ``GET /health`` (gateway-synthesized)
* ``Sandbox.pause``           → ``POST /sandboxes/{id}/pause``
* ``Sandbox.connect``         → ``POST /v2/sandboxes/{id}/connect`` (e2b >= 2.61)
* ``Sandbox.kill``            → ``POST /sandboxes/{id}/kill``

``get_info`` and ``set_timeout`` route to the endpoints landed by #189.
``is_running`` was *expected* to hit ``GET /sandboxes/{id}`` when #189
was filed, but the installed SDK (>=2.21) implements it as a ``GET
/health`` probe of the in-sandbox envd. Since our envd is gRPC-only the
gateway synthesizes that route (``routes::envd_proxy::health``), so the
test exercises the per-sandbox proxy rather than the #189 info endpoint.

The compound flow ``pause → resume → exec`` (spec §13 acceptance #2)
lives in :func:`test_pause_resume_exec_roundtrip`.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.lifecycle


def test_create_and_kill(template_id: str) -> None:
    """A freshly-created sandbox has a sandbox_id, then kill returns truthy."""
    from e2b import Sandbox

    sbx = Sandbox.create(template=template_id, timeout=120)
    try:
        assert sbx.sandbox_id, "sandbox_id should be populated after create"
    finally:
        killed = sbx.kill()
    assert killed is True


def test_list_includes_running_sandbox(sandbox) -> None:
    """The just-created sandbox appears in the team's sandbox list."""
    from e2b import Sandbox

    paginator = Sandbox.list()
    ids = {item.sandbox_id for item in paginator.next_items()}
    assert sandbox.sandbox_id in ids


def test_get_info_returns_detail(sandbox) -> None:
    """``Sandbox.get_info`` round-trips ``GET /sandboxes/{id}`` (#189).

    The SDK parses the response through ``SandboxDetail.from_dict``,
    which ``pop``s a required field set (``sandboxID`` / ``templateID``
    / ``clientID`` / ``envdVersion`` / ``state`` / ``cpuCount`` /
    ``memoryMB`` / ``diskSizeMB`` / ``startedAt`` / ``endAt``). A
    missing field raises ``KeyError`` *inside the SDK* before this test
    can assert anything, so simply getting a populated ``SandboxInfo``
    back is itself the contract check.
    """
    info = sandbox.get_info()
    assert info.sandbox_id == sandbox.sandbox_id
    assert info.state == "running"
    assert info.template_id, "template_id should be populated"
    assert info.envd_version, "envd_version should be populated"


def test_is_running_after_create(sandbox) -> None:
    """``Sandbox.is_running`` returns True for a freshly-created sandbox.

    NB: in the installed SDK (>=2.21) this issues ``GET /health`` against
    the sandbox's envd, *not* ``GET /sandboxes/{id}`` as #189 originally
    assumed. Our envd is gRPC-only, so the gateway synthesizes that probe
    in ``routes::envd_proxy::health`` — it resolves the sandbox by the
    ``E2b-Sandbox-Id`` header and issues a cheap ``Process.List`` RPC,
    returning 200 when envd answers and 502 when it doesn't.
    """
    assert sandbox.is_running() is True


def test_set_timeout_extends_deadline(sandbox) -> None:
    """``Sandbox.set_timeout`` posts ``POST /sandboxes/{id}/timeout`` (#189).

    The fixture creates the sandbox with a 300s timeout; bumping it to
    600s sets ``endAt`` to ~now+600s, which must be strictly later than
    the original ~now+300s deadline read back via ``get_info``.
    """
    before = sandbox.get_info().end_at
    sandbox.set_timeout(600)
    after = sandbox.get_info().end_at
    assert after > before, f"end_at should advance: {before!r} -> {after!r}"


@pytest.mark.compound
def test_pause_resume_exec_roundtrip(template_id: str) -> None:
    """Spec §13 acceptance #2 — pause → resume → exec preserves state.

    Writes a marker file, pauses the sandbox, reconnects via the SDK's
    ``connect`` classmethod, and asserts the marker is still there
    *and* a fresh ``commands.run`` succeeds on the resumed sandbox.

    ``files.write`` / ``commands.run`` route through the per-sandbox
    envd RPC proxy in ``crates/sandbawks-api/src/routes/envd_proxy.rs``
    (issue #191). The cluster-side spec §13 acceptance #2 is also
    exercised end-to-end by ``deploy/kind/fixtures/pause-resume-smoke.sh``
    (Plan 05 T14).
    """
    from e2b import Sandbox

    sbx = Sandbox.create(template=template_id, timeout=300)
    try:
        sbx.files.write("/tmp/roundtrip.txt", "hello-from-pre-pause")
        sandbox_id = sbx.sandbox_id
        sbx.pause()

        resumed = Sandbox.connect(sandbox_id)
        contents = resumed.files.read("/tmp/roundtrip.txt")
        assert contents == "hello-from-pre-pause"

        # And a brand-new exec works on the resumed sandbox.
        result = resumed.commands.run("echo resumed", timeout=30)
        assert "resumed" in (result.stdout or "")
    finally:
        try:
            sbx.kill()
        except Exception:  # noqa: BLE001
            pass
