"""Command execution compatibility tests.

Covers the spec §7 exec / code-run path the e2b SDK drives over the
``GET /sandboxes/{id}/exec`` WebSocket.

Every test in this module exercises a per-sandbox envd RPC: the SDK
posts to the gateway base URL carrying ``E2b-Sandbox-Id`` /
``E2b-Sandbox-Port`` headers, and the gateway's
``crates/sandbawks-api/src/routes/envd_proxy.rs`` reverse-proxies the
call to the matching sandbox's envd (issue #191).
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.commands


def test_commands_run_blocking(sandbox) -> None:
    """A synchronous ``commands.run`` returns stdout and exit code 0."""
    result = sandbox.commands.run("echo hello-sandbawks", timeout=30)
    assert result.exit_code == 0
    assert "hello-sandbawks" in (result.stdout or "")


def test_commands_run_nonzero_exit(sandbox) -> None:
    """Non-zero exits raise ``CommandExitException`` per the SDK contract."""
    from e2b import CommandExitException

    with pytest.raises(CommandExitException) as excinfo:
        sandbox.commands.run("sh -c 'exit 17'", timeout=30)
    assert excinfo.value.exit_code == 17


def test_commands_run_streams_stdout(sandbox) -> None:
    """``on_stdout`` callback receives each chunk in order."""
    chunks: list[str] = []
    result = sandbox.commands.run(
        "sh -c 'for i in 1 2 3; do echo line-$i; done'",
        on_stdout=chunks.append,
        timeout=30,
    )
    assert result.exit_code == 0
    joined = "".join(chunks)
    assert "line-1" in joined and "line-2" in joined and "line-3" in joined


def test_commands_run_background(sandbox) -> None:
    """``background=True`` returns a handle that can be waited on."""
    handle = sandbox.commands.run(
        "sh -c 'echo bg-out; sleep 0.1; echo done'",
        background=True,
        timeout=30,
    )
    result = handle.wait()
    assert result.exit_code == 0
    assert "bg-out" in (result.stdout or "")
    assert "done" in (result.stdout or "")


def test_commands_list_includes_background_pid(sandbox) -> None:
    """``commands.list`` shows a running background process."""
    handle = sandbox.commands.run(
        "sh -c 'sleep 5'",
        background=True,
        timeout=30,
    )
    try:
        processes = sandbox.commands.list()
        pids = {p.pid for p in processes}
        assert handle.pid in pids
    finally:
        sandbox.commands.kill(handle.pid)


def test_commands_envs_passthrough(sandbox) -> None:
    """``envs`` are surfaced to the spawned process."""
    result = sandbox.commands.run(
        "sh -c 'echo $SBX_MARKER'",
        envs={"SBX_MARKER": "compat-harness"},
        timeout=30,
    )
    assert result.exit_code == 0
    assert "compat-harness" in (result.stdout or "")
