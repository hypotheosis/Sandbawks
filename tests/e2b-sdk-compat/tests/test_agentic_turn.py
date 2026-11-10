"""Multi-turn agentic execution smoke tests.

Validates the sandbox as a stateful code-execution environment across
sequential turns — the pattern LLM agents use when driving E2B.

Each test drives a realistic multi-step agent session where the output
of one command becomes the input of the next. The compound test
(:func:`test_agentic_pause_resume_preserves_state`) additionally
exercises a pause/resume boundary between turns, covering the full
gVisor + S3 snapshot path.

These tests are tagged ``agentic``. Run them in isolation with::

    cd tests/e2b-sdk-compat
    uv run pytest -m agentic -v
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.agentic


def test_multi_turn_exec_preserves_state(sandbox) -> None:
    """Three chained ``commands.run`` calls where each uses stdout from the prior.

    Models the LLM agent pattern: turn N's output is fed as input to
    turn N+1. Asserts the sandbox maintains a coherent execution
    environment across the chain.

    # Arguments

    * `sandbox` — per-test sandbox fixture from conftest.py
    """
    # Turn 1: produce initial data — simulates the LLM generating a token.
    r1 = sandbox.commands.run("echo step1-payload", timeout=30)
    assert r1.exit_code == 0
    turn1 = (r1.stdout or "").strip()
    assert turn1 == "step1-payload"

    # Turn 2: use turn 1 output as argument — simulates the LLM reading
    # the result and issuing a follow-up command.
    r2 = sandbox.commands.run(f"echo step2-from-{turn1}", timeout=30)
    assert r2.exit_code == 0
    turn2 = (r2.stdout or "").strip()
    assert turn2 == "step2-from-step1-payload"

    # Turn 3: compose the chain — simulates a third reasoning step.
    r3 = sandbox.commands.run(f"echo step3-from-{turn2}", timeout=30)
    assert r3.exit_code == 0
    assert "step3-from-step2-from-step1-payload" in (r3.stdout or "")


def test_agentic_file_write_then_exec(sandbox) -> None:
    """Write a shell script via ``files.write``, execute via ``commands.run``.

    Models the LLM write-then-run pattern: the agent generates code,
    writes it to the filesystem, then executes it and captures the
    result for the next reasoning step.

    # Arguments

    * `sandbox` — per-test sandbox fixture from conftest.py
    """
    script = "#!/bin/sh\necho agent-wrote-this\n"
    sandbox.files.write("/tmp/agent_script.sh", script)

    result = sandbox.commands.run("sh /tmp/agent_script.sh", timeout=30)
    assert result.exit_code == 0
    assert "agent-wrote-this" in (result.stdout or "")


@pytest.mark.compound
def test_agentic_pause_resume_preserves_state(template_id: str) -> None:
    """Compound — write state, run, pause, ``Sandbox.connect``, run with prior output.

    This is the critical gate: an agent session that spans a
    pause/resume boundary must preserve both filesystem state
    (via gVisor + S3 snapshot) and the ability to continue executing
    commands that depend on output captured before the pause.

    Flow:

    1. Turn 1 — execute a command and capture its stdout.
    2. Persist that output to the filesystem.
    3. Pause the sandbox (triggers the gVisor + S3 snapshot path).
    4. Reconnect via ``Sandbox.connect``.
    5. Turn 3 — read the persisted output and use it in a new command.

    # Arguments

    * `template_id` — session-scoped fixture with the SandboxTemplate id
    """
    from e2b import Sandbox

    sbx = Sandbox.create(template=template_id, timeout=300)
    sandbox_id = sbx.sandbox_id
    try:
        # Turn 1: run a command and capture its output.
        r1 = sbx.commands.run("echo pre-pause-data", timeout=30)
        assert r1.exit_code == 0
        turn1_output = (r1.stdout or "").strip()
        assert turn1_output == "pre-pause-data"

        # Persist the turn-1 result so it survives the snapshot, then pause.
        sbx.files.write("/tmp/agent_state.txt", turn1_output)
        sbx.pause()

        # Resume and verify cross-turn state continuity.
        resumed = Sandbox.connect(sandbox_id)
        stored = resumed.files.read("/tmp/agent_state.txt")
        assert stored == turn1_output

        # Turn 3: use the persisted output as a command argument, simulating
        # the agent reading the prior result and acting on it post-resume.
        r3 = resumed.commands.run(f"echo post-resume-{stored}", timeout=30)
        assert r3.exit_code == 0
        assert f"post-resume-{turn1_output}" in (r3.stdout or "")
    finally:
        try:
            sbx.kill()
        except Exception:  # noqa: BLE001
            pass


def test_agentic_background_process_across_turns(sandbox) -> None:
    """Background process started in turn 1 is visible in turn 2 and killed in turn 3.

    Models the agent pattern of spawning a long-running tool (e.g., a
    server or data pipeline) in turn 1, checking its status in turn 2,
    and cleaning it up in turn 3 — all within the same sandbox session.

    # Arguments

    * `sandbox` — per-test sandbox fixture from conftest.py
    """
    # Turn 1: spawn a background process — simulates the agent starting a tool.
    handle = sandbox.commands.run(
        "sh -c 'sleep 30'",
        background=True,
        timeout=30,
    )
    assert handle.pid is not None

    try:
        # Turn 2: verify it's still running via commands.list — simulates the
        # agent checking tool status before issuing the next instruction.
        processes = sandbox.commands.list()
        pids = {p.pid for p in processes}
        assert handle.pid in pids, (
            f"background PID {handle.pid!r} not found in {pids!r}"
        )
    finally:
        # Turn 3: clean up the process — simulates the agent finishing its task.
        sandbox.commands.kill(handle.pid)
