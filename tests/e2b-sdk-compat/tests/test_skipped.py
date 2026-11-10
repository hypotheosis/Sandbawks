"""SDK methods explicitly skipped until the backing feature lands.

Each test below is wrapped in ``pytest.skip`` with a TODO pointing at
the plan / epic that will deliver the missing functionality. Keeping
the skipped tests in the suite (rather than deleting them) means the
test matrix output advertises what's *not* yet wired up — reviewers
of a later plan can grep this file for their plan number.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.skipped_until_plan


def test_create_snapshot_pending_plan_07() -> None:
    """``Sandbox.create_snapshot`` requires the warm-pool / snapshot CRD path.

    TODO(plan-07): exercise once ``SandboxWarmPool`` lands and the API
    surfaces an explicit snapshot management endpoint.
    """
    pytest.skip("warm-pool + explicit snapshot management lands in Plan 07")


def test_delete_snapshot_pending_plan_07() -> None:
    """``Sandbox.delete_snapshot`` mirrors ``create_snapshot`` — same gating."""
    pytest.skip("warm-pool + explicit snapshot management lands in Plan 07")


def test_list_snapshots_pending_plan_07() -> None:
    """``Sandbox.list_snapshots`` mirrors ``create_snapshot`` — same gating."""
    pytest.skip("warm-pool + explicit snapshot management lands in Plan 07")


def test_template_build_pending_future_plan() -> None:
    """``Template.build`` requires the template build pipeline.

    TODO(future-plan): out of scope for v0.1 MVP; tracked alongside
    template registry work post-MVP.
    """
    pytest.skip("template build pipeline not in v0.1 MVP scope")


def test_get_metrics_pending_observability_plan() -> None:
    """``Sandbox.get_metrics`` reads the Prometheus surface in spec §11.

    TODO: enable once the observability plan lands the per-sandbox
    metrics endpoint the SDK probes.
    """
    pytest.skip("per-sandbox metrics endpoint not yet implemented (spec §11)")


def test_volume_mounts_pending_future_plan() -> None:
    """``Sandbox.create(volume_mounts=...)`` requires the Volume CRD.

    TODO(future-plan): Volume / persistent-storage support is post-MVP.
    """
    pytest.skip("Volume / persistent-storage support is post-MVP")


# ``test_is_running`` / ``test_get_info`` / ``test_set_timeout`` used to
# live here as skips pending #189. The backing routes (``GET
# /sandboxes/{id}`` and ``POST /sandboxes/{id}/timeout``) have landed,
# so those tests now run for real in ``test_lifecycle.py``.


def test_watch_dir_pending_sdk_contract_verification() -> None:
    """``files.watch_dir`` — SDK contract (callback vs generator) is unverified.

    The first draft of this test in ``test_files.py`` hedged across
    both shapes of the SDK API at once (``on_event=...`` keyword *and*
    ``next(iter(handle))``), which collapsed into dead code that
    asserted nothing observable about the watch surface. T2 (#184)
    will bring up a live cluster against which the actual contract
    can be observed; rewrite this test at that point against the
    real ``WatchHandle`` shape rather than guessing.

    TODO(plan-06-t2): rewrite against the live SDK contract once the
    CI job exercises a real gvisor kind cluster.
    """
    pytest.skip("watch_dir SDK contract not yet verified against a live gateway (T2 #184)")
