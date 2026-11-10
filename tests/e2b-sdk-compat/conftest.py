"""Shared pytest fixtures for the sandbawks E2B SDK compatibility harness.

The harness expects two pieces of environment, both defaulted so a
fresh `just cluster-up` works without any exports:

* ``E2B_API_URL`` — the gateway base URL. Defaults to
  ``http://localhost:8080`` because the workspace ``cluster-up`` recipe
  arranges a background ``kubectl port-forward svc/sandbawks-api
  8080:8080``.
* ``E2B_API_KEY`` / ``SMOKE_API_KEY`` — the team API key the gateway
  authenticates against. Defaults to the literal
  ``sbk_smoke_smoke_smoke_smoke_smoke`` used by
  ``deploy/kind/fixtures/api-smoke.sh``; the matching ``TeamAPIKey`` CR
  is applied by that fixture, so the two stay in lockstep.

Both values are passed to the official ``e2b`` SDK via ``os.environ``
*before* any ``Sandbox`` is constructed, since the SDK snapshots them
at ``ConnectionConfig`` construction time.
"""

from __future__ import annotations

import os
import warnings
from collections.abc import Iterator

import pytest

# Matches deploy/kind/fixtures/api-smoke.sh. NOT A SECRET — published
# in plain text in this repo; the matching TeamAPIKey CR is seeded by
# the api-smoke fixture.
DEFAULT_API_URL = "http://localhost:8080"
DEFAULT_API_KEY = "sbk_smoke_smoke_smoke_smoke_smoke"
DEFAULT_TEMPLATE_ID = "api-smoke-template"


@pytest.fixture(scope="session", autouse=True)
def _e2b_env() -> Iterator[None]:
    """Populate the env vars the E2B SDK reads at construction time.

    Autouse + session-scoped so every test sees the same gateway
    without having to take a fixture explicitly. The SDK does not
    re-read env vars per call; setting them here is sufficient.
    """
    previous: dict[str, str | None] = {}
    api_url = os.environ.get("E2B_API_URL", DEFAULT_API_URL)
    overrides = {
        "E2B_API_URL": api_url,
        "E2B_API_KEY": os.environ.get(
            "E2B_API_KEY",
            os.environ.get("SMOKE_API_KEY", DEFAULT_API_KEY),
        ),
        # The E2B SDK's `Sandbox.create` builds `envd_api_url` from the
        # sandbox id + domain by default (`https://49983-sbx-….e2b.app`).
        # That URL is unreachable in our kind-fixture setup where the
        # SDK should instead talk to the gateway on `localhost:8080` —
        # the gateway's `routes::envd_proxy` reads the `E2b-Sandbox-Id`
        # header to demux back to the right per-sandbox envd (issue
        # #191). Setting `E2B_SANDBOX_URL` overrides the default URL
        # construction in `ConnectionConfig.get_sandbox_url`. Since e2b
        # 2.61 `Sandbox.connect` sends those routing headers too, so no
        # SDK patching is needed for the resume path.
        "E2B_SANDBOX_URL": os.environ.get("E2B_SANDBOX_URL", api_url),
    }
    for key, value in overrides.items():
        previous[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@pytest.fixture(scope="session")
def gateway_url() -> str:
    """Return the configured gateway base URL."""
    return os.environ["E2B_API_URL"]


@pytest.fixture(scope="session")
def api_key() -> str:
    """Return the configured team API key."""
    return os.environ["E2B_API_KEY"]


@pytest.fixture(scope="session")
def template_id() -> str:
    """Return the SandboxTemplate id used by the smoke fixtures."""
    return os.environ.get("SANDBAWKS_TEMPLATE_ID", DEFAULT_TEMPLATE_ID)


@pytest.fixture
def sandbox(template_id: str):
    """Yield a freshly-created sandbox, killed on teardown.

    Imported lazily so `pytest --collect-only` succeeds even on a
    machine where the SDK constructor would otherwise probe the
    gateway eagerly.
    """
    from e2b import Sandbox

    sbx = Sandbox.create(template=template_id, timeout=300)
    try:
        yield sbx
    finally:
        try:
            sbx.kill()
        except Exception as exc:  # noqa: BLE001 — best-effort teardown
            # Surface the failure so a flaky teardown does not silently
            # leak sandboxes across the session — `test_list_includes_
            # running_sandbox` later iterates the team list and would
            # otherwise grow noisier each run. `warnings.warn` so pytest's
            # `-W` filters and CI log capture see it, without failing the
            # test that owned the fixture.
            warnings.warn(
                f"sandbox {sbx.sandbox_id!r} teardown kill() raised: {exc!r}",
                stacklevel=2,
            )
