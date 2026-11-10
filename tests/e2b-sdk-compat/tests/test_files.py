"""Filesystem compatibility tests.

Covers the SDK ``files.*`` surface, which drives both the gRPC
metadata pass-through (list/stat/remove/move/makedir/watch) and the
signed-URL upload/download path (read/write) per spec §7. The
compound flow ``write → list → read → remove`` (spec §13 acceptance #2)
lives in :func:`test_write_list_read_remove_roundtrip`.

Every test in this module exercises a per-sandbox envd RPC — the
SDK proxies through the gateway with ``E2b-Sandbox-Id`` /
``E2b-Sandbox-Port`` headers, and the gateway's
``crates/sandbawks-api/src/routes/envd_proxy.rs`` reverse-proxies the
Connect-RPC call to the matching sandbox's envd (issue #191).
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.files


def test_make_dir_then_list(sandbox) -> None:
    """``make_dir`` creates a directory that ``list`` then sees."""
    sandbox.files.make_dir("/tmp/compat-mkdir")
    entries = sandbox.files.list("/tmp")
    names = {e.name for e in entries}
    assert "compat-mkdir" in names


def test_write_text_then_read_text(sandbox) -> None:
    """A text round-trip through ``write`` + ``read``."""
    sandbox.files.write("/tmp/compat-text.txt", "round-trip-payload")
    assert sandbox.files.read("/tmp/compat-text.txt") == "round-trip-payload"


def test_write_bytes_then_read_bytes(sandbox) -> None:
    """A binary round-trip through ``write`` + ``read(format='bytes')``."""
    payload = bytes(range(256)) * 4  # 1 KiB, every byte value
    sandbox.files.write("/tmp/compat-bin.dat", payload)
    got = sandbox.files.read("/tmp/compat-bin.dat", format="bytes")
    assert got == payload


def test_exists_true_then_false(sandbox) -> None:
    """``exists`` flips across a ``remove``."""
    sandbox.files.write("/tmp/compat-exists.txt", "marker")
    assert sandbox.files.exists("/tmp/compat-exists.txt") is True
    sandbox.files.remove("/tmp/compat-exists.txt")
    assert sandbox.files.exists("/tmp/compat-exists.txt") is False


def test_get_info_reports_size(sandbox) -> None:
    """``get_info`` reports the byte size of a freshly-written file."""
    sandbox.files.write("/tmp/compat-info.bin", b"x" * 1024)
    info = sandbox.files.get_info("/tmp/compat-info.bin")
    assert info.size == 1024


def test_rename_moves_file(sandbox) -> None:
    """``rename`` moves a file; old path stops existing, new one shows up."""
    sandbox.files.write("/tmp/compat-src.txt", "moveme")
    sandbox.files.rename("/tmp/compat-src.txt", "/tmp/compat-dst.txt")
    assert sandbox.files.exists("/tmp/compat-src.txt") is False
    assert sandbox.files.read("/tmp/compat-dst.txt") == "moveme"


@pytest.mark.compound
def test_write_list_read_remove_roundtrip(sandbox) -> None:
    """Spec §13 acceptance #2 — write → list → read → remove."""
    payload = b"compound-flow-payload\n" * 64
    path = "/tmp/compat-roundtrip.bin"

    sandbox.files.write(path, payload)

    entries = sandbox.files.list("/tmp")
    assert any(e.name == "compat-roundtrip.bin" for e in entries)

    assert sandbox.files.read(path, format="bytes") == payload

    sandbox.files.remove(path)
    assert sandbox.files.exists(path) is False
