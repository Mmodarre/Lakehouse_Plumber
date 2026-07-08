"""UTF-8 round-trip tests for the file read/write surface.

The service reads and writes with an explicit ``encoding="utf-8"``, so a file
containing non-ASCII characters must survive a PUT then GET byte-for-byte, and
must land on disk as UTF-8 rather than the platform default codec (the Windows
``cp1252`` path that would silently mangle ``€`` / CJK / accented characters).

Uses ``mutable_client`` for the API round-trip and ``mutable_project`` to read
the raw bytes back off disk.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp

# Deliberately spans the Latin-1-incompatible euro sign, CJK, and an accented
# Latin char — cp1252 cannot represent the CJK code points, so a non-UTF-8 write
# path would corrupt (or raise on) this content.
_UNICODE_BODY = "note: € 中文 émojis-free-unicode\n"


class TestEncodingRoundTrip:
    """PUT/GET preserve non-ASCII content and persist it as UTF-8."""

    def test_put_then_get_is_byte_identical(self, mutable_client: TestClient) -> None:
        path = "/api/files/pipelines/unicode_roundtrip.yaml"
        put = mutable_client.put(path, json={"content": _UNICODE_BODY})
        assert put.status_code == 200
        # Valid YAML scalar — no syntax diagnostic.
        assert put.json()["yaml_error"] is None

        got = mutable_client.get(path)
        assert got.status_code == 200
        assert got.text == _UNICODE_BODY

    def test_on_disk_bytes_are_utf8(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        rel = Path("pipelines") / "unicode_disk.yaml"
        mutable_client.put(
            f"/api/files/{rel.as_posix()}", json={"content": _UNICODE_BODY}
        )

        disk_bytes = (mutable_project / rel).read_bytes()
        # The bytes on disk are exactly the UTF-8 encoding — proving the write
        # did not fall back to a platform codec such as cp1252.
        assert disk_bytes == _UNICODE_BODY.encode("utf-8")
        assert disk_bytes.decode("utf-8") == _UNICODE_BODY
