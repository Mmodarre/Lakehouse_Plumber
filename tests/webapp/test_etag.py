"""ETag / If-Match optimistic-concurrency tests for ``/api/files``.

Every GET tags the response with a strong ``ETag`` of the current bytes; PUT and
DELETE accept an optional ``If-Match`` header. A present-and-stale ``If-Match``
against an existing file is rejected with ``412`` and no mutation; a matching or
absent header (or a PUT-create on a missing path) proceeds. These tests pin that
contract end-to-end through the HTTP surface, including byte fidelity: ETags are
computed from raw disk bytes on every leg, so CRLF files round-trip without the
GET-side newline translation that would make their ETag permanently stale.

Writes/deletes use ``mutable_client`` (per-test deep copy); ``mutable_project``
is used to assert on-disk state after a rejected/accepted mutation.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.dependencies import compute_etag

pytestmark = pytest.mark.webapp


def _strip_quotes(etag_header: str) -> str:
    """Return the bare validator from a quoted strong-ETag header value."""
    return etag_header.strip('"')


class TestGetEtag:
    """GET returns a quoted strong ETag of the current content."""

    def test_get_returns_quoted_etag(self, mutable_client: TestClient) -> None:
        resp = mutable_client.get("/api/files/lhp.yaml")
        assert resp.status_code == 200
        etag = resp.headers["etag"]
        assert etag.startswith('"') and etag.endswith('"')
        # compute_etag emits a 16-hex-char digest.
        bare = _strip_quotes(etag)
        assert len(bare) == 16
        assert all(c in "0123456789abcdef" for c in bare)

    def test_get_etag_is_a_valid_if_match_for_put(
        self, mutable_client: TestClient
    ) -> None:
        """The ETag GET hands out must be accepted verbatim as a PUT If-Match.

        Proves GET's ETag (computed from the raw disk bytes) equals the
        PUT-side comparison ETag (also computed from raw disk bytes) for a
        plain-LF UTF-8 file — the two must not drift.
        """
        path = "/api/files/pipelines/etag_roundtrip.yaml"
        mutable_client.put(path, json={"content": "a: 1\n"})
        etag_header = mutable_client.get(path).headers["etag"]

        resp = mutable_client.put(
            path, json={"content": "a: 2\n"}, headers={"If-Match": etag_header}
        )
        assert resp.status_code == 200


class TestEtagByteFidelity:
    """ETags and bodies are raw-bytes-faithful: CRLF survives, non-UTF-8 is 415.

    Regression tests for the GET-side newline-translation defect: hashing
    ``read_text`` output (CRLF→LF translated) while enforcement hashed raw
    disk bytes made every ETag of a CRLF file permanently stale — GET → 412 →
    refetch → same ETag → 412 forever.
    """

    def test_crlf_file_get_preserves_body_and_etag_is_valid_if_match(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        rel = Path("pipelines") / "etag_crlf.yaml"
        disk = mutable_project / rel
        disk.write_bytes(b"a: 1\r\nb: 2\r\n")

        got = mutable_client.get(f"/api/files/{rel.as_posix()}")
        assert got.status_code == 200
        # The body preserves CRLF — no universal-newline translation.
        assert "\r\n" in got.text
        assert got.content == b"a: 1\r\nb: 2\r\n"
        etag_header = got.headers["etag"]
        assert _strip_quotes(etag_header) == compute_etag(b"a: 1\r\nb: 2\r\n")

        # The GET ETag is accepted as If-Match: 200, not the perpetual 412.
        new_content = "a: 1\r\nb: 22\r\n"
        put = mutable_client.put(
            f"/api/files/{rel.as_posix()}",
            json={"content": new_content},
            headers={"If-Match": etag_header},
        )
        assert put.status_code == 200

        # The PUT-returned etag matches the NEW raw disk bytes exactly (the
        # write persisted content.encode() verbatim — no newline rewriting).
        assert disk.read_bytes() == new_content.encode("utf-8")
        assert put.json()["etag"] == compute_etag(disk.read_bytes())

    def test_lf_file_roundtrip_still_works(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        rel = Path("pipelines") / "etag_lf.yaml"
        path = f"/api/files/{rel.as_posix()}"
        put = mutable_client.put(path, json={"content": "a: 1\n"})
        assert put.status_code == 200
        assert (mutable_project / rel).read_bytes() == b"a: 1\n"

        etag_header = mutable_client.get(path).headers["etag"]
        assert etag_header == f'"{put.json()["etag"]}"'

        resp = mutable_client.put(
            path, json={"content": "a: 2\n"}, headers={"If-Match": etag_header}
        )
        assert resp.status_code == 200

    def test_non_utf8_file_get_is_415(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        rel = Path("pipelines") / "etag_binary.bin"
        (mutable_project / rel).write_bytes(b"\xff\xfe\x00\x01not-utf8")

        resp = mutable_client.get(f"/api/files/{rel.as_posix()}")
        assert resp.status_code == 415
        assert resp.json()["detail"] == "Not a UTF-8 text file"


class TestPutIfMatch:
    """PUT honours If-Match (412 on stale, overwrite on match/absent)."""

    def test_put_matching_if_match_succeeds_and_returns_new_etag(
        self, mutable_client: TestClient
    ) -> None:
        path = "/api/files/pipelines/etag_match.yaml"
        first = mutable_client.put(path, json={"content": "a: 1\n"})
        assert first.status_code == 200
        etag = first.json()["etag"]
        assert etag is not None
        # Body field and response header agree (header quoted, field bare).
        assert first.headers["etag"] == f'"{etag}"'

        second = mutable_client.put(
            path, json={"content": "a: 2\n"}, headers={"If-Match": f'"{etag}"'}
        )
        assert second.status_code == 200
        new_etag = second.json()["etag"]
        assert new_etag is not None
        assert new_etag != etag
        assert second.headers["etag"] == f'"{new_etag}"'

    def test_put_stale_if_match_412_and_content_unchanged(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        path = "/api/files/pipelines/etag_stale.yaml"
        mutable_client.put(path, json={"content": "a: 1\n"})

        resp = mutable_client.put(
            path,
            json={"content": "a: 999\n"},
            headers={"If-Match": '"deadbeefdeadbeef"'},
        )
        assert resp.status_code == 412
        # The write did NOT happen — original bytes intact on disk.
        disk = mutable_project / "pipelines" / "etag_stale.yaml"
        assert disk.read_text(encoding="utf-8") == "a: 1\n"

    def test_put_absent_if_match_overwrites_legacy(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        path = "/api/files/pipelines/etag_legacy.yaml"
        mutable_client.put(path, json={"content": "a: 1\n"})

        resp = mutable_client.put(path, json={"content": "a: 2\n"})
        assert resp.status_code == 200
        disk = mutable_project / "pipelines" / "etag_legacy.yaml"
        assert disk.read_text(encoding="utf-8") == "a: 2\n"

    def test_put_if_match_on_missing_path_creates(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        """If-Match on a non-existent target proceeds (plan-pinned create)."""
        rel = Path("pipelines") / "etag_created.yaml"
        target = mutable_project / rel
        assert not target.exists()

        resp = mutable_client.put(
            f"/api/files/{rel.as_posix()}",
            json={"content": "a: 1\n"},
            headers={"If-Match": '"anyetagvalue0000"'},
        )
        assert resp.status_code == 200
        assert target.exists()


class TestDeleteIfMatch:
    """DELETE honours If-Match (412 on stale keeps the file)."""

    def test_delete_stale_if_match_412_keeps_file(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        path = "/api/files/pipelines/etag_del_stale.yaml"
        mutable_client.put(path, json={"content": "a: 1\n"})

        resp = mutable_client.delete(path, headers={"If-Match": '"staleetag0000000"'})
        assert resp.status_code == 412
        assert (mutable_project / "pipelines" / "etag_del_stale.yaml").exists()

    def test_delete_matching_if_match_deletes(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        path = "/api/files/pipelines/etag_del_ok.yaml"
        etag = mutable_client.put(path, json={"content": "a: 1\n"}).json()["etag"]

        resp = mutable_client.delete(path, headers={"If-Match": f'"{etag}"'})
        assert resp.status_code == 200
        assert not (mutable_project / "pipelines" / "etag_del_ok.yaml").exists()
