"""Contract tests for ``lhp.core.packaging.wheel_builder.WheelBuilder``.

These pins are load-bearing: the wheel bytes are a content-addressed artifact
(WHEEL_PACKAGING_SPEC §6, R3, D7). Byte-reproducibility, a valid PEP 427
archive layout, a self-consistent RECORD, and pinned member metadata are the
properties downstream content-addressing and deploy upload-skip rely on.
"""

from __future__ import annotations

import base64
import hashlib
import io
import zipfile

import pytest

from lhp.core.packaging.wheel_builder import WheelBuilder

# A fixed multi-member payload exercising nested package paths.
_PAYLOAD = {
    "pkg/mod_b.py": b"import dlt\n\nVALUE = 2\n",
    "pkg/__init__.py": b"",
    "pkg/mod_a.py": b"import dlt\n\nVALUE = 1\n",
    "custom_python_functions/__init__.py": b"# vendored helpers\n",
    "custom_python_functions/helpers.py": b"def f():\n    return 1\n",
}
_DIST_NAME = "lhp-pre-brz-bp-1524-preprod-a1b2c3d4e5f6"
_VERSION = "0.9.0"
# dist-info dir: name component PEP 427-escaped, version PEP 440-normalized.
_DIST_INFO = "lhp_pre_brz_bp_1524_preprod_a1b2c3d4e5f6-0.9.0.dist-info"


def _urlsafe_b64_nopad(digest: bytes) -> str:
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _build() -> bytes:
    return WheelBuilder().build(_PAYLOAD, dist_name=_DIST_NAME, version=_VERSION)


@pytest.mark.unit
class TestReproducibility:
    def test_byte_identical_across_calls(self) -> None:
        """R3: identical input yields byte-identical archives."""
        assert _build() == _build()

    def test_byte_identical_regardless_of_payload_order(self) -> None:
        """Insertion order of payload members does not affect output bytes."""
        reordered = dict(reversed(list(_PAYLOAD.items())))
        assert list(reordered) != list(_PAYLOAD)
        a = WheelBuilder().build(reordered, dist_name=_DIST_NAME, version=_VERSION)
        b = WheelBuilder().build(_PAYLOAD, dist_name=_DIST_NAME, version=_VERSION)
        assert a == b

    def test_accepts_iterable_of_pairs(self) -> None:
        as_pairs = list(reversed(list(_PAYLOAD.items())))
        from_pairs = WheelBuilder().build(
            as_pairs, dist_name=_DIST_NAME, version=_VERSION
        )
        assert from_pairs == _build()


@pytest.mark.unit
class TestArchiveValidity:
    def test_opens_as_zip(self) -> None:
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            assert zf.testzip() is None

    def test_dist_info_members_present(self) -> None:
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            names = set(zf.namelist())
        assert f"{_DIST_INFO}/METADATA" in names
        assert f"{_DIST_INFO}/WHEEL" in names
        assert f"{_DIST_INFO}/RECORD" in names

    def test_all_payload_members_present(self) -> None:
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            names = set(zf.namelist())
        assert _PAYLOAD.keys() <= names

    def test_no_directory_entries(self) -> None:
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            assert all(not name.endswith("/") for name in zf.namelist())

    def test_members_in_ascending_arcname_order(self) -> None:
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            names = zf.namelist()
        assert names == sorted(names)

    def test_metadata_contents(self) -> None:
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            text = zf.read(f"{_DIST_INFO}/METADATA").decode("utf-8")
        assert "Metadata-Version: 2.1" in text
        # Name is the canonical dist name verbatim — NOT re-canonicalized.
        assert f"Name: {_DIST_NAME}" in text
        assert f"Version: {_VERSION}" in text

    def test_wheel_contents(self) -> None:
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            text = zf.read(f"{_DIST_INFO}/WHEEL").decode("utf-8")
        assert "Wheel-Version: 1.0" in text
        assert f"Generator: lhp {_VERSION}" in text
        assert "Root-Is-Purelib: true" in text
        assert "Tag: py3-none-any" in text


@pytest.mark.unit
class TestRecord:
    def test_record_validates_against_archive(self) -> None:
        """Each non-RECORD line's hash+size match the archive's bytes."""
        record_arc = f"{_DIST_INFO}/RECORD"
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            record_text = zf.read(record_arc).decode("utf-8")
            member_bytes = {name: zf.read(name) for name in zf.namelist()}

        seen: set[str] = set()
        for line in record_text.splitlines():
            arc, hash_field, size_field = line.split(",")
            seen.add(arc)
            if arc == record_arc:
                # RECORD's own line carries no hash and no size.
                assert hash_field == ""
                assert size_field == ""
                continue
            data = member_bytes[arc]
            expected = _urlsafe_b64_nopad(hashlib.sha256(data).digest())
            assert hash_field == f"sha256={expected}"
            assert int(size_field) == len(data)

        # Every archive member appears in RECORD (payload + all dist-info).
        assert seen == set(member_bytes)

    def test_record_own_line_ends_in_double_comma(self) -> None:
        record_arc = f"{_DIST_INFO}/RECORD"
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            record_text = zf.read(record_arc).decode("utf-8")
        own = next(
            line for line in record_text.splitlines() if line.startswith(record_arc)
        )
        assert own.endswith(",,")
        assert own == f"{record_arc},,"


@pytest.mark.unit
class TestMemberMetadata:
    def test_every_member_has_pinned_date_time(self) -> None:
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            for info in zf.infolist():
                assert info.date_time == (1980, 1, 1, 0, 0, 0)

    def test_every_member_is_stored_not_deflated(self) -> None:
        with zipfile.ZipFile(io.BytesIO(_build())) as zf:
            for info in zf.infolist():
                assert info.compress_type == zipfile.ZIP_STORED
