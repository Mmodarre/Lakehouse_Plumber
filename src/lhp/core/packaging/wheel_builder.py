"""Deterministic, byte-reproducible wheel assembly from in-memory members.

Builds a PEP 427 wheel archive purely from a payload of ``arcname -> bytes``
members, using only the standard library. The produced ``.whl`` bytes are
identical across machines and CI images for identical input — the property
that makes content-addressing meaningful (WHEEL_PACKAGING_SPEC §6, R3, D7).

Reproducibility is achieved by pinning every source of nondeterminism:

* ``zipfile.ZIP_STORED`` (no DEFLATE) — eliminates zlib-version skew.
* a fixed ``date_time`` of ``(1980, 1, 1, 0, 0, 0)`` on every member,
* a pinned POSIX ``create_system`` and a fixed ``external_attr``,
* members written in ascending ``arcname`` order with no directory entries.

The ``.dist-info`` directory name is PEP 427-escaped identically to
``identity.wheel_filename`` (runs of ``[-_.]+`` collapse to a single ``_``),
so the in-wheel metadata agrees with the on-disk ``.whl`` filename.
"""

from __future__ import annotations

import base64
import hashlib
import io
import re
import zipfile
from collections.abc import Iterable, Mapping

# PEP 427 escaping: runs of ``-``/``_``/``.`` collapse to a single underscore.
# MUST stay in lockstep with ``identity._PEP427_RUN`` so the in-wheel
# ``.dist-info`` directory matches the on-disk wheel filename stem.
_PEP427_RUN = re.compile(r"[-_.]+")

# Pinned epoch for every member — DOS epoch start. Any wall-clock here would
# void byte-reproducibility (R3).
_FIXED_DATE_TIME = (1980, 1, 1, 0, 0, 0)

# Pinned POSIX file mode (0o644) shifted into the high 16 bits of
# ``external_attr``, exactly as CPython's zipfile encodes Unix permissions.
_EXTERNAL_ATTR = 0o644 << 16

# Pinned ``create_system``: 3 == Unix/POSIX. Pinning this keeps the byte at a
# stable value instead of inheriting the host platform's default (0 on Windows,
# 3 on Unix), which would otherwise make the archive host-dependent.
_CREATE_SYSTEM_POSIX = 3


def _pep427_escape(s: str) -> str:
    """Collapse runs of ``-``/``_``/``.`` to a single ``_`` (PEP 427).

    Applied independently to the distribution name and version to form the
    ``.dist-info`` directory name, matching ``identity.wheel_filename``.
    """
    return _PEP427_RUN.sub("_", s)


def _urlsafe_b64_nopad(digest: bytes) -> str:
    """Return RFC 4648 url-safe base64 of ``digest`` with padding stripped.

    This is the digest encoding PEP 427 RECORD lines use
    (``sha256=<value>``).
    """
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


class WheelBuilder:
    """Assemble a deterministic PEP 427 wheel from in-memory members.

    Stateless: a single instance may build any number of wheels. The builder
    owns archive *layout and metadata* only; it never renders or mutates member
    content — callers supply the exact bytes of every flowgroup/helper module.
    """

    def build(
        self,
        payload: Mapping[str, bytes] | Iterable[tuple[str, bytes]],
        *,
        dist_name: str,
        version: str,
    ) -> bytes:
        """Return the complete ``.whl`` archive as bytes.

        ``payload`` maps in-wheel POSIX arcnames to member bytes (a mapping or
        any iterable of pairs). The three ``.dist-info`` files (``METADATA``,
        ``WHEEL``, ``RECORD``) are synthesized and merged with the payload;
        every member — payload and metadata alike — appears in ``RECORD`` and
        is written in ascending arcname order. ``dist_name`` is the PEP 503
        canonical distribution name (used verbatim in ``METADATA``); it is NOT
        re-canonicalized here.
        """
        items = payload.items() if isinstance(payload, Mapping) else payload
        members: dict[str, bytes] = dict(items)

        dist_info = f"{_pep427_escape(dist_name)}-{_pep427_escape(version)}.dist-info"
        metadata_arc = f"{dist_info}/METADATA"
        wheel_arc = f"{dist_info}/WHEEL"
        record_arc = f"{dist_info}/RECORD"

        members[metadata_arc] = self._metadata(dist_name, version)
        members[wheel_arc] = self._wheel_metadata(version)
        # RECORD lists itself, so seed a placeholder to fix its sort position
        # before computing the file (its own bytes are never hashed).
        members[record_arc] = b""

        # RECORD covers every member; its own line carries no hash/size. Build
        # it over the final ascending arcname order so the file mirrors the
        # write order, then overwrite the placeholder.
        members[record_arc] = self._record(
            sorted(members.items()), record_arc=record_arc
        )

        return self._zip(sorted(members.items()))

    @staticmethod
    def _metadata(dist_name: str, version: str) -> bytes:
        """Render the ``METADATA`` member (PEP 566 minimal core fields)."""
        lines = [
            "Metadata-Version: 2.1",
            f"Name: {dist_name}",
            f"Version: {version}",
        ]
        return ("\n".join(lines) + "\n").encode("utf-8")

    @staticmethod
    def _wheel_metadata(version: str) -> bytes:
        """Render the ``WHEEL`` member (pure-Python, universal py3 tag)."""
        lines = [
            "Wheel-Version: 1.0",
            f"Generator: lhp {version}",
            "Root-Is-Purelib: true",
            "Tag: py3-none-any",
        ]
        return ("\n".join(lines) + "\n").encode("utf-8")

    @staticmethod
    def _record(members: list[tuple[str, bytes]], *, record_arc: str) -> bytes:
        """Render the ``RECORD`` member over ``members`` (already arc-sorted).

        Each non-RECORD member contributes ``<arc>,sha256=<b64>,<size>``;
        RECORD's own line is ``<record_arc>,,`` (empty hash + empty size), as
        its digest cannot include itself.
        """
        lines: list[str] = []
        for arc, data in members:
            if arc == record_arc:
                lines.append(f"{record_arc},,")
                continue
            digest = hashlib.sha256(data).digest()
            lines.append(f"{arc},sha256={_urlsafe_b64_nopad(digest)},{len(data)}")
        return ("\n".join(lines) + "\n").encode("utf-8")

    @staticmethod
    def _zip(members: list[tuple[str, bytes]]) -> bytes:
        """Write ``members`` (already arc-sorted) into a deterministic archive.

        One stored (uncompressed) file entry per member, no directory entries,
        each carrying pinned ``date_time``/``external_attr``/``create_system``
        so the resulting bytes are identical for identical input (R3).
        """
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, mode="w") as zf:
            for arc, data in members:
                zinfo = zipfile.ZipInfo(filename=arc, date_time=_FIXED_DATE_TIME)
                zinfo.external_attr = _EXTERNAL_ATTR
                zinfo.create_system = _CREATE_SYSTEM_POSIX
                zinfo.compress_type = zipfile.ZIP_STORED
                zf.writestr(zinfo, data, compress_type=zipfile.ZIP_STORED)
        return buffer.getvalue()
