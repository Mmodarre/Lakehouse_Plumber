"""Pure identity math for deterministic per-pipeline wheel packaging.

No I/O, no wall-clock, no filesystem access. Every function here is a pure
function whose output is fully determined by its arguments, so the same
pipeline payload yields the same content hash, distribution name, and wheel
filename on every machine (WHEEL_PACKAGING_SPEC §6, R3/R4/D6/D7).

Three distinct name spaces are produced and MUST NOT be conflated:

* ``import_package_name`` — a valid Python *import* identifier (what the
  runner imports).
* ``distribution_name`` — the PEP 503-canonical distribution *Name* written
  into the wheel METADATA.
* ``wheel_filename`` — the on-disk ``.whl`` filename. Its name component
  collapses ``[-_.]+`` runs to a single ``_`` (PEP 427), while its version
  component is PEP 440-normalized (NOT collapsed) — ``0.9.0`` stays ``0.9.0``.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping

from packaging.utils import canonicalize_name
from packaging.version import Version

# A separate reserved top-level package that travels in every wheel; an
# import-package name may never collide with it (WHEEL_PACKAGING_SPEC §6.3, R6).
_RESERVED_PACKAGE = "custom_python_functions"

# PEP 427 escaping: runs of ``-``/``_``/``.`` collapse to a single underscore.
# Applies to the NAME component only — never to the version (PEP 440).
_PEP427_RUN = re.compile(r"[-_.]+")

# Anything outside the lowercase identifier alphabet is replaced with ``_``.
_NON_IDENTIFIER = re.compile(r"[^0-9a-z_]")


def escape_name_component(name: str) -> str:
    """Collapse runs of ``-``/``_``/``.`` in a NAME component to a single ``_``.

    The PEP 427 filename escaping for the distribution-name part of a wheel
    filename (and of the ``.dist-info`` directory). MUST NOT be applied to a
    version — versions are PEP 440-normalized via :func:`escape_version`.
    """
    return _PEP427_RUN.sub("_", name)


def escape_version(version: str) -> str:
    """Return the PEP 440 public version (normalized, local segment stripped).

    ``0.9.0`` stays ``0.9.0`` (dots preserved); ``0.9.1.dev5+g1`` normalizes to
    ``0.9.1.dev5``. This is the version component of the wheel filename and of
    the ``.dist-info`` directory — it is NEVER run through the name collapse.
    """
    return str(Version(version).public)


def content_hash(payload: Mapping[str, bytes] | Iterable[tuple[str, bytes]]) -> str:
    """Return a stable 12-char hex digest over ``(relpath, bytes)`` members.

    ``payload`` maps POSIX-style relative paths to member bytes (a mapping or
    any iterable of pairs). Members are hashed in ascending relpath order so
    insertion order does not affect the result; for each member the relpath
    (UTF-8) is fed, then a NUL separator, then the exact member bytes
    (WHEEL_PACKAGING_SPEC §6.5, D7). The caller decides which members are
    included — ``content_hash`` only hashes what it is given.
    """
    items = payload.items() if isinstance(payload, Mapping) else payload
    digest = hashlib.sha256()
    for relpath, member_bytes in sorted(items, key=lambda pair: pair[0]):
        digest.update(relpath.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(member_bytes)
    return digest.hexdigest()[:12]


def import_package_name(pipeline: str) -> str:
    """Return a valid Python import identifier for an arbitrary pipeline name.

    Real pipeline names (``15_python_load``, ``02_bronze``) are not valid
    identifiers. The name is lowercased, non-identifier characters become
    ``_``, a leading digit is prefixed with ``p_``, and the result is
    guaranteed never to equal the reserved ``custom_python_functions`` top-level
    package (WHEEL_PACKAGING_SPEC §6.3, R6). ``result.isidentifier()`` is always
    True.
    """
    name = _NON_IDENTIFIER.sub("_", pipeline.lower())
    if not name or name[0].isdigit():
        name = "p_" + name
    if name == _RESERVED_PACKAGE:
        name = name + "_pkg"
    return name


def distribution_name(*, pipeline: str, env: str, content_hash: str) -> str:
    """Return the PEP 503-canonical distribution name (the METADATA ``Name``).

    The underlying name is ``lhp_<pipeline>_<env>_<hash>`` (the ``lhp_`` brand
    prefix marks LHP-generated wheels; WHEEL_PACKAGING_SPEC §6.3, D6), normalized
    via ``packaging.utils.canonicalize_name``. This is DISTINCT from
    ``import_package_name`` and from the PEP 427 filename escaping.
    """
    return str(canonicalize_name(f"lhp_{pipeline}_{env}_{content_hash}"))


def wheel_filename(*, dist_name: str, version: str) -> str:
    """Return the on-disk ``.whl`` filename for ``dist_name`` at ``version``.

    Matches ``<dist>-<version>-py3-none-any.whl``. The distribution component is
    PEP 427-escaped (``[-_.]+`` runs collapse to a single ``_``) while the
    version is PEP 440-normalized (dots preserved); the compatibility tag is
    fixed at ``py3-none-any`` (WHEEL_PACKAGING_SPEC §6.3).
    """
    dist = escape_name_component(dist_name)
    ver = escape_version(version)
    return f"{dist}-{ver}-py3-none-any.whl"
