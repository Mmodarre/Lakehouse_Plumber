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
* ``wheel_filename`` — the PEP 427-escaped on-disk ``.whl`` filename, whose
  distribution/version components collapse ``[-_.]+`` runs to a single ``_``.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping

from packaging.utils import canonicalize_name

# A separate reserved top-level package that travels in every wheel; an
# import-package name may never collide with it (WHEEL_PACKAGING_SPEC §6.3, R6).
_RESERVED_PACKAGE = "custom_python_functions"

# PEP 427 escaping: runs of ``-``/``_``/``.`` collapse to a single underscore.
_PEP427_RUN = re.compile(r"[-_.]+")

# Anything outside the lowercase identifier alphabet is replaced with ``_``.
_NON_IDENTIFIER = re.compile(r"[^0-9a-z_]")


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

    The underlying name is ``<pipeline>_<env>_<hash>`` (WHEEL_PACKAGING_SPEC
    §6.3, D6), normalized via ``packaging.utils.canonicalize_name``. This is
    DISTINCT from ``import_package_name`` and from the PEP 427 filename escaping.
    """
    return str(canonicalize_name(f"{pipeline}_{env}_{content_hash}"))


def wheel_filename(*, dist_name: str, version: str) -> str:
    """Return the on-disk ``.whl`` filename for ``dist_name`` at ``version``.

    Matches ``<dist>-<version>-py3-none-any.whl`` where the distribution and
    version components are PEP 427-escaped (runs of ``[-_.]+`` collapse to a
    single ``_``); the compatibility tag is fixed at ``py3-none-any``
    (WHEEL_PACKAGING_SPEC §6.3).
    """
    dist = _PEP427_RUN.sub("_", dist_name)
    ver = _PEP427_RUN.sub("_", version)
    return f"{dist}-{ver}-py3-none-any.whl"
