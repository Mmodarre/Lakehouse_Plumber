"""Domain-aware reader for BUILT per-pipeline wheels (no bundle, no api).

The consumer-side complement to ``wheel_builder``/``packager``: locate a wheel
LHP already built under the pipeline's dist dir, validate it is a usable
``.whl`` archive, and expose its ``.py`` members for inspection or extraction.
``.dist-info`` files never end in ``.py`` so are naturally excluded (§6.4).

Returns PRIMITIVES ONLY (``tuple``/``Path``) and MUST NOT import the public API
package — ``core -> api`` is a forbidden upward edge; the facade emits DTOs.
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path
from typing import Tuple

from ...errors import ErrorFactory, codes
from ...errors.codes import ErrorCode
from ...errors.types import LHPFileError

logger = logging.getLogger(__name__)


def _io(code: ErrorCode, title: str, details: str, ctx: dict[str, str]) -> LHPFileError:
    """Build (not raise) an ``LHPFileError`` for a wheel-reader I/O failure."""
    return ErrorFactory.io_error(code, title=title, details=details, context=ctx)


def locate_pipeline_wheel(project_root: Path, pipeline: str, env: str) -> Path:
    """Return the single ``.whl`` built for ``pipeline`` under ``env``.

    Globs ``generated/<env>/_wheels/<pipeline>/dist/*.whl`` and requires exactly
    one match. Raises ``LHP-GEN-001`` on zero or more than one wheel.
    """
    dist_dir = project_root / "generated" / env / "_wheels" / pipeline / "dist"
    matches = sorted(dist_dir.glob("*.whl"))
    if len(matches) != 1:
        raise ErrorFactory.general_error(
            codes.GEN_001,
            title="Expected exactly one built wheel for pipeline",
            details=(
                f"Pipeline '{pipeline}' (env '{env}') has {len(matches)} wheel "
                f"file(s) under {dist_dir} (expected exactly one)."
            ),
            suggestions=[
                "Run 'lhp generate' so the wheel is built",
                "Verify the pipeline's wheel build did not fail",
            ],
            context={
                "pipeline": pipeline,
                "env": env,
                "dist_dir": str(dist_dir),
                "matches": [m.name for m in matches],
            },
        )
    return matches[0]


def _open_wheel(wheel_path: Path) -> zipfile.ZipFile:
    """Validate ``wheel_path`` and return an open archive.

    Raises IO-022 (missing), IO-023 (not a usable ``.whl``), IO-024 (corrupt).
    """
    ctx = {"Wheel Path": str(wheel_path)}
    p = str(wheel_path)
    if not wheel_path.exists():
        raise _io(codes.IO_022, "Wheel file not found", f"No wheel file at '{p}'.", ctx)
    if not wheel_path.is_file() or wheel_path.suffix != ".whl":
        raise _io(codes.IO_023, "Not a wheel file", f"'{p}' is not a '.whl' file.", ctx)
    try:
        return zipfile.ZipFile(wheel_path)
    except zipfile.BadZipFile as e:
        raise _io(
            codes.IO_024, "Corrupt wheel archive", f"Wheel '{p}' is not valid zip.", ctx
        ) from e


def list_wheel_py_modules(wheel_path: Path) -> Tuple[Tuple[str, int], ...]:
    """Return ``(arcname, uncompressed_size)`` for every ``.py`` wheel member.

    Sorted by arcname (deterministic). A foreign wheel with no ``.py`` members
    yields an empty tuple — NOT an error.
    """
    with _open_wheel(wheel_path) as zf:
        members = [
            (info.filename, info.file_size)
            for info in zf.infolist()
            if info.filename.endswith(".py")
        ]
    return tuple(sorted(members))


def extract_wheel_py_modules(wheel_path: Path, output_dir: Path) -> Tuple[Path, ...]:
    """Extract every ``.py`` member into ``output_dir``, preserving structure.

    Creates ``output_dir`` (and parents) if missing, overwrites existing files,
    and returns the written paths (sorted by arcname). Each target is confirmed
    to resolve within ``output_dir`` (zip path-traversal guard); an escaping
    member is skipped. Raises ``LHP-IO-005`` if writing a destination is denied.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    root = output_dir.resolve()
    written: list[Path] = []
    with _open_wheel(wheel_path) as zf:
        for arcname in sorted(zf.namelist()):
            if not arcname.endswith(".py"):
                continue
            dest = (output_dir / arcname).resolve()
            if not dest.is_relative_to(root):
                logger.warning(f"Skipping zip member outside output dir: {arcname}")
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            try:
                dest.write_bytes(zf.read(arcname))
            except PermissionError as e:
                d = f"Cannot write '{dest}': permission denied."
                raise _io(
                    codes.IO_005, "Permission denied", d, {"File": str(dest)}
                ) from e
            written.append(dest)
    return tuple(written)


__all__ = [
    "extract_wheel_py_modules",
    "list_wheel_py_modules",
    "locate_pipeline_wheel",
]
