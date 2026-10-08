"""Isolated project copies for unsaved editor requests.

Only the temporary copy is writable. Symlinks are not copied, so editor
operations cannot follow an indirect path outside the selected project.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

import yaml

from lhp.api.editor_views import EditorDocumentOverlay

_SKIP_DIRS = frozenset(
    {
        ".git",
        ".lhp",
        ".venv",
        "venv",
        "generated",
        "node_modules",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
    }
)
_MAX_TOTAL_BYTES = 256 * 1024 * 1024
_MAX_FILES = 20000
_MAX_OVERLAY_BYTES = 2 * 1024 * 1024


def _checked_overlay_path(root: Path, overlay: EditorDocumentOverlay) -> Path:
    relative = Path(overlay.path)
    if (
        not overlay.path
        or relative.is_absolute()
        or ".." in relative.parts
        or "\\" in overlay.path
    ):
        raise ValueError("Overlay paths must be relative to the project")
    target = (root / relative).resolve()
    if not target.is_relative_to(root.resolve()):
        raise ValueError("Overlay path leaves the project")
    if len(overlay.text.encode("utf-8")) > _MAX_OVERLAY_BYTES:
        raise ValueError("Overlay document exceeds the 2 MiB editor limit")
    if any(part in _SKIP_DIRS for part in relative.parts):
        raise ValueError("Overlay path names a generated or private directory")
    return relative


def _check_discovery_patterns(project_config: Path) -> None:
    """Reject include globs whose inputs the mirror deliberately omits."""
    try:
        config = yaml.safe_load(project_config.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError):
        return  # Canonical project loading reports malformed configuration.
    if not isinstance(config, dict):
        return
    for key in ("include", "blueprint_include", "instance_include"):
        patterns = config.get(key)
        if not isinstance(patterns, list):
            continue
        for pattern in patterns:
            if not isinstance(pattern, str):
                continue
            path = Path(pattern)
            if (
                path.is_absolute()
                or ".." in path.parts
                or any(part in _SKIP_DIRS for part in path.parts)
            ):
                raise ValueError(
                    f"Editor mirror cannot include configured {key} path: {pattern}"
                )


@contextmanager
def mirrored_project(
    root: Path, overlays: Sequence[EditorDocumentOverlay]
) -> Iterator[Path]:
    """Yield a short-lived project mirror with overlays applied atomically.

    :stability: provisional
    """
    root = root.resolve()
    if not (root / "lhp.yaml").is_file():
        raise FileNotFoundError(f"No lhp.yaml at {root}")
    paths = [_checked_overlay_path(root, item) for item in overlays]
    if len(set(paths)) != len(paths):
        raise ValueError("Duplicate overlay paths")
    with tempfile.TemporaryDirectory(prefix="lhp-editor-") as temporary:
        mirror = Path(temporary)
        copied_bytes = 0
        copied_files = 0
        for base, dirs, files in os.walk(root, followlinks=False):
            base_path = Path(base)
            dirs[:] = [name for name in dirs if name not in _SKIP_DIRS]
            symlink_dirs = [name for name in dirs if (base_path / name).is_symlink()]
            if symlink_dirs:
                raise ValueError(
                    "Editor preview cannot mirror symlinked project directories: "
                    + ", ".join(sorted(symlink_dirs))
                )
            relative_base = base_path.relative_to(root)
            (mirror / relative_base).mkdir(parents=True, exist_ok=True)
            for name in files:
                source = base_path / name
                if source.is_symlink():
                    raise ValueError(
                        f"Editor preview cannot mirror symlinked project file: {source.relative_to(root)}"
                    )
                if not source.is_file():
                    continue
                copied_bytes += source.stat().st_size
                copied_files += 1
                if copied_bytes > _MAX_TOTAL_BYTES or copied_files > _MAX_FILES:
                    raise ValueError("Project exceeds the editor preview copy limit")
                shutil.copy2(source, mirror / relative_base / name)
        for overlay, relative in zip(overlays, paths, strict=True):
            target = mirror / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(overlay.text, encoding="utf-8")
        _check_discovery_patterns(mirror / "lhp.yaml")
        yield mirror
