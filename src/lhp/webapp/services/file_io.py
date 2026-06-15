"""Project file I/O service: the security boundary of the LHP web IDE.

This module owns *bytes on disk* for the web IDE. It deliberately does NOT
route through ``lhp.api`` — ``lhp.api`` is the boundary for *meaning* (parsing
YAML into domain models, validating, generating), whereas raw file read /
write / delete / tree-listing is webapp-owned plumbing. The only LHP coupling
permitted here is the import-linter "webapp-uses-public-api" contract, which
allows ``lhp.api`` and ``lhp.errors`` plus stdlib / yaml.

Two invariants are enforced on *every* operation:

1. **Path-traversal guard** (``_resolve_in_root``): the target path is resolved
   (symlinks included) and must be relative to the resolved project root.
   Applied unconditionally — reads included. Violations raise
   :class:`PathTraversalError`.

2. **Write/delete protection** (``_assert_writable``): writes and deletes are
   blocked under four project-relative prefixes — ``.git/``, ``generated/``,
   ``.lhp/logs/``, ``.lhp/dependencies/``. Reads are unrestricted (the UI must
   be able to display generated code). Violations raise
   :class:`WriteProtectedError`.

The two exception types are intentionally distinct so the router can map them
to different HTTP responses while still distinguishing a security violation
(traversal) from a policy rejection (write-protected path).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)


# Project-relative prefixes under which writes AND deletes are blocked.
# D9 (amended): reads are unrestricted; only mutations are blocked here.
# ``generated/`` must remain readable so the UI can show generated code.
WRITE_PROTECTED_PREFIXES: tuple[str, ...] = (
    ".git/",
    "generated/",
    ".lhp/logs/",
    ".lhp/dependencies/",
)

# Directory names excluded from tree listing (a node whose name matches is
# skipped entirely, children included).
_EXCLUDED_DIR_NAMES: frozenset[str] = frozenset(
    {
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        ".env",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "node_modules",
        ".idea",
        ".vscode",
        ".DS_Store",
        ".tox",
        ".eggs",
    }
)

# YAML file suffixes that trigger post-write syntax feedback.
_YAML_SUFFIXES: tuple[str, ...] = (".yaml", ".yml")


class FileIOError(Exception):
    """Base class for file-I/O service errors.

    Webapp-owned (not an ``lhp.errors.LHPError``): these describe HTTP-facing
    policy/security outcomes of byte I/O, not LHP domain failures.
    """


class PathTraversalError(FileIOError):
    """The requested path resolved outside the project root.

    Raised by every operation (reads included) when the resolved target is not
    relative to the resolved project root. The router maps this to HTTP 403.
    """


class WriteProtectedError(FileIOError):
    """A write or delete targeted a write-protected prefix.

    Distinct from :class:`PathTraversalError`: the path is *inside* the project
    root but lands under one of :data:`WRITE_PROTECTED_PREFIXES`. The router
    maps this to HTTP 403 with a different detail message.
    """


@dataclass(frozen=True)
class YamlSyntaxError:
    """Structured YAML syntax diagnostic for the Monaco editor.

    ``line`` / ``column`` are **1-based** to map directly onto Monaco's
    ``IMarkerData.startLineNumber`` / ``startColumn`` (PyYAML's ``problem_mark``
    is 0-based; this record adds 1).
    """

    line: int
    column: int
    message: str


@dataclass(frozen=True)
class WriteResult:
    """Outcome of a :func:`write_file` call.

    ``yaml_error`` is populated only when a ``*.yaml`` / ``*.yml`` file was
    written and re-parsing it raised a positioned ``yaml.YAMLError``. The write
    persists regardless (the user may save broken YAML mid-edit); the field
    carries the diagnostic for the editor to surface.
    """

    path: str
    yaml_error: Optional[YamlSyntaxError] = None


class FileIOService:
    """Path-guarded file operations rooted at a single project directory.

    A new instance is bound to one ``project_root``; the router injects it.
    Every public method enforces the traversal guard, and the mutating methods
    additionally enforce the write/delete protection list.
    """

    def __init__(self, project_root: Path) -> None:
        self._project_root = project_root
        # Resolved once; reused by the per-call guard. Resolving the root up
        # front means a moved/symlinked root is captured at construction time.
        self._root_resolved = project_root.resolve()

    def _resolve_in_root(self, relative_path: str) -> Path:
        """Resolve ``relative_path`` and assert it stays within the root.

        ``.resolve()`` on both root and target (symlinks followed) then
        ``is_relative_to`` to defeat prefix-matching and symlink escapes.
        Applied to *every* operation, reads included.
        """
        target_resolved = (self._project_root / relative_path).resolve()
        if not target_resolved.is_relative_to(self._root_resolved):
            raise PathTraversalError(
                f"Path traversal not allowed: {relative_path!r} resolves outside "
                f"the project root"
            )
        return target_resolved

    def _assert_writable(self, relative_path: str) -> None:
        """Reject writes/deletes under a protected prefix.

        Normalizes Windows separators before prefix-matching so a
        ``generated\\foo`` request is caught the same as ``generated/foo``.
        """
        normalized = relative_path.replace("\\", "/").lstrip("/")
        if any(normalized.startswith(prefix) for prefix in WRITE_PROTECTED_PREFIXES):
            raise WriteProtectedError(
                f"Write-protected path: {relative_path!r} is under a read-only prefix"
            )

    def list_tree(self) -> dict[str, Any]:
        """Return the recursive project file tree.

        Shape mirrors what the front-end ``FileBrowser`` consumes, expanded to
        a single recursive payload: each node is
        ``{"name", "path", "type", "children"?}`` where ``type`` is
        ``"file"`` | ``"directory"`` and ``children`` is present only on
        directory nodes. ``path`` is project-relative with ``/`` separators.
        Excluded directories (see :data:`_EXCLUDED_DIR_NAMES`) are skipped
        wholesale. Entries are sorted directories-first then case-insensitively
        by name.
        """
        return {
            "name": self._project_root.name,
            "path": "",
            "type": "directory",
            "children": self._list_children(self._project_root, ""),
        }

    def _list_children(self, directory: Path, rel_prefix: str) -> list[dict[str, Any]]:
        """Build child nodes for ``directory`` (project-relative ``rel_prefix``)."""
        try:
            entries = list(directory.iterdir())
        except OSError:
            logger.warning("Could not list directory: %s", directory)
            return []

        # Directories first, then files; each group sorted case-insensitively.
        entries.sort(key=lambda p: (p.is_file(), p.name.lower()))

        children: list[dict[str, Any]] = []
        for entry in entries:
            if entry.is_dir() and entry.name in _EXCLUDED_DIR_NAMES:
                continue
            child_rel = f"{rel_prefix}/{entry.name}" if rel_prefix else entry.name
            if entry.is_dir():
                children.append(
                    {
                        "name": entry.name,
                        "path": child_rel,
                        "type": "directory",
                        "children": self._list_children(entry, child_rel),
                    }
                )
            else:
                children.append(
                    {
                        "name": entry.name,
                        "path": child_rel,
                        "type": "file",
                    }
                )
        return children

    def read_file(self, relative_path: str) -> str:
        """Read a text file. Traversal-guarded; reads are otherwise unrestricted.

        Raises:
            PathTraversalError: target resolves outside the project root.
            FileNotFoundError: target does not exist.
            IsADirectoryError: target is a directory.
        """
        target = self._resolve_in_root(relative_path)
        if not target.exists():
            raise FileNotFoundError(f"File not found: {relative_path}")
        if target.is_dir():
            raise IsADirectoryError(f"Not a file: {relative_path}")
        return target.read_text()

    def write_file(self, relative_path: str, content: str) -> WriteResult:
        """Write ``content`` to a file (PUT semantics: creates dirs / new files).

        Traversal-guarded and write-protection-guarded. After a successful
        write of a ``*.yaml`` / ``*.yml`` file, the content is re-parsed with
        ``yaml.safe_load``; a positioned ``yaml.YAMLError`` is captured into the
        returned :class:`WriteResult` (1-based line/column for Monaco). The
        write is NOT rolled back on a YAML error — the user may be saving broken
        YAML deliberately mid-edit.

        Raises:
            PathTraversalError: target resolves outside the project root.
            WriteProtectedError: target is under a write-protected prefix.
        """
        self._assert_writable(relative_path)
        target = self._resolve_in_root(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

        yaml_error: Optional[YamlSyntaxError] = None
        if target.suffix.lower() in _YAML_SUFFIXES:
            yaml_error = _check_yaml_syntax(content)

        return WriteResult(path=relative_path, yaml_error=yaml_error)

    def delete_file(self, relative_path: str) -> None:
        """Delete a file. Traversal-guarded and write-protection-guarded.

        Raises:
            PathTraversalError: target resolves outside the project root.
            WriteProtectedError: target is under a write-protected prefix.
            FileNotFoundError: target does not exist.
            IsADirectoryError: target is a directory.
        """
        self._assert_writable(relative_path)
        target = self._resolve_in_root(relative_path)
        if not target.exists():
            raise FileNotFoundError(f"File not found: {relative_path}")
        if target.is_dir():
            raise IsADirectoryError(f"Not a file: {relative_path}")
        target.unlink()


def _check_yaml_syntax(content: str) -> Optional[YamlSyntaxError]:
    """Parse ``content`` as YAML; return a positioned diagnostic on failure.

    Returns ``None`` when the content parses cleanly. On a ``yaml.YAMLError``
    carrying a ``problem_mark``, returns a :class:`YamlSyntaxError` with 1-based
    line/column (PyYAML marks are 0-based). When no mark is available, the
    diagnostic points at line 1 / column 1 with the stringified error.
    """
    try:
        yaml.safe_load(content)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        problem = getattr(exc, "problem", None) or str(exc)
        if mark is not None:
            return YamlSyntaxError(
                line=mark.line + 1,
                column=mark.column + 1,
                message=str(problem),
            )
        return YamlSyntaxError(line=1, column=1, message=str(exc))
    return None
