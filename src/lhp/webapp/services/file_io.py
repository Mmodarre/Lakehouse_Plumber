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
   blocked under the project-relative prefixes in
   :data:`WRITE_PROTECTED_PREFIXES` — ``.git/``, ``generated/``,
   ``.lhp/logs/``, ``.lhp/dependencies/``, and the ``.lhp/webapp.db`` SQLite
   files. Reads are unrestricted (the UI must be able to display generated
   code). Violations raise :class:`WriteProtectedError`.

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
# ``.lhp/webapp.db`` is a PREFIX on purpose: it also covers the SQLite WAL
# sidecars (``webapp.db-wal`` / ``webapp.db-shm``) so a stray PUT cannot
# corrupt the run-history database.
WRITE_PROTECTED_PREFIXES: tuple[str, ...] = (
    ".git/",
    "generated/",
    ".lhp/logs/",
    ".lhp/dependencies/",
    ".lhp/webapp.db",
)

# Project-relative prefixes whose mutation carries no flowgroup / preset /
# template meaning, so a successful write/delete there must NOT invalidate the
# cached application facade. ``.lhp/`` covers logs, dependency graphs, and the
# profile alike; ``.git/`` and ``generated/`` are the VCS and codegen trees.
_GENERATED_OR_INTERNAL_PREFIXES: tuple[str, ...] = (
    "generated/",
    ".lhp/",
    ".git/",
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

# File-name prefixes excluded from tree listing. ``webapp.db`` hides the
# run-history SQLite database and its WAL sidecars (``webapp.db-wal`` /
# ``webapp.db-shm``) while the rest of ``.lhp/`` stays visible (deliberate).
_EXCLUDED_FILE_NAME_PREFIXES: tuple[str, ...] = ("webapp.db",)

# YAML file suffixes that trigger post-write syntax feedback.
_YAML_SUFFIXES: tuple[str, ...] = (".yaml", ".yml")


def _normalize_relative(relative_path: str) -> str:
    """Normalize a project-relative path for prefix matching.

    Converts Windows separators to ``/``, collapses duplicate slashes, strips
    leading slashes, and loop-strips leading ``./`` segments so
    ``generated\\foo``, ``/generated/foo``, ``generated//foo``, and
    ``./generated/foo`` all match the same prefix as ``generated/foo``. Case
    is preserved by design: paths are compared exactly as given. Shared by
    :meth:`FileIOService._assert_writable` and :func:`is_generated_or_internal`.
    """
    normalized = relative_path.replace("\\", "/")
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    normalized = normalized.lstrip("/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def is_generated_or_internal(relative_path: str) -> bool:
    """Return ``True`` when ``relative_path`` targets a generated/internal tree.

    ``True`` iff the normalized path is under ``generated/``, ``.lhp/``, or
    ``.git/`` — mutations there carry no flowgroup / preset / template meaning
    and therefore must not invalidate the cached application facade.
    """
    return _normalize_relative(relative_path).startswith(
        _GENERATED_OR_INTERNAL_PREFIXES
    )


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
        normalized = _normalize_relative(relative_path)
        if normalized.startswith(WRITE_PROTECTED_PREFIXES):
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
        A directory whose contents cannot be listed additionally carries an
        ``"error"`` marker (see :meth:`_dir_node`). Excluded directories (see
        :data:`_EXCLUDED_DIR_NAMES`) are skipped wholesale. Entries are sorted
        directories-first then case-insensitively by name.
        """
        return self._dir_node(self._project_root, "", self._project_root.name)

    def _dir_node(self, directory: Path, rel: str, name: str) -> dict[str, Any]:
        """Build one directory node (recursively) for ``directory``.

        On an ``OSError`` from ``iterdir()`` the node is returned with empty
        ``children`` and a generic ``"error"`` marker instead of silently
        dropping the subtree, so the UI can surface an unreadable directory.
        """
        node: dict[str, Any] = {"name": name, "path": rel, "type": "directory"}
        try:
            entries = list(directory.iterdir())
        except OSError:
            logger.warning("Could not list directory: %s", directory)
            node["children"] = []
            node["error"] = "unreadable"
            return node

        # Directories first, then files; each group sorted case-insensitively.
        entries.sort(key=lambda p: (p.is_file(), p.name.lower()))

        children: list[dict[str, Any]] = []
        for entry in entries:
            if entry.is_dir() and entry.name in _EXCLUDED_DIR_NAMES:
                continue
            if not entry.is_dir() and entry.name.startswith(
                _EXCLUDED_FILE_NAME_PREFIXES
            ):
                continue
            child_rel = f"{rel}/{entry.name}" if rel else entry.name
            if entry.is_dir():
                children.append(self._dir_node(entry, child_rel, entry.name))
            else:
                children.append(
                    {
                        "name": entry.name,
                        "path": child_rel,
                        "type": "file",
                    }
                )
        node["children"] = children
        return node

    def read_file(self, relative_path: str) -> str:
        """Read a text file. Traversal-guarded; reads are otherwise unrestricted.

        Decodes :meth:`read_file_bytes` as UTF-8 with NO newline translation
        (CRLF content is returned verbatim), so the text always corresponds
        byte-for-byte to what an ETag of the raw bytes describes.

        Raises:
            PathTraversalError: target resolves outside the project root.
            FileNotFoundError: target does not exist.
            IsADirectoryError: target is a directory.
            UnicodeDecodeError: target is not valid UTF-8.
        """
        return self.read_file_bytes(relative_path).decode("utf-8")

    def read_file_bytes(self, relative_path: str) -> bytes:
        """Read a file's raw bytes. Same error contract as :meth:`read_file`.

        The single source of truth for ETag computation: hashing these bytes
        matches hashing the on-disk content exactly (no universal-newline
        translation, no re-encoding).

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
        return target.read_bytes()

    def read_bytes_if_exists(self, relative_path: str) -> Optional[bytes]:
        """Return the target's raw bytes, or ``None`` when absent / a directory.

        Traversal-guarded like every operation. Unlike :meth:`read_file`, a
        missing target is not an error: it yields ``None`` so optimistic-
        concurrency (``If-Match``) checks can treat "no file yet" as a valid
        create state rather than a 404.

        Raises:
            PathTraversalError: target resolves outside the project root.
        """
        target = self._resolve_in_root(relative_path)
        if not target.exists() or target.is_dir():
            return None
        return target.read_bytes()

    def write_file(self, relative_path: str, content: str) -> WriteResult:
        """Write ``content`` to a file (PUT semantics: creates dirs / new files).

        Traversal-guarded and write-protection-guarded. The bytes written are
        EXACTLY ``content.encode("utf-8")`` — ``write_bytes``, not
        ``write_text``, so no platform newline translation ever occurs (on
        Windows, ``write_text`` would rewrite ``\\n`` as ``\\r\\n`` and
        immediately invalidate any ETag computed from ``content``). After a
        successful write of a ``*.yaml`` / ``*.yml`` file, the content is
        re-parsed with ``yaml.safe_load``; a positioned ``yaml.YAMLError`` is
        captured into the returned :class:`WriteResult` (1-based line/column
        for Monaco). The write is NOT rolled back on a YAML error — the user
        may be saving broken YAML deliberately mid-edit.

        Raises:
            PathTraversalError: target resolves outside the project root.
            WriteProtectedError: target is under a write-protected prefix.
        """
        self._assert_writable(relative_path)
        target = self._resolve_in_root(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content.encode("utf-8"))

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
