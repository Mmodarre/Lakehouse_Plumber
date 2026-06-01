"""Transitive local-dependency resolution for copied user Python modules.

When a user entry module (python load/transform, custom data source/sink,
snapshot-CDC source function) imports a *local* helper, the helper and its
whole transitive local closure must be copied into the generated pipeline
so the import resolves at runtime. This module computes that closure.

Named distinctly from :mod:`lhp.core.codegen.imports.resolver`, which
resolves clashing *import statements* — a different concern. Here
"resolve" means resolving modules on disk and mirroring their package
structure under ``custom_python_functions/``.

The import root ``R`` is the entry file's own directory (interpretation
(a)). An import is *local* iff its top dotted segment resolves to a
module/package directly under ``R``. Local closures are copied with their
sub-package structure preserved; relative imports inside helper packages
are followed relative to the helper's own package, not ``R``.
"""

import ast
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn, Optional

from lhp.core.codegen.imports import local_import_targets, parse_user_module
from lhp.errors import ErrorCategory, LHPValidationError


@dataclass(frozen=True)
class ResolvedModule:
    """One module (or synthesized package marker) in the local closure.

    ``source_path`` is the resolved on-disk location; ``rel_path`` is its
    path relative to the closure root ``R`` (drives the destination under
    ``custom_python_functions/``). ``content_source`` is the file whose
    bytes seed the copied content, or ``None`` for a synthesized
    ``__init__.py`` (``is_synthesized_init`` is then ``True`` and the copier
    writes an empty file).
    """

    source_path: Path
    rel_path: Path
    content_source: Optional[Path]
    is_synthesized_init: bool


def _raise_root_is_package(root: Path) -> NoReturn:
    """Rule A violation: the import root itself is a package (has ``__init__.py``)."""
    raise LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="023",
        title="Python function root directory is a package",
        details=(
            f"The directory containing your Python function, '{root}', "
            f"contains an __init__.py and is therefore an importable "
            f"package. Helper imports are reconciled against this directory "
            f"as the closure root, which requires it NOT to be a package."
        ),
        suggestions=[
            "Remove the __init__.py from the directory holding your entry function",
            "Move helper modules into a sub-directory (a sub-package) and "
            "keep the entry file flat in its own directory",
            "Reference helpers with 'from <subpackage> import ...' rather "
            "than making the entry directory itself a package",
        ],
        context={"Import root": str(root)},
    )


def _raise_missing_helper(module: str, importer: Path) -> NoReturn:
    """VAL-025: a local-classified import has no file/package on disk."""
    raise LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="025",
        title="Local helper module not found",
        details=(
            f"The module '{module}', imported by '{importer}', was classified "
            f"as a local helper but no matching file or package exists on disk "
            f"under the import root."
        ),
        suggestions=[
            f"Create the missing helper module for '{module}'",
            "Check the import path for a typo against the files on disk",
            "If this is an external dependency, ensure it is installed in the "
            "environment rather than imported as a local helper",
        ],
        context={"Missing module": module, "Imported by": str(importer)},
    )


def resolve_module_on_disk(dotted: str, root: Path) -> Optional[Path]:
    """Resolve a dotted module path under ``root`` to a file or package dir.

    Returns the ``.py`` file for a module, the package directory for a
    package, or ``None`` when neither exists. This is the single
    local-resolution predicate shared by the closure (here) and the import
    rewriter (:mod:`lhp.core.codegen.python_import_rewriter`), so both agree
    exactly on what counts as a local module — including PEP 420
    namespace-package members the substrate reports ``is_local=False``.
    """
    parts = dotted.split(".")
    module_file = root.joinpath(*parts).with_suffix(".py")
    if module_file.is_file():
        return module_file
    package_dir = root.joinpath(*parts)
    if (package_dir / "__init__.py").is_file():
        return package_dir
    return None


def _resolve_relative_on_disk(
    level: int, module: Optional[str], importer: Path, root: Path
) -> Optional[Path]:
    """Resolve a relative import (``level > 0``) against the importer's package.

    ``level`` ascends from the importer's directory (``level == 1`` is the
    importer's package); ``module`` names a sub-module/sub-package below it.
    Returns a ``.py`` file, a package dir, or ``None`` when the target
    ascends at/above ``root`` or is absent on disk.
    """
    base = importer.parent
    for _ in range(level - 1):
        base = base.parent
    if base != root:
        try:
            base.relative_to(root)
        except ValueError:
            return None
    if module:
        return resolve_module_on_disk(module, base)
    return base if (base / "__init__.py").is_file() else None


def _iter_package_py_files(package_dir: Path) -> list[Path]:
    """All ``.py`` files anywhere under ``package_dir`` (whole-sub-package copy)."""
    return sorted(p for p in package_dir.rglob("*.py") if p.is_file())


def _synthesized_inits(
    copied_rel_paths: set[Path], on_disk_init_dirs: set[Path]
) -> list[ResolvedModule]:
    """Emit a synthesized ``__init__.py`` for every closure dir lacking one.

    ``copied_rel_paths`` are the rel-paths of real copied files; their
    ancestor directories define the namespace tree that must be importable.
    ``on_disk_init_dirs`` are rel-dirs whose ``__init__.py`` was copied from
    disk and so needs no synthesis.
    """
    needed_dirs: set[Path] = set()
    for rel in copied_rel_paths:
        for parent in rel.parents:
            if parent == Path("."):
                continue
            needed_dirs.add(parent)

    records: list[ResolvedModule] = []
    for rel_dir in sorted(needed_dirs):
        if rel_dir in on_disk_init_dirs:
            continue
        records.append(
            ResolvedModule(
                source_path=rel_dir / "__init__.py",
                rel_path=rel_dir / "__init__.py",
                content_source=None,
                is_synthesized_init=True,
            )
        )
    return records


def resolve_local_closure(
    entry_file: Path, root: Path, *, cache: Optional[dict]
) -> list[ResolvedModule]:
    """Compute the transitive local-helper closure of ``entry_file``.

    BFS from ``entry_file`` over :func:`parse_user_module` +
    :func:`local_import_targets`, threading ``cache`` (the per-pipeline AST
    tree cache). A local-absolute import that roots into a *package* pulls in
    the **whole** sub-package (all ``.py`` under it, per spec §3.8); a flat
    sibling module pulls in just that file. Relative imports inside helper
    packages are followed relative to the helper's own package. Cycles
    terminate via a resolved-path visited set. ``root`` is ``R`` (the entry
    file's directory). Returns the transitive helper modules plus any
    synthesized namespace ``__init__.py`` records, **excluding** the entry.

    ``root`` is normalised with :meth:`Path.resolve` on entry: every discovered
    module is resolved before ``relative_to(root)``, so an unresolved ``root``
    (e.g. a symlinked ``/tmp`` -> ``/private/tmp`` parent on macOS) would
    otherwise raise ``ValueError``. Resolving here makes the function safe to
    call with either a resolved or unresolved ``root``; it is idempotent for
    the already-resolved roots production and the tests pass.

    Raises:
        LHPValidationError: ``LHP-VAL-023`` when ``root`` is itself a package;
            ``LHP-VAL-025`` when a local-classified import is absent on disk.
        LHPError: ``LHP-IO-003`` (via ``parse_user_module``) when any file in
            the closure contains invalid Python syntax.
    """
    # Normalise ``root`` so ``resolved.relative_to(root)`` below never trips a
    # symlink on the path (see the contract note in the docstring). Idempotent
    # for the resolved roots all current callers pass.
    root = root.resolve()

    if (root / "__init__.py").exists():
        _raise_root_is_package(root)

    entry_resolved = entry_file.resolve()
    visited: set[str] = {str(entry_resolved)}
    queue: deque[Path] = deque([entry_resolved])

    copied_files: dict[str, Path] = {}
    on_disk_init_dirs: set[Path] = set()

    def _enqueue(path: Path) -> None:
        resolved = path.resolve()
        key = str(resolved)
        if key in visited:
            return
        visited.add(key)
        queue.append(resolved)
        if resolved != entry_resolved:
            copied_files[key] = resolved

    def _absorb_package(package_dir: Path) -> None:
        for py_file in _iter_package_py_files(package_dir):
            _enqueue(py_file)

    def _absorb_resolved(resolved: Path) -> None:
        """Pull a resolved local target in at whole-sub-package granularity.

        A target inside a top-level package under ``root`` drags the entire
        package along (§3.8); a flat module is enqueued on its own.
        """
        if resolved.is_dir():
            _absorb_package(resolved)
            return
        top = resolved.relative_to(root).parts[0]
        package_dir = root / top
        if (package_dir / "__init__.py").is_file():
            _absorb_package(package_dir)
        else:
            _enqueue(resolved)

    while queue:
        current = queue.popleft()
        tree: ast.Module = parse_user_module(current, cache=cache)
        for target in local_import_targets(tree, root):
            if target.level > 0:
                resolved = _resolve_relative_on_disk(
                    target.level, target.module, current, root
                )
                if resolved is not None:
                    _absorb_resolved(resolved)
                continue

            if not target.module:
                continue

            resolved = resolve_module_on_disk(target.module, root)
            if resolved is None:
                # The substrate gates ``is_local`` on an ``__init__.py``/``.py``
                # existing for the *top* segment; a PEP 420 namespace-package
                # member (dir without ``__init__.py``) is therefore reported
                # external. Resolve on disk regardless and only error when the
                # substrate was confident the import was local (VAL-025) — a
                # genuinely external import that does not resolve is just
                # skipped.
                if target.is_local:
                    _raise_missing_helper(target.module, current)
                continue
            _absorb_resolved(resolved)

    records: list[ResolvedModule] = []
    copied_rel_paths: set[Path] = set()
    for resolved in copied_files.values():
        rel = resolved.relative_to(root)
        copied_rel_paths.add(rel)
        if resolved.name == "__init__.py":
            on_disk_init_dirs.add(rel.parent)
        records.append(
            ResolvedModule(
                source_path=resolved,
                rel_path=rel,
                content_source=resolved,
                is_synthesized_init=False,
            )
        )

    records.extend(_synthesized_inits(copied_rel_paths, on_disk_init_dirs))
    records.sort(key=lambda r: str(r.rel_path))
    return records
