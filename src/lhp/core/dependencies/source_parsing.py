"""Source extraction from action bodies (SQL / Python, inline and file).

Collaborator that turns a single ``Action`` into the list of upstream table
references it reads from plus the extraction advisories the parsers emitted
(:class:`ActionSources`). It locates every SQL/Python body (inline strings and
on-disk files referenced by ``sql_path`` / ``module_path`` / ``batch_handler``
/ ``snapshot_cdc_config``), resolves relative file paths, parses each via the
SQL / Python table extractors — seeding the Python extractor with the
statically-known YAML parameter bindings codegen would apply — and stamps
every parser warning with its flowgroup/action/file context.

File-path resolution takes the populated ``file_paths`` index (flowgroup name ->
YAML path) plus the project root as constructor inputs.
"""

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from pathlib import Path, PurePath
from typing import Any, Optional

from lhp.models import Action

from ...errors import ErrorCategory, LHPError
from ...models.dependencies import DependencyWarning
from ._binding_rules import (
    python_load_bindings,
    snapshot_cdc_bindings,
    transform_bindings,
)
from ._bindings import ParameterBindings
from ._canonical import canonicalize_table_ref
from .source_extractor import extract_action_sources
from .sql_extraction import extract_tables_from_sql

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActionSources:
    """Upstream sources + stamped extraction advisories for one action.

    ``sources`` keeps the exact element type the builder consumed before
    this dataclass existed (plain table/view reference strings).
    ``warnings`` carries the LHP-DEP-* advisories emitted while parsing the
    action's bodies, already stamped with flowgroup/action/file context.
    """

    sources: list[str]
    warnings: list[DependencyWarning]


class SourceParser:
    """Extract upstream table references from an action's SQL/Python bodies.

    Holds the file-path index and project root needed to resolve relative
    ``sql_path`` / ``module_path`` references. It produces
    :class:`ActionSources` (source name lists + stamped extraction
    warnings); matching sources to producers is the builder's job.
    """

    def __init__(self, file_paths: dict[str, Path], project_root: Path) -> None:
        """Wire the parser with the resolution context.

        Args:
            file_paths: Mapping of flowgroup name -> the YAML file it was
                discovered in. Used to resolve relative file references against
                the YAML's directory before falling back to the project root.
                Must be the *populated* index; an empty mapping silently yields
                project-root-only resolution.
            project_root: Root directory of the project, used as the fallback
                resolution base.
        """
        self._file_paths = file_paths
        self.project_root = project_root
        self.logger = logger

    def extract_action_sources(
        self, action: Action, flowgroup_name: str
    ) -> ActionSources:
        """Precedence: SQL parsing (reliable, wins alone) > Python parsing (union with explicit source:) > explicit source declaration (fallback).

        ``action.depends_on`` is ADDITIVE: every declared entry is canonicalized
        and unioned on top of the parsed/declared sources above, so it always
        contributes edges (the builder's edge-matcher forms an INTERNAL edge when
        a producer for that table exists). The escape hatch lets an action whose
        upstream cannot be parsed from its SQL/Python body still declare the
        dependency explicitly.

        Returns an :class:`ActionSources` pairing the source list with the
        stamped extraction warnings collected from every parsed body.
        """
        parsed = self._extract_parsed_sources(action, flowgroup_name)
        sources = self._union_depends_on(parsed.sources, action, flowgroup_name)
        return ActionSources(sources=sources, warnings=parsed.warnings)

    def _extract_parsed_sources(
        self, action: Action, flowgroup_name: str
    ) -> ActionSources:
        """Resolve the parsed/declared sources via the SQL > Python > explicit precedence.

        Warnings accumulate from every parse that actually ran: the SQL
        branch returning early carries only SQL warnings (Python parsing
        never ran); the Python and explicit-fallback branches carry both.
        """
        sql_sources, sql_warnings = self._extract_sql_sources(action, flowgroup_name)
        if sql_sources:
            self.logger.debug(
                f"Using {len(sql_sources)} SQL sources for {flowgroup_name}.{action.name}: {sql_sources}"
            )
            return ActionSources(sources=sql_sources, warnings=sql_warnings)

        python_sources, python_warnings = self._extract_python_sources(
            action, flowgroup_name
        )
        warnings = sql_warnings + python_warnings
        if python_sources:
            explicit = extract_action_sources(action)
            merged = sorted(set(python_sources) | set(explicit))
            self.logger.debug(
                f"Using {len(merged)} Python sources for {flowgroup_name}.{action.name} "
                f"(parser: {python_sources}, explicit: {explicit})"
            )
            return ActionSources(sources=merged, warnings=warnings)

        sources = extract_action_sources(action)

        if sources:
            self.logger.debug(
                f"Using {len(sources)} explicit sources for {flowgroup_name}.{action.name}: {sources}"
            )
        else:
            self.logger.debug(f"No sources found for {flowgroup_name}.{action.name}")

        return ActionSources(sources=sources, warnings=warnings)

    def _union_depends_on(
        self, parsed: list[str], action: Action, flowgroup_name: str
    ) -> list[str]:
        """Union canonicalized ``action.depends_on`` entries onto ``parsed``.

        ``depends_on`` is additive and deduped against the parsed sources. A
        ``None`` (unset) or empty list leaves ``parsed`` unchanged.
        """
        declared = getattr(action, "depends_on", None)
        if not declared:
            return parsed

        canonical_declared = [canonicalize_table_ref(ref) for ref in declared]
        merged = sorted(set(parsed) | set(canonical_declared))
        self.logger.debug(
            f"Unioned {len(canonical_declared)} depends_on sources for "
            f"{flowgroup_name}.{action.name}: {canonical_declared} "
            f"(total {len(merged)})"
        )
        return merged

    def _write_target_as_dict(self, action: Action) -> Optional[dict[str, Any]]:
        """Normalize ``action.write_target`` to a ``dict`` for uniform lookup.

        Returns ``None`` when the action has no write target. Handles both the
        Pydantic ``WriteTarget`` form and a raw dict (some code paths pre-dump
        it before reaching the analyzer).
        """
        wt = getattr(action, "write_target", None)
        if wt is None:
            return None
        if isinstance(wt, dict):
            return wt
        return wt.model_dump()

    def _iter_sql_bodies(
        self, action: Action
    ) -> Iterator[tuple[Optional[str], Optional[str]]]:
        """Yield ``(inline_sql, sql_path)`` for every known SQL location.

        Covers:
          - ``action.sql`` / ``action.sql_path``
          - ``action.source["sql"]`` / ``action.source["sql_path"]`` (when
            ``source["type"] == "sql"``)
          - ``write_target["sql"]`` / ``write_target["sql_path"]``
            (materialized-view SQL)

        Either element of a yielded tuple may be ``None``; callers should
        treat each independently.
        """
        yield (
            getattr(action, "sql", None),
            getattr(action, "sql_path", None),
        )

        source = getattr(action, "source", None)
        if isinstance(source, dict) and source.get("type") == "sql":
            yield source.get("sql"), source.get("sql_path")

        wt = self._write_target_as_dict(action)
        if wt is not None:
            yield wt.get("sql"), wt.get("sql_path")

    def _iter_python_bodies(
        self, action: Action
    ) -> Iterator[tuple[Optional[str], Optional[str], Optional[ParameterBindings]]]:
        """Yield ``(inline_python, file_path, bindings)`` for every Python location.

        Covers top-level ``action.module_path`` (Python transforms),
        python-load ``source["module_path"]``, ``write_target["module_path"]``
        (custom sinks), ``write_target["batch_handler"]`` (inline ForEachBatch
        code), and ``write_target["snapshot_cdc_config"]["source_function"]["file"]``.

        ``bindings`` carries the statically-known YAML parameter values for
        the body's entry function, mirroring exactly how codegen applies them
        (see the per-shape ``_*_bindings`` builders). Custom-sink and
        batch-handler bodies have NO parameters mechanism in codegen (the
        sink is class-based, the batch handler is an inlined function body —
        see ``generators/write/sinks/custom_sink.py`` /
        ``foreachbatch_sink.py``), so their bindings are always ``None``.
        """
        module_path = getattr(action, "module_path", None)
        if module_path:
            yield None, module_path, transform_bindings(action)

        source = getattr(action, "source", None)
        if isinstance(source, dict) and source.get("type") == "python":
            # Python-load actions reference their module via
            # ``source["module_path"]`` (generators/load/python.py). The file
            # resolves through the same YAML-dir-then-project-root logic as
            # every other shape (``_resolve_and_parse_file``).
            source_module_path = source.get("module_path")
            if source_module_path:
                yield None, source_module_path, python_load_bindings(source)

        wt = self._write_target_as_dict(action)
        if wt is None:
            return

        wt_module_path = wt.get("module_path")
        if wt_module_path:
            yield None, wt_module_path, None

        batch_handler = wt.get("batch_handler")
        if batch_handler:
            yield batch_handler, None, None

        cdc = wt.get("snapshot_cdc_config") or {}
        source_function = cdc.get("source_function") if isinstance(cdc, dict) else None
        if isinstance(source_function, dict):
            fn_file = source_function.get("file")
            if fn_file:
                yield None, fn_file, snapshot_cdc_bindings(source_function)

    def _stamp_warnings(
        self,
        warnings: list[DependencyWarning],
        action: Action,
        flowgroup_name: str,
        file_path: Optional[str],
    ) -> list[DependencyWarning]:
        """Stamp parser-emitted advisories with their originating context.

        Parsers emit warnings with blank ``flowgroup``/``action`` and no
        ``file_path``; this fills all three (``file_path`` is the resolved
        ``.py`` file for file bodies, the flowgroup YAML for inline bodies),
        normalized to POSIX separators so JSON output is platform-stable.
        Each warning's own ``line`` is preserved.
        """
        posix_path = PurePath(file_path).as_posix() if file_path else file_path
        return [
            replace(
                w, flowgroup=flowgroup_name, action=action.name, file_path=posix_path
            )
            for w in warnings
        ]

    def _resolve_and_parse_file(
        self,
        file_path_str: str,
        flowgroup_name: str,
        action: Action,
        parser_fn: Callable[[str], tuple[list[str], list[DependencyWarning]]],
        file_type_label: str,
        code_number: str,
        path_context_key: str,
    ) -> tuple[list[str], list[DependencyWarning]]:
        """Resolve a relative file path, read it, and parse via ``parser_fn``.

        Resolution order: first relative to the flowgroup YAML file (if the
        mapping is available), then relative to the project root. Raises
        ``LHPError(IO, code_number)`` when no candidate resolves to an existing
        file. Parse-time failures log a warning and return ``([], [])`` instead
        of raising (preserving pre-refactor behavior). Returned warnings are
        already stamped with the resolved file path.
        """
        yaml_file_path = self._file_paths.get(flowgroup_name)

        candidate_paths: list[Path] = []
        if yaml_file_path is not None:
            candidate_paths.append(yaml_file_path.parent / file_path_str)
        candidate_paths.append(self.project_root / file_path_str)

        resolved_path = next((p for p in candidate_paths if p.exists()), None)

        if resolved_path is None:
            # Use the last candidate as the "expected" path in the error.
            reported_path = candidate_paths[-1]
            context: dict[str, str] = {
                "Action": action.name,
                "Flowgroup": flowgroup_name,
                path_context_key: file_path_str,
                "Full Path": str(reported_path),
            }
            if yaml_file_path is not None:
                context["YAML File"] = str(yaml_file_path)
            else:
                context["Project Root"] = str(self.project_root)

            raise LHPError(
                category=ErrorCategory.IO,
                code_number=code_number,
                title=f"{file_type_label} file not found for action '{action.name}'",
                details=(
                    f"{file_type_label} file '{file_path_str}' referenced by "
                    f"action '{action.name}' does not exist."
                ),
                suggestions=[
                    f"Check that the {file_type_label} file exists at: {reported_path}",
                    f"Verify the {path_context_key.lower()} is correct "
                    f"relative to the YAML file or project root",
                    "Ensure the file has proper read permissions",
                ],
                context=context,
            )

        try:
            content = resolved_path.read_text(encoding="utf-8")
            parsed, raw_warnings = parser_fn(content)
            stamped = self._stamp_warnings(
                raw_warnings, action, flowgroup_name, str(resolved_path)
            )
            self.logger.debug(
                f"Extracted {len(parsed)} sources from {file_type_label} file "
                f"{resolved_path} for {flowgroup_name}.{action.name}"
            )
            return list(parsed), stamped
        except Exception as e:
            self.logger.warning(
                f"Could not analyze {file_type_label} file {resolved_path} for "
                f"{flowgroup_name}.{action.name}: {e}"
            )
            return [], []

    def _extract_sql_sources(
        self, action: Action, flowgroup_name: str
    ) -> tuple[list[str], list[DependencyWarning]]:
        """Extract table references from every SQL location via ``_iter_sql_bodies``.

        Each body parses through the sqlglot-based
        :func:`~lhp.core.dependencies.sql_extraction.extract_tables_from_sql`:
        ``.tables`` become sources, ``.warnings`` (at most one LHP-DEP-003 per
        unparseable body) are stamped with this action's context — file bodies
        get the resolved ``.sql`` path, inline bodies the flowgroup YAML path.

        Ordered after ``_resolve_and_parse_file`` so its ``ErrorCategory.IO`` precedes
        the ``code_number="002"`` call-arg here (keeps tests/errors/test_codes.py's
        raise-site scanner pairing intact).
        """

        def _parse_sql(sql_text: str) -> tuple[list[str], list[DependencyWarning]]:
            result = extract_tables_from_sql(sql_text)
            return result.tables, result.warnings

        yaml_file = self._file_paths.get(flowgroup_name)
        inline_context_path = str(yaml_file) if yaml_file is not None else None

        sources: list[str] = []
        warnings: list[DependencyWarning] = []
        for inline_sql, sql_path in self._iter_sql_bodies(action):
            if inline_sql:
                try:
                    parsed, raw_warnings = _parse_sql(inline_sql)
                    sources.extend(parsed)
                    warnings.extend(
                        self._stamp_warnings(
                            raw_warnings, action, flowgroup_name, inline_context_path
                        )
                    )
                    self.logger.debug(
                        f"Extracted {len(parsed)} sources from inline SQL in {flowgroup_name}.{action.name}"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Could not parse inline SQL in {flowgroup_name}.{action.name}: {e}"
                    )

            if sql_path:
                file_sources, file_warnings = self._resolve_and_parse_file(
                    sql_path,
                    flowgroup_name,
                    action,
                    _parse_sql,
                    file_type_label="SQL",
                    code_number="002",
                    path_context_key="SQL Path",
                )
                sources.extend(file_sources)
                warnings.extend(file_warnings)

        return sources, warnings

    def _extract_python_sources(
        self, action: Action, flowgroup_name: str
    ) -> tuple[list[str], list[DependencyWarning]]:
        """Extract table references and advisories from every Python location.

        Each body from ``_iter_python_bodies`` parses with its own parameter
        bindings; the full :class:`PythonExtractionResult` is consumed —
        tables become sources, warnings are stamped with this action's
        context (inline bodies get the flowgroup YAML path, file bodies the
        resolved ``.py`` path).
        """
        from .python_parser import extract_tables_from_python

        def _parser_for(
            bindings: Optional[ParameterBindings],
        ) -> Callable[[str], tuple[list[str], list[DependencyWarning]]]:
            def _parse(code: str) -> tuple[list[str], list[DependencyWarning]]:
                result = extract_tables_from_python(code, bindings=bindings)
                return result.tables, result.warnings

            return _parse

        yaml_file = self._file_paths.get(flowgroup_name)
        inline_context_path = str(yaml_file) if yaml_file is not None else None

        sources: list[str] = []
        warnings: list[DependencyWarning] = []
        for inline_python, file_path, bindings in self._iter_python_bodies(action):
            parse_fn = _parser_for(bindings)

            if inline_python:
                try:
                    parsed, raw_warnings = parse_fn(inline_python)
                    sources.extend(parsed)
                    warnings.extend(
                        self._stamp_warnings(
                            raw_warnings, action, flowgroup_name, inline_context_path
                        )
                    )
                    self.logger.debug(
                        f"Extracted {len(parsed)} sources from inline Python in {flowgroup_name}.{action.name}"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Could not parse inline Python in {flowgroup_name}.{action.name}: {e}"
                    )

            if file_path:
                file_sources, file_warnings = self._resolve_and_parse_file(
                    file_path,
                    flowgroup_name,
                    action,
                    parse_fn,
                    file_type_label="Python",
                    code_number="003",
                    path_context_key="Module Path",
                )
                sources.extend(file_sources)
                warnings.extend(file_warnings)

        return sources, warnings
