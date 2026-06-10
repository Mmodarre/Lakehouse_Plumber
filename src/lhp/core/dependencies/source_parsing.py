"""Source extraction from action bodies (SQL / Python, inline and file).

Collaborator that turns a single ``Action`` into the list of upstream table
references it reads from. It locates every SQL/Python body (inline strings and
on-disk files referenced by ``sql_path`` / ``module_path`` / ``batch_handler``
/ ``snapshot_cdc_config``), resolves relative file paths, and parses each via
the SQL / Python table extractors.

File-path resolution takes the populated ``file_paths`` index (flowgroup name ->
YAML path) plus the project root as constructor inputs.
"""

import logging
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Optional

from lhp.models import Action

from ...errors import ErrorCategory, LHPError
from ._canonical import canonicalize_table_ref
from .source_extractor import extract_action_sources

logger = logging.getLogger(__name__)


class SourceParser:
    """Extract upstream table references from an action's SQL/Python bodies.

    Holds the file-path index and project root needed to resolve relative
    ``sql_path`` / ``module_path`` references. It produces source name lists;
    matching them to producers is the builder's job.
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

    def extract_action_sources(self, action: Action, flowgroup_name: str) -> list[str]:
        """Precedence: SQL parsing (reliable, wins alone) > Python parsing (union with explicit source:) > explicit source declaration (fallback).

        ``action.depends_on`` is ADDITIVE: every declared entry is canonicalized
        and unioned on top of the parsed/declared sources above, so it always
        contributes edges (the builder's edge-matcher forms an INTERNAL edge when
        a producer for that table exists). The escape hatch lets an action whose
        upstream cannot be parsed from its SQL/Python body still declare the
        dependency explicitly.
        """
        parsed = self._extract_parsed_sources(action, flowgroup_name)
        return self._union_depends_on(parsed, action, flowgroup_name)

    def _extract_parsed_sources(self, action: Action, flowgroup_name: str) -> list[str]:
        """Resolve the parsed/declared sources via the SQL > Python > explicit precedence."""
        sql_sources = self._extract_sql_sources(action, flowgroup_name)
        if sql_sources:
            self.logger.debug(
                f"Using {len(sql_sources)} SQL sources for {flowgroup_name}.{action.name}: {sql_sources}"
            )
            return sql_sources

        python_sources = self._extract_python_sources(action, flowgroup_name)
        if python_sources:
            explicit = extract_action_sources(action)
            merged = sorted(set(python_sources) | set(explicit))
            self.logger.debug(
                f"Using {len(merged)} Python sources for {flowgroup_name}.{action.name} "
                f"(parser: {python_sources}, explicit: {explicit})"
            )
            return merged

        sources = extract_action_sources(action)

        if sources:
            self.logger.debug(
                f"Using {len(sources)} explicit sources for {flowgroup_name}.{action.name}: {sources}"
            )
        else:
            self.logger.debug(f"No sources found for {flowgroup_name}.{action.name}")

        return sources

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
    ) -> Iterator[tuple[Optional[str], Optional[str]]]:
        """Yield ``(inline_python, file_path)`` for every known Python location.

        Covers top-level ``action.module_path`` (Python transforms / custom
        sources), ``write_target["module_path"]`` (custom sinks),
        ``write_target["batch_handler"]`` (inline ForEachBatch code), and
        ``write_target["snapshot_cdc_config"]["source_function"]["file"]``.
        """
        module_path = getattr(action, "module_path", None)
        if module_path:
            yield None, module_path

        wt = self._write_target_as_dict(action)
        if wt is None:
            return

        wt_module_path = wt.get("module_path")
        if wt_module_path:
            yield None, wt_module_path

        batch_handler = wt.get("batch_handler")
        if batch_handler:
            yield batch_handler, None

        cdc = wt.get("snapshot_cdc_config") or {}
        source_function = cdc.get("source_function") if isinstance(cdc, dict) else None
        if isinstance(source_function, dict):
            fn_file = source_function.get("file")
            if fn_file:
                yield None, fn_file

    def _resolve_and_parse_file(
        self,
        file_path_str: str,
        flowgroup_name: str,
        action: Action,
        parser_fn: Callable[[str], list[str]],
        file_type_label: str,
        code_number: str,
        path_context_key: str,
    ) -> list[str]:
        """Resolve a relative file path, read it, and parse via ``parser_fn``.

        Resolution order: first relative to the flowgroup YAML file (if the
        mapping is available), then relative to the project root. Raises
        ``LHPError(IO, code_number)`` when no candidate resolves to an existing
        file. Parse-time failures log a warning and return ``[]`` instead of
        raising (preserving pre-refactor behavior).
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
            parsed = list(parser_fn(content))
            self.logger.debug(
                f"Extracted {len(parsed)} sources from {file_type_label} file "
                f"{resolved_path} for {flowgroup_name}.{action.name}"
            )
            return parsed
        except Exception as e:
            self.logger.warning(
                f"Could not analyze {file_type_label} file {resolved_path} for "
                f"{flowgroup_name}.{action.name}: {e}"
            )
            return []

    def _extract_sql_sources(self, action: Action, flowgroup_name: str) -> list[str]:
        """Extract table references from every SQL location via ``_iter_sql_bodies``.

        Ordered after ``_resolve_and_parse_file`` so its ``ErrorCategory.IO`` precedes
        the ``code_number="002"`` call-arg here (keeps tests/errors/test_codes.py's
        raise-site scanner pairing intact).
        """
        from ...utils.sql_parser import extract_tables_from_sql

        sources: list[str] = []
        for inline_sql, sql_path in self._iter_sql_bodies(action):
            if inline_sql:
                try:
                    parsed = extract_tables_from_sql(inline_sql)
                    sources.extend(parsed)
                    self.logger.debug(
                        f"Extracted {len(parsed)} sources from inline SQL in {flowgroup_name}.{action.name}"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Could not parse inline SQL in {flowgroup_name}.{action.name}: {e}"
                    )

            if sql_path:
                sources.extend(
                    self._resolve_and_parse_file(
                        sql_path,
                        flowgroup_name,
                        action,
                        extract_tables_from_sql,
                        file_type_label="SQL",
                        code_number="002",
                        path_context_key="SQL Path",
                    )
                )

        return sources

    def _extract_python_sources(self, action: Action, flowgroup_name: str) -> list[str]:
        """Extract table references from every Python location via ``_iter_python_bodies``."""
        from .python_parser import extract_tables_from_python

        sources: list[str] = []
        for inline_python, file_path in self._iter_python_bodies(action):
            if inline_python:
                try:
                    parsed = extract_tables_from_python(inline_python)
                    sources.extend(parsed)
                    self.logger.debug(
                        f"Extracted {len(parsed)} sources from inline Python in {flowgroup_name}.{action.name}"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Could not parse inline Python in {flowgroup_name}.{action.name}: {e}"
                    )

            if file_path:
                sources.extend(
                    self._resolve_and_parse_file(
                        file_path,
                        flowgroup_name,
                        action,
                        extract_tables_from_python,
                        file_type_label="Python",
                        code_number="003",
                        path_context_key="Module Path",
                    )
                )

        return sources
