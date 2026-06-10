"""Preflight validation for bundle resource generation.

Validates that catalog and schema are defined for every pipeline that will
be generated, BEFORE any side effects (directory wipes, code generation).
Failures are aggregated and grouped by category for actionable error
messages.

Pure-function design: this module owns no state. It constructs short-lived
``PipelineConfigLoader`` and ``EnhancedSubstitutionManager`` instances,
matching the construction pattern in ``core/factories.py`` and
``core/services/``.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from ..core.loaders.pipeline_config_loader import PipelineConfigLoader
from ..core.processing.substitution import EnhancedSubstitutionManager
from ..errors import ErrorFactory, LHPConfigError, codes

logger = logging.getLogger(__name__)

_CATALOG_SCHEMA_DOC_LINK = (
    "https://lakehouse-plumber.readthedocs.io/en/latest/configure_catalog_schema.html"
)


@dataclass(frozen=True)
class _PipelineDiagnostic:
    pipeline_name: str
    catalog: str | None
    schema: str | None


@dataclass
class _PreflightFailures:
    both_missing: list[str] = field(default_factory=list)
    incomplete: list[_PipelineDiagnostic] = field(default_factory=list)
    empty_after_substitution: list[_PipelineDiagnostic] = field(default_factory=list)

    def has_failures(self) -> bool:
        return bool(
            self.both_missing or self.incomplete or self.empty_after_substitution
        )

    def total(self) -> int:
        return (
            len(self.both_missing)
            + len(self.incomplete)
            + len(self.empty_after_substitution)
        )

    def as_context_dict(self) -> dict[str, Any]:
        """Structured form for LHPConfigError.context — tests assert on this."""
        return {
            "both_missing": list(self.both_missing),
            "incomplete": [
                {
                    "pipeline_name": d.pipeline_name,
                    "catalog": d.catalog,
                    "schema": d.schema,
                }
                for d in self.incomplete
            ],
            "empty_after_substitution": [
                {
                    "pipeline_name": d.pipeline_name,
                    "catalog": d.catalog,
                    "schema": d.schema,
                }
                for d in self.empty_after_substitution
            ],
        }


def validate_catalog_schema(
    *,
    project_root: Path,
    pipeline_config_path: str,
    pipeline_names: Iterable[str],
    env: str,
    monitoring_pipeline_name: str | None = None,
) -> None:
    """Validate catalog/schema for every pipeline (incl. monitoring).

    Walks each pipeline, merges its config via ``PipelineConfigLoader``,
    applies token substitution, and bins failures by category. Raises a
    single aggregated ``LHPConfigError`` (``LHP-CFG-026``) when any pipeline
    fails, with structured ``context["failures"]`` for programmatic access.
    """
    loader = PipelineConfigLoader(
        project_root=project_root,
        config_file_path=pipeline_config_path,
        monitoring_pipeline_name=monitoring_pipeline_name,
    )
    sub_mgr = _build_substitution_manager(project_root, env)

    # Include the monitoring synthetic pipeline if enabled — it gets its own
    # bundle YAML but is added to the discovery set only AFTER batch
    # generation, so preflight must add it explicitly.
    names_to_check = list(pipeline_names)
    if monitoring_pipeline_name and monitoring_pipeline_name not in names_to_check:
        names_to_check.append(monitoring_pipeline_name)

    failures = _PreflightFailures()
    for name in names_to_check:
        merged = loader.get_pipeline_config(name)
        if sub_mgr is not None:
            merged = sub_mgr.substitute_yaml(merged)
        _categorize(name, merged, failures)

    if failures.has_failures():
        raise _build_aggregated_error(failures, env)


def _build_substitution_manager(
    project_root: Path, env: str
) -> EnhancedSubstitutionManager | None:
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if not substitution_file.exists():
        return None
    return EnhancedSubstitutionManager(substitution_file, env)


def _categorize(name: str, merged: dict, failures: _PreflightFailures) -> None:
    catalog = merged.get("catalog")
    schema = merged.get("schema")

    if not catalog and not schema:
        failures.both_missing.append(name)
    elif not (catalog and schema):
        failures.incomplete.append(_PipelineDiagnostic(name, catalog, schema))
    elif not str(catalog).strip() or not str(schema).strip():
        failures.empty_after_substitution.append(
            _PipelineDiagnostic(name, catalog, schema)
        )


def _format_both_missing(items: list[str]) -> list[str]:
    return [f"  - {n}" for n in items]


def _format_incomplete(items: list[_PipelineDiagnostic]) -> list[str]:
    return [
        f"  - {d.pipeline_name}"
        f" (catalog={'defined' if d.catalog else 'missing'},"
        f" schema={'defined' if d.schema else 'missing'})"
        for d in items
    ]


def _format_empty_after_substitution(items: list[_PipelineDiagnostic]) -> list[str]:
    return [
        f"  - {d.pipeline_name} (catalog={d.catalog!r}, schema={d.schema!r})"
        for d in items
    ]


_SECTION_SPECS = (
    ("both_missing", "Pipelines missing BOTH catalog and schema", _format_both_missing),
    ("incomplete", "Pipelines with incomplete pairing", _format_incomplete),
    (
        "empty_after_substitution",
        "Pipelines with empty catalog/schema AFTER substitution",
        _format_empty_after_substitution,
    ),
)


def _build_aggregated_error(failures: _PreflightFailures, env: str) -> LHPConfigError:
    sections: list[str] = []
    for attr, header, formatter in _SECTION_SPECS:
        items = getattr(failures, attr)
        if not items:
            continue
        lines = "\n".join(formatter(items))
        sections.append(f"{header} ({len(items)}):\n{lines}")

    return ErrorFactory.config_error(
        codes.CFG_026,
        title=f"Catalog/schema validation failed for {failures.total()} pipeline(s)",
        details="\n\n".join(sections),
        suggestions=[
            (
                "Define `catalog` and `schema` under `project_defaults` in "
                "pipeline_config.yaml (applies to all pipelines)"
            ),
            "Or define them per-pipeline (overrides project_defaults)",
            (
                "For empty-after-substitution failures, check substitution "
                "tokens in substitutions/<env>.yaml"
            ),
        ],
        context={
            "env": env,
            "total_failures": failures.total(),
            "failures": failures.as_context_dict(),
        },
        doc_link=_CATALOG_SCHEMA_DOC_LINK,
    )
