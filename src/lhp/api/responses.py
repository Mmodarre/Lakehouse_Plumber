"""Frozen response DTOs — return shapes for public facade operations.

Each runtime operation on :class:`LakehousePlumberApplicationFacade`
returns one of the response types declared here. Responses are
immutable (``@dataclass(frozen=True)``) and carry only flat,
JSON-serialisable fields (str / int / bool / Optional / Tuple /
Mapping[str, JSONValue]) per constitution §4.4 + §4.8 — no live
exception instances, no mutable collections.

:stability: provisional
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Mapping, Optional, Sequence, Tuple, Union

if TYPE_CHECKING:
    # Forward references for the view DTOs used in nested response
    # types. Runtime-deferred to avoid a cycle (``views`` imports
    # ``JSONValue`` from this module) while still letting
    # ``mypy --strict`` resolve the string annotations.
    from lhp.api.views import (  # noqa: F401
        PipelineStats,
        ValidationIssueView,
    )

JSONValue = Union[
    None,
    bool,
    int,
    float,
    str,
    Sequence["JSONValue"],
    Mapping[str, "JSONValue"],
]
"""Recursive JSON-shape type alias.

Use exclusively as the value type in ``Mapping[str, JSONValue]`` for
the ``context`` field of view / response DTOs. All other DTO fields
must use precise types (``str``, ``int``, ``bool``, ``Optional``,
``Tuple``, etc.) — never ``Any``.

:stability: provisional
"""


@dataclass(frozen=True)
class GenerationResponse:
    """Per-pipeline generation outcome.

    Returned for each pipeline in a batch run. Failure detail is split
    across three optional fields:

    - ``error_code``: the LHP error code (e.g. ``"LHP-VAL-021"``), or
      ``None`` for non-LHP / unstructured failures.
    - ``error_message``: human-readable summary.
    - ``error``: a frozen :class:`ValidationIssueView` carrying the
      full structured payload (suggestions, context, doc_link) when
      the failure originated from an :class:`LHPError`. CLI panels
      render from this field alone — no exception instance needed.

    :stability: provisional
    """

    success: bool
    generated_filenames: Tuple[str, ...]
    files_written: int
    total_flowgroups: int
    output_location: Optional[Path]
    performance_info: Mapping[str, JSONValue]
    duration_s: float = 0.0
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    error: Optional["ValidationIssueView"] = None

    def is_successful(self) -> bool:
        return self.success


@dataclass(frozen=True)
class BatchGenerationResponse:
    """Aggregate response for a multi-pipeline generation run.

    The flat-pool architecture raises aggregate errors only at the end,
    so some pipelines may have completed successfully — their state is
    already persisted via per-pipeline atomic save. ``error_code``
    carries the LHP code of the aggregate :class:`LHPError` (if any);
    individual per-pipeline failures live in their
    :class:`GenerationResponse` inside ``pipeline_responses``.

    :stability: provisional
    """

    success: bool
    pipeline_responses: Mapping[str, "GenerationResponse"]
    total_files_written: int
    aggregate_generated_filenames: Tuple[str, ...]
    output_location: Optional[Path]
    error_message: Optional[str] = None
    error_code: Optional[str] = None

    def is_successful(self) -> bool:
        return self.success


@dataclass(frozen=True)
class ValidationResponse:
    """Per-pipeline validation outcome.

    Issues are a flat tuple of :class:`ValidationIssueView` records
    carrying severity, code, title, details, and pipeline / flowgroup
    location. Convenience properties expose per-severity counts;
    ``has_errors`` / ``has_warnings`` remain available for predicate
    use.

    :stability: provisional
    """

    success: bool
    issues: Tuple["ValidationIssueView", ...]
    validated_pipelines: Tuple[str, ...]
    error_message: Optional[str] = None

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")

    def has_errors(self) -> bool:
        return self.error_count > 0

    def has_warnings(self) -> bool:
        return self.warning_count > 0


@dataclass(frozen=True)
class BatchValidationResponse:
    """Aggregate response for a multi-pipeline validation run.

    Mirrors :class:`BatchGenerationResponse`. The aggregate exception
    instance previously carried as ``original_error`` is replaced by
    the flat string field ``error_code`` (the LHP code of the
    underlying :class:`LHPError`, or ``None`` for non-LHP exceptions),
    per constitution §4.8 ban on exception-typed DTO fields.

    :stability: provisional
    """

    success: bool
    pipeline_responses: Mapping[str, "ValidationResponse"]
    total_errors: int
    total_warnings: int
    validated_pipelines: Tuple[str, ...]
    error_message: Optional[str] = None
    error_code: Optional[str] = None

    def is_successful(self) -> bool:
        return self.success


@dataclass(frozen=True)
class InitProjectResult:
    """Outcome of a project scaffolding run.

    Returned by :class:`LakehousePlumberBootstrap.init_project`. A
    failure surfaces ``success=False`` plus ``error_message`` /
    ``error_code`` rather than raising — callers can branch on the
    result and render it themselves. ``created_files`` and
    ``created_dirs`` are tuples of absolute paths to filesystem entries
    that did not exist before the call and that this run produced.

    :stability: provisional
    """

    success: bool
    target_dir: Path
    created_files: Tuple[Path, ...]
    created_dirs: Tuple[Path, ...]
    bundle_enabled: bool
    error_message: Optional[str] = None
    error_code: Optional[str] = None

    def is_successful(self) -> bool:
        return self.success


@dataclass(frozen=True)
class StatsResult:
    """Aggregate pipeline / flowgroup / action statistics.

    Returned by :meth:`InspectionFacade.compute_stats`. Aggregates a
    full project walk (``pipeline_count``, ``flowgroup_count``) plus
    a per-action-type breakdown and a sequence of per-pipeline rows.
    All fields are immutable, JSON-shape-compatible (§4.8).

    :stability: provisional
    """

    pipeline_count: int
    flowgroup_count: int
    total_actions: int
    action_counts_by_type: Mapping[str, int] = field(default_factory=dict)
    pipeline_breakdown: Tuple["PipelineStats", ...] = ()
    templates_used: Tuple[str, ...] = ()
    presets_used: Tuple[str, ...] = ()


@dataclass(frozen=True)
class DependencyAnalysisResult:
    """Outcome of pipeline-level dependency analysis.

    Distinct from the internal
    :class:`lhp.models.dependencies.DependencyAnalysisResult`: this
    public view flattens the networkx-backed graph state into plain
    tuples / mappings so the API surface remains free of internal
    types. Public consumers receive the cycle / order / external-source
    summary but not the live graph objects.

    :stability: provisional
    """

    pipeline_dependencies: Mapping[str, Tuple[str, ...]] = field(default_factory=dict)
    execution_stages: Tuple[Tuple[str, ...], ...] = ()
    circular_dependencies: Tuple[Tuple[str, ...], ...] = ()
    external_sources: Tuple[str, ...] = ()
    total_pipelines: int = 0
    total_external_sources: int = 0

    @property
    def has_cycles(self) -> bool:
        """Predicate: any circular dependency was detected."""
        return len(self.circular_dependencies) > 0


@dataclass(frozen=True)
class DependencyOutputEntry:
    """A single dependency-analysis output file.

    Returned inside :class:`DependencyOutputsResult.entries`. ``label``
    is empty for single-file outputs; for the multi-job ``job`` format
    it carries the job name or ``"_master"`` for the master
    orchestration file.

    :stability: provisional
    """

    format_name: str
    label: str
    path: Path


@dataclass(frozen=True)
class DependencyOutputsResult:
    """Outcome of writing dependency-analysis outputs to disk.

    Returned by :meth:`InspectionFacade.save_dependency_outputs`.
    ``entries`` enumerates every generated file with its format name
    and optional job-name label, preserving the multi-job ``job`` format
    structure (per-job + ``_master``). Failures surface as
    ``success=False`` plus flat ``error_message`` / ``error_code`` per
    §4.8.

    :stability: provisional
    """

    success: bool
    entries: Tuple["DependencyOutputEntry", ...]
    output_dir: Optional[Path] = None
    error_message: Optional[str] = None
    error_code: Optional[str] = None


@dataclass(frozen=True)
class FinalizeMonitoringResult:
    """Outcome of post-generation monitoring-artifact finalization.

    Returned by :meth:`GenerationFacade.finalize_monitoring_artifacts`.
    Failure information is carried via flat ``error_message`` /
    ``error_code`` fields rather than a live exception so the DTO
    remains picklable and constitution §4.8 compliant. ``success=True``
    with ``monitoring_pipeline_path=None`` indicates a no-op
    (monitoring not configured or no synthetic flowgroup was built).

    :stability: provisional
    """

    success: bool
    monitoring_pipeline_path: Optional[Path]
    event_log_table_created: bool
    error_message: Optional[str] = None
    error_code: Optional[str] = None


@dataclass(frozen=True)
class BundleSyncResult:
    """Outcome of a bundle resource sync run.

    Returned by :meth:`BundleFacade.sync_resources`. ``synced_file_count``
    counts new or rewritten resource YAML files; ``deleted_file_count``
    counts files removed during the optional pre-sync wipe of
    ``resources/lhp/``. Failures surface as ``success=False`` plus
    structured ``error_message`` / ``error_code`` per §4.8.

    :stability: provisional
    """

    success: bool
    synced_file_count: int
    deleted_file_count: int
    bundle_path: Optional[Path]
    error_message: Optional[str] = None
    error_code: Optional[str] = None


@dataclass(frozen=True)
class BundleValidationResult:
    """Outcome of bundle preflight validation.

    Returned by :meth:`BundleFacade.validate_bundle_assets`. ``issues``
    is a frozen tuple of human-readable preflight findings (empty when
    ``success=True``). Aggregate failures from
    :mod:`lhp.bundle.preflight` are flattened onto the structured
    ``error_message`` / ``error_code`` fields per §4.8 — no live
    :class:`LHPError` instance is retained.

    :stability: provisional
    """

    success: bool
    issues: Tuple[str, ...]
    error_message: Optional[str] = None
    error_code: Optional[str] = None


@dataclass(frozen=True)
class BundleEnableResult:
    """Outcome of enabling Databricks Asset Bundle support on a project.

    Returned by :meth:`BundleFacade.enable_bundle`. Mirrors the shape of
    :class:`InitProjectResult` for the bundle-only scaffolding case:
    ``created_files`` / ``created_dirs`` enumerate filesystem entries
    that did not exist before the call and that this run produced.
    One-time scaffolding — distinct from :meth:`BundleFacade.sync_resources`
    which runs after every generation.

    :stability: provisional
    """

    success: bool
    target_dir: Path
    created_files: Tuple[Path, ...]
    created_dirs: Tuple[Path, ...]
    error_message: Optional[str] = None
    error_code: Optional[str] = None


# ``ValidationIssueView`` / ``PipelineStats`` are referenced by string
# inside generic annotations above. Thanks to ``from __future__ import
# annotations`` they are never evaluated at class-construction time, so
# no import is needed here — and importing them would create a cycle
# (``views`` imports ``JSONValue`` from this module).
