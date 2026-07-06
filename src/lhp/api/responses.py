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
from typing import TYPE_CHECKING, Literal, Mapping, Optional, Sequence, Tuple, Union

if TYPE_CHECKING:
    # Forward references for the view DTOs used in nested response
    # types. Runtime-deferred to avoid a cycle (``views`` imports
    # ``JSONValue`` from this module) while still letting
    # ``mypy --strict`` resolve the string annotations.
    from lhp.api.views import (
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

    :stability: stable
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

    :stability: stable
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
    ``git_initialized`` is True when this run created a project-local git
    repository (``lhp init --sample`` only).

    :stability: provisional
    """

    success: bool
    target_dir: Path
    created_files: Tuple[Path, ...]
    created_dirs: Tuple[Path, ...]
    bundle_enabled: bool
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    git_initialized: bool = False

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
class AffectedActionView:
    """One action affected by an aggregated extraction-warning site.

    ``edit_yaml_path`` names the YAML file where a ``depends_on`` entry
    for this action belongs — the flowgroup YAML, or the blueprint YAML
    for a blueprint-expanded synthetic flowgroup.

    :stability: provisional
    """

    flowgroup: str
    action: str
    edit_yaml_path: Optional[str] = None


@dataclass(frozen=True)
class DependencyWarningView:
    """Advisory warning surfaced by dependency extraction.

    Public projection of the internal
    :class:`lhp.models.dependencies.DependencyWarning`. Carries the
    advisory extraction warnings (codes ``LHP-DEP-002`` /
    ``LHP-DEP-003``) that recommend an explicit ``depends_on``
    declaration — warning-only, never raised as errors.

    One record per unresolved read SITE: ``flowgroup`` / ``action`` are
    the representative (first sorted) affected action, while
    ``affected_actions`` / ``affected_count`` enumerate every distinct
    action referencing the site. ``edit_yaml_path`` is the YAML file a
    ``depends_on`` fix belongs in.

    :stability: provisional
    """

    code: str
    message: str
    flowgroup: str
    action: str
    suggestion: str
    file_path: Optional[str] = None
    line: Optional[int] = None
    edit_yaml_path: Optional[str] = None
    affected_actions: Tuple[AffectedActionView, ...] = ()
    affected_count: int = 1


@dataclass(frozen=True)
class DependencyAnalysisResult:
    """Outcome of pipeline-level dependency analysis.

    Distinct from the internal
    :class:`lhp.models.dependencies.DependencyAnalysisResult`: this
    public view flattens the networkx-backed graph state into plain
    tuples / mappings so the API surface remains free of internal
    types. Public consumers receive the cycle / order / external-source
    summary but not the live graph objects. ``warnings`` carries the
    advisory extraction warnings (:class:`DependencyWarningView`,
    codes ``LHP-DEP-002`` / ``LHP-DEP-003``) — never raised as errors.

    :stability: provisional
    """

    pipeline_dependencies: Mapping[str, Tuple[str, ...]] = field(default_factory=dict)
    execution_stages: Tuple[Tuple[str, ...], ...] = ()
    circular_dependencies: Tuple[Tuple[str, ...], ...] = ()
    external_sources: Tuple[str, ...] = ()
    total_pipelines: int = 0
    total_external_sources: int = 0
    warnings: Tuple[DependencyWarningView, ...] = ()

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
class WheelExtractionResult:
    """Outcome of extracting the modules from a built wheel to disk.

    A one-shot result DTO (§1.3) returned by the wheel-extraction
    operation. Deliberately departs from
    :class:`DependencyOutputsResult`'s shape: that DTO carries
    ``success`` / ``error_message`` / ``error_code`` and reports
    failure via ``success=False``. Wheel extraction instead *raises*
    (the wheel reader raises :class:`~lhp.errors.LHPError` on failure)
    and only ever constructs this result on success, so the same
    ``success`` / ``error_*`` fields would be permanently
    ``True`` / ``None`` / ``None`` — dead state — and are omitted by
    convention. ``written_paths`` enumerates every file written under
    ``output_dir``; ``written_count`` mirrors its length.

    :stability: provisional
    """

    wheel_path: Path
    output_dir: Path
    written_paths: Tuple[Path, ...]
    written_count: int


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


@dataclass(frozen=True)
class PlannedFileView:
    """A single file a generation run intends to write.

    Element of :class:`GenerationPlan.files`. Carries the resolved
    target ``path``, the rendered ``content``, the owning ``pipeline``,
    and a ``kind`` discriminator that classifies the file's role in the
    output tree. ``kind`` is a ``Literal`` discriminator (§4.8), not a
    free-form string, so consumers can branch exhaustively:

    - ``"flowgroup"`` — a generated flowgroup module.
    - ``"aux"`` — an auxiliary file (e.g. ``__init__.py``).
    - ``"helper"`` — a copied transitive helper module.
    - ``"test_hook"`` — a generated test-action hook file.
    - ``"uc_tagging_hook"`` — a generated per-pipeline UC tagging hook file.
    - ``"monitoring"`` — a synthetic monitoring-pipeline file.

    Immutable and JSON-shape-compatible (§4.8): ``path`` serialises to
    ``str`` via :func:`lhp.api.to_dict` and reconstructs to :class:`Path`.

    :stability: provisional
    """

    path: Path
    content: str
    pipeline: str
    kind: Literal[
        "flowgroup", "aux", "helper", "test_hook", "uc_tagging_hook", "monitoring"
    ]


@dataclass(frozen=True)
class GenerationPlan:
    """The full set of files a generation run intends to write.

    Returned (wrapped in :class:`lhp.api.events.GenerationPlanCompleted`)
    by the plan-only generation path: the run resolves every flowgroup
    and renders every file but writes nothing to disk. ``files`` is a
    frozen ``Tuple`` of :class:`PlannedFileView` — a tuple, not a
    ``Mapping[Path, str]``, so the contract never depends on a
    ``Path``-typed mapping key (§4.8). ``pipeline_count`` /
    ``file_count`` are the aggregate counts; ``output_location`` is the
    root directory the files would be written under.

    :stability: provisional
    """

    files: Tuple["PlannedFileView", ...]
    output_location: Optional[Path]
    pipeline_count: int
    file_count: int


# ``ValidationIssueView`` / ``PipelineStats`` are referenced by string
# inside generic annotations above. Thanks to ``from __future__ import
# annotations`` they are never evaluated at class-construction time, so
# no import is needed here — and importing them would create a cycle
# (``views`` imports ``JSONValue`` from this module).
