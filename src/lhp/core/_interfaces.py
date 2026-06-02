"""Service ABCs for the LHP coordination layer.

Defines the eight internal service contracts the :class:`ActionOrchestrator`
composes. Each ABC is the load-bearing seam between the orchestrator and one
concrete implementation; the orchestrator type-hints these ABCs so future
re-wires (test doubles, alternate transports, future plugin surfaces) only
have to satisfy the contract rather than mimic a concrete class shape.

**Why `abc.ABC` and not `typing.Protocol`.** Per constitution §4.12, internal
contracts with one or more implementations are defined as
:class:`abc.ABC` subclasses with :func:`abc.abstractmethod`. ABC fails fast at
construction when a method is missing (services are wired at orchestrator
init time, so a TypeError there is operationally superior to an AttributeError
on first call). ABC also gives IDE "show subclasses" support immediately,
covers the ``mypy --strict`` gap outside ``lhp/api/`` (§4.3), and leaves room
to add shared entry/exit telemetry in one place.

**§5.5 reminder.** No ABC in this file type-hints another ABC's class.
Services do not call each other — composition is the orchestrator's job.

:stability: provisional
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from lhp.models import FlowGroup, FlowGroupContext

from ..models.dependencies import DependencyAnalysisResult, DependencyGraphs
from ..models.processing import CopiedModuleRecord
from .processing.substitution import EnhancedSubstitutionManager as SubstitutionManager


@dataclass(frozen=True, slots=True)
class CrossFlowgroupCheckResult:
    """Categorised cross-flowgroup compatibility errors.

    Returned by :meth:`BaseValidationService.validate_cross_flowgroup`.
    Each list holds the raw error strings produced by the corresponding
    validator. The categories are preserved (rather than flattened into
    :class:`ValidationIssueView`) so callers — notably the per-pipeline
    worker in :class:`PipelineProcessor` — can re-raise them as
    :class:`LHPValidationError` with the correct error code and
    suggestions per category.

    LHPError exceptions raised by the underlying validators are NOT
    caught here; they propagate to the caller, matching the historical
    behaviour of the worker's ``_validate_cross_fg`` path.
    """

    table_creation_errors: List[str] = field(default_factory=list)
    cdc_fanin_errors: List[str] = field(default_factory=list)


if TYPE_CHECKING:
    from lhp.models import ProjectConfig

    # PipelineValidationOutcome currently lives in `core/coordination/executor.py`;
    # it will move to `models/processing.py` (or `lhp/api/`) once the public DTO
    # surface is consolidated. Importing under TYPE_CHECKING avoids freezing
    # that destination here.
    from .coordination.executor import PipelineValidationOutcome


class BaseFlowgroupDiscoveryService(ABC):
    """Discovers flowgroup YAML files in a project.

    Replaces the current ``discover_flowgroups`` / ``discover_all_flowgroups``
    / ``discover_flowgroups_by_pipeline_field`` triple with a single canonical
    read method (§4.1): :meth:`discover_flowgroups`, gated by an explicit
    ``pipeline_filter`` keyword (``None`` returns every flowgroup).

    :meth:`find_source_yaml_for_flowgroup` is a distinct source-path-lookup
    operation, not a discover-variant (§4.1): it maps an already-discovered
    flowgroup back to the YAML file it came from rather than enumerating
    flowgroups.

    :stability: provisional
    """

    @abstractmethod
    def discover_flowgroups(
        self, *, pipeline_filter: Optional[str] = None
    ) -> Tuple[FlowGroup, ...]:
        """Return flowgroups belonging to the given pipeline (or all if ``None``)."""
        raise NotImplementedError

    @abstractmethod
    def get_include_patterns(self) -> Tuple[str, ...]:
        """Return the glob patterns used for discovery."""
        raise NotImplementedError

    @abstractmethod
    def find_source_yaml_for_flowgroup(self, flowgroup: FlowGroup) -> Optional[Path]:
        """Return the source YAML path for a flowgroup, or ``None`` if unresolved.

        Multi-document (``---``) and flowgroups-array files are supported.
        """
        raise NotImplementedError


class BaseFlowgroupResolutionService(ABC):
    """Applies presets, templates, and substitutions to a single flowgroup.

    Per §4.12 the ABC mirrors the concrete worker contract: the envelope
    :class:`FlowGroupContext` (defined in ``models/config.py``) carries
    flowgroup + per-flowgroup provenance (source YAML path, synthetic flag,
    inline auxiliary Python files) across the worker boundary, so the ABC
    method is envelope-in / envelope-out. Per §4.10 the ABC is named
    ``Base<Subject><Verb>Service``.

    :stability: provisional
    """

    @abstractmethod
    def resolve(
        self,
        ctx: FlowGroupContext,
        substitution_mgr: SubstitutionManager,
        *,
        include_tests: bool = True,
    ) -> FlowGroupContext:
        """Expand templates, apply presets, apply substitutions, validate.

        Takes a :class:`FlowGroupContext` envelope, returns a new envelope
        wrapping the resolved :class:`FlowGroup` (provenance fields are
        propagated unchanged).
        """
        raise NotImplementedError


class BaseValidationService(ABC):
    """Single external validation surface.

    Composes :class:`ConfigValidator` (``core/validator.py``) plus every
    action/compatibility validator under ``core/validators/`` behind one
    interface. Callers (orchestrator, CLI, worker code) get one method per
    operation; the fan-out to underlying validators is an implementation
    detail.

    :stability: provisional
    """

    @abstractmethod
    def validate_duplicates(self, flowgroups: Sequence[FlowGroup]) -> None:
        """Raise :class:`LHPValidationError` if duplicate pipeline+flowgroup pairs exist."""
        raise NotImplementedError

    @abstractmethod
    def validate_cross_flowgroup(
        self,
        flowgroups: Sequence[FlowGroup],
        *,
        pipeline_filter: Optional[str] = None,
    ) -> CrossFlowgroupCheckResult:
        """Run only the cross-flowgroup compatibility checks.

        Runs only the table-creation and CDC fan-in compatibility
        validators, omitting the per-flowgroup :class:`ConfigValidator`
        pass. Used by the
        per-pipeline worker in :class:`PipelineProcessor` to keep
        Phase B's validation footprint identical to the pre-§9.24
        ``_validate_cross_fg`` behaviour.

        LHPError exceptions raised by the underlying validators are
        NOT caught — they propagate to the caller (matching historical
        worker behaviour, where the worker's own catch-and-convert
        boundary translates them into a :class:`PipelineDelta`
        failure).
        """
        raise NotImplementedError


class BaseCodeGenerationService(ABC):
    """Generates Python code for one flowgroup.

    Mirrors the current :meth:`CodeGenerationService.generate_flowgroup_code` shape
    including the Phase A / Phase B worker contract: when ``phase_a_records``
    is supplied, user Python module copies are recorded as
    :class:`CopiedModuleRecord` entries (pure compute) instead of being
    written to disk inline; ``auxiliary_files`` carries inline Python
    modules attached to the :class:`FlowGroupContext` envelope.

    :stability: provisional
    """

    @abstractmethod
    def generate(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: SubstitutionManager,
        *,
        output_dir: Optional[Path] = None,
        source_yaml: Optional[Path] = None,
        env: Optional[str] = None,
        include_tests: bool = False,
        phase_a_records: Optional[Tuple[CopiedModuleRecord, ...]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> str:
        """Return complete Python code for the flowgroup.

        ``phase_a_records`` accumulates pure-compute descriptions of user
        Python module copies for deferred Phase B replay (no inline disk
        writes when supplied). ``auxiliary_files`` is the
        ``{module_path: source_str}`` mapping of inline Python modules
        carried on the envelope.
        """
        raise NotImplementedError


class BasePipelineExecutionService(ABC):
    """Manages parallel execution across pipelines.

    The typed execution surface behind ``core/coordination/executor.py``.
    Both :meth:`run_validate` and :meth:`run_generate` take the
    flat four-map worklist shape and drive the
    consolidated flat per-flowgroup engine, differing only in ``mode`` and the
    generate-only gate/commit extras. :meth:`run_validate` returns one
    :class:`PipelineValidationOutcome` per pipeline (REPORTS, never raises);
    :meth:`run_generate` returns ``{pipeline -> generated filenames}`` and is
    all-or-nothing (RAISES on any failure before any write), surfacing
    per-pipeline :class:`PipelineDelta`s via its completion callback.

    :stability: provisional
    """

    @abstractmethod
    def run_generate(
        self,
        *,
        flowgroups_by_pipeline: Mapping[str, Sequence["FlowGroupContext"]],
        substitution_managers: Mapping[str, "SubstitutionManager"],
        output_dirs: Mapping[str, Optional[Path]],
        discovery_errors: Mapping[str, str],
        output_dir: Optional[Path],
        project_config: Optional["ProjectConfig"],
        project_root: Path,
        max_workers: Optional[int] = None,
        apply_formatting: bool = True,
    ) -> Dict[str, tuple[str, ...]]:
        """Run generate across the flat worklist; return per-pipeline filenames.

        The flat four-map shape — produced by
        ``flowgroup_worklist_builder.build_flowgroup_worklist`` with a REAL
        ``output_dir`` (unlike validate's ``None``) — mirrors
        :meth:`run_validate`, plus the generate-only extras the gate/commit
        driver needs: ``output_dir`` (env-level, for the single whole-env
        wipe), ``project_config`` (gates the test-reporting hook), and
        ``project_root``. ``flowgroups_by_pipeline`` is keyed in pipeline-input
        order; ``discovery_errors`` maps a pipeline to its discovery-failure
        message (a non-empty map aborts the whole batch — generate is
        all-or-nothing).

        Drives the consolidated flat per-flowgroup engine in
        ``mode="generate"`` (codegen + AST-parse guard in the worker;
        ``apply_formatting`` gates the terminal ruff pass), applies the
        all-or-nothing gate (RAISES on any failure, before any write), then
        commits each clean pipeline. Returns ``{pipeline -> generated
        filenames}`` for the committed pipelines, in input order; the
        per-pipeline :class:`PipelineDelta`s flow out-of-band via the
        ``on_generate_pipeline_complete`` callback registered on the concrete
        service.

        :raises LHPError: the sole failure's error, or ``LHP-VAL-902`` (many);
            also raised for a non-empty ``discovery_errors``.
        """
        raise NotImplementedError

    @abstractmethod
    def run_validate(
        self,
        *,
        flowgroups_by_pipeline: Mapping[str, Sequence["FlowGroupContext"]],
        substitution_managers: Mapping[str, "SubstitutionManager"],
        output_dirs: Mapping[str, Optional[Path]],
        discovery_errors: Mapping[str, str],
    ) -> Tuple["PipelineValidationOutcome", ...]:
        """Run validate across the flat worklist and return one outcome per pipeline.

        The flat four-map shape — produced by
        ``flowgroup_worklist_builder.build_flowgroup_worklist`` — replaces the
        legacy ``Sequence[PipelineWorkUnit]`` input: ``flowgroups_by_pipeline``
        keyed in pipeline-input order, the per-pipeline ``substitution_managers``
        and ``output_dirs``, and ``discovery_errors`` mapping a pipeline to its
        discovery-failure message.
        """
        raise NotImplementedError


class BaseDependencyAnalysisService(ABC):
    """Analyzes flowgroup and action dependencies.

    The canonical surface is three verbs (``build_graphs``, ``analyze``,
    ``export``); the export sink unifies under a single ``format`` enum
    parameter (§4.1).

    :stability: provisional
    """

    @abstractmethod
    def build_graphs(self, flowgroups: Sequence[FlowGroup]) -> DependencyGraphs:
        """Build the action / flowgroup / pipeline dependency graphs."""
        raise NotImplementedError

    @abstractmethod
    def analyze(self, graphs: DependencyGraphs) -> DependencyAnalysisResult:
        """Run topological / cycle / external-source analysis on the given graphs."""
        raise NotImplementedError

    @abstractmethod
    def export(
        self,
        result: DependencyAnalysisResult,
        format: Literal["dot", "json", "text"],
    ) -> str:
        """Serialize the analysis result in the requested format."""
        raise NotImplementedError


class BaseMonitoringFinalizerService(ABC):
    """Builds and finalizes monitoring artifacts.

    Encapsulates the build/finalize/cleanup trio behind a single service;
    callers reach it via ``ActionOrchestrator.finalize_monitoring_artifacts``.

    :stability: provisional
    """

    @abstractmethod
    def build_flowgroup(
        self, discovered_flowgroups: Sequence[FlowGroup]
    ) -> Optional[FlowGroup]:
        """Build the synthetic monitoring flowgroup, or ``None`` if not configured."""
        raise NotImplementedError

    @abstractmethod
    def finalize_artifacts(self, env: str, output_dir: Path) -> None:
        """Write monitoring notebook + job resource for the given environment."""
        raise NotImplementedError

    @abstractmethod
    def cleanup_artifacts(self, env: str, output_dir: Path) -> None:
        """Remove stale monitoring artifacts (notebook, job resource, generated dirs)."""
        raise NotImplementedError


class BaseFlowgroupBootstrapService(ABC):
    """Bootstraps the on-disk flowgroup set into typed contexts.

    Owns the discovery → blueprint expansion → monitoring chain and the
    synthetic-context provenance table that records, for each discovered
    flowgroup, whether it was synthesised (e.g. by a blueprint) and what
    auxiliary Python files and source YAML it came with.
    :meth:`make_context` is the public lookup into that table, used by the
    orchestrator (and worker code) to wrap a :class:`FlowGroup` in its
    :class:`FlowGroupContext` envelope without re-running discovery.

    :stability: provisional
    """

    @abstractmethod
    def discover_all_flowgroups(self) -> Tuple[FlowGroup, ...]:
        """Discover every flowgroup on disk, expand blueprints, attach monitoring.

        Side effects: populates the service's internal synthetic-context
        table (consulted by :meth:`make_context`) and refreshes the
        monitoring view owned by the service.
        """
        raise NotImplementedError

    @abstractmethod
    def make_context(self, fg: FlowGroup) -> FlowGroupContext:
        """Wrap ``fg`` in its :class:`FlowGroupContext` envelope.

        Looks up provenance (synthetic flag, auxiliary files, source YAML)
        accumulated by :meth:`discover_all_flowgroups`; falls back to a
        non-synthetic, empty-provenance envelope when no entry is found.
        """
        raise NotImplementedError


class BaseWarningCollector(ABC):
    """Sink for non-fatal warnings surfaced during generation/validation.

    Lets ``core`` code accept a warning sink without importing the concrete
    :class:`lhp.api.callbacks.WarningCollector` (which inherits this ABC),
    keeping the ``core`` → ``api`` dependency edge severed.

    :stability: provisional
    """

    @abstractmethod
    def add(self, category: str, message: str) -> None:
        """Record a warning under the given category."""
        raise NotImplementedError
