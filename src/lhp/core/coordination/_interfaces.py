"""Service ABCs for the LHP coordination layer.

Defines the seven internal service contracts the :class:`ActionOrchestrator`
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
from typing import TYPE_CHECKING, List, Literal, Mapping, Optional, Sequence, Tuple

from ...generators.python_file_copier import CopiedModuleRecord
from ...models.config import FlowGroup, FlowGroupContext
from ...models.dependencies import DependencyAnalysisResult, DependencyGraphs
from ...models.processing import PipelineDelta
from ...utils.substitution import EnhancedSubstitutionManager as SubstitutionManager


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

    @property
    def has_errors(self) -> bool:
        """True if any cross-flowgroup error was reported."""
        return bool(self.table_creation_errors or self.cdc_fanin_errors)

if TYPE_CHECKING:
    # ``ValidationIssueView`` lives in :mod:`lhp.api.views` — the frozen
    # public projection of an internal validation outcome (Phase B2b).
    # Importing under TYPE_CHECKING keeps this ABC decoupled from the
    # runtime import order.
    from lhp.api.views import ValidationIssueView
    # PipelineValidationOutcome currently lives in `core/coordination/executor.py`;
    # it will move to `models/processing.py` (or `lhp/api/`) once the public DTO
    # surface is consolidated. Importing under TYPE_CHECKING avoids freezing
    # that destination here.
    from .executor import PipelineValidationOutcome


class BaseFlowgroupDiscoveryService(ABC):
    """Discovers flowgroup YAML files in a project.

    Replaces the current ``discover_flowgroups`` / ``discover_all_flowgroups``
    / ``discover_flowgroups_by_pipeline_field`` triple with a single canonical
    read method (§4.1): :meth:`discover_flowgroups`, gated by an explicit
    ``pipeline_filter`` keyword (``None`` returns every flowgroup).

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
    def validate_flowgroups(
        self,
        flowgroups: Sequence[FlowGroup],
        *,
        pipeline_filter: Optional[str] = None,
    ) -> Tuple["ValidationIssueView", ...]:
        """Return all validation issues across the given flowgroups."""
        raise NotImplementedError

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

        Narrower than :meth:`validate_flowgroups` — runs only the
        table-creation and CDC fan-in compatibility validators, omits
        the per-flowgroup :class:`ConfigValidator` pass. Used by the
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

    Wraps :func:`run_generate_pool` and :func:`run_validate_pool` (today in
    ``core/coordination/executor.py``) behind a typed surface. Workers receive a
    :class:`PipelineWorkUnit` (frozen DTO, forward-referenced — formalized in
    Week 3) and return :class:`PipelineDelta` / :class:`PipelineValidationOutcome`.

    :stability: provisional
    """

    @abstractmethod
    def run_generate(
        self,
        work_units: Sequence["PipelineWorkUnit"],
    ) -> Tuple[PipelineDelta, ...]:
        """Run generate across the given work units and return one delta per pipeline."""
        raise NotImplementedError

    @abstractmethod
    def run_validate(
        self,
        work_units: Sequence["PipelineWorkUnit"],
    ) -> Tuple["PipelineValidationOutcome", ...]:
        """Run validate across the given work units and return one outcome per pipeline."""
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

    Encapsulates the build/finalize/cleanup trio currently spread across
    ``ActionOrchestrator._build_monitoring``,
    ``ActionOrchestrator.finalize_monitoring_artifacts``, and
    ``ActionOrchestrator._cleanup_monitoring_artifacts``.

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
