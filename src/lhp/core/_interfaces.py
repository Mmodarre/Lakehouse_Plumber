"""Service ABCs for the LHP coordination layer.

Each ABC is the load-bearing seam between the orchestrator and one
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
    Callable,
    Generator,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from lhp.models import FlowGroup, FlowGroupContext

from ..models.processing import CopiedModuleRecord, DeprecationWarningRecord
from .processing.substitution import EnhancedSubstitutionManager as SubstitutionManager


@dataclass(frozen=True, slots=True)
class CrossFlowgroupCheckResult:
    """Categorised cross-flowgroup compatibility errors.

    Returned by :meth:`BaseValidationService.validate_cross_flowgroup`.
    Categories are preserved (rather than flattened into
    :class:`ValidationIssueView`) so callers — notably the per-pipeline
    worker in :class:`PipelineProcessor` — can re-raise them as
    :class:`LHPValidationError` with the correct error code and
    suggestions per category.

    LHPError exceptions raised by the underlying validators are NOT
    caught here; they propagate to the caller.
    """

    table_creation_errors: List[str] = field(default_factory=list)
    cdc_fanin_errors: List[str] = field(default_factory=list)


if TYPE_CHECKING:
    from lhp.models import ProjectConfig

    # Annotation-only here (used solely in BaseDependencyAnalysisService's
    # abstractmethod signatures). models.dependencies eagerly imports
    # networkx (~190ms), needed only when `dag` actually runs — keeping
    # this import out of module scope keeps networkx off the `import
    # lhp.api` / general `core` import path. Resolved lazily by the
    # concrete service (core/dependencies/) when dependency analysis runs.
    from ..models.dependencies import DependencyAnalysisResult, DependencyGraphs
    from ..models.processing import PipelineDelta

    # PipelineValidationOutcome lives in the leaf module
    # `core/coordination/_validation_outcome.py` (re-exported from `.executor`);
    # imported under TYPE_CHECKING here only for the ABC return annotation.
    from .coordination._validation_outcome import PipelineValidationOutcome


class BaseFlowgroupDiscoveryService(ABC):
    """:meth:`find_source_yaml_for_flowgroup` is a distinct source-path-lookup
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
        raise NotImplementedError

    @abstractmethod
    def find_source_yaml_for_flowgroup(self, flowgroup: FlowGroup) -> Optional[Path]:
        """Return the source YAML path for a flowgroup, or ``None`` if unresolved.

        Multi-document (``---``) and flowgroups-array files are supported.
        """
        raise NotImplementedError

    @abstractmethod
    def scan_deprecation_warnings(
        self, *, pipeline_filter: Optional[str] = None
    ) -> Tuple[DeprecationWarningRecord, ...]:
        """Detect deprecated bare-``{token}`` syntax (``LHP-DEPR-001``; use
        ``${token}`` — ``%{local_var}`` stays valid). Returns EXACTLY ONE
        :class:`DeprecationWarningRecord` per offending file (multiple bare
        tokens in a file collapse to one record; ``flowgroup`` is ``None``).
        Files using only ``${...}`` / ``%{...}`` / ``{{...}}`` yield none.

        When ``pipeline_filter`` is given, only the source files contributing a
        flowgroup to that pipeline are scanned (mirrors the ``--pipeline``
        slice so a one-pipeline run does not emit the whole project's
        deprecations); when ``None``, every pipeline file is scanned.

        Main-thread warnings (read path precedes the worker pool), distinct
        from the worker-side warnings that ride back on
        ``FlowgroupOutcome.warnings``. The facade wrapper merges this tuple
        into the event stream as ``WarningEmitted`` events.
        """
        raise NotImplementedError


class BaseFlowgroupResolutionService(ABC):
    """Per §4.12 the ABC mirrors the concrete worker contract: the envelope
    :class:`FlowGroupContext` carries flowgroup + per-flowgroup provenance
    (source YAML path, synthetic flag, inline auxiliary Python files) across
    the worker boundary, so the ABC method is envelope-in / envelope-out.
    Per §4.10 the ABC is named ``Base<Subject><Verb>Service``.

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
        """Returns a new envelope wrapping the resolved :class:`FlowGroup`
        (provenance fields are propagated unchanged).
        """
        raise NotImplementedError


class BaseValidationService(ABC):
    """Composes :class:`ConfigValidator` plus every action/compatibility
    validator under ``core/validators/`` behind one interface. Callers get
    one method per operation; the fan-out to underlying validators is an
    implementation detail.

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
        """Runs only the table-creation and CDC fan-in compatibility
        validators, omitting the per-flowgroup :class:`ConfigValidator` pass.

        LHPError exceptions raised by the underlying validators are NOT
        caught — they propagate to the caller (the worker's catch-and-convert
        boundary translates them into a :class:`PipelineDelta` failure).
        """
        raise NotImplementedError


class BaseCodeGenerationService(ABC):
    """Phase A / Phase B worker contract: when ``phase_a_records`` is
    supplied, user Python module copies are recorded as
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
        """``phase_a_records`` accumulates pure-compute descriptions of user
        Python module copies for deferred Phase B replay (no inline disk
        writes when supplied). ``auxiliary_files`` is the
        ``{module_path: source_str}`` mapping of inline Python modules
        carried on the envelope.
        """
        raise NotImplementedError


class BasePipelineExecutionService(ABC):
    """The typed execution surface behind ``core/coordination/executor.py``.
    Both :meth:`run_validate` and :meth:`run_generate` take the flat
    four-map worklist shape, differing only in ``mode`` and the
    generate-only gate/commit extras. :meth:`run_validate` returns one
    :class:`PipelineValidationOutcome` per pipeline (REPORTS, never raises);
    :meth:`run_generate` is a generator yielding one :class:`PipelineDelta`
    per pipeline and is all-or-nothing (RAISES on any failure before any
    write — failure deltas are yielded first, then the raise closes the
    stream per §1.4).

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
        on_total: Optional[Callable[[int], None]] = None,
        on_flowgroup_done: Optional[Callable[[], None]] = None,
    ) -> Generator["PipelineDelta", None, Tuple[DeprecationWarningRecord, ...]]:
        """The flat four-map shape — produced by
        ``flowgroup_worklist_builder.build_flowgroup_worklist`` with a REAL
        ``output_dir`` (unlike validate's ``None``) — plus the generate-only
        extras the gate/commit driver needs: ``output_dir`` (env-level, for the
        single whole-env wipe), ``project_config`` (gates the test-reporting
        hook), and ``project_root``. ``flowgroups_by_pipeline`` is keyed in
        pipeline-input order; ``discovery_errors`` maps a pipeline to its
        discovery-failure message (a non-empty map aborts the whole batch —
        generate is all-or-nothing).

        This is a GENERATOR yielding one :class:`PipelineDelta` per pipeline in
        DETERMINISTIC INPUT PIPELINE ORDER: every failure delta first, then —
        after the gate raises on any failure (§1.4 closes the stream) — a
        success delta per committed pipeline. Consumers MUST fully drain it:
        the terminal whole-env format pass runs after the last success delta.

        RETURNS (via ``StopIteration.value``, captured by a ``yield from``) the
        batch's worker-attached deprecation warnings, merged + deduped by
        ``(code, file)``, for the facade to re-emit as
        :class:`~lhp.api.WarningEmitted` events. Not delivered on the
        gate-raise path (the §1.4 raise closes the stream).

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
        on_total: Optional[Callable[[int], None]] = None,
        on_flowgroup_done: Optional[Callable[[], None]] = None,
    ) -> Generator[
        "PipelineValidationOutcome", None, Tuple[DeprecationWarningRecord, ...]
    ]:
        """Flat four-map shape — produced by
        ``flowgroup_worklist_builder.build_flowgroup_worklist``:
        ``flowgroups_by_pipeline`` keyed in pipeline-input order, the
        per-pipeline ``substitution_managers`` and ``output_dirs``, and
        ``discovery_errors`` mapping a pipeline to its discovery-failure message.

        This is a GENERATOR yielding one :class:`PipelineValidationOutcome` per
        pipeline in DETERMINISTIC INPUT PIPELINE ORDER. Validate REPORTS and
        never raises on findings — there is no gate, so (unlike
        :meth:`run_generate`'s failures-then-raise) the stream simply runs to
        completion.

        RETURNS (via ``StopIteration.value``, captured by a ``yield from``) the
        batch's worker-attached deprecation warnings, merged + deduped by
        ``(code, file)``, for the facade to re-emit as
        :class:`~lhp.api.WarningEmitted` events.
        """
        raise NotImplementedError


class BaseDependencyAnalysisService(ABC):
    """The canonical surface is three verbs (``build_graphs``, ``analyze``,
    ``export``); the export sink unifies under a single ``format`` enum
    parameter (§4.1).

    :stability: provisional
    """

    @abstractmethod
    def build_graphs(self, flowgroups: Sequence[FlowGroup]) -> "DependencyGraphs":
        raise NotImplementedError

    @abstractmethod
    def analyze(self, graphs: "DependencyGraphs") -> "DependencyAnalysisResult":
        """Run topological / cycle / external-source analysis on the given graphs."""
        raise NotImplementedError

    @abstractmethod
    def export(
        self,
        result: "DependencyAnalysisResult",
        format: Literal["dot", "json", "text"],
    ) -> str:
        raise NotImplementedError


class BaseMonitoringFinalizerService(ABC):
    """Callers reach it via ``ActionOrchestrator.finalize_monitoring_artifacts``.

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
        raise NotImplementedError

    @abstractmethod
    def cleanup_artifacts(self, env: str, output_dir: Path) -> None:
        raise NotImplementedError


class BaseFlowgroupBootstrapService(ABC):
    """Maintains the synthetic-context provenance table that records, for each
    discovered flowgroup, whether it was synthesised (e.g. by a blueprint) and
    what auxiliary Python files and source YAML it came with.
    :meth:`make_context` is the public lookup into that table, used by the
    orchestrator (and worker code) to wrap a :class:`FlowGroup` in its
    :class:`FlowGroupContext` envelope without re-running discovery.

    :stability: provisional
    """

    @abstractmethod
    def discover_all_flowgroups(self) -> Tuple[FlowGroup, ...]:
        """Side effects: populates the service's internal synthetic-context
        table (consulted by :meth:`make_context`) and refreshes the
        monitoring view owned by the service.
        """
        raise NotImplementedError

    @abstractmethod
    def make_context(self, fg: FlowGroup) -> FlowGroupContext:
        """Looks up provenance (synthetic flag, auxiliary files, source YAML)
        accumulated by :meth:`discover_all_flowgroups`; falls back to a
        non-synthetic, empty-provenance envelope when no entry is found.
        """
        raise NotImplementedError
