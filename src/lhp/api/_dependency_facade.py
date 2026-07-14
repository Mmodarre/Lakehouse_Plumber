"""Private module — implementation of the public :class:`DependencyFacade`.

Underscore-prefixed: not part of the import surface; external callers
MUST import :class:`DependencyFacade` from :mod:`lhp.api` (re-exported
via :mod:`lhp.api.facade`). Groups the project-level dependency-graph
analysis and dependency-output generation paths — split out of
:class:`InspectionFacade` so that facade stays under the §3.2 method cap
while the dependency surface grows its own refresh/persistence paths.

:stability: internal
"""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
    cast,
)

from lhp.api._dependency_graph_converters import _graph_to_view
from lhp.api._inspection_converters import _dependency_result_to_view
from lhp.api.responses import (
    DatasetIndexResult,
    DependencyAnalysisResult,
    DependencyOutputEntry,
    DependencyOutputsResult,
    DependencyStalenessResult,
)

if TYPE_CHECKING:
    # Internal orchestrator type, referenced only as a quoted annotation
    # below; never named directly in the public API surface (§1.10, §9.13).
    _Orchestrator = Any


class DependencyFacade:
    """Project-level dependency-graph analysis and output generation.

    Public methods, all returning frozen DTOs:

    - ``analyze_dependencies`` — run analysis and return a flattened,
      networkx-free :class:`DependencyAnalysisResult` (optionally with
      per-level graph snapshots).
    - ``describe_graph_staleness`` — cheap freshness metadata for the
      persisted graph build, backing the ``lhp web`` serve-stale model.
    - ``save_dependency_outputs`` — run analysis and write the requested
      ``dot`` / ``json`` / ``text`` / ``job`` outputs to disk.
    - ``build_dataset_index`` — env-resolve every flowgroup and join its
      write actions onto the action graph to produce a
      :class:`DatasetIndexResult` (table→table lineage).

    They share the orchestrator's memoized per-option analysis, so a
    matching analyze + save pair discovers and analyzes the project once.

    :stability: provisional
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self._logger = logging.getLogger(__name__)

    def analyze_dependencies(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        blueprint_filter: Optional[str] = None,
        trust_depends_on: bool = False,
        include_graphs: bool = False,
        force_rebuild: bool = False,
    ) -> DependencyAnalysisResult:
        """Run dependency analysis and return a frozen, flattened result.

        ``pipeline_filter`` restricts the graph to the named pipeline.
        ``blueprint_filter`` restricts the graph to synthetic flowgroups
        expanded from the named blueprint. Blueprint expansion is always
        full — one node per blueprint × instance × spec — so pipelines a
        blueprint parameterizes per instance are never dropped. Analysis
        is memoized per option triple: a subsequent
        :meth:`save_dependency_outputs` with the same options reuses this
        call's analysis.

        ``trust_depends_on`` (opt-in) makes a non-empty ``depends_on``
        AUTHORITATIVE for its action: SQL/Python bodies are not read or
        parsed for that action, and its source set is exactly the explicit
        ``source:`` declarations plus the declared ``depends_on`` entries.
        Default (False) keeps the additive contract — declared entries
        union on top of parsed sources. Trust mode is a performance
        escape hatch for large projects whose actions fully declare their
        upstreams; an action whose declarations are incomplete loses the
        parsed edges for its undeclared reads.

        ``include_graphs`` (opt-in) additionally populates the result's
        ``action_graph`` / ``flowgroup_graph`` / ``pipeline_graph``
        fields with frozen, networkx-free :class:`DependencyGraphView`
        snapshots of the three graph levels. Default (False) leaves
        them ``None`` — the flattened summary alone. Same return type
        either way; only the optional fields' population changes (the
        constitution's sanctioned opt-in-field pattern, §13.7).

        The result's ``warnings`` may carry ``LHP-DEP-002`` /
        ``LHP-DEP-003`` advisory codes (never raised), aggregated per
        unresolved read SITE with the affected actions enumerated, each
        suggesting an explicit ``depends_on`` declaration.

        ``force_rebuild`` (opt-in) bypasses BOTH the in-process analysis memo
        AND the on-disk graph cache: it re-reads the project from disk, forces
        a fresh build, and re-persists the result. It backs the ``lhp web``
        Refresh action, whose serve-stale model otherwise keeps serving the
        last-good graph while the project is edited. Default (False) is the
        normal cached path (memo -> disk cache -> build).

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` / ``LHP-VAL-*`` /
            ``LHP-FILE-*`` / ``LHP-MULT-*`` propagated from flowgroup
            discovery and dependency analysis.
        """
        internal = self._orchestrator.dependencies.analyze_project(
            pipeline_filter=pipeline_filter,
            blueprint_filter=blueprint_filter,
            trust_depends_on=trust_depends_on,
            force_rebuild=force_rebuild,
        )
        view = _dependency_result_to_view(internal)
        if not include_graphs:
            return view
        return replace(
            view,
            action_graph=_graph_to_view(internal.graphs.action_graph, "action"),
            flowgroup_graph=_graph_to_view(
                internal.graphs.flowgroup_graph, "flowgroup"
            ),
            pipeline_graph=_graph_to_view(internal.graphs.pipeline_graph, "pipeline"),
        )

    def describe_graph_staleness(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        blueprint_filter: Optional[str] = None,
        trust_depends_on: bool = False,
    ) -> DependencyStalenessResult:
        """Report freshness metadata for the persisted dependency graph.

        Backs the ``lhp web`` serve-stale model. Returns a frozen
        :class:`DependencyStalenessResult` describing the last persisted build
        for the given option triple: its ``fingerprint`` (the graph cache
        version tag) and ``built_at`` (ISO-8601 UTC timestamp, or ``None`` when
        nothing has been persisted yet). ``stale`` is ``True`` only when no
        persisted build exists — the facade never re-hashes or re-reads the
        project on this call; the webapp overlays its watcher-set edit flag as
        the live staleness signal.

        Cheap by construction: at most a single ``os.stat`` on the shard file
        (see :class:`~lhp.core.dependencies.graph_cache.PersistentGraphCache`),
        never an unpickle of the result nor a content re-hash.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` propagated only if project
            configuration is unreadable while the cache location is resolved.
        """
        built_at, fingerprint = self._orchestrator.dependencies.describe_cached_graph(
            pipeline_filter=pipeline_filter,
            blueprint_filter=blueprint_filter,
            trust_depends_on=trust_depends_on,
        )
        return DependencyStalenessResult(
            stale=built_at is None,
            fingerprint=fingerprint or "",
            built_at=built_at,
        )

    def save_dependency_outputs(
        self,
        *,
        formats: Sequence[str],
        output_dir: Path,
        pipeline_filter: Optional[str] = None,
        blueprint_filter: Optional[str] = None,
        trust_depends_on: bool = False,
        job_name: Optional[str] = None,
        job_config_path: Optional[str] = None,
        bundle_output: bool = False,
    ) -> DependencyOutputsResult:
        """Run dependency analysis and write requested outputs to disk.

        ``formats`` accepts any combination of ``"dot"``, ``"json"``,
        ``"text"``, ``"job"``, or ``"all"`` (expands to all four).
        Filter and ``trust_depends_on`` parameters mirror
        :meth:`analyze_dependencies` (and share its memoized analysis).
        ``job_name`` / ``job_config_path`` / ``bundle_output`` shape the
        ``"job"`` format only and have no effect on the others. The
        ``"job"`` format is a whole-project deployment artifact: when a
        pipeline/blueprint filter is active it is skipped with a logged
        warning instead of being written from a different flowgroup set
        than the filtered analysis outputs.

        Returns a frozen :class:`DependencyOutputsResult` enumerating
        every generated file with its format name and optional job-name
        label.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` / ``LHP-VAL-*`` /
            ``LHP-FILE-*`` / ``LHP-MULT-*`` propagated from discovery
            and analysis, plus ``LHP-IO-*`` / :class:`OSError` for
            filesystem failures while writing the requested formats.
        """
        from lhp.core.dependencies import DependencyOutputWriter

        dep_service = self._orchestrator.dependencies
        internal = dep_service.analyze_project(
            pipeline_filter=pipeline_filter,
            blueprint_filter=blueprint_filter,
            trust_depends_on=trust_depends_on,
        )

        output_manager = DependencyOutputWriter()
        # ``save_outputs`` is typed ``Dict[str, Path]`` but the multi-job
        # branch returns a nested ``Dict[str, Path]`` value per format —
        # the cast lets mypy see both legs of the isinstance below.
        generated = cast(
            Dict[str, Union[Path, Dict[str, Path]]],
            output_manager.save_outputs(
                dep_service,
                internal,
                list(formats),
                output_dir,
                job_name,
                job_config_path,
                bundle_output,
                trust_depends_on=trust_depends_on,
                filters_active=bool(pipeline_filter or blueprint_filter),
            ),
        )

        entries: List[DependencyOutputEntry] = []
        for format_name, path_or_dict in generated.items():
            if isinstance(path_or_dict, dict):
                for sub_name, sub_path in path_or_dict.items():
                    entries.append(
                        DependencyOutputEntry(
                            format_name=format_name,
                            label=sub_name,
                            path=sub_path,
                        )
                    )
            else:
                entries.append(
                    DependencyOutputEntry(
                        format_name=format_name,
                        label="",
                        path=path_or_dict,
                    )
                )

        return DependencyOutputsResult(
            success=True,
            entries=tuple(entries),
            output_dir=output_dir,
        )

    def build_dataset_index(
        self, *, env: str, force_rebuild: bool = False
    ) -> DatasetIndexResult:
        """Build the project's table→table lineage index for ``env``.

        Joins each flowgroup's env-resolved write actions (fully-qualified
        target names from :meth:`InspectionFacade.process_flowgroup`) onto the
        substitution-agnostic action dependency graph by action id, then emits
        one :class:`~lhp.api.responses.DatasetIndexResult` covering every
        produced table / sink with its upstream lineage chain (same-flowgroup
        load/transform hops, ``external`` source nodes, and cross-flowgroup
        upstream ``dataset`` nodes) plus its downstream consumers.

        The graph is fetched via the memoized / serve-stale ``analyze_project``
        path shared with :meth:`analyze_dependencies`; ``force_rebuild`` (opt-in)
        bypasses both the in-process memo and the on-disk graph cache, backing
        the ``lhp web`` Refresh action. Per-flowgroup env-resolution failures
        are collected on ``warnings`` (the flowgroup is skipped) rather than
        failing the whole call, mirroring the tables catalog. When a write
        action's name embeds an unresolved ``${token}`` the action-id join
        misses and a positional fallback within the flowgroup recovers it,
        also recording a warning. ``fingerprint`` is a content hash of the
        index for freshness comparison across rebuilds.

        Implementation lives in the private :mod:`lhp.api._dataset_index`
        module; the facade stays thin.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` / ``LHP-VAL-*`` /
            ``LHP-FILE-*`` / ``LHP-MULT-*`` propagated from flowgroup
            discovery and dependency analysis.
        """
        from lhp.api._dataset_index import _build_dataset_index

        return _build_dataset_index(
            self._orchestrator, env=env, force_rebuild=force_rebuild
        )
