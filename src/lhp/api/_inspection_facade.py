"""Private module — implementation of the public :class:`InspectionFacade`.

Underscore-prefixed: not part of the import surface; external callers
MUST import :class:`InspectionFacade` from :mod:`lhp.api` (re-exported
via :mod:`lhp.api.facade`). This split exists purely to keep
``lhp/api/facade.py`` under the constitution §3.3 soft cap (500 lines)
while the inspection surface absorbs twelve read-only methods plus the
two dependency-output paths.

:stability: internal
"""

# JUSTIFIED: This module's :class:`InspectionFacade` exposes fourteen
# public methods (§3.2 requires justification at 10–15; hard cap at 15),
# explicitly enumerated in the class docstring to satisfy the
# constitution's exception clause. The methods are cohesive — all
# read-only project introspection plus the two dependency-output paths
# — and splitting further would fracture a single semantic group across
# multiple facades. Heavy DTO-conversion bodies live in
# :mod:`lhp.api._converters`; what remains is the per-method delegation
# surface plus the dependency-output enumeration path.
# TODO(INSPECTION-FACADE-SPLIT): trim or split InspectionFacade's fourteen methods into sub-facades grouped by DTO family once the inspection surface stabilises; see LOCAL/REMAINING_WORK.md §9.5.
from __future__ import annotations

import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from lhp.api._converters import (
    _build_stats_result,
    _build_substitution_manager_for_env,
    _dependency_result_to_view,
    _duplicates_to_validation_response,
    _flowgroup_file_paths,
    _flowgroup_to_processed_view,
    _flowgroups_to_views,
    _locate_flowgroup_by_name,
    _preset_to_view,
    _project_config_to_view,
    _template_to_view,
)
from lhp.api._listings import _build_blueprint_views
from lhp.api.responses import (
    DependencyAnalysisResult,
    DependencyOutputEntry,
    DependencyOutputsResult,
    StatsResult,
    ValidationResponse,
)
from lhp.api.views import (
    BlueprintView,
    FlowgroupView,
    GeneratedCodeView,
    PresetView,
    ProcessedFlowgroupView,
    ProjectConfigView,
    SecretReferenceView,
    SubstitutionView,
    TemplateView,
)

if TYPE_CHECKING:
    # Internal orchestrator type, referenced only as a quoted annotation
    # below; never named directly in the public API surface (§1.10, §9.13).
    _Orchestrator = Any


class InspectionFacade:
    """Inspection / read-only operations on a constructed project.

    Fourteen public methods grouped by responsibility — inspection-style
    read-only / informational operations plus the two dependency-output
    paths:

    - ``list_flowgroups``
    - ``process_flowgroup``
    - ``generate_flowgroup_code``
    - ``find_source_yaml_for_flowgroup``
    - ``get_include_patterns``
    - ``get_project_config``
    - ``compute_stats``
    - ``list_blueprints``
    - ``list_presets``
    - ``list_templates``
    - ``analyze_dependencies``
    - ``save_dependency_outputs``
    - ``validate_duplicate_flowgroups``
    - ``build_substitution_view``

    All return frozen DTOs from :mod:`lhp.api.views` /
    :mod:`lhp.api.responses`. No method mutates state; this facade is
    read-only.

    :stability: provisional
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self._logger = logging.getLogger(__name__)

    def list_flowgroups(
        self, *, pipeline_filter: Optional[str] = None
    ) -> Tuple[FlowgroupView, ...]:
        """List discovered flowgroups, optionally filtered by pipeline name.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` from project-config or
            substitution loading, ``LHP-VAL-*`` from flowgroup validation
            during discovery, ``LHP-FILE-*`` for missing paths, and
            ``LHP-MULT-*`` for malformed multi-document YAML.
        """
        all_fgs = self._orchestrator.bootstrap.discover_all_flowgroups()
        flowgroups = (
            all_fgs
            if pipeline_filter is None
            else tuple(fg for fg in all_fgs if fg.pipeline == pipeline_filter)
        )
        file_paths = _flowgroup_file_paths(self._orchestrator, flowgroups)
        return _flowgroups_to_views(flowgroups, file_paths=file_paths)

    def process_flowgroup(
        self, flowgroup_name: str, *, env: str
    ) -> ProcessedFlowgroupView:
        """Process a single flowgroup (expand templates, merge presets, substitute).

        :stability: provisional
        :raises LookupError: if no flowgroup with ``flowgroup_name``
            exists in the project.
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` from substitution-file
            loading, ``LHP-TPL-*`` from template expansion, and
            ``LHP-VAL-*`` from flowgroup-level validation during
            resolution.
        """
        target = _locate_flowgroup_by_name(self._orchestrator, flowgroup_name)
        substitution_mgr = _build_substitution_manager_for_env(
            self._orchestrator.project_root, env
        )
        ctx_in = self._orchestrator.bootstrap.make_context(target)
        ctx_out = self._orchestrator.processing.resolve(ctx_in, substitution_mgr)
        return _flowgroup_to_processed_view(
            ctx_out.flowgroup, file_path=ctx_out.source_yaml
        )

    def generate_flowgroup_code(
        self, flowgroup_name: str, *, env: str
    ) -> GeneratedCodeView:
        """Generate Python source for a single flowgroup without writing to disk.

        :stability: provisional
        :raises LookupError: if no flowgroup with ``flowgroup_name``
            exists in the project.
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` (substitution),
            ``LHP-TPL-*`` (template expansion), ``LHP-VAL-*`` (per-action
            validation during code-generation), and ``LHP-ACTION-*``
            (action-generator failure).
        """
        target = _locate_flowgroup_by_name(self._orchestrator, flowgroup_name)
        substitution_mgr = _build_substitution_manager_for_env(
            self._orchestrator.project_root, env
        )
        ctx_in = self._orchestrator.bootstrap.make_context(target)
        ctx_out = self._orchestrator.processing.resolve(ctx_in, substitution_mgr)
        code = self._orchestrator.codegen.generate(
            ctx_out.flowgroup,
            substitution_mgr,
            output_dir=None,
            source_yaml=ctx_out.source_yaml,
            env=env,
        )
        return GeneratedCodeView(
            flowgroup_name=ctx_out.flowgroup.flowgroup,
            pipeline=ctx_out.flowgroup.pipeline,
            generated_code=code,
            target_filename=f"{ctx_out.flowgroup.flowgroup}.py",
        )

    def find_source_yaml_for_flowgroup(self, flowgroup_name: str) -> Optional[Path]:
        """Return the source YAML path for the named flowgroup, or ``None``.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` / ``LHP-VAL-*`` /
            ``LHP-FILE-*`` if flowgroup discovery (driven on first
            access) fails. A pure name-miss returns ``None`` rather
            than raising :class:`LookupError`.
        """
        try:
            target = _locate_flowgroup_by_name(self._orchestrator, flowgroup_name)
        except LookupError:
            return None
        result: Optional[Path] = (
            self._orchestrator.discovery.find_source_yaml_for_flowgroup(target)
        )
        return result

    def get_include_patterns(self) -> Tuple[str, ...]:
        """Return the project's flowgroup include glob patterns.

        :stability: provisional
        :raises: None — reads already-loaded project configuration.
        """
        return tuple(self._orchestrator.discovery.get_include_patterns())

    def get_project_config(self) -> ProjectConfigView:
        """Return a frozen view of the project's loaded ``lhp.yaml``.

        :stability: provisional
        :raises: None — converts the already-loaded project configuration
            into a frozen DTO.
        """
        return _project_config_to_view(self._orchestrator.project_config)

    def compute_stats(self) -> StatsResult:
        """Aggregate project statistics over every discovered flowgroup.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` / ``LHP-VAL-*`` /
            ``LHP-FILE-*`` / ``LHP-MULT-*`` propagated from flowgroup
            discovery — same families as :meth:`list_flowgroups`.
        """
        flowgroups = self._orchestrator.bootstrap.discover_all_flowgroups()
        return _build_stats_result(flowgroups)

    def list_blueprints(
        self, *, include_instances: bool = False
    ) -> Tuple[BlueprintView, ...]:
        """List all blueprints declared in the project.

        ``include_instances=True`` populates each view's ``instances``
        tuple (resolved flowgroup count + pipelines per instance file).

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` / ``LHP-VAL-*`` /
            ``LHP-FILE-*`` propagated from blueprint discovery and
            instance-file parsing.
        """
        return _build_blueprint_views(
            self._orchestrator.blueprint_discoverer,
            include_instances=include_instances,
        )

    def list_presets(self) -> Tuple[PresetView, ...]:
        """List all presets declared under the project's ``presets/`` directory.

        :stability: provisional
        :raises: None — per-file parse errors are logged and skipped so
            the inspection CLI can still enumerate the remaining presets.
        """
        from lhp.parsers.yaml_parser import YAMLParser

        presets_dir = self._orchestrator.project_root / "presets"
        if not presets_dir.exists():
            return ()
        preset_files = sorted(
            list(presets_dir.glob("*.yaml")) + list(presets_dir.glob("*.yml"))
        )
        parser = YAMLParser()  # type: ignore[no-untyped-call]
        views: List[PresetView] = []
        for path in preset_files:
            try:
                preset = parser.parse_preset(path)
            except Exception as exc:  # noqa: BLE001 — soft-fail per CLI
                self._logger.warning(f"Could not parse preset {path}: {exc}")
                continue
            views.append(_preset_to_view(preset, path))
        return tuple(views)

    def list_templates(self) -> Tuple[TemplateView, ...]:
        """List all templates declared under the project's ``templates/`` directory.

        :stability: provisional
        :raises: None — per-file parse errors are logged and skipped so
            the inspection CLI can still enumerate the remaining templates.
        """
        from lhp.parsers.yaml_parser import YAMLParser

        templates_dir = self._orchestrator.project_root / "templates"
        if not templates_dir.exists():
            return ()
        template_files = sorted(
            list(templates_dir.glob("*.yaml")) + list(templates_dir.glob("*.yml"))
        )
        parser = YAMLParser()  # type: ignore[no-untyped-call]
        views: List[TemplateView] = []
        for path in template_files:
            try:
                template = parser.parse_template_raw(path)
            except Exception as exc:  # noqa: BLE001 — soft-fail per CLI
                self._logger.warning(f"Could not parse template {path}: {exc}")
                continue
            views.append(_template_to_view(template, path))
        return tuple(views)

    def analyze_dependencies(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        expand_blueprints: bool = False,
        blueprint_filter: Optional[str] = None,
    ) -> DependencyAnalysisResult:
        """Run dependency analysis and return a frozen, flattened result.

        ``pipeline_filter`` restricts the graph to the named pipeline.
        ``expand_blueprints=False`` dedupes synthetic flowgroups by
        ``(blueprint_name, spec_index)`` so the resulting graph stays
        readable at scale; ``True`` renders the literal expansion (one
        node per blueprint × instance × spec). ``blueprint_filter``
        further restricts the graph to synthetic flowgroups expanded
        from the named blueprint.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` / ``LHP-VAL-*`` /
            ``LHP-FILE-*`` / ``LHP-MULT-*`` propagated from flowgroup
            discovery and dependency analysis.
        """
        dep_service = self._orchestrator.dependencies
        dep_service.set_blueprint_view_mode(
            expand_blueprints=expand_blueprints, blueprint=blueprint_filter
        )
        flowgroups = dep_service.get_flowgroups(pipeline_filter=pipeline_filter)
        internal = dep_service.analyze(dep_service.build_graphs(flowgroups))
        return _dependency_result_to_view(internal)

    def save_dependency_outputs(
        self,
        *,
        formats: Sequence[str],
        output_dir: Path,
        pipeline_filter: Optional[str] = None,
        expand_blueprints: bool = False,
        blueprint_filter: Optional[str] = None,
        job_name: Optional[str] = None,
        job_config_path: Optional[str] = None,
        bundle_output: bool = False,
    ) -> DependencyOutputsResult:
        """Run dependency analysis and write requested outputs to disk.

        ``formats`` accepts any combination of ``"dot"``, ``"json"``,
        ``"text"``, ``"job"``, or ``"all"`` (expands to all four).
        Blueprint-view and pipeline-filter parameters mirror
        :meth:`analyze_dependencies`. ``job_name`` /
        ``job_config_path`` / ``bundle_output`` shape the ``"job"``
        format only and have no effect on the others.

        Returns a frozen :class:`DependencyOutputsResult` enumerating
        every generated file with its format name and optional job-name
        label.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` / ``LHP-VAL-*`` /
            ``LHP-FILE-*`` / ``LHP-MULT-*`` propagated from discovery
            and analysis, plus ``LHP-IO-*`` / :class:`OSError` for
            filesystem failures while writing the requested formats.
        """
        from lhp.core.dependencies.output import DependencyOutputManager

        dep_service = self._orchestrator.dependencies
        dep_service.set_blueprint_view_mode(
            expand_blueprints=expand_blueprints, blueprint=blueprint_filter
        )
        flowgroups = dep_service.get_flowgroups(pipeline_filter=pipeline_filter)
        graphs = dep_service.build_graphs(flowgroups)
        internal = dep_service.analyze(graphs)

        output_manager = DependencyOutputManager()
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

    def build_substitution_view(self, env: str) -> SubstitutionView:
        """Build a frozen view of the resolved substitution context for ``env``.

        Constructs an :class:`EnhancedSubstitutionManager` for the
        named environment (falling back to an empty manager when the
        ``substitutions/<env>.yaml`` file is absent — same convention
        as :meth:`process_flowgroup`) and projects its state onto a
        :class:`SubstitutionView`:

        - ``tokens`` — fully-expanded mappings, flattened to strings.
          Internal nested ``dict`` / ``list`` values (used by
          prefix/suffix rules) are coerced to ``str`` since DTO
          fields must be flat per §4.8.
        - ``raw_mappings`` — the same expanded mappings, but un-coerced:
          nested ``dict`` / ``list`` values are preserved as-is so
          consumers can inspect the structure that ``tokens`` flattens
          (per §4.8, ``raw_mappings`` is the canonical ``JSONValue``
          companion to a flat string field).
        - ``secret_references`` — every observed
          ``${secret:scope/key}`` reference, sorted by
          ``(scope, key)`` for deterministic ordering.
        - ``default_secret_scope`` — the configured fallback scope or
          ``None``.

        Note: ``secret_references`` is populated only when the manager
        has actually run ``substitute_yaml`` over some payload. A
        freshly-constructed manager has an empty set; this method
        returns that empty state when the manager has not yet
        processed any data.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` from substitution-file
            loading (YAML parse failures, malformed structure). A
            missing substitution file is not an error — an empty
            manager is constructed instead.
        """
        from lhp.core.processing.substitution import EnhancedSubstitutionManager

        manager: EnhancedSubstitutionManager = _build_substitution_manager_for_env(
            self._orchestrator.project_root, env
        )

        # ``manager.mappings`` is annotated ``Dict[str, str]`` on the
        # internal class, but the YAML loader can store nested
        # ``dict``/``list`` values when prefix/suffix rules are used
        # (see substitution.py lines 155, 169). ``str(value)`` is a
        # no-op for actual strings and collapses nested rule objects
        # to a flat repr for ``tokens`` — that flat field must stay
        # flat per §4.8. The nesting is not lost: it is preserved
        # verbatim on ``raw_mappings`` below (a ``JSONValue`` field).
        tokens: Dict[str, str] = {
            key: str(value) for key, value in manager.mappings.items()
        }

        sorted_refs = sorted(manager.secret_references, key=lambda r: (r.scope, r.key))
        secret_views: Tuple[SecretReferenceView, ...] = tuple(
            SecretReferenceView(scope=ref.scope, key=ref.key) for ref in sorted_refs
        )

        return SubstitutionView(
            env=env,
            tokens=tokens,
            raw_mappings=dict(manager.mappings),
            secret_references=secret_views,
            default_secret_scope=manager.default_secret_scope,
        )

    def validate_duplicate_flowgroups(
        self, flowgroups: Sequence[FlowgroupView]
    ) -> ValidationResponse:
        """Validate uniqueness of ``(pipeline, flowgroup)`` pairs across a view set.

        :stability: provisional
        :raises: None — duplicate findings are surfaced on the returned
            :class:`ValidationResponse` rather than raised (§4.8).
        """
        return _duplicates_to_validation_response(flowgroups)
