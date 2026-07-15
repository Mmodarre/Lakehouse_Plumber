"""Private module — implementation of the public :class:`InspectionFacade`.

Underscore-prefixed: not part of the import surface; external callers
MUST import :class:`InspectionFacade` from :mod:`lhp.api` (re-exported
via :mod:`lhp.api.facade`). This split exists purely to keep
``lhp/api/facade.py`` under the constitution §3.3 soft cap (500 lines)
while the inspection surface absorbs its read-only methods (plus one
cache-management method).

:stability: internal
"""

from __future__ import annotations

import copy
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
)

from lhp.api._inspection_converters import (
    _build_stats_result,
    _build_substitution_manager_for_env,
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
from lhp.api._operational_metadata_converter import _operational_metadata_to_view
from lhp.api.responses import (
    StatsResult,
    ValidationResponse,
)
from lhp.api.views import (
    BlueprintView,
    FlowgroupView,
    GeneratedCodeView,
    OperationalMetadataView,
    PresetResolutionResult,
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

    Fifteen public methods grouped by responsibility — inspection-style
    read-only / informational operations:

    - ``list_flowgroups``
    - ``process_flowgroup``
    - ``generate_flowgroup_code``
    - ``find_source_yaml_for_flowgroup``
    - ``get_include_patterns``
    - ``get_project_config``
    - ``get_operational_metadata``
    - ``compute_stats``
    - ``list_blueprints``
    - ``list_presets``
    - ``resolve_preset``
    - ``list_templates``
    - ``validate_duplicate_flowgroups``
    - ``build_substitution_view``

    plus one cache-management operation:

    - ``invalidate_discovery_cache``

    The read methods return frozen DTOs from :mod:`lhp.api.views` /
    :mod:`lhp.api.responses` and never mutate project state.
    ``invalidate_discovery_cache`` is the sole exception: it clears the
    memoized flowgroup discovery so the next read re-reads disk.

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

    def get_operational_metadata(self) -> OperationalMetadataView:
        """Return the operational-metadata columns and presets for the project.

        Resolves the available columns with the generator's REPLACE
        semantics (project-declared ``operational_metadata.columns`` suppress
        the five built-ins wholesale; otherwise the built-ins are returned),
        so a UI offering these columns matches what generation would apply.
        ``presets`` is populated from ``operational_metadata.presets`` and is
        empty when the project declares none. Column ``expression`` values
        pass substitution tokens through verbatim.

        :stability: provisional
        :raises: None — projects the already-loaded project configuration.
        """
        return _operational_metadata_to_view(self._orchestrator.project_config)

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
            except Exception as exc:
                self._logger.warning(f"Could not parse preset {path}: {exc}")
                continue
            views.append(_preset_to_view(preset, path))
        return tuple(views)

    def resolve_preset(self, name: str) -> PresetResolutionResult:
        """Resolve a preset's ``extends`` chain and deep-merged configuration.

        Resolution is delegated to the project's preset manager:
        ``merged_config`` is the base→leaf deep merge of every
        ``defaults`` payload along the chain (per key, the more-derived
        preset's value wins; nested mappings merge recursively;
        ``operational_metadata`` lists are concatenated with
        order-preserving dedup instead of replaced). ``chain`` holds the
        preset names base→leaf, derived by walking ``extends`` upward
        from ``name``.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-ACT-001`` when ``name`` — or
            any preset referenced through ``extends`` along the chain —
            is not declared in the project's ``presets/`` directory;
            ``LHP-DEP-022`` when the ``extends`` chain is circular.
        """
        manager = self._orchestrator.preset_manager
        # Raises LHP-ACT-001 / LHP-DEP-022 before the chain walk below,
        # so the walk can assume every name resolves acyclically.
        merged = manager.resolve_preset_chain([name])

        leaf_to_base: List[str] = []
        visited: set[str] = set()
        current: Optional[str] = name
        while current is not None and current not in visited:
            visited.add(current)
            leaf_to_base.append(current)
            current = manager.presets[current].extends

        # Deep-copied so the DTO payload cannot alias the orchestrator's
        # live preset cache: ``_resolve_preset_inheritance`` returns
        # ``preset.defaults`` itself for base presets and ``_deep_merge``
        # copies only the touched levels, so without this copy a caller
        # mutating ``merged_config`` would corrupt later preset
        # resolution in-process (§4.4).
        return PresetResolutionResult(
            name=name,
            chain=tuple(reversed(leaf_to_base)),
            merged_config=copy.deepcopy(merged),
        )

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
            except Exception as exc:
                self._logger.warning(f"Could not parse template {path}: {exc}")
                continue
            views.append(_template_to_view(template, path))
        return tuple(views)

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

    def invalidate_discovery_cache(self) -> None:
        """Drop the memoized flowgroup discovery so the next read re-reads disk.

        The one cache-management operation on this otherwise read-only facade:
        it clears the orchestrator's flowgroup-discovery memo (and the
        synthetic-flowgroup / monitoring state derived from it) so a project
        edit made outside this process becomes visible to the inspection reads
        above without rebuilding the whole service graph. Deliberately narrow —
        it does NOT touch the dependency-analysis graph memo or the persisted
        graph cache (which back the ``lhp web`` serve-stale model and are
        refreshed only by an explicit graph rebuild).

        :stability: provisional
        :raises: None.
        """
        self._orchestrator.bootstrap.reset_discovery_cache()
