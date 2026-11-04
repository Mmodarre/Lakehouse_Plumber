"""Per-action generator context builder + post-call output collector.

:stability: internal
"""

import logging
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Set, Tuple

from lhp.models import Action, ActionType, FlowGroup, TransformType
from lhp.models.processing import CopiedModuleRecord

from ..processing.substitution import EnhancedSubstitutionManager
from ..registry import ReadMode, determine_action_subtype

if TYPE_CHECKING:
    from ..registry import ActionRegistry
    from .dq_fusion import DqFusionPlan, FusedDq


class GenerationContextBuilder:
    """Build the context dict passed into every action generator + collect outputs.

    ``project_config`` and ``project_root`` are stored once on the instance
    (they are project-scoped — they do not change across flowgroups in the
    same run). All other inputs to :meth:`build` are flowgroup-scoped and
    flow through as method arguments.

    :stability: internal
    """

    def __init__(
        self,
        *,
        project_config=None,
        project_root: Optional[Path] = None,
    ) -> None:
        self.project_config = project_config
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        # Per-builder signature cache shared across every flowgroup context
        # this builder produces. Lifecycle is per-builder-instance: the
        # coordinator constructs one builder per pipeline run, and a
        # spawn-pool worker constructs its own builder, so this dict is
        # effectively per-pipeline / per-worker. Phase A is serial, so no
        # lock is needed. Keyed by the *resolved absolute path* (str) of the
        # source-function file; downstream tasks that write into the cache
        # MUST use that same key convention. Value type is the parsed
        # signature record (``FunctionSignature``, defined in
        # ``generators/write/``); it is typed ``Any`` here because importing
        # that type would invert the §5 layering direction (``core/codegen``
        # must not import ``generators/``). This module is ``:stability:
        # internal`` so strict-mypy (§4.3, binds only ``lhp/api/``) does not
        # gate the loosened type.
        self._source_function_signature_cache: Dict[str, Any] = {}
        # Companion per-builder cache for parsed ASTs (``ast.Module``),
        # shared by every path-keyed AST consumer (snapshot-CDC signature
        # extraction and helper-closure discovery) so each user file is
        # parsed at most once per pipeline. Same lifecycle, key convention
        # (resolved absolute path str), and §4.3 ``Any`` rationale as the
        # signature cache above; the value is an ``ast.Module``, typed
        # ``Any`` to avoid importing ``ast`` into a typing-only position.
        self._source_parse_cache: Dict[str, Any] = {}

    def build(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        output_dir: Optional[Path],
        source_yaml: Optional[Path],
        env: Optional[str],
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
        *,
        action: Optional[Action] = None,
        fusion_plan: Optional["DqFusionPlan"] = None,
        action_registry: Optional["ActionRegistry"] = None,
    ) -> Dict[str, Any]:
        """The context handed to ``action``'s generator.

        ``action`` (the one about to be generated) decides two entries:
        ``effective_read_mode`` for a data_quality transform, which
        ``action_registry`` supplies the generator declarations for, and
        ``fused_dq`` for a write that ``fusion_plan`` fuses a data_quality
        action onto. Both are ``None`` for any other action.
        """
        project_root = self.project_root or Path.cwd()
        return {
            # The read the action's code makes, for a generator whose read
            # follows its source view (see :meth:`effective_read_mode`);
            # ``None`` for the others, which read their own declaration.
            "effective_read_mode": self._action_read_mode(
                action, flowgroup, substitution_mgr, action_registry
            ),
            # The data_quality action fused onto a materialized view (its
            # rules and source view move onto the view); ``None`` otherwise.
            "fused_dq": (
                fusion_plan.for_write(action)
                if fusion_plan is not None
                and action is not None
                and action.type == ActionType.WRITE
                else None
            ),
            "flowgroup": flowgroup,
            "substitution_manager": substitution_mgr,
            "spec_dir": project_root,  # For backward compatibility
            "project_root": project_root,  # Explicit project root for external file loading
            "preset_config": preset_config,
            "project_config": self.project_config,
            "output_dir": output_dir,
            "source_yaml": source_yaml,
            "environment": env,
            # Per-flowgroup accumulator for secret references collected by
            # generators that run their own _process_string calls. The
            # substitution manager keeps the canonical set; this mirror is
            # populated by generators for legacy callers that read from
            # the context dict.
            "secret_references": set(),
            # When present, ``copy_user_module_for_pipeline`` appends
            # CopiedModuleRecord entries here instead of writing to disk, so
            # writes can be replayed on the main thread.
            "phase_a_records": phase_a_records,
            # Inline auxiliary Python modules carried on the FlowGroupContext
            # (e.g. monitoring's jobs_stats_loader.py). Read by
            # ``copy_user_module_for_pipeline`` to skip on-disk lookup.
            "auxiliary_files": auxiliary_files or {},
            # Per-builder signature cache (same dict object across every
            # flowgroup context this builder produces). Keyed by the resolved
            # absolute path (str) of the source-function file; parse + signature
            # extraction happens once per unique source file per pipeline, and
            # the per-flowgroup work is the cheap param check. See __init__ for
            # the §4.3 type rationale.
            "source_function_signature_cache": self._source_function_signature_cache,
            # Per-builder tree cache (same dict object across every flowgroup
            # context this builder produces). Keyed by the resolved absolute
            # path (str) of a user source file; shared by snapshot-CDC
            # signature extraction and helper-closure discovery so each file
            # is parsed once per pipeline. See __init__ for the §4.3 type
            # rationale.
            "source_parse_cache": self._source_parse_cache,
        }

    def _action_read_mode(
        self,
        action: Optional[Action],
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        action_registry: Optional["ActionRegistry"],
    ) -> Optional[ReadMode]:
        """:meth:`effective_read_mode` of a data_quality transform, else ``None``."""
        if (
            action is None
            or action.type != ActionType.TRANSFORM
            or action.transform_type != TransformType.DATA_QUALITY
        ):
            return None
        if action_registry is None:
            raise TypeError("build() needs action_registry for a data_quality action")
        return self.effective_read_mode(
            action, flowgroup, substitution_mgr, action_registry
        )

    def effective_read_mode(
        self,
        action: Action,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        action_registry: "ActionRegistry",
    ) -> Optional[ReadMode]:
        """The read ``action``'s code makes, among ``flowgroup``'s actions.

        :func:`lhp.core.dependencies.effective_read_mode`, reading SQL bodies
        under the project root through ``substitution_mgr`` and each action's
        generator through ``action_registry``. A spawned worker's default
        registry is empty, so code generation passes the one it dispatches by.
        """
        # Lazy: lhp.core.dependencies imports services that import codegen.
        from ..dependencies import effective_read_mode

        return effective_read_mode(
            action,
            flowgroup.actions,
            registry=action_registry,
            project_root=self.project_root or Path.cwd(),
            substitution_mgr=substitution_mgr,
        )

    def dq_fusion_plan(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        action_registry: "ActionRegistry",
    ) -> "DqFusionPlan":
        """The data_quality actions of ``flowgroup`` that fuse into a
        materialized view (:func:`~.dq_fusion.plan_dq_fusion`), each with the
        operational-metadata columns its own generator renders.

        The columns come from the data_quality generator's own
        ``_get_operational_metadata``, so they are the ones validation checks
        under the data_quality action; the view renders them as given.
        """
        from .dq_fusion import plan_dq_fusion

        plan = plan_dq_fusion(
            flowgroup.actions,
            registry=action_registry,
            project_root=self.project_root or Path.cwd(),
            substitution_mgr=substitution_mgr,
        )
        return replace(
            plan,
            fusions=tuple(
                self._with_metadata(
                    f, flowgroup, substitution_mgr, preset_config, action_registry
                )
                for f in plan.fusions
            ),
        )

    def _with_metadata(
        self,
        fusion: "FusedDq",
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        action_registry: "ActionRegistry",
    ) -> "FusedDq":
        generator = action_registry.get_generator(
            fusion.dq_action.type, determine_action_subtype(fusion.dq_action)
        )
        before = set(generator.imports)
        context = self.build(
            flowgroup, substitution_mgr, preset_config, None, None, None
        )
        _, columns = generator._get_operational_metadata(fusion.dq_action, context)
        return replace(
            fusion,
            metadata_columns=dict(columns),
            metadata_imports=tuple(sorted(set(generator.imports) - before)),
        )

    def collect_outputs(self, generator) -> Tuple[Set[str], Set[str]]:
        imports: Set[str] = set()
        pre_pipeline_statements: Set[str] = set()

        import_manager = getattr(generator, "get_import_manager", lambda: None)()
        if import_manager:
            consolidated_imports = import_manager.get_consolidated_imports()
            imports.update(consolidated_imports)
            self.logger.debug(
                f"Used ImportManager: {len(consolidated_imports)} imports"
            )
        else:
            imports.update(generator.imports)

        # Collect pre-pipeline statements (e.g. cloudpickle registration for
        # custom data sources/sinks).
        get_pre = getattr(generator, "get_pre_pipeline_statements", None)
        if callable(get_pre):
            pre_pipeline_statements.update(get_pre())

        return imports, pre_pipeline_statements
