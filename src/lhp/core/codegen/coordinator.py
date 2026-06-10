"""Code generation service for LakehousePlumber.

Composition root for codegen. Delegates section dispatch, write-action
grouping, per-action context building, secret rewriting, and final module
assembly to dedicated sub-services. See :mod:`lhp.core.codegen` modules
``action_dispatch``, ``grouping``, ``context``, ``secrets``, ``assembler``.

:stability: provisional
"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional, Tuple

from lhp.models import Action, ActionType, FlowGroup
from lhp.models.processing import CopiedModuleRecord

from ...utils.performance_timer import perf_timer
from .._interfaces import BaseCodeGenerationService
from ..processing.substitution import EnhancedSubstitutionManager
from .action_dispatch import ActionDispatcher
from .assembler import CodeAssembler
from .context import GenerationContextBuilder
from .grouping import WriteActionGrouper
from .secrets import SecretSubstitutor

if TYPE_CHECKING:
    pass


class CodeGenerationService(BaseCodeGenerationService):
    """
    Service for generating Python code from flowgroup configurations.

    Composition root. The five internal sub-services
    (:class:`ActionDispatcher`, :class:`WriteActionGrouper`,
    :class:`GenerationContextBuilder`, :class:`SecretSubstitutor`,
    :class:`CodeAssembler`) carry out the per-stage work; this class wires
    them together and runs the top-level orchestration in
    :meth:`generate_flowgroup_code`.

    :stability: provisional
    """

    def __init__(
        self,
        action_registry=None,
        dependency_resolver=None,
        preset_manager=None,
        project_config=None,
        project_root=None,
    ):
        self.action_registry = action_registry
        self.dependency_resolver = dependency_resolver
        self.preset_manager = preset_manager
        self.project_config = project_config
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)

        # Internal sub-services (composition — none escape via the public surface).
        self._grouping = WriteActionGrouper()
        self._context = GenerationContextBuilder(
            project_config=project_config,
            project_root=project_root,
        )
        self._secrets = SecretSubstitutor()
        self._assembler = CodeAssembler()
        self._dispatch = ActionDispatcher(
            action_registry=action_registry,
            grouping=self._grouping,
            context_builder=self._context,
        )

    def generate(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        *,
        output_dir: Optional[Path] = None,
        source_yaml: Optional[Path] = None,
        env: Optional[str] = None,
        include_tests: bool = False,
        phase_a_records: Optional[Tuple[CopiedModuleRecord, ...]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> str:
        """Canonical entry point — satisfies BaseCodeGenerationService.generate (§4.12)."""
        return self.generate_flowgroup_code(
            flowgroup,
            substitution_mgr,
            output_dir=output_dir,
            source_yaml=source_yaml,
            env=env,
            include_tests=include_tests,
            phase_a_records=(
                list(phase_a_records) if phase_a_records is not None else None
            ),
            auxiliary_files=auxiliary_files,
        )

    def generate_flowgroup_code(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        output_dir: Optional[Path] = None,
        source_yaml: Optional[Path] = None,
        env: Optional[str] = None,
        include_tests: bool = False,
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> str:
        """Generate complete Python code for a flowgroup.

        Args:
            phase_a_records: When supplied, ``CopiedModuleRecord`` entries are
                appended here instead of writing to disk (parallel worker path).
                ``None`` means disk writes happen inline (legacy single-threaded path).
            auxiliary_files: ``{module_path: source_str}`` inline Python modules
                carried on :class:`FlowGroupContext`; used by
                ``custom_python_functions`` generators in lieu of an on-disk file.
        """
        fg = flowgroup.flowgroup
        self.logger.debug(
            f"Starting code generation for flowgroup '{fg}' in pipeline '{flowgroup.pipeline}'"
        )

        with perf_timer(
            f"resolve_dependencies [{fg}]", category="resolve_dependencies"
        ):
            ordered_actions = self.dependency_resolver.resolve_dependencies(
                flowgroup.actions
            )
        self.logger.debug(
            f"Resolved action ordering: {[a.name for a in ordered_actions]} ({len(ordered_actions)} actions)"
        )

        preset_config = {}
        if flowgroup.presets:
            preset_config = self.preset_manager.resolve_preset_chain(flowgroup.presets)
            self.logger.debug(
                f"Resolved preset chain for flowgroup '{fg}': {flowgroup.presets}"
            )

        if not include_tests:
            non_test_actions = [
                action for action in ordered_actions if action.type != ActionType.TEST
            ]
            if not non_test_actions:
                self.logger.info(
                    f"Skipping test-only flowgroup: {fg} (--include-tests not specified)"
                )
                return ""

        with perf_timer(
            f"generate_action_sections [{fg}]", category="generate_action_sections"
        ):
            (
                generated_sections,
                all_imports,
                pre_pipeline_statements,
            ) = self._dispatch.generate_action_sections(
                flowgroup,
                ordered_actions,
                substitution_mgr,
                preset_config,
                output_dir,
                source_yaml,
                env,
                include_tests,
                phase_a_records=phase_a_records,
                auxiliary_files=auxiliary_files,
            )

        # Substitution emits ``__SECRET_scope_key__`` placeholders so Jinja
        # templates can naively wrap values in Python string literals; the
        # post-pass rewrites whole-string placeholders to bare
        # ``dbutils.secrets.get(...)`` calls and embedded ones to f-strings.
        with perf_timer(f"assemble_code [{fg}]", category="assemble_code"):
            complete_code = self._secrets.apply(generated_sections, substitution_mgr)
            return self._assembler.assemble(
                flowgroup,
                all_imports,
                pre_pipeline_statements,
                complete_code,
            )

    # Public forwarders preserved as part of the CodeGenerationService
    # surface for callers that pin to the class. See manifest §5.2.

    def determine_action_subtype(self, action: Action) -> str:
        """Forward to :meth:`ActionDispatcher.determine_action_subtype`."""
        return self._dispatch.determine_action_subtype(action)

    def group_write_actions_by_target(
        self, write_actions: List[Action]
    ) -> Dict[str, List[Action]]:
        """Forward to :meth:`WriteActionGrouper.group_write_actions_by_target`."""
        return self._grouping.group_write_actions_by_target(write_actions)

    def create_combined_write_action(
        self, actions: List[Action], target_table: str
    ) -> Action:
        """Forward to :meth:`WriteActionGrouper.create_combined_write_action`."""
        return self._grouping.create_combined_write_action(actions, target_table)

    # Private-name forwarders preserved so existing tests that pin to
    # the previous private surface continue to work. Intentionally
    # underscore-prefixed (NOT public API); delegate to canonical
    # implementations on the internal sub-services.

    def _extract_source_views_from_action(self, source) -> List[str]:
        """Test-surface forwarder to :meth:`WriteActionGrouper._extract_source_views_from_action`."""
        return self._grouping._extract_source_views_from_action(source)
