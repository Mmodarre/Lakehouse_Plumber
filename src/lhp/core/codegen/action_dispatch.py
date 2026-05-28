"""Action-type dispatch for code generation.

:stability: internal
"""

import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Set, Tuple

from ...errors import ErrorCategory, LHPError, LHPValidationError
from ...generators.python_file_copier import CopiedModuleRecord
from ...models.config import Action, ActionType, FlowGroup, TransformType
from ..processing.substitution import EnhancedSubstitutionManager

if TYPE_CHECKING:
    from .context import GenerationContextBuilder
    from .grouping import WriteActionGrouper


class ActionDispatcher:
    """Dispatch ordered actions to per-type code generators.

    Owns the section-header text, partitions transforms into regular vs
    data-quality, groups write actions by target table (delegating to
    :class:`WriteActionGrouper`), and builds the per-call generator context
    (delegating to :class:`GenerationContextBuilder`).

    Stateless apart from the three injected collaborators and the logger.

    :stability: internal
    """

    def __init__(
        self,
        *,
        action_registry,
        grouping: "WriteActionGrouper",
        context_builder: "GenerationContextBuilder",
    ) -> None:
        self.action_registry = action_registry
        self._grouping = grouping
        self._context = context_builder
        self.logger = logging.getLogger(__name__)

    def generate_action_sections(
        self,
        flowgroup: FlowGroup,
        ordered_actions: List[Action],
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        output_dir: Optional[Path],
        source_yaml: Optional[Path],
        env: Optional[str],
        include_tests: bool,
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> Tuple[List[str], Set[str], Set[str]]:
        """Generate code sections for all actions."""
        action_groups = defaultdict(list)
        for action in ordered_actions:
            action_groups[action.type].append(action)

        generated_sections = []
        all_imports: Set[str] = set()
        pre_pipeline_statements: Set[str] = set()

        all_imports.add("from pyspark import pipelines as dp")

        section_headers = {
            ActionType.LOAD: "SOURCE VIEWS",
            ActionType.WRITE: "TARGET TABLES",
            ActionType.TEST: "DATA QUALITY TESTS",
        }

        action_types = [ActionType.LOAD, ActionType.TRANSFORM, ActionType.WRITE]
        if include_tests:
            action_types.append(ActionType.TEST)

        self.logger.debug(
            f"Action type groups: {{{', '.join(f'{t.value}: {len(action_groups[t])}' for t in action_groups)}}}"
        )

        common_kwargs = dict(
            flowgroup=flowgroup,
            substitution_mgr=substitution_mgr,
            preset_config=preset_config,
            output_dir=output_dir,
            source_yaml=source_yaml,
            env=env,
            phase_a_records=phase_a_records,
            auxiliary_files=auxiliary_files,
        )

        for action_type in action_types:
            if action_type not in action_groups:
                continue

            if action_type == ActionType.TRANSFORM:
                regular_transforms = [
                    a
                    for a in action_groups[ActionType.TRANSFORM]
                    if a.transform_type != TransformType.DATA_QUALITY
                ]
                dq_transforms = [
                    a
                    for a in action_groups[ActionType.TRANSFORM]
                    if a.transform_type == TransformType.DATA_QUALITY
                ]

                if regular_transforms:
                    section_header = f"""
# {"=" * 76}
# TRANSFORMATION VIEWS
# {"=" * 76}"""
                    generated_sections.append(section_header)
                    sections, imports, pre_stmts = self._generate_regular_actions(
                        regular_transforms, **common_kwargs
                    )
                    generated_sections.extend(sections)
                    all_imports.update(imports)
                    pre_pipeline_statements.update(pre_stmts)

                if dq_transforms:
                    section_header = f"""
# {"=" * 76}
# DATA QUALITY & QUARANTINE
# {"=" * 76}"""
                    generated_sections.append(section_header)
                    sections, imports, pre_stmts = self._generate_dq_actions(
                        dq_transforms, **common_kwargs
                    )
                    generated_sections.extend(sections)
                    all_imports.update(imports)
                    pre_pipeline_statements.update(pre_stmts)
            else:
                header_text = section_headers.get(action_type, str(action_type).upper())
                section_header = f"""
# {"=" * 76}
# {header_text}
# {"=" * 76}"""
                generated_sections.append(section_header)

                if action_type == ActionType.WRITE:
                    sections, imports, pre_stmts = self._generate_write_actions(
                        action_groups[action_type], **common_kwargs
                    )
                else:
                    sections, imports, pre_stmts = self._generate_regular_actions(
                        action_groups[action_type], **common_kwargs
                    )

                generated_sections.extend(sections)
                all_imports.update(imports)
                pre_pipeline_statements.update(pre_stmts)

        self.logger.debug(
            f"Collected {len(all_imports)} total imports, "
            f"{len(pre_pipeline_statements)} pre-pipeline statements"
        )
        return generated_sections, all_imports, pre_pipeline_statements

    def _generate_write_actions(
        self,
        write_actions: List[Action],
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        output_dir: Optional[Path],
        source_yaml: Optional[Path],
        env: Optional[str],
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> Tuple[List[str], Set[str], Set[str]]:
        """Generate code for write actions with target grouping."""
        sections = []
        imports: Set[str] = set()
        pre_pipeline_statements: Set[str] = set()

        grouped_actions = self._grouping.group_write_actions_by_target(write_actions)
        self.logger.debug(
            f"Grouped {len(write_actions)} write actions into {len(grouped_actions)} target table(s)"
        )

        for target_table, actions in grouped_actions.items():
            try:
                first_action = actions[0]
                sub_type = self.determine_action_subtype(first_action)
                generator = self.action_registry.get_generator(
                    first_action.type, sub_type
                )

                combined_action = self._grouping.create_combined_write_action(
                    actions, target_table
                )

                context = self._context.build(
                    flowgroup,
                    substitution_mgr,
                    preset_config,
                    output_dir,
                    source_yaml,
                    env,
                    phase_a_records=phase_a_records,
                    auxiliary_files=auxiliary_files,
                )
                action_code = generator.generate(combined_action, context)
                sections.append(action_code)

                section_imports, section_pre = self._context.collect_outputs(generator)
                imports.update(section_imports)
                pre_pipeline_statements.update(section_pre)

            except LHPError:
                raise  # Re-raise LHPError as-is
            except Exception as e:
                action_names = [a.name for a in actions]
                raise LHPError(
                    category=ErrorCategory.ACTION,
                    code_number="002",
                    title="Write action code generation failed",
                    details=(
                        f"Error generating code for write actions {action_names}: {e}"
                    ),
                    suggestions=[
                        "Check write action configuration for the target table",
                        "Verify source views and write target settings",
                        "Run 'lhp validate' for detailed diagnostics",
                    ],
                    context={
                        "Target Table": target_table,
                        "Actions": action_names,
                    },
                ) from e

        return sections, imports, pre_pipeline_statements

    def _generate_regular_actions(
        self,
        actions: List[Action],
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        output_dir: Optional[Path],
        source_yaml: Optional[Path],
        env: Optional[str],
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> Tuple[List[str], Set[str], Set[str]]:
        """Generate code for regular (non-write) actions."""
        sections = []
        imports: Set[str] = set()
        pre_pipeline_statements: Set[str] = set()

        for action in actions:
            code, section_imports, section_pre = self._generate_one_action_code(
                action,
                flowgroup,
                substitution_mgr,
                preset_config,
                output_dir,
                source_yaml,
                env,
                phase_a_records=phase_a_records,
                auxiliary_files=auxiliary_files,
            )
            sections.append(code)
            imports.update(section_imports)
            pre_pipeline_statements.update(section_pre)

        return sections, imports, pre_pipeline_statements

    def _generate_dq_actions(
        self,
        actions: List[Action],
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        output_dir: Optional[Path],
        source_yaml: Optional[Path],
        env: Optional[str],
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> Tuple[List[str], Set[str], Set[str]]:
        """Generate code for data quality actions with sub-headers."""
        sections = []
        imports: Set[str] = set()
        pre_pipeline_statements: Set[str] = set()

        for action in actions:
            source = action.source if isinstance(action.source, str) else "source"
            target = action.target or "target"
            mode = getattr(action, "mode", None) or "dqe"

            if mode == "quarantine":
                sub_header_text = f"Quarantine: {source} → {target}"
            else:
                sub_header_text = f"Expectations: {source} → {target}"

            sub_header = f"\n# {'-' * 76}\n# {sub_header_text}\n# {'-' * 76}"
            sections.append(sub_header)

            code, section_imports, section_pre = self._generate_one_action_code(
                action,
                flowgroup,
                substitution_mgr,
                preset_config,
                output_dir,
                source_yaml,
                env,
                phase_a_records=phase_a_records,
                auxiliary_files=auxiliary_files,
            )
            sections.append(code)
            imports.update(section_imports)
            pre_pipeline_statements.update(section_pre)

        return sections, imports, pre_pipeline_statements

    def _generate_one_action_code(
        self,
        action: Action,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        output_dir: Optional[Path],
        source_yaml: Optional[Path],
        env: Optional[str],
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> Tuple[str, Set[str], Set[str]]:
        """Resolve generator, build context, produce code + outputs, and wrap action-level errors as LHPError."""
        try:
            sub_type = self.determine_action_subtype(action)
            generator = self.action_registry.get_generator(action.type, sub_type)
            context = self._context.build(
                flowgroup,
                substitution_mgr,
                preset_config,
                output_dir,
                source_yaml,
                env,
                phase_a_records=phase_a_records,
                auxiliary_files=auxiliary_files,
            )
            code = generator.generate(action, context)
            imports, pre_stmts = self._context.collect_outputs(generator)
            return code, imports, pre_stmts

        except LHPError:
            raise
        except Exception as e:
            raise LHPError(
                category=ErrorCategory.ACTION,
                code_number="002",
                title=f"Action code generation failed for '{action.name}'",
                details=f"Error generating code for action '{action.name}': {e}",
                suggestions=[
                    "Check the action configuration for errors",
                    "Verify source and target references are correct",
                    "Run 'lhp validate' for detailed diagnostics",
                ],
                context={
                    "Action": action.name,
                    "Action Type": str(action.type),
                },
            ) from e

    def determine_action_subtype(self, action: Action) -> str:
        """
        Determine the sub-type of an action for generator selection.

        Args:
            action: Action to determine sub-type for

        Returns:
            Sub-type string for generator selection
        """
        if action.type == ActionType.LOAD:
            if isinstance(action.source, dict):
                return action.source.get("type", "sql")
            else:
                return "sql"  # String source is SQL

        elif action.type == ActionType.TRANSFORM:
            return action.transform_type or "sql"

        elif action.type == ActionType.WRITE:
            if action.write_target and isinstance(action.write_target, dict):
                return action.write_target.get("type", "streaming_table")
            else:
                return "streaming_table"  # Default to streaming table

        elif action.type == ActionType.TEST:
            return action.test_type or "row_count"  # Default to row_count test

        else:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="009",
                title=f"Unknown action type: {action.type}",
                details=f"Cannot determine sub-type for unknown action type '{action.type}'.",
                suggestions=[
                    "Use a valid action type: load, transform, write, test",
                    "Check the 'type' field in your action configuration",
                ],
                context={"Action": action.name, "Type": str(action.type)},
            )
