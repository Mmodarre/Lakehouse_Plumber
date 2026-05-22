"""Code generation service for LakehousePlumber."""

import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Set, Tuple

from ...models.config import Action, ActionType, FlowGroup, TransformType

if TYPE_CHECKING:
    from ...generators.python_file_copier import CopiedModuleRecord
from ...utils.error_formatter import ErrorCategory, LHPError, LHPValidationError
from ...utils.performance_timer import perf_timer
from ...utils.source_extractor import extract_source_views_from_action
from ...utils.substitution import EnhancedSubstitutionManager


class CodeGenerator:
    """
    Service for generating Python code from flowgroup configurations.

    Handles the complete code generation pipeline including action processing,
    dependency resolution, import management, and final code assembly.
    """

    def __init__(
        self,
        action_registry=None,
        dependency_resolver=None,
        preset_manager=None,
        project_config=None,
        project_root=None,
    ):
        """
        Initialize code generator.

        Args:
            action_registry: Action registry for getting generators
            dependency_resolver: Dependency resolver for action ordering
            preset_manager: Preset manager for preset configurations
            project_config: Project configuration for context
            project_root: Project root directory for spec_dir context
        """
        self.action_registry = action_registry
        self.dependency_resolver = dependency_resolver
        self.preset_manager = preset_manager
        self.project_config = project_config
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)

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
        """
        Generate complete Python code for a flowgroup.

        Args:
            flowgroup: FlowGroup to generate code for
            substitution_mgr: Substitution manager for the environment
            output_dir: Output directory for generated files
            source_yaml: Source YAML path for file tracking
            env: Environment name for file tracking
            include_tests: Whether to include test actions
            phase_a_records: Optional list passed by Phase A workers to
                receive ``CopiedModuleRecord`` entries instead of writing
                user Python modules to disk. ``None`` (the default) means
                disk writes happen inline — the legacy single-threaded path.
            auxiliary_files: Optional ``{module_path: source_str}`` mapping
                of inline Python modules (carried on
                :class:`FlowGroupContext`). Used by ``custom_python_functions``
                generators in lieu of an on-disk file.

        Returns:
            Complete Python code for the flowgroup
        """
        fg = flowgroup.flowgroup
        self.logger.debug(
            f"Starting code generation for flowgroup '{fg}' in pipeline '{flowgroup.pipeline}'"
        )

        # 1. Resolve action dependencies
        with perf_timer(f"resolve_dependencies [{fg}]"):
            ordered_actions = self.dependency_resolver.resolve_dependencies(
                flowgroup.actions
            )
        self.logger.debug(
            f"Resolved action ordering: {[a.name for a in ordered_actions]} ({len(ordered_actions)} actions)"
        )

        # 2. Get preset configuration if any
        preset_config = {}
        if flowgroup.presets:
            preset_config = self.preset_manager.resolve_preset_chain(flowgroup.presets)
            self.logger.debug(
                f"Resolved preset chain for flowgroup '{fg}': {flowgroup.presets}"
            )

        # 3. Check for test-only flowgroups when include_tests is False
        if not include_tests:
            non_test_actions = [
                action for action in ordered_actions if action.type != ActionType.TEST
            ]
            if not non_test_actions:
                # This is a test-only flowgroup, skip entirely
                self.logger.info(
                    f"Skipping test-only flowgroup: {fg} (--include-tests not specified)"
                )
                return ""  # Return empty string to skip this flowgroup

        # 4. Generate code sections
        with perf_timer(f"generate_action_sections [{fg}]"):
            (
                generated_sections,
                all_imports,
                pre_pipeline_statements,
            ) = self._generate_action_sections(
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

        # 5-6. Apply secret substitutions and assemble final code.
        # Substitution emits ``__SECRET_scope_key__`` placeholders so Jinja
        # templates can naively wrap values in Python string literals; this
        # post-pass rewrites whole-string placeholders to bare
        # ``dbutils.secrets.get(...)`` calls and embedded placeholders to
        # f-strings.
        with perf_timer(f"assemble_code [{fg}]"):
            complete_code = self._apply_secret_substitutions(
                generated_sections, substitution_mgr
            )
            return self._assemble_final_code(
                flowgroup,
                all_imports,
                pre_pipeline_statements,
                complete_code,
            )

    def _generate_action_sections(
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
        # Group actions by type while preserving order
        action_groups = defaultdict(list)
        for action in ordered_actions:
            action_groups[action.type].append(action)

        # Initialize collections
        generated_sections = []
        all_imports: Set[str] = set()
        pre_pipeline_statements: Set[str] = set()

        # Add base imports
        all_imports.add("from pyspark import pipelines as dp")

        # Define section headers
        section_headers = {
            ActionType.LOAD: "SOURCE VIEWS",
            ActionType.WRITE: "TARGET TABLES",
            ActionType.TEST: "DATA QUALITY TESTS",
        }

        # Process each action type in order
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
                # Partition transforms into regular and data quality
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
                # Non-transform action types
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

        # Group write actions by target table
        grouped_actions = self.group_write_actions_by_target(write_actions)
        self.logger.debug(
            f"Grouped {len(write_actions)} write actions into {len(grouped_actions)} target table(s)"
        )

        for target_table, actions in grouped_actions.items():
            try:
                # Use the first action to determine sub-type and get generator
                first_action = actions[0]
                sub_type = self.determine_action_subtype(first_action)
                generator = self.action_registry.get_generator(
                    first_action.type, sub_type
                )

                # Create a combined action with multiple source views
                combined_action = self.create_combined_write_action(
                    actions, target_table
                )

                # Generate code
                context = self._build_generation_context(
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

                # Collect imports and pre-pipeline statements
                section_imports, section_pre = self._collect_generator_outputs(
                    generator
                )
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
            context = self._build_generation_context(
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
            imports, pre_stmts = self._collect_generator_outputs(generator)
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

    def _build_generation_context(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        preset_config: Dict[str, Any],
        output_dir: Optional[Path],
        source_yaml: Optional[Path],
        env: Optional[str],
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
        auxiliary_files: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, Any]:
        """Build context dictionary for generator execution."""
        project_root = self.project_root or Path.cwd()
        return {
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
            # Phase A collect carrier: when present, copy_user_module_for_pipeline
            # appends CopiedModuleRecord entries here instead of writing to disk,
            # so Phase B can replay the writes on the main thread.
            "phase_a_records": phase_a_records,
            # Inline auxiliary Python modules carried on the FlowGroupContext
            # (e.g. monitoring's jobs_stats_loader.py). Read by
            # ``copy_user_module_for_pipeline`` to skip on-disk lookup.
            "auxiliary_files": auxiliary_files or {},
        }

    def _collect_generator_outputs(self, generator) -> Tuple[Set[str], Set[str]]:
        """Collect imports and pre-pipeline statements from a generator."""
        imports: Set[str] = set()
        pre_pipeline_statements: Set[str] = set()

        # Enhanced import collection - use ImportManager if available
        import_manager = getattr(generator, "get_import_manager", lambda: None)()
        if import_manager:
            consolidated_imports = import_manager.get_consolidated_imports()
            imports.update(consolidated_imports)
            self.logger.debug(
                f"Used ImportManager: {len(consolidated_imports)} imports"
            )
        else:
            # Legacy generator - use simple import collection
            imports.update(generator.imports)

        # Collect pre-pipeline statements (e.g. cloudpickle registration for
        # custom data sources/sinks).
        get_pre = getattr(generator, "get_pre_pipeline_statements", None)
        if callable(get_pre):
            pre_pipeline_statements.update(get_pre())

        return imports, pre_pipeline_statements

    def _apply_secret_substitutions(
        self,
        generated_sections: List[str],
        substitution_mgr: EnhancedSubstitutionManager,
    ) -> str:
        """Apply secret substitutions to generated code.

        Secrets are emitted as ``__SECRET_scope_key__`` placeholders during
        YAML substitution so that Jinja templates can wrap values in Python
        string literals without disturbing the secret expression. This
        post-pass walks the assembled code and rewrites placeholders:

        - When a string literal's content is exactly one placeholder, it is
          replaced with a bare ``dbutils.secrets.get(...)`` call so that
          fields like ``.option("user", "${secret:db/user}")`` become
          ``.option("user", dbutils.secrets.get(...))``.
        - When a placeholder is embedded in a larger string literal, the
          literal is rewritten as an f-string so the dbutils call evaluates
          at runtime.
        """
        complete_code = "\n\n".join(generated_sections)

        # Use SecretCodeGenerator to convert secret placeholders to valid
        # Python (bare calls or f-strings). Pull references from the
        # substitution manager — it is the canonical source.
        try:
            from ...utils.secret_code_generator import SecretCodeGenerator

            secret_generator = SecretCodeGenerator()
            complete_code = secret_generator.generate_python_code(
                complete_code, substitution_mgr.secret_references
            )
        except ImportError:
            self.logger.warning(
                "SecretCodeGenerator not available, skipping secret substitutions"
            )
        except Exception as e:
            self.logger.warning(f"Error applying secret substitutions: {e}")

        return complete_code

    def _assemble_final_code(
        self,
        flowgroup: FlowGroup,
        all_imports: Set[str],
        pre_pipeline_statements: Set[str],
        complete_code: str,
    ) -> str:
        """Assemble final Python code with headers and imports.

        Enforces PEP 236 by hoisting every ``from __future__ import ...``
        statement collected from the imports set and the main generated body
        to the top of the assembled module — a single chokepoint so individual
        generators do not need to know about future imports.

        The pre-pipeline statements set (e.g. ``cloudpickle.register_pickle_by_value``
        for custom data sources/sinks) is emitted as a sorted block immediately
        after the imports block and before ``PIPELINE_ID``.
        """
        from ...utils.import_manager import ImportManager, extract_future_imports

        self.logger.debug(
            f"Assembling final code for flowgroup '{flowgroup.flowgroup}' with "
            f"{len(all_imports)} imports, "
            f"{len(pre_pipeline_statements)} pre-pipeline statements"
        )

        # (a) Pull __future__ statements out of the collected imports set.
        future_imports: List[str] = []
        non_future: Set[str] = set()
        for imp in all_imports:
            if imp.lstrip().startswith("from __future__"):
                future_imports.append(imp.strip())
            else:
                non_future.add(imp)

        # (b) Pull __future__ out of the main generated code body (e.g.
        # snapshot-CDC source_function_code inlined into the streaming_table
        # template).
        fl, complete_code = extract_future_imports(complete_code)
        future_imports.extend(fl)

        # Dedupe in declaration order.
        seen: Set[str] = set()
        ordered_futures = [f for f in future_imports if not (f in seen or seen.add(f))]

        # Category-aware ordering for the rest (replaces sorted(all_imports)).
        # Reuses ImportManager._sort_imports via a transient instance so we get
        # PEP 8 style grouping (stdlib -> third-party -> pyspark -> dlt -> custom)
        # rather than ASCII-sorted output.
        mgr = ImportManager()
        for imp in non_future:
            mgr.add_import(imp)
        sorted_non_future = mgr.get_consolidated_imports()

        # Compose the final imports block: __future__ first, blank line, then
        # the categorized remainder.
        import_block_lines: List[str] = []
        if ordered_futures:
            import_block_lines.extend(ordered_futures)
            import_block_lines.append("")
        import_block_lines.extend(sorted_non_future)
        imports_section = "\n".join(import_block_lines)

        # Pre-pipeline statements block (sorted for deterministic output).
        pre_pipeline_block = ""
        if pre_pipeline_statements:
            pre_pipeline_block = (
                "\n" + "\n".join(sorted(pre_pipeline_statements)) + "\n"
            )

        # Add pipeline configuration section
        pipeline_config = f"""
# Pipeline Configuration
PIPELINE_ID = "{flowgroup.pipeline}"
FLOWGROUP_ID = "{flowgroup.flowgroup}"
"""

        # Build header
        header = f"""# Generated by LakehousePlumber
# Pipeline: {flowgroup.pipeline}
# FlowGroup: {flowgroup.flowgroup}

{imports_section}
{pre_pipeline_block}{pipeline_config}"""

        return header + "\n\n" + complete_code

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

    def group_write_actions_by_target(
        self, write_actions: List[Action]
    ) -> Dict[str, List[Action]]:
        """
        Group write actions by their target table.

        Args:
            write_actions: List of write actions

        Returns:
            Dictionary mapping target table names to lists of actions
        """
        grouped = defaultdict(list)

        for action in write_actions:
            target_config = action.write_target
            if not target_config:
                # Handle legacy structure
                target_config = action.source if isinstance(action.source, dict) else {}

            # Build full table name
            catalog = target_config.get("catalog", "")
            schema = target_config.get("schema", "")
            table = target_config.get("table") or target_config.get("name", "")

            if catalog and schema and table:
                full_table_name = f"{catalog}.{schema}.{table}"
            elif table:
                full_table_name = table
            else:
                # Use action name as fallback
                full_table_name = action.name

            grouped[full_table_name].append(action)

        return dict(grouped)

    def create_combined_write_action(
        self, actions: List[Action], target_table: str
    ) -> Action:
        """
        Create a combined write action with individual action metadata preserved.

        Args:
            actions: List of write actions targeting the same table
            target_table: Full target table name

        Returns:
            Combined action with individual action metadata
        """
        # Determine which action should create the table based on existing validation logic
        from ..validators import TableCreationValidator

        table_creator = None
        table_creation_validator = TableCreationValidator()
        for action in actions:
            if table_creation_validator._action_creates_table(action):
                table_creator = action
                break

        # If no explicit creator found, use the first action (default behavior)
        if not table_creator:
            table_creator = actions[0]

        # Build individual action metadata for each append flow
        action_metadata = []
        for action in actions:
            # Extract source views (can be multiple per action)
            source_views_for_action = self._extract_source_views_from_action(
                action.source
            )

            # Generate base flow name from action name
            base_flow_name = action.name.replace("-", "_").replace(" ", "_")
            if base_flow_name.startswith("write_"):
                base_flow_name = base_flow_name[6:]  # Remove "write_" prefix
            base_flow_name = (
                f"f_{base_flow_name}"
                if not base_flow_name.startswith("f_")
                else base_flow_name
            )

            # Per-flow CDC params (only populated for CDC-mode contributors)
            flow_cdc_config = self._build_flow_cdc_config(action)

            if len(source_views_for_action) > 1:
                # Multiple sources in this action: create separate append flow for each
                for i, source_view in enumerate(source_views_for_action):
                    flow_name = f"{base_flow_name}_{i + 1}"
                    action_metadata.append(
                        {
                            "action_name": f"{action.name}_{i + 1}",
                            "source_view": source_view,
                            "once": action.once or False,
                            "readMode": action.readMode,  # Preserve individual readMode
                            "flow_name": flow_name,
                            "description": action.description
                            or f"Append flow to {target_table} from {source_view}",
                            "flow_cdc_config": flow_cdc_config,
                        }
                    )
            else:
                # Single source in this action: create one append flow
                source_view = (
                    source_views_for_action[0] if source_views_for_action else ""
                )
                action_metadata.append(
                    {
                        "action_name": action.name,
                        "source_view": source_view,
                        "once": action.once or False,
                        "readMode": action.readMode,  # Preserve individual readMode
                        "flow_name": base_flow_name,
                        "description": action.description
                        or f"Append flow to {target_table}",
                        "flow_cdc_config": flow_cdc_config,
                    }
                )

        # Create combined action using the table creator as the base
        combined_action = table_creator.model_copy(deep=True)

        # Store metadata as private attribute (Pydantic compatible)
        object.__setattr__(combined_action, "_action_metadata", action_metadata)
        object.__setattr__(combined_action, "_table_creator", table_creator)

        return combined_action

    def _extract_source_views_from_action(self, source) -> List[str]:
        """
        Extract all source views from an action source configuration.

        Delegates to utility function for consistency across codebase.

        Args:
            source: Source configuration (string, list, or dict)

        Returns:
            List of source view names
        """
        return extract_source_views_from_action(source)

    def _build_flow_cdc_config(self, action: Action) -> Dict[str, Any]:
        """Build the per-flow CDC config dict for a contributing action.

        Returns an empty dict for non-CDC actions so the template can safely
        index into it without branching on mode.
        """
        wt = action.write_target
        if not isinstance(wt, dict) or wt.get("mode") != "cdc":
            return {}
        ac = wt.get("cdc_config", {}) or {}
        return {
            "ignore_null_updates": ac.get("ignore_null_updates"),
            "apply_as_deletes": ac.get("apply_as_deletes"),
            "apply_as_truncates": ac.get("apply_as_truncates"),
            "column_list": ac.get("column_list"),
            "except_column_list": ac.get("except_column_list"),
        }
