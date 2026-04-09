"""Flowgroup processing service for LakehousePlumber."""

import logging
from typing import Any, Dict

from ...models.config import FlowGroup
from ...utils.error_formatter import LHPError, LHPValidationError
from ...utils.local_variables import LocalVariableResolver
from ...utils.performance_timer import perf_timer
from ...utils.substitution import EnhancedSubstitutionManager


class FlowgroupProcessor:
    """
    Service for processing flowgroups through templates, presets, and substitutions.

    Handles the complete flowgroup processing pipeline including template expansion,
    preset application, substitution processing, and validation.
    """

    def __init__(
        self,
        template_engine=None,
        preset_manager=None,
        config_validator=None,
        secret_validator=None,
    ):
        """
        Initialize flowgroup processor.

        Args:
            template_engine: Template engine for template expansion
            preset_manager: Preset manager for preset chain resolution
            config_validator: Config validator for flowgroup validation
            secret_validator: Secret validator for secret reference validation
        """
        self.template_engine = template_engine
        self.preset_manager = preset_manager
        self.config_validator = config_validator
        self.secret_validator = secret_validator
        self.logger = logging.getLogger(__name__)

    def process_flowgroup(
        self, flowgroup: FlowGroup, substitution_mgr: EnhancedSubstitutionManager
    ) -> FlowGroup:
        """
        Process flowgroup: expand templates, apply presets, apply substitutions.

        Template presets are applied first, then flowgroup presets can override them.
        This allows templates to define sensible defaults while flowgroups can
        customize as needed.

        Args:
            flowgroup: FlowGroup to process
            substitution_mgr: Substitution manager for the environment

        Returns:
            Processed flowgroup
        """
        self.logger.debug(
            f"Processing flowgroup '{flowgroup.flowgroup}' in pipeline '{flowgroup.pipeline}' ({len(flowgroup.actions)} actions)"
        )

        fg = flowgroup.flowgroup

        # Step 0.5: Resolve local variables FIRST (before templates)
        if flowgroup.variables:
            with perf_timer(f"local_vars [{fg}]"):
                self.logger.debug(
                    f"Resolving {len(flowgroup.variables)} local variable(s): {list(flowgroup.variables.keys())}"
                )
                resolver = LocalVariableResolver(flowgroup.variables)
                flowgroup_dict = flowgroup.model_dump()
                # Don't resolve variables in the 'variables' section itself
                variables_backup = flowgroup_dict.pop("variables", None)
                resolved_dict = resolver.resolve(flowgroup_dict)
                resolved_dict["variables"] = variables_backup  # Preserve for debugging
                flowgroup = FlowGroup(**resolved_dict)

        # Step 1: Expand templates
        if flowgroup.use_template:
            with perf_timer(f"template_expand [{fg}]"):
                self.logger.debug(
                    f"Expanding template '{flowgroup.use_template}' with parameters: {list((flowgroup.template_parameters or {}).keys())}"
                )
                template = self.template_engine.get_template(flowgroup.use_template)
                template_actions = self.template_engine.render_template(
                    flowgroup.use_template, flowgroup.template_parameters or {}
                )
                self.logger.debug(
                    f"Template '{flowgroup.use_template}' expanded into {len(template_actions)} action(s)"
                )
                # Add template actions to existing actions
                flowgroup.actions.extend(template_actions)

            # Step 1.5: Apply template-level presets to template-generated actions
            if template and template.presets:
                with perf_timer(f"template_presets [{fg}]"):
                    self.logger.debug(f"Applying template presets: {template.presets}")
                    template_preset_config = self.preset_manager.resolve_preset_chain(
                        template.presets
                    )
                    flowgroup = self.apply_preset_config(flowgroup, template_preset_config)

        # Step 2: Apply flowgroup-level presets (may override template presets)
        if flowgroup.presets:
            with perf_timer(f"fg_presets [{fg}]"):
                self.logger.debug(f"Applying flowgroup-level presets: {flowgroup.presets}")
                preset_config = self.preset_manager.resolve_preset_chain(flowgroup.presets)
                flowgroup = self.apply_preset_config(flowgroup, preset_config)

        # Step 3: Apply substitutions
        with perf_timer(f"substitution [{fg}]"):
            self.logger.debug(
                f"Applying environment substitutions for env '{substitution_mgr.env}'"
            )
            flowgroup_dict = flowgroup.model_dump()
            substituted_dict = substitution_mgr.substitute_yaml(flowgroup_dict)

        # Step 3.5: Validate no unresolved tokens (skip if validation disabled)
        with perf_timer(f"token_validation [{fg}]"):
            if not substitution_mgr.skip_validation:
                validation_errors = substitution_mgr.validate_no_unresolved_tokens(
                    substituted_dict
                )
                if validation_errors:
                    from ...utils.error_formatter import ErrorCategory

                    raise LHPError(
                        category=ErrorCategory.CONFIG,
                        code_number="010",
                        title="Unresolved substitution tokens detected",
                        details=f"Found {len(validation_errors)} unresolved token(s):\n\n"
                        + "\n".join(f"  • {e}" for e in validation_errors[:5]),
                        suggestions=[
                            f"Check substitutions/{substitution_mgr.env}.yaml for missing token definitions",
                            "Verify token names match exactly (including case)",
                            "For map lookups (Phase 2), ensure both map and key exist: {map[key]}",
                            "Check for typos in token names",
                        ],
                        context={
                            "Environment": substitution_mgr.env,
                            "Pipeline": flowgroup.pipeline,
                            "Flowgroup": flowgroup.flowgroup,
                            "Total Unresolved": len(validation_errors),
                            "Showing": min(5, len(validation_errors)),
                        },
                    )

        # Step 3.7: Normalize legacy 'database' fields to catalog/schema
        # REMOVE_AT_V1.0.0: Remove this import + call when database field is dropped
        with perf_timer(f"namespace_normalize [{fg}]"):
            from .namespace_normalizer import normalize_namespace_fields

            substituted_dict = normalize_namespace_fields(substituted_dict)

        processed_flowgroup = FlowGroup(**substituted_dict)

        self.logger.debug(
            f"Validating processed flowgroup '{processed_flowgroup.flowgroup}'"
        )
        # Step 4: Validate individual flowgroup
        with perf_timer(f"fg_validation [{fg}]"):
            try:
                errors = self.config_validator.validate_flowgroup(processed_flowgroup)
                if errors:
                    from ...utils.error_formatter import ErrorCategory

                    raise LHPValidationError(
                        category=ErrorCategory.VALIDATION,
                        code_number="007",
                        title="FlowGroup validation failed",
                        details="\n\n".join(str(e) for e in errors),
                        suggestions=[
                            "Check flowgroup configuration for the errors listed above",
                            "Run 'lhp validate' for detailed diagnostics",
                        ],
                        context={
                            "Pipeline": processed_flowgroup.pipeline,
                            "FlowGroup": processed_flowgroup.flowgroup,
                            "Error Count": len(errors),
                        },
                    )
            except LHPError:
                # Re-raise LHPError as-is (it's already well-formatted)
                raise

        # Step 5: Validate secret references
        with perf_timer(f"secret_validation [{fg}]"):
            secret_errors = self.secret_validator.validate_secret_references(
                substitution_mgr.get_secret_references()
            )
            if secret_errors:
                from ...utils.error_formatter import ErrorCategory

                raise LHPError(
                    category=ErrorCategory.VALIDATION,
                    code_number="008",
                    title="Secret validation failed",
                    details="\n\n".join(secret_errors),
                    suggestions=[
                        "Verify secret scope and key names are correct",
                        "Check that referenced secrets exist in your Databricks workspace",
                        "Use the format ${secret:scope/key}",
                    ],
                    context={
                        "Pipeline": processed_flowgroup.pipeline,
                        "FlowGroup": processed_flowgroup.flowgroup,
                        "Error Count": len(secret_errors),
                    },
                )

        self.logger.debug(
            f"Flowgroup '{processed_flowgroup.flowgroup}' processing complete ({len(processed_flowgroup.actions)} actions)"
        )
        return processed_flowgroup

    def apply_preset_config(
        self, flowgroup: FlowGroup, preset_config: Dict[str, Any]
    ) -> FlowGroup:
        """
        Apply preset configuration to flowgroup.

        Args:
            flowgroup: FlowGroup to apply presets to
            preset_config: Resolved preset configuration

        Returns:
            FlowGroup with preset defaults applied
        """
        flowgroup_dict = flowgroup.model_dump()

        # Apply preset defaults to actions
        for action in flowgroup_dict.get("actions", []):
            action_type = action.get("type")

            # Apply type-specific defaults
            if action_type == "load" and "load_actions" in preset_config:
                source_type = action.get("source", {}).get("type")
                if source_type and source_type in preset_config["load_actions"]:
                    # Merge preset defaults with action source
                    # Preset overrides existing values (preset is applied on top)
                    preset_defaults = preset_config["load_actions"][source_type]
                    action["source"] = self.deep_merge(
                        action.get("source", {}), preset_defaults
                    )

            elif action_type == "transform" and "transform_actions" in preset_config:
                transform_type = action.get("transform_type")
                if (
                    transform_type
                    and transform_type in preset_config["transform_actions"]
                ):
                    # Apply transform defaults
                    preset_defaults = preset_config["transform_actions"][transform_type]
                    for key, value in preset_defaults.items():
                        if key not in action:
                            action[key] = value

            elif action_type == "write" and "write_actions" in preset_config:
                # For new structure, check write_target
                if action.get("write_target") and isinstance(
                    action["write_target"], dict
                ):
                    target_type = action["write_target"].get("type")
                    if target_type and target_type in preset_config["write_actions"]:
                        # Merge preset defaults with write_target configuration
                        # Preset overrides existing values (preset is applied on top)
                        preset_defaults = preset_config["write_actions"][target_type]
                        action["write_target"] = self.deep_merge(
                            action.get("write_target", {}), preset_defaults
                        )

                        # Handle special cases like database_suffix / schema_suffix
                        self._apply_suffix(action["write_target"], preset_defaults)

                # Handle old structure for backward compatibility during migration
                elif action.get("source") and isinstance(action["source"], dict):
                    target_type = action["source"].get("type")
                    if target_type and target_type in preset_config["write_actions"]:
                        # Merge preset defaults with write configuration
                        # Preset overrides existing values (preset is applied on top)
                        preset_defaults = preset_config["write_actions"][target_type]
                        action["source"] = self.deep_merge(
                            action.get("source", {}), preset_defaults
                        )

                        # Handle special cases like database_suffix / schema_suffix
                        self._apply_suffix(action["source"], preset_defaults)

        # Apply global preset settings
        if "defaults" in preset_config:
            for key, value in preset_config["defaults"].items():
                if key not in flowgroup_dict:
                    flowgroup_dict[key] = value

        return FlowGroup(**flowgroup_dict)

    @staticmethod
    def _apply_suffix(target: dict, preset_defaults: dict) -> None:
        """Apply schema_suffix or database_suffix from preset to the target config.

        Supports both new format (schema) and legacy format (database).
        database_suffix is deprecated — use schema_suffix instead.

        REMOVE_AT_V1.0.0: Drop database_suffix support entirely.
        """
        suffix = preset_defaults.get("schema_suffix") or preset_defaults.get(
            "database_suffix"
        )
        if not suffix:
            return

        if "schema" in target:
            target["schema"] += suffix
        elif "database" in target:
            # REMOVE_AT_V1.0.0: Legacy database_suffix path
            target["database"] += suffix

    def deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.

        Args:
            base: Base dictionary
            override: Dictionary to override with

        Returns:
            Merged dictionary
        """
        result = base.copy()
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self.deep_merge(result[key], value)
            else:
                result[key] = value
        return result
