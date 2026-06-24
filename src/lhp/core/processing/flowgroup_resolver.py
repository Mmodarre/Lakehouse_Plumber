"""Flowgroup resolution service for LakehousePlumber.

Applies presets, templates, and substitutions to a single flowgroup. This is
the canonical concrete implementation of
:class:`lhp.core._interfaces.BaseFlowgroupResolutionService`.

:stability: provisional
"""

import dataclasses
import logging
from typing import Any, Dict

from lhp.models import ActionType, FlowGroup, FlowGroupContext
from lhp.models.deprecations import record_deprecation

from ...errors import ErrorFactory, codes
from ...utils.performance_timer import perf_timer
from .._interfaces import BaseFlowgroupResolutionService
from .local_variables import LocalVariableResolver
from .substitution import EnhancedSubstitutionManager


class FlowgroupResolutionService(BaseFlowgroupResolutionService):
    """
    Service for processing flowgroups through templates, presets, and substitutions.

    Handles the complete flowgroup processing pipeline including template expansion,
    preset application, substitution processing, and validation.

    :stability: provisional
    """

    def __init__(
        self,
        template_engine=None,
        preset_manager=None,
        config_validator=None,
        secret_validator=None,
        project_root=None,
    ):
        self.template_engine = template_engine
        self.preset_manager = preset_manager
        self.config_validator = config_validator
        self.secret_validator = secret_validator
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)

    def resolve(
        self,
        ctx: FlowGroupContext,
        substitution_mgr: EnhancedSubstitutionManager,
        *,
        include_tests: bool = True,
    ) -> FlowGroupContext:
        """Canonical entry point — delegates to :meth:`process_flowgroup`.

        Satisfies :class:`BaseFlowgroupResolutionService.resolve` (§4.12).
        """
        return self.process_flowgroup(
            ctx, substitution_mgr, include_tests=include_tests
        )

    def process_flowgroup(
        self,
        ctx: FlowGroupContext,
        substitution_mgr: EnhancedSubstitutionManager,
        include_tests: bool = True,
    ) -> FlowGroupContext:
        """Process flowgroup: expand templates, apply presets, apply substitutions.

        Template presets are applied first so templates define sensible defaults
        while flowgroup-level presets can override them.
        """
        flowgroup = ctx.flowgroup
        self.logger.debug(
            f"Processing flowgroup '{flowgroup.flowgroup}' in pipeline '{flowgroup.pipeline}' ({len(flowgroup.actions)} actions)"
        )

        fg = flowgroup.flowgroup

        if flowgroup.variables:
            with perf_timer(f"local_vars [{fg}]", category="local_vars"):
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

        if flowgroup.use_template:
            with perf_timer(f"template_expand [{fg}]", category="template_expand"):
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
                flowgroup = flowgroup.model_copy(
                    update={"actions": [*flowgroup.actions, *template_actions]}
                )

        # Placed after template expansion so template-generated test actions are also caught.
        tests_were_filtered = False
        if not include_tests:
            pre_filter_count = len(flowgroup.actions)
            flowgroup = flowgroup.model_copy(
                update={
                    "actions": [
                        a for a in flowgroup.actions if a.type != ActionType.TEST
                    ]
                }
            )
            filtered_count = pre_filter_count - len(flowgroup.actions)
            if filtered_count > 0:
                tests_were_filtered = True
                self.logger.debug(
                    f"Filtered {filtered_count} test action(s), "
                    f"{len(flowgroup.actions)} remaining"
                )

        if flowgroup.use_template:
            if template and template.presets:
                with perf_timer(
                    f"template_presets [{fg}]", category="template_presets"
                ):
                    self.logger.debug(f"Applying template presets: {template.presets}")
                    template_preset_config = self.preset_manager.resolve_preset_chain(
                        template.presets
                    )
                    flowgroup = self.apply_preset_config(
                        flowgroup, template_preset_config
                    )

        if flowgroup.presets:
            with perf_timer(f"fg_presets [{fg}]", category="fg_presets"):
                self.logger.debug(
                    f"Applying flowgroup-level presets: {flowgroup.presets}"
                )
                preset_config = self.preset_manager.resolve_preset_chain(
                    flowgroup.presets
                )
                flowgroup = self.apply_preset_config(flowgroup, preset_config)

        with perf_timer(f"substitution [{fg}]", category="substitution"):
            self.logger.debug(
                f"Applying environment substitutions for env '{substitution_mgr.env}'"
            )
            flowgroup_dict = flowgroup.model_dump()
            substituted_dict = substitution_mgr.substitute_yaml(flowgroup_dict)

        with perf_timer(f"token_validation [{fg}]", category="token_validation"):
            if not substitution_mgr.skip_validation:
                validation_errors = substitution_mgr.validate_no_unresolved_tokens(
                    substituted_dict
                )
                if validation_errors:
                    raise ErrorFactory.config_error(
                        codes.CFG_010,
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

        # Normalize legacy 'database' fields to catalog/schema
        # REMOVE_AT_V1.0.0: Remove this import + call when database field is dropped
        with perf_timer(f"namespace_normalize [{fg}]", category="namespace_normalize"):
            from .namespace_normalizer import normalize_namespace_fields

            substituted_dict = normalize_namespace_fields(substituted_dict)

        # Resolve ``contract`` action fields into inline schema / expectations
        # before model construction & validation, so contract-derived fields
        # (e.g. data_quality ``expectations``) are present when the flowgroup
        # is validated and generated.
        with perf_timer(f"contract_resolve [{fg}]", category="contract_resolve"):
            from .contract_resolver import ContractResolver

            substituted_dict = ContractResolver().resolve(
                substituted_dict, project_root=self.project_root
            )

        processed_flowgroup = FlowGroup(**substituted_dict)

        # Skip validation only when test filtering caused zero actions
        # (genuinely empty flowgroups from YAML should still fail validation)
        if processed_flowgroup.actions or not tests_were_filtered:
            self.logger.debug(
                f"Validating processed flowgroup '{processed_flowgroup.flowgroup}'"
            )
            with perf_timer(f"fg_validation [{fg}]", category="fg_validation"):
                errors = self.config_validator.validate_flowgroup(processed_flowgroup)
                if errors:
                    # ErrorFactory.validation_error returns an LHPError, which
                    # is already well-formatted and propagates as-is.
                    raise ErrorFactory.validation_error(
                        codes.VAL_007,
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

        with perf_timer(f"secret_validation [{fg}]", category="secret_validation"):
            secret_errors = self.secret_validator.validate_secret_references(
                substitution_mgr.secret_references
            )
            if secret_errors:
                raise ErrorFactory.validation_error(
                    codes.VAL_008,
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
        return dataclasses.replace(
            ctx,
            flowgroup=processed_flowgroup,
        )

    def apply_preset_config(
        self, flowgroup: FlowGroup, preset_config: Dict[str, Any]
    ) -> FlowGroup:
        flowgroup_dict = flowgroup.model_dump()

        for action in flowgroup_dict.get("actions", []):
            action_type = action.get("type")

            if action_type == "load" and "load_actions" in preset_config:
                source_type = action.get("source", {}).get("type")
                if source_type and source_type in preset_config["load_actions"]:
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
                        preset_defaults = preset_config["write_actions"][target_type]
                        action["write_target"] = self.deep_merge(
                            action.get("write_target", {}), preset_defaults
                        )

                        self._apply_suffix(action["write_target"], preset_defaults)

                # Handle old structure for backward compatibility during migration
                elif action.get("source") and isinstance(action["source"], dict):
                    target_type = action["source"].get("type")
                    if target_type and target_type in preset_config["write_actions"]:
                        preset_defaults = preset_config["write_actions"][target_type]
                        action["source"] = self.deep_merge(
                            action.get("source", {}), preset_defaults
                        )

                        self._apply_suffix(action["source"], preset_defaults)

        if "defaults" in preset_config:
            for key, value in preset_config["defaults"].items():
                if key not in flowgroup_dict:
                    flowgroup_dict[key] = value

        return FlowGroup(**flowgroup_dict)

    @staticmethod
    def _apply_suffix(target: dict, preset_defaults: dict) -> None:
        """Apply schema_suffix or database_suffix from preset to the target config.

        Supports both new format (schema) and legacy format (database).
        ``database_suffix`` is deprecated — use ``schema_suffix`` instead; using
        it emits a soft-deprecation warning (``LHP-DEPR-004``) onto the active
        worker scope (see :mod:`lhp.models.deprecations`).

        REMOVE_AT_V1.0.0: Drop database_suffix support entirely.
        """
        schema_suffix = preset_defaults.get("schema_suffix")
        # REMOVE_AT_V1.0.0: Legacy database_suffix preset key.
        database_suffix = preset_defaults.get("database_suffix")

        suffix = schema_suffix or database_suffix
        if not suffix:
            return

        # Warn only when the suffix actually came from the deprecated key
        # (``schema_suffix`` takes precedence above, so database_suffix is only
        # the source when schema_suffix is absent/empty).
        if not schema_suffix and database_suffix:
            record_deprecation(
                codes.DEPR_004,
                title="The preset 'database_suffix' field is deprecated "
                "and will be removed in v1.0.0.",
                details=(
                    "Use 'schema_suffix' instead. The deprecated 'database_suffix' "
                    f"value ('{database_suffix}') was applied for now."
                ),
            )

        if "schema" in target:
            target["schema"] += suffix
        elif "database" in target:
            # REMOVE_AT_V1.0.0: Legacy database_suffix target path
            target["database"] += suffix

    def deep_merge(
        self, base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
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
