"""Blueprint expansion service for LakehousePlumber.

Key semantics:
  - Eager resolution scope: only `pipeline` and `flowgroup` fields are resolved
    eagerly via `%{var}`. All other `%{var}` patterns stay intact and are
    resolved later in `FlowgroupResolutionService`. `${env_token}` substrings
    inside `pipeline`/`flowgroup` are rejected (they only resolve after
    the source-path index is already built).
  - Variables-merge precedence: `merged = {**effective_params,
    **(spec.variables or {})}`. spec.variables wins on key conflict — protects
    blueprint-author-defined derived state (e.g. spec defines
    `variables: {raw_table: "raw_%{site_name}_orders"}`); without this, an
    instance accidentally setting `raw_table` would silently break the spec.
  - Uniqueness validation: emits a duplicate-tuple error citing both instance
    file paths BEFORE returning. This cannot be deferred to the downstream
    duplicate-pipeline+flowgroup guard — that error has lost provenance by then.
"""

import copy
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

from lhp.models import (
    Blueprint,
    BlueprintFlowgroupSpec,
    BlueprintInstance,
    FlowGroup,
    FlowGroupContext,
)

from ...errors import ErrorFactory, codes
from ...utils.performance_timer import perf_timer, record_count
from .local_variables import LocalVariableResolver

# Matches any `${...}` token (env or secret). Both kinds are forbidden inside
# `pipeline`/`flowgroup` strings because they resolve at Step 3, after the
# source-path index has been built from the already-resolved tuples.
SUBSTITUTION_TOKEN_PATTERN = re.compile(r"\$\{[^}]+\}")


@dataclass(frozen=True)
class BlueprintProvenance:
    """Where an expanded synthetic flowgroup came from.

    Attached by the expander, queried by:
      - DependencyTracker to set FileState.synthetic=True.
      - FlowgroupDiscoveryService.find_source_yaml_for_flowgroup to point at the
        blueprint, not a non-existent flowgroup file.
      - DependencyAnalysisService to dedupe synthetic flowgroups by
        (blueprint_name, spec_index) when rendering `lhp deps`.
    """

    blueprint_name: str
    blueprint_path: Path
    instance_path: Path
    flowgroup: FlowGroup
    spec_index: int


class BlueprintExpander:
    """Cartesian-product expansion of blueprints x instances into FlowGroups.

    Inputs come pre-validated from `BlueprintParser` (parameter keys checked
    against blueprint declarations, required parameters present).
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def expand(
        self,
        blueprints: Dict[str, Tuple[Blueprint, Path]],
        instances: List[Tuple[BlueprintInstance, Path]],
    ) -> Tuple[List[FlowGroupContext], Dict[Tuple[str, str], BlueprintProvenance]]:
        """Expand all instances against their referenced blueprints."""
        with perf_timer("expand_blueprints", category="blueprint_expansion"):
            contexts: List[FlowGroupContext] = []
            provenance: Dict[Tuple[str, str], BlueprintProvenance] = {}
            # Track first-emitter file path per resolved tuple, to produce
            # rich duplicate-detection errors.
            seen: Dict[Tuple[str, str], Path] = {}

            for instance, instance_path in instances:
                blueprint, blueprint_path = blueprints[instance.blueprint_name]
                effective_params = self._effective_params(blueprint, instance)

                for spec_index, spec in enumerate(blueprint.flowgroups):
                    self._reject_env_tokens_in_identity(spec, blueprint, instance_path)
                    merged_vars = self._merge_variables(spec, effective_params)
                    resolved_pipeline = self._resolve_identity_field(
                        spec.pipeline,
                        merged_vars,
                        field="pipeline",
                        blueprint=blueprint,
                        instance_path=instance_path,
                    )
                    resolved_flowgroup = self._resolve_identity_field(
                        spec.flowgroup,
                        merged_vars,
                        field="flowgroup",
                        blueprint=blueprint,
                        instance_path=instance_path,
                    )
                    key = (resolved_pipeline, resolved_flowgroup)

                    if key in seen:
                        existing_path = seen[key]
                        raise ErrorFactory.validation_error(
                            codes.VAL_045,
                            title="Duplicate (pipeline, flowgroup) after expansion",
                            details=(
                                f"Two instances produce the same flowgroup "
                                f"'{resolved_flowgroup}' in pipeline "
                                f"'{resolved_pipeline}':\n"
                                f"  - {existing_path}\n"
                                f"  - {instance_path}\n"
                                "Each instance must produce distinct "
                                "(pipeline, flowgroup) tuples."
                            ),
                            suggestions=[
                                "Inspect both instances for copy-paste errors",
                                "Vary parameter values that flow into the "
                                "pipeline/flowgroup template strings",
                            ],
                            context={
                                "pipeline": resolved_pipeline,
                                "flowgroup": resolved_flowgroup,
                                "instance_a": str(existing_path),
                                "instance_b": str(instance_path),
                            },
                        )

                    fg = self._build_flowgroup(
                        spec=spec,
                        resolved_pipeline=resolved_pipeline,
                        resolved_flowgroup=resolved_flowgroup,
                        merged_vars=merged_vars,
                    )
                    ctx = FlowGroupContext(
                        flowgroup=fg,
                        source_yaml=blueprint_path,
                        synthetic=True,
                    )
                    contexts.append(ctx)
                    seen[key] = instance_path
                    provenance[key] = BlueprintProvenance(
                        blueprint_name=blueprint.name,
                        blueprint_path=blueprint_path,
                        instance_path=instance_path,
                        flowgroup=fg,
                        spec_index=spec_index,
                    )

            self.logger.info(
                f"Expanded {len(instances)} instance(s) into "
                f"{len(contexts)} synthetic flowgroup(s)"
            )
            record_count("synthetic_flowgroups", len(contexts))
            return contexts, provenance

    def expand_single_instance(
        self,
        instance: BlueprintInstance,
        instance_path: Path,
        blueprints: Dict[str, Tuple[Blueprint, Path]],
    ) -> Tuple[List[FlowGroupContext], Dict[Tuple[str, str], BlueprintProvenance]]:
        """Expand exactly one instance — used by `lhp show --instance`."""
        return self.expand(blueprints, [(instance, instance_path)])

    @staticmethod
    def _effective_params(
        blueprint: Blueprint, instance: BlueprintInstance
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        for p in blueprint.parameters:
            if p.default is not None:
                params[p.name] = p.default
        params.update(instance.parameter_values())
        return params

    @staticmethod
    def _merge_variables(
        spec: BlueprintFlowgroupSpec, effective_params: Dict[str, Any]
    ) -> Dict[str, str]:
        """spec.variables overlaid last — blueprint author retains control of derived state."""
        merged: Dict[str, str] = {}
        for k, v in effective_params.items():
            merged[k] = v if isinstance(v, str) else str(v)
        if spec.variables:
            for k, v in spec.variables.items():
                merged[k] = v if isinstance(v, str) else str(v)
        return merged

    @staticmethod
    def _reject_env_tokens_in_identity(
        spec: BlueprintFlowgroupSpec,
        blueprint: Blueprint,
        instance_path: Path,
    ) -> None:
        """Reject `${env_token}` in `pipeline`/`flowgroup`.

        The source-path index, state tracking, and `--pipeline` filter all use
        the resolved tuple as a key immediately after expansion. `${env_token}`
        resolves later in the per-flowgroup pipeline, so allowing it here would
        yield unresolved tokens in the index.
        """
        for field, value in (
            ("pipeline", spec.pipeline),
            ("flowgroup", spec.flowgroup),
        ):
            if SUBSTITUTION_TOKEN_PATTERN.search(value):
                raise ErrorFactory.validation_error(
                    codes.VAL_044,
                    title=(f"${{env_token}} not allowed in blueprint '{field}' field"),
                    details=(
                        f"Blueprint '{blueprint.name}' defines a flowgroup spec "
                        f"with `{field}: {value!r}`, which contains a "
                        "${...} substitution. Only %{var} (instance "
                        f"parameters) is permitted in `{field}` fields, "
                        "because the resolved tuple is used as an index key "
                        "before ${env_token} substitution runs."
                    ),
                    suggestions=[
                        f"Use %{{var}} (instance parameters) in `{field}`, "
                        "or move the env-driven part into other fields where "
                        "${env_token} resolves later in the pipeline.",
                    ],
                    context={
                        "blueprint": blueprint.name,
                        "field": field,
                        "value": value,
                        "instance": str(instance_path),
                    },
                )

    @staticmethod
    def _resolve_identity_field(
        template: str,
        merged_vars: Dict[str, str],
        *,
        field: str,
        blueprint: Blueprint,
        instance_path: Path,
    ) -> str:
        """Eagerly resolve `%{var}` in a `pipeline` or `flowgroup` template.

        Strict: any unresolved `%{var}` raises. Reuses `LocalVariableResolver`
        for parity with the flowgroup-resolution runtime path.
        """
        resolver = LocalVariableResolver(merged_vars)
        try:
            resolved = resolver.resolve({field: template})[field]
        except Exception as e:
            raise ErrorFactory.validation_error(
                codes.VAL_055,
                title=f"Unresolved %{{var}} in blueprint `{field}` template",
                details=(
                    f"Blueprint '{blueprint.name}' has `{field}: {template!r}` "
                    f"with unresolved %{{var}} placeholders for instance "
                    f"{instance_path}: {e}"
                ),
                suggestions=[
                    "Verify the parameter name in the template matches a "
                    "declared blueprint parameter",
                    "If the parameter is optional and the instance omits it, "
                    "give it a default value on the blueprint",
                ],
                context={
                    "blueprint": blueprint.name,
                    "field": field,
                    "template": template,
                    "instance": str(instance_path),
                },
            ) from e
        return resolved

    @staticmethod
    def _build_flowgroup(
        *,
        spec: BlueprintFlowgroupSpec,
        resolved_pipeline: str,
        resolved_flowgroup: str,
        merged_vars: Dict[str, str],
    ) -> FlowGroup:
        # Build a dict to mirror the disk-sourced parser path (preserves Pydantic validation).
        flowgroup_dict: Dict[str, Any] = {
            "pipeline": resolved_pipeline,
            "flowgroup": resolved_flowgroup,
            "variables": dict(merged_vars),
            "presets": list(spec.presets) if spec.presets else [],
            "actions": [a.model_copy(deep=True) for a in spec.actions],
        }
        if spec.job_name is not None:
            flowgroup_dict["job_name"] = spec.job_name
        if spec.use_template is not None:
            flowgroup_dict["use_template"] = spec.use_template
        if spec.template_parameters is not None:
            # Deep copy so spec stays unmodified across instances
            flowgroup_dict["template_parameters"] = copy.deepcopy(
                spec.template_parameters
            )
        if spec.operational_metadata is not None:
            flowgroup_dict["operational_metadata"] = spec.operational_metadata

        return FlowGroup(**flowgroup_dict)
