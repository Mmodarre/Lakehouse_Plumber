"""Draft template expansion/resolution without file writes or shared caches."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from jinja2 import TemplateError
from jinja2.exceptions import SecurityError
from jinja2.sandbox import ImmutableSandboxedEnvironment
from pydantic import ValidationError

from lhp.core.loaders.project_config_loader import ProjectConfigLoader
from lhp.core.validators.field.secret_reference import SecretValidator
from lhp.errors import LHPError
from lhp.models import FlowGroup, FlowGroupContext, Template
from lhp.presets.preset_manager import PresetManager

from .flowgroup_resolver import FlowgroupResolutionService
from .substitution import EnhancedSubstitutionManager
from .template_authoring import (
    _check_json_value,
    diagnostic,
    inspect_source,
    project_path,
    source_hash,
)
from .template_engine import TemplateEngine
from .template_preview_dependencies import (
    DependencySnapshot,
    PreviewConfigValidator,
    SourceChangedError,
)


class DraftTemplateEngine(TemplateEngine):
    """An isolated provider for exactly one supplied template model."""

    def __init__(self, reference: str, model: Template) -> None:
        super().__init__(jinja_environment=ImmutableSandboxedEnvironment())
        self.effective_parameters: dict[str, Any] = {}
        self.reference = reference
        self.model = model

    def get_template(self, template_name: str) -> Template | None:
        return self.model if template_name == self.reference else None

    def render_model(self, template: Template, parameters: dict[str, Any]) -> list[Any]:
        self.effective_parameters = self._apply_parameter_defaults(template, parameters)
        return super().render_model(template, parameters)


def initial_result(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_revision": request.get("request_revision", ""),
        "source_hash": source_hash(request.get("source_yaml", "")),
        "stage": request.get("stage", "inspect"),
        "status": "ready",
        "diagnostics": [],
        "missing_parameters": [],
        "saved_dependencies": [],
    }


def run_preview(root: Path, request: dict[str, Any]) -> dict[str, Any]:
    """Worker implementation; the public adapter provides timeout/process bounds."""
    root = root.resolve()
    path = request["source_path"]
    stage = request["stage"]
    result = initial_result(request)
    snapshot = DependencySnapshot(root)
    try:
        source_path = project_path(root, path, template=True)
        model, _, issues = inspect_source(request["source_yaml"], path)
        result["diagnostics"] = issues
        if model is None or any(item["severity"] == "error" for item in issues):
            result["status"] = "invalid"
            return result
        if stage == "inspect":
            return result
        if Path(path).suffix != ".yaml":
            raise ValueError(
                "The current runtime invokes .yaml templates only. This .yml source remains editable in Code."
            )
        parameters = request.get("sample_parameters", {})
        _check_json_value(parameters)
        if not isinstance(parameters, dict):
            raise TypeError("Sample parameters must be a mapping.")
        missing = sorted(
            {p["name"] for p in model.parameters if p.get("required", False)}
            - set(parameters)
        )
        result["missing_parameters"] = missing
        if missing:
            result["status"] = "needs_parameters"
            for index, parameter in enumerate(model.parameters):
                if parameter["name"] in missing:
                    result["diagnostics"].append(
                        diagnostic(
                            path,
                            f"Supply a sample value for required parameter '{parameter['name']}'.",
                            code="LHP-TEMPLATE-SAMPLE",
                            stage=stage,
                            severity="info",
                            field=("parameters", index),
                        )
                    )
            return result
        effective = {
            p["name"]: p["default"] for p in model.parameters if "default" in p
        }
        effective.update(parameters)
        result["effective_parameters"] = effective
        reference = Path(path).relative_to("templates").with_suffix("").as_posix()
        engine = DraftTemplateEngine(reference, model)
        if stage == "expanded":
            result["expanded_actions"] = [
                action.model_dump(mode="json", exclude_none=True)
                for action in engine.render_model(model, parameters)
            ]
            return result
        context = request.get("context") or {}
        if not all(
            isinstance(context.get(key), str) and context[key].strip()
            for key in ("pipeline", "flowgroup", "environment")
        ):
            result["status"] = "needs_context"
            result["diagnostics"].append(
                diagnostic(
                    path,
                    "Choose a sample pipeline, flowgroup and environment to resolve this template.",
                    code="LHP-TEMPLATE-CONTEXT",
                    stage=stage,
                    severity="info",
                )
            )
            return result
        env = context["environment"]
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", env) or env in {".", ".."}:
            raise ValueError("Choose an environment name without path separators.")
        snapshot.watch("lhp.yaml")
        snapshot.preset_paths()
        substitution_path = snapshot.watch(f"substitutions/{env}.yaml")
        project_config = ProjectConfigLoader(root).load_project_config()
        preset_manager = PresetManager(root / "presets")
        substitution = EnhancedSubstitutionManager(
            substitution_path if substitution_path.is_file() else None, env
        )
        flowgroup = FlowGroup(
            pipeline=context["pipeline"],
            flowgroup=context["flowgroup"],
            use_template=reference,
            template_parameters=parameters,
            presets=context.get("presets", []),
            variables=context.get("variables") or None,
        )
        resolver = FlowgroupResolutionService(
            template_engine=engine,
            preset_manager=preset_manager,
            config_validator=PreviewConfigValidator(snapshot, project_config),
            secret_validator=SecretValidator(),
        )
        processed = resolver.resolve(
            FlowGroupContext(flowgroup, source_path, synthetic=True), substitution
        )
        snapshot.assert_current()
        result["effective_parameters"] = engine.effective_parameters
        result["resolved_flowgroup"] = processed.flowgroup.model_dump(
            mode="json", exclude_none=True
        )
    except SecurityError as exc:
        result["status"] = "invalid"
        result["diagnostics"].append(
            diagnostic(
                path,
                f"Preview cannot evaluate this restricted expression: {exc}. Source is unchanged; this restriction applies to preview only.",
                code="LHP-TEMPLATE-PREVIEW-RESTRICTED",
                stage=stage,
            )
        )
    except SourceChangedError as exc:
        result["status"] = "stale"
        result["diagnostics"].append(
            diagnostic(
                path,
                str(exc),
                code="LHP-TEMPLATE-STALE",
                stage=stage,
                severity="warning",
            )
        )
    except (
        LHPError,
        ValidationError,
        ValueError,
        TypeError,
        KeyError,
        OSError,
        TemplateError,
        RecursionError,
    ) as exc:
        result["status"] = "invalid"
        result["diagnostics"].append(
            diagnostic(
                path,
                str(exc),
                code=getattr(exc, "code", "LHP-TEMPLATE-PREVIEW"),
                stage=stage,
                suggestion="Check the selected sample values and the indicated template or saved configuration.",
            )
        )
        try:
            snapshot.assert_current()
        except (SourceChangedError, OSError):
            result["status"] = "stale"
    result["saved_dependencies"] = snapshot.views()
    return result
