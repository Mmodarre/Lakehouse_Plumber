"""Blueprint definitions and instance normalization (legacy + new syntaxes)."""

import logging
from typing import Any, ClassVar, Dict, List, Optional, Set, Union

from pydantic import BaseModel, ConfigDict, model_validator

from ..errors import ErrorCategory, LHPValidationError
from ._action import Action

logger = logging.getLogger(__name__)
# `_legacy_logger` is intentionally named "lhp.models.config" (NOT __name__) for back-compat with the 3 caplog tests in test_blueprint_use_syntax.py / test_blueprint_parser.py that subscribe to this stable identifier across the Phase A1 models.config → _blueprint split.
_legacy_logger = logging.getLogger("lhp.models.config")


class BlueprintParameter(BaseModel):
    """A declared parameter on a blueprint, with optional default and required flag."""

    name: str
    required: bool = False
    default: Optional[Any] = None
    description: Optional[str] = None


class BlueprintFlowgroupSpec(BaseModel):
    """A flowgroup template inside a blueprint.

    Same shape as FlowGroup, but `pipeline` and `flowgroup` are templates that
    contain `%{var}` placeholders resolved at expansion time against instance
    parameter values.
    """

    pipeline: str
    flowgroup: str
    job_name: Optional[str] = None
    variables: Optional[Dict[str, str]] = None
    presets: List[str] = []
    use_template: Optional[str] = None
    template_parameters: Optional[Dict[str, Any]] = None
    actions: List[Action] = []
    operational_metadata: Optional[Union[bool, List[str]]] = None


class Blueprint(BaseModel):
    """A reusable collection of flowgroups instantiated once per BlueprintInstance.

    Distinguishing fields from a regular flowgroup file are `parameters` and
    `flowgroups` (the array of BlueprintFlowgroupSpec). `looks_like_blueprint()`
    keys on the presence of both fields together with the absence of `actions`.
    """

    name: str
    version: str = "1.0"
    description: Optional[str] = None
    parameters: List[BlueprintParameter] = []
    flowgroups: List[BlueprintFlowgroupSpec]


class BlueprintInstance(BaseModel):
    """An instance of a blueprint with concrete parameter values.

    Two input shapes are accepted:

    - **New (preferred):** ``use_blueprint:`` references the blueprint and a
      nested ``parameters:`` block holds parameter values; an optional
      ``overrides:`` block is reserved for future use. This shape mirrors the
      ``use_template:`` / ``template_parameters:`` pattern operators already
      know.
    - **Legacy (deprecated, removed in V0.9):** ``blueprint:`` plus flat
      top-level parameter keys. A deprecation warning is emitted once per
      file when this form is encountered.

    A `model_validator(mode='before')` is the single normalization point: it
    converts both shapes into the canonical (`use_blueprint`, `parameters`)
    form so all downstream code reads from one place. Mixing the two forms
    in the same file raises LHP-CFG-061.
    """

    model_config = ConfigDict(extra="forbid")

    use_blueprint: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    overrides: Optional[Dict[str, Any]] = None
    # Legacy field retained so existing readers (e.g. CLI display, expander)
    # continue to work; populated from `use_blueprint` after normalization.
    blueprint: Optional[str] = None

    # Tracks instance file paths for which we've already emitted the legacy
    # deprecation warning, so callers that re-parse the same file (e.g.
    # validate then generate) only see the warning once per process.
    _legacy_warned_paths: "ClassVar[Set[str]]" = set()

    @model_validator(mode="before")
    @classmethod
    def _normalize_syntax(cls, data: Any, info: Any) -> Any:
        if not isinstance(data, dict):
            return data

        has_use = "use_blueprint" in data
        has_legacy = "blueprint" in data

        file_path: Optional[str] = None
        if info is not None and getattr(info, "context", None):
            file_path = info.context.get("file_path")

        if has_use and has_legacy:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="061",
                title="Conflicting blueprint instance syntax",
                details=(
                    "Instance file uses both 'use_blueprint:' (new) and "
                    "'blueprint:' (legacy) keys. Pick exactly one."
                ),
                suggestions=[
                    "Use 'use_blueprint:' + nested 'parameters:' block (preferred)",
                    "Or use legacy 'blueprint:' with flat parameters "
                    "(deprecated, removed in V0.9)",
                ],
                context={"file": file_path or "<unknown>"},
            )

        if has_use:
            allowed = {"use_blueprint", "parameters", "overrides"}
            extras = sorted(k for k in data if k not in allowed)
            if extras:
                raise LHPValidationError(
                    category=ErrorCategory.VALIDATION,
                    code_number="061",
                    title="Mixing blueprint instance syntax forms",
                    details=(
                        "Instance file uses 'use_blueprint:' but also has "
                        f"flat top-level keys {extras}. With the new syntax, "
                        "all parameter values must live under 'parameters:'."
                    ),
                    suggestions=[
                        f"Move {extras} under the 'parameters:' block",
                    ],
                    context={"file": file_path or "<unknown>", "extras": extras},
                )
            return data

        if has_legacy:
            blueprint_name = data["blueprint"]
            flat_params = {k: v for k, v in data.items() if k != "blueprint"}

            warn_key = file_path or repr(sorted(data.items()))
            if warn_key not in cls._legacy_warned_paths:
                cls._legacy_warned_paths.add(warn_key)
                _legacy_logger.warning(
                    "Deprecated blueprint instance syntax in %s: the "
                    "'blueprint:' + flat parameters form will be removed in "
                    "V0.9. Migrate to 'use_blueprint:' + nested 'parameters:' "
                    "block.",
                    file_path or "<unknown>",
                )

            return {
                "use_blueprint": blueprint_name,
                "blueprint": blueprint_name,
                "parameters": flat_params,
            }

        return data

    @model_validator(mode="after")
    def _mirror_blueprint_field(self) -> "BlueprintInstance":
        if self.use_blueprint and not self.blueprint:
            object.__setattr__(self, "blueprint", self.use_blueprint)
        return self

    @property
    def blueprint_name(self) -> str:
        """Normalized blueprint name (works for both input shapes)."""
        return self.use_blueprint or self.blueprint or ""

    def parameter_values(self) -> Dict[str, Any]:
        """Return the parameter values supplied in this instance file."""
        return dict(self.parameters or {})
