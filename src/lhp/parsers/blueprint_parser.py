"""Parser for blueprint and instance YAML files.

Separated from `yaml_parser.py` so the blueprint discriminator is checked early
and errors are clear. Blueprint files always live under the configured
`blueprint_include` patterns; instance files under `instance_include` patterns.
"""

import difflib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from lhp.models import Blueprint, BlueprintInstance

from ..errors import ErrorFactory, LHPError, codes
from .yaml_loader import load_yaml_documents_all

if TYPE_CHECKING:
    from .yaml_parser import CachingYAMLParser


class BlueprintParser:
    def __init__(
        self, caching_yaml_parser: Optional["CachingYAMLParser"] = None
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.caching_yaml_parser = caching_yaml_parser

    def _load_documents(self, path: Path, error_context: str) -> List[Dict[str, Any]]:
        """Load all YAML documents from ``path``, routing through the cache
        when one was wired at construction time.
        """
        if self.caching_yaml_parser is not None:
            return self.caching_yaml_parser.load_documents_all(
                path, error_context=error_context
            )
        return load_yaml_documents_all(path, error_context=error_context)

    def parse_blueprint_file(self, path: Path) -> Blueprint:
        documents = self._load_documents(path, error_context=f"blueprint file {path}")

        if not documents:
            raise ErrorFactory.config_error(
                codes.CFG_047,
                title="Empty blueprint file",
                details=f"No content found in blueprint file {path}",
                suggestions=[
                    "Ensure the file contains valid YAML content",
                    "Verify the file has 'name', 'parameters', and 'flowgroups' top-level keys",
                ],
                context={"file": str(path)},
            )

        if len(documents) > 1:
            raise ErrorFactory.config_error(
                codes.CFG_048,
                title="Multi-document blueprint file",
                details=(
                    f"Blueprint file {path} contains {len(documents)} YAML "
                    "documents; blueprints must be single-document files."
                ),
                suggestions=[
                    "Remove '---' document separators from blueprint files",
                    "If you need multiple blueprints, place each in its own file",
                ],
                context={"file": str(path), "documents": len(documents)},
            )

        doc = documents[0]
        if not self.looks_like_blueprint(doc):
            raise ErrorFactory.config_error(
                codes.CFG_049,
                title="File is not a blueprint",
                details=(
                    f"{path} is configured as a blueprint (matches blueprint_include "
                    "patterns) but does not have the shape of a blueprint. Expected "
                    "top-level 'name', 'parameters', and 'flowgroups' keys."
                ),
                suggestions=[
                    f"Ensure {path} has 'name', 'parameters', and 'flowgroups' keys",
                    "Or move it out of the blueprint_include glob",
                ],
                context={"file": str(path)},
            )

        try:
            return Blueprint(**doc)
        except LHPError:
            raise
        except Exception as e:
            raise ErrorFactory.config_error(
                codes.CFG_050,
                title="Invalid blueprint definition",
                details=f"Error parsing blueprint {path}: {e}",
                suggestions=[
                    "Verify required keys 'name' and 'flowgroups' are present",
                    "Each flowgroup spec must have 'pipeline' and 'flowgroup' fields",
                    "Each parameter must have a 'name' field",
                ],
                context={"file": str(path)},
            ) from e

    def parse_instance_file(
        self, path: Path, blueprints: Dict[str, Blueprint]
    ) -> BlueprintInstance:
        """Two-pass parse: reads the blueprint reference (``use_blueprint:`` or
        legacy ``blueprint:``) to identify the referenced blueprint, then
        re-validates the supplied parameter dict against
        ``{p.name for p in blueprint.parameters}`` and raises on unknown keys
        with a `difflib.get_close_matches` suggestion.
        """
        documents = self._load_documents(path, error_context=f"instance file {path}")

        if not documents:
            raise ErrorFactory.config_error(
                codes.CFG_051,
                title="Empty instance file",
                details=f"No content found in instance file {path}",
                suggestions=[
                    "Ensure the file has 'use_blueprint:' + nested 'parameters:'",
                ],
                context={"file": str(path)},
            )

        if len(documents) > 1:
            raise ErrorFactory.config_error(
                codes.CFG_052,
                title="Multi-document instance file",
                details=(
                    f"Instance file {path} contains {len(documents)} YAML "
                    "documents; instance files must be single-document."
                ),
                suggestions=[
                    "Place each instance file under "
                    "pipelines/<system>/<layer>/ or your configured "
                    "instance_include patterns",
                ],
                context={"file": str(path), "documents": len(documents)},
            )

        doc = documents[0]

        if not isinstance(doc, dict) or not (
            "use_blueprint" in doc or "blueprint" in doc
        ):
            raise ErrorFactory.validation_error(
                codes.VAL_053,
                title="Instance file missing blueprint reference",
                details=(
                    f"Instance file {path} must declare a top-level "
                    "'use_blueprint:' (preferred) or legacy 'blueprint:' key "
                    "naming the blueprint it instantiates."
                ),
                suggestions=[
                    "Add 'use_blueprint: <blueprint_name>' as the first key, "
                    "with a nested 'parameters:' block underneath",
                    "Verify the value matches an existing blueprint's name",
                ],
                context={"file": str(path)},
            )

        # Pre-validate dual-syntax conflicts here so we can raise an
        # LHPValidationError directly. (Pydantic wraps any ValueError raised
        # in a model_validator into its own ValidationError, which would
        # otherwise be swallowed by the generic Exception handler below.)
        self._validate_instance_syntax(path, doc)

        blueprint_name = doc.get("use_blueprint") or doc.get("blueprint")
        if not isinstance(blueprint_name, str) or not blueprint_name:
            raise ErrorFactory.config_error(
                codes.CFG_054,
                title="Invalid instance definition",
                details=(
                    f"Instance {path} has an invalid blueprint reference: "
                    "'use_blueprint:' / 'blueprint:' must name the blueprint as a "
                    f"non-empty string, got {type(blueprint_name).__name__} "
                    f"({blueprint_name!r})."
                ),
                suggestions=[
                    "Set 'use_blueprint: <blueprint_name>' to a single string "
                    "naming an existing blueprint",
                    "Do not use a list, mapping, or other value for the "
                    "blueprint reference",
                ],
                context={"file": str(path)},
            )
        if blueprint_name not in blueprints:
            available = ", ".join(sorted(blueprints.keys())) or "(none)"
            close = difflib.get_close_matches(
                blueprint_name, blueprints.keys(), n=1, cutoff=0.6
            )
            details = (
                f"Instance {path} references blueprint '{blueprint_name}' which "
                f"does not exist. Available blueprints: {available}."
            )
            suggestions = [
                "Check the blueprint name in the instance file",
                "Ensure the blueprint file is present and matches blueprint_include",
            ]
            if close:
                suggestions.insert(0, f"Did you mean '{close[0]}'?")
            raise ErrorFactory.validation_error(
                codes.VAL_041,
                title="Instance references unknown blueprint",
                details=details,
                suggestions=suggestions,
                context={"file": str(path), "blueprint": blueprint_name},
            )

        blueprint = blueprints[blueprint_name]
        param_dict = self._extract_param_dict(doc)
        self._validate_instance_keys(path, param_dict, blueprint)
        self._validate_required_parameters(path, param_dict, blueprint)

        try:
            return BlueprintInstance.model_validate(
                doc, context={"file_path": str(path)}
            )
        except LHPError:
            raise
        except Exception as e:
            raise ErrorFactory.config_error(
                codes.CFG_054,
                title="Invalid instance definition",
                details=f"Error parsing instance {path}: {e}",
                suggestions=[
                    "Verify the file uses 'use_blueprint:' + nested "
                    "'parameters:' (preferred) or legacy 'blueprint:' + flat "
                    "parameters",
                ],
                context={"file": str(path)},
            ) from e

    @staticmethod
    def _validate_instance_syntax(path: Path, doc: Dict[str, Any]) -> None:
        """Reject mixed/conflicting blueprint instance syntax (LHP-VAL-061).

        Detects two failure modes:
        - Both ``use_blueprint:`` and ``blueprint:`` declared in the same file.
        - ``use_blueprint:`` plus flat top-level keys outside the allowed
          set ``{use_blueprint, parameters, overrides}``.
        """
        if "use_blueprint" in doc and "blueprint" in doc:
            raise ErrorFactory.validation_error(
                codes.VAL_061,
                title="Conflicting blueprint instance syntax",
                details=(
                    f"Instance file {path} uses both 'use_blueprint:' (new) "
                    "and 'blueprint:' (legacy) keys. Pick exactly one."
                ),
                suggestions=[
                    "Use 'use_blueprint:' + nested 'parameters:' block (preferred)",
                    "Or use legacy 'blueprint:' with flat parameters "
                    "(deprecated, removed in V0.9)",
                ],
                context={"file": str(path)},
            )
        if "use_blueprint" not in doc:
            return
        allowed = {"use_blueprint", "parameters", "overrides"}
        extras = sorted(k for k in doc if k not in allowed)
        if not extras:
            return
        raise ErrorFactory.validation_error(
            codes.VAL_061,
            title="Mixing blueprint instance syntax forms",
            details=(
                f"Instance file {path} uses 'use_blueprint:' but also has "
                f"flat top-level keys {extras}. With the new syntax, all "
                "parameter values must live under 'parameters:'."
            ),
            suggestions=[
                f"Move {extras} under the 'parameters:' block",
            ],
            context={"file": str(path), "extras": extras},
        )

    @staticmethod
    def _extract_param_dict(doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extract the supplied parameter values from either input shape.

        - New shape: parameters live under the ``parameters:`` key.
        - Legacy shape: parameters are flat top-level keys (everything except
          ``blueprint``).
        """
        if "use_blueprint" in doc:
            params = doc.get("parameters") or {}
            return params if isinstance(params, dict) else {}
        # Legacy: flat keys minus 'blueprint'
        return {k: v for k, v in doc.items() if k != "blueprint"}

    @staticmethod
    def _validate_instance_keys(
        path: Path, params: Dict[str, Any], blueprint: Blueprint
    ) -> None:
        valid_keys = {p.name for p in blueprint.parameters}
        unknown = [k for k in params if k not in valid_keys]
        if not unknown:
            return

        param_names = [p.name for p in blueprint.parameters]
        suggestions: List[str] = []
        for bad_key in unknown:
            close = difflib.get_close_matches(bad_key, param_names, n=1, cutoff=0.6)
            if close:
                suggestions.append(f"Did you mean '{close[0]}' instead of '{bad_key}'?")
            else:
                suggestions.append(
                    f"Remove '{bad_key}' or declare it as a parameter on "
                    f"blueprint '{blueprint.name}'"
                )

        raise ErrorFactory.validation_error(
            codes.VAL_043,
            title="Unknown parameter key in instance",
            details=(
                f"Instance {path} contains unknown parameter key(s) {unknown!r}. "
                f"Blueprint '{blueprint.name}' declares parameters: "
                f"{sorted(param_names) or '(none)'}."
            ),
            suggestions=suggestions,
            context={
                "file": str(path),
                "blueprint": blueprint.name,
                "unknown_keys": unknown,
            },
        )

    @staticmethod
    def _validate_required_parameters(
        path: Path, params: Dict[str, Any], blueprint: Blueprint
    ) -> None:
        missing = [
            p.name for p in blueprint.parameters if p.required and p.name not in params
        ]
        if not missing:
            return

        raise ErrorFactory.validation_error(
            codes.VAL_042,
            title="Required parameter missing in instance",
            details=(
                f"Instance {path} for blueprint '{blueprint.name}' is missing "
                f"required parameter(s): {missing}."
            ),
            suggestions=[
                f"Add the missing parameter(s) {missing} to {path}",
                f"Or declare the parameter as optional on blueprint '{blueprint.name}'",
            ],
            context={
                "file": str(path),
                "blueprint": blueprint.name,
                "missing": missing,
            },
        )

    @staticmethod
    def looks_like_blueprint(doc: Any) -> bool:
        """Return True if a YAML document has the shape of a blueprint.

        Used both by the blueprint parser itself and by the defensive
        discriminator in `yaml_parser.py:parse_flowgroups_from_file`
        so a blueprint accidentally placed in `pipelines/` raises a clear error
        instead of crashing on a missing `actions:` key.

        Heuristic: top-level keys 'parameters' and 'flowgroups' are present,
        AND there is no top-level 'actions' key. Regular flowgroup files always
        carry 'actions' (or use the array syntax with per-element 'actions');
        blueprints carry 'parameters' and 'flowgroups[*].actions' instead.
        """
        if not isinstance(doc, dict):
            return False
        if "actions" in doc:
            return False
        return "parameters" in doc and "flowgroups" in doc

    @staticmethod
    def looks_like_instance(doc: Any) -> bool:
        """Return True if a YAML document has the shape of a blueprint instance.

        Used by the routing logic in `yaml_parser.parse_flowgroups_from_file`
        and by `BlueprintDiscoverer.discover_instances` to skip files that
        match an overlapping include/instance_include glob but belong to the
        other discoverer.

        Heuristic:
        - New shape: top-level ``use_blueprint:`` key is present.
        - Legacy shape: top-level ``blueprint:`` key is present (string),
          AND there is no ``flowgroups:`` and no ``actions:`` key (those would
          indicate a flowgroup or blueprint definition).
        """
        if not isinstance(doc, dict):
            return False
        if "use_blueprint" in doc:
            return True
        if "blueprint" in doc and isinstance(doc["blueprint"], str):
            if "flowgroups" in doc or "actions" in doc:
                return False
            return True
        return False
