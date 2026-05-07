"""Parser for blueprint and instance YAML files.

Separated from `yaml_parser.py` so the blueprint discriminator is checked early
and errors are clear. Blueprint files always live under the configured
`blueprint_include` patterns; instance files under `instance_include` patterns.
"""

import difflib
import logging
from pathlib import Path
from typing import Any, Dict, List

from ..models.config import (
    Blueprint,
    BlueprintInstance,
)
from ..utils.error_formatter import (
    ErrorCategory,
    LHPConfigError,
    LHPError,
    LHPValidationError,
)
from ..utils.yaml_loader import load_yaml_documents_all


class BlueprintParser:
    """Parses blueprint and instance YAML files into validated Pydantic models."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def parse_blueprint_file(self, path: Path) -> Blueprint:
        """Parse a blueprint YAML file into a `Blueprint` model.

        Args:
            path: Path to a blueprint YAML file (single-document).

        Returns:
            A validated Blueprint model.

        Raises:
            LHPConfigError: If the file is empty, multi-document, or fails Pydantic validation.
        """
        documents = load_yaml_documents_all(
            path, error_context=f"blueprint file {path}"
        )

        if not documents:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="047",
                title="Empty blueprint file",
                details=f"No content found in blueprint file {path}",
                suggestions=[
                    "Ensure the file contains valid YAML content",
                    "Verify the file has 'name', 'parameters', and 'flowgroups' top-level keys",
                ],
                context={"file": str(path)},
            )

        if len(documents) > 1:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="048",
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
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="049",
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
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="050",
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
        """Parse an instance YAML file into a validated `BlueprintInstance`.

        Two-pass parse: first reads the `blueprint:` field to identify the referenced
        blueprint, then re-validates the instance file's keys against
        `{"blueprint"} | {p.name for p in blueprint.parameters}` and raises on
        unknown keys with a `difflib.get_close_matches` suggestion.

        Args:
            path: Path to an instance YAML file (single-document).
            blueprints: Mapping of blueprint name -> Blueprint, used to look up
                the referenced blueprint and its declared parameter list.

        Returns:
            A validated BlueprintInstance.

        Raises:
            LHPConfigError: For empty/multi-doc files or YAML errors.
            LHPValidationError: For unknown blueprint references, unknown parameter
                keys (M5), or missing required parameters.
        """
        documents = load_yaml_documents_all(path, error_context=f"instance file {path}")

        if not documents:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="051",
                title="Empty instance file",
                details=f"No content found in instance file {path}",
                suggestions=[
                    "Ensure the file has a 'blueprint' key plus parameter values",
                ],
                context={"file": str(path)},
            )

        if len(documents) > 1:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="052",
                title="Multi-document instance file",
                details=(
                    f"Instance file {path} contains {len(documents)} YAML "
                    "documents; instance files must be single-document."
                ),
                suggestions=[
                    "Place each instance in its own file under instance_include",
                ],
                context={"file": str(path), "documents": len(documents)},
            )

        doc = documents[0]

        if not isinstance(doc, dict) or "blueprint" not in doc:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="053",
                title="Instance file missing 'blueprint' key",
                details=(
                    f"Instance file {path} must declare a top-level 'blueprint:' "
                    "key naming the blueprint it instantiates."
                ),
                suggestions=[
                    "Add 'blueprint: <blueprint_name>' as the first key",
                    "Verify the value matches an existing blueprint's name",
                ],
                context={"file": str(path)},
            )

        blueprint_name = doc["blueprint"]
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
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="041",
                title="Instance references unknown blueprint",
                details=details,
                suggestions=suggestions,
                context={"file": str(path), "blueprint": blueprint_name},
            )

        blueprint = blueprints[blueprint_name]
        self._validate_instance_keys(path, doc, blueprint)
        self._validate_required_parameters(path, doc, blueprint)

        try:
            return BlueprintInstance(**doc)
        except LHPError:
            raise
        except Exception as e:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="054",
                title="Invalid instance definition",
                details=f"Error parsing instance {path}: {e}",
                suggestions=[
                    "Verify the file has a 'blueprint' key plus parameter values",
                ],
                context={"file": str(path)},
            ) from e

    @staticmethod
    def _validate_instance_keys(
        path: Path, doc: Dict[str, Any], blueprint: Blueprint
    ) -> None:
        """Verify each instance key is `blueprint` or a declared parameter."""
        valid_keys = {"blueprint"} | {p.name for p in blueprint.parameters}
        unknown = [k for k in doc if k not in valid_keys]
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

        raise LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="043",
            title="Unknown parameter key in instance",
            details=(
                f"Instance {path} contains unknown key(s) {unknown!r}. "
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
        path: Path, doc: Dict[str, Any], blueprint: Blueprint
    ) -> None:
        """Verify required parameters are present in the instance file."""
        missing = [
            p.name for p in blueprint.parameters if p.required and p.name not in doc
        ]
        if not missing:
            return

        raise LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="042",
            title="Required parameter missing in instance",
            details=(
                f"Instance {path} for blueprint '{blueprint.name}' is missing "
                f"required parameter(s): {missing}."
            ),
            suggestions=[
                f"Add the missing parameter(s) {missing} to {path}",
                f"Or declare the parameter as optional on blueprint "
                f"'{blueprint.name}'",
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
