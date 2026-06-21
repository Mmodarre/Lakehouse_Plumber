"""Parse and validate ODCS data-contract YAML files."""

from __future__ import annotations

import json
import logging
from importlib.resources import files
from pathlib import Path
from typing import Any, Dict

import jsonschema

from ..errors import ErrorFactory, codes
from .yaml_loader import load_yaml_file

logger = logging.getLogger(__name__)

# The vendored ODCS JSON Schema lives in the ``lhp.schemas`` package alongside
# the other ``*.schema.json`` files (sourced verbatim from bitol-io
# ``schema/odcs-json-schema-latest.json``). Resolve it via importlib.resources
# so it is found in a built wheel, not just an editable install — matching how
# ``lhp.core.loaders.init_template_loader`` loads the LHP schemas.
ODCS_SCHEMA_RESOURCE = ("lhp.schemas", "odcs.schema.json")


class OdcsParser:
    """Load an ODCS contract from disk and validate it against the ODCS schema."""

    def __init__(self) -> None:
        package, resource = ODCS_SCHEMA_RESOURCE
        schema_text = (files(package) / resource).read_text(encoding="utf-8")
        self._schema: Dict[str, Any] = json.loads(schema_text)
        self._validator = jsonschema.Draft201909Validator(self._schema)

    def parse(self, path: Path) -> Dict[str, Any]:
        """Load ``path`` as YAML and validate it against the ODCS JSON Schema.

        :returns: the parsed contract as a dict.
        :raises lhp.errors.LHPError: ``LHP-CFG-062`` if the file is not a valid
            ODCS contract (schema-validation failure), or the standard YAML /
            IO error codes if the file cannot be read.
        """
        path = Path(path)
        contract = load_yaml_file(path, error_context=f"ODCS contract {path}")

        try:
            self._validator.validate(contract)
        except jsonschema.ValidationError as e:
            raise ErrorFactory.config_error(
                codes.CFG_062,
                title="Invalid ODCS data contract",
                details=(f"Contract {path} failed ODCS schema validation: {e.message}"),
                suggestions=[
                    "Check the contract against the ODCS specification",
                    "Ensure required top-level fields are present "
                    "(version, kind, apiVersion, id, status)",
                ],
                context={"file": str(path)},
            ) from e

        return contract
