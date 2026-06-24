"""Resolve ``contract`` action fields into inline schema / expectations.

A pre-generation pass (invoked from :class:`FlowgroupResolutionService` after
substitution, mirroring how blueprints expand in-memory): for every action that
carries a ``contract`` field, parse the referenced ODCS contract, select the
entity, and rewrite the action so it carries the resolved artifact **inline** —
then strip the ``contract`` field. No files are written.

Injection is implicit by action type:
  - cloudfiles **load**        → ``source.schema`` (enforced read schema, inline DDL);
    or, when ``schema_hints`` is set, ``cloudFiles.schemaHints`` **instead** (hints-only,
    never both)
  - **write** (streaming_table / materialized_view) → ``write_target.table_schema`` (inline DDL)
  - **schema** transform       → ``schema_inline`` (cast-only ``<col>: <type>`` entries)
  - **data_quality** transform → inline ``expectations`` (list form), action from
    ``expectations_action`` else per-property ``criticalDataElement``

Reuses the pure translators (``lhp.parsers.odcs_parser`` + ``lhp.utils.odcs_mapper``);
raises ``LHPError`` on a missing file, unknown ``type``, unresolved/ambiguous entity,
option/action-type mismatch, or a conflict with an explicitly-set field.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ...errors import ErrorFactory, LHPError, codes
from ...parsers.odcs_parser import OdcsParser
from ...parsers.schema_parser import SchemaParser
from ...utils.odcs_mapper import odcs_property_to_constraints
from .odcs_translator import OdcsTranslator

logger = logging.getLogger(__name__)


class ContractResolver:
    """Rewrite actions that carry a ``contract`` field into inline artifacts."""

    def __init__(self) -> None:
        self._parser = OdcsParser()
        self._translator = OdcsTranslator()
        self._schema_parser = SchemaParser()
        # Cache parsed contracts per resolved path (avoid re-parsing the same
        # file once per contract-bearing action).
        self._parse_cache: Dict[Path, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def resolve(
        self, flowgroup_dict: Dict[str, Any], *, project_root: Path
    ) -> Dict[str, Any]:
        """Return ``flowgroup_dict`` with every ``contract``-bearing action resolved.

        Actions without a ``contract`` are left untouched; the whole dict is
        returned unchanged when no action carries a ``contract`` (cheap no-op).

        :raises lhp.errors.LHPError: invalid/missing contract file, unknown
            ``type``, unresolved or unknown entity, option/action-type mismatch,
            or a conflict with an already-set target field.
        """
        actions = flowgroup_dict.get("actions") or []
        if not any(isinstance(a, dict) and a.get("contract") for a in actions):
            return flowgroup_dict

        for action in actions:
            if not isinstance(action, dict) or not action.get("contract"):
                continue
            self._resolve_action(action, project_root=Path(project_root))

        return flowgroup_dict

    # ------------------------------------------------------------------
    # Per-action resolution
    # ------------------------------------------------------------------
    def _resolve_action(self, action: Dict[str, Any], *, project_root: Path) -> None:
        contract = action["contract"]
        action_name = action.get("name", "<unnamed>")

        # 1. Validate basic contract options.
        file_ref = contract.get("file")
        if not file_ref:
            raise self._error(
                action_name,
                "Contract reference is missing a 'file'.",
                "Set 'contract.file' to a path (relative to the project root).",
            )

        contract_type = contract.get("type", "odcs")
        if contract_type != "odcs":
            raise self._error(
                action_name,
                f"Unsupported contract type {contract_type!r}.",
                "Only 'odcs' contracts are supported (contract.type: odcs).",
            )

        action_kind = self._action_kind(action)

        if contract.get("schema_hints") and action_kind != "cloudfiles_load":
            raise self._error(
                action_name,
                "'schema_hints' is only valid on a cloudfiles load action.",
                "Remove 'schema_hints' or attach the contract to a cloudfiles load.",
            )

        if (
            contract.get("expectations_action") is not None
            and action_kind != "data_quality"
        ):
            raise self._error(
                action_name,
                "'expectations_action' is only valid on a data_quality transform.",
                "Remove 'expectations_action' or attach the contract to a "
                "data_quality transform.",
            )

        if action_kind == "unsupported":
            raise self._error(
                action_name,
                "A 'contract' is not supported on this action type.",
                "Attach contracts to cloudfiles loads, streaming_table / "
                "materialized_view writes, schema transforms, or data_quality "
                "transforms.",
            )

        # 2. Parse the contract.
        contract_dict = self._parse_contract(file_ref, action_name, project_root)
        contract_stem = Path(file_ref).stem

        # 3. Select the entity object.
        obj = self._select_entity(
            contract_dict, contract.get("entity_name"), action_name, file_ref
        )

        # 4. Inject by action type.
        if action_kind == "cloudfiles_load":
            self._inject_cloudfiles(
                action,
                obj,
                contract_stem,
                bool(contract.get("schema_hints")),
                action_name,
            )
        elif action_kind == "write":
            self._inject_write(action, obj, contract_stem, action_name)
        elif action_kind == "schema_transform":
            self._inject_schema_transform(action, obj, action_name)
        elif action_kind == "data_quality":
            self._inject_data_quality(
                action, obj, contract.get("expectations_action"), action_name
            )

        # 6. Strip the contract field.
        action.pop("contract", None)

    # ------------------------------------------------------------------
    # Action-type classification
    # ------------------------------------------------------------------
    @staticmethod
    def _action_kind(action: Dict[str, Any]) -> str:
        action_type = action.get("type")
        if action_type == "load":
            source = action.get("source")
            if isinstance(source, dict) and source.get("type") == "cloudfiles":
                return "cloudfiles_load"
            return "unsupported"
        if action_type == "write":
            target = action.get("write_target")
            if isinstance(target, dict) and target.get("type") in (
                "streaming_table",
                "materialized_view",
            ):
                return "write"
            return "unsupported"
        if action_type == "transform":
            transform_type = action.get("transform_type")
            if transform_type == "schema":
                return "schema_transform"
            if transform_type == "data_quality":
                return "data_quality"
            return "unsupported"
        return "unsupported"

    # ------------------------------------------------------------------
    # Parsing / entity selection
    # ------------------------------------------------------------------
    def _parse_contract(
        self, file_ref: str, action_name: str, project_root: Path
    ) -> Dict[str, Any]:
        resolved = (project_root / file_ref).resolve()
        if resolved in self._parse_cache:
            return self._parse_cache[resolved]

        if not resolved.exists():
            raise self._error(
                action_name,
                f"Contract file not found: {resolved}.",
                "Check 'contract.file' points to an existing ODCS contract "
                "(path is relative to the project root).",
            )

        try:
            contract_dict = self._parser.parse(resolved)
        except LHPError as exc:
            raise self._error(
                action_name,
                f"Could not parse contract {resolved}: {exc.title}.",
                "Ensure the file is a valid ODCS data contract.",
            ) from exc

        self._parse_cache[resolved] = contract_dict
        return contract_dict

    def _select_entity(
        self,
        contract_dict: Dict[str, Any],
        entity_name: Optional[str],
        action_name: str,
        file_ref: str,
    ) -> Dict[str, Any]:
        objects = contract_dict.get("schema", []) or []

        if entity_name is not None:
            for obj in objects:
                if obj.get("name") == entity_name:
                    return obj
            raise self._error(
                action_name,
                f"Entity {entity_name!r} not found in contract {file_ref}.",
                "Set 'contract.entity_name' to one of the contract's schema "
                "object names.",
            )

        if len(objects) == 1:
            return objects[0]

        raise self._error(
            action_name,
            f"Contract {file_ref} has {len(objects)} schema objects; "
            "'entity_name' is required to select one.",
            "Set 'contract.entity_name' to the schema object to resolve.",
        )

    # ------------------------------------------------------------------
    # Injection by action type
    # ------------------------------------------------------------------
    def _entity_ddl(self, obj: Dict[str, Any], contract_stem: str) -> str:
        """Build the schema-hints DDL string for the selected entity object."""
        artifacts = self._translator.translate_schemas(
            {"schema": [obj]}, contract_stem=contract_stem
        )
        return self._schema_parser.to_schema_hints(artifacts[0].schema_dict)

    def _inject_cloudfiles(
        self,
        action: Dict[str, Any],
        obj: Dict[str, Any],
        contract_stem: str,
        schema_hints: bool,
        action_name: str,
    ) -> None:
        source = action.setdefault("source", {})
        ddl = self._entity_ddl(obj, contract_stem)

        if schema_hints:
            # Hints-only: emit ``cloudFiles.schemaHints`` and NEVER an enforced
            # read schema — the two are mutually exclusive. Auto Loader infers
            # the schema and the hints pin the contract's declared types.
            options = source.setdefault("options", {})
            if options.get("cloudFiles.schemaHints"):
                raise self._conflict(action_name, "cloudFiles.schemaHints")
            options["cloudFiles.schemaHints"] = ddl
        else:
            # Default: inject the full enforced read schema.
            if source.get("schema"):
                raise self._conflict(action_name, "source.schema")
            source["schema"] = ddl

    def _inject_write(
        self,
        action: Dict[str, Any],
        obj: Dict[str, Any],
        contract_stem: str,
        action_name: str,
    ) -> None:
        target = action.setdefault("write_target", {})
        if target.get("table_schema"):
            raise self._conflict(action_name, "write_target.table_schema")

        target["table_schema"] = self._entity_ddl(obj, contract_stem)

    def _inject_schema_transform(
        self, action: Dict[str, Any], obj: Dict[str, Any], action_name: str
    ) -> None:
        if action.get("schema_inline"):
            raise self._conflict(action_name, "schema_inline")

        artifacts = self._translator.translate_schemas(
            {"schema": [obj]}, contract_stem="contract"
        )
        schema_columns = artifacts[0].schema_dict.get("columns", [])
        entries = [f"{col['name']}: {col['type']}" for col in schema_columns]
        action["schema_inline"] = "\n".join(entries)

    def _inject_data_quality(
        self,
        action: Dict[str, Any],
        obj: Dict[str, Any],
        expectations_action: Optional[str],
        action_name: str,
    ) -> None:
        if action.get("expectations"):
            raise self._conflict(action_name, "expectations")
        if action.get("expectations_file"):
            raise self._conflict(action_name, "expectations_file")

        expectations: List[Dict[str, str]] = []
        for prop in obj.get("properties", []) or []:
            constraints = odcs_property_to_constraints(prop)
            if not constraints:
                continue
            if expectations_action is not None:
                failure_action = expectations_action
            else:
                failure_action = "fail" if prop.get("criticalDataElement") else "warn"
            for predicate, name in constraints:
                expectations.append(
                    {
                        "name": name,
                        "expression": predicate,
                        "failureAction": failure_action,
                    }
                )

        action["expectations"] = expectations

    # ------------------------------------------------------------------
    # Error helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _error(action_name: str, details: str, suggestion: str) -> LHPError:
        return ErrorFactory.config_error(
            codes.CFG_064,
            title="Could not resolve action contract",
            details=f"Action '{action_name}': {details}",
            suggestions=[suggestion],
            context={"Action": action_name},
        )

    @classmethod
    def _conflict(cls, action_name: str, field: str) -> LHPError:
        return cls._error(
            action_name,
            f"Field '{field}' is already set; a contract would overwrite it.",
            f"Remove either '{field}' or the 'contract' reference on this action.",
        )
