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

import yaml

from ...errors import ErrorFactory, LHPError, codes
from ...parsers.odcs_parser import OdcsParser
from ...parsers.schema_parser import SchemaParser
from ...utils.odcs_mapper import odcs_property_to_constraints, odcs_quality_to_tests
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

        root = Path(project_root)
        # Rebuild the actions list: most contract actions resolve 1:1 (mutated in
        # place), but a ``test`` action expands 1→N (one action per quality rule).
        new_actions: list = []
        for action in actions:
            if not isinstance(action, dict) or not action.get("contract"):
                new_actions.append(action)
                continue
            if self._action_kind(action) == "test_expand":
                new_actions.extend(
                    self._expand_test_action(action, project_root=root)
                )
            else:
                self._resolve_action(action, project_root=root)
                new_actions.append(action)

        flowgroup_dict["actions"] = new_actions
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
            contract_dict, contract.get("object"), action_name, file_ref
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
    # Test-action expansion (1 → N)
    # ------------------------------------------------------------------
    def _expand_test_action(
        self, action: Dict[str, Any], *, project_root: Path
    ) -> List[Dict[str, Any]]:
        """Expand a ``contract``-bearing ``test`` action into many test actions.

        One concrete LHP test action is produced per mappable ODCS ``quality``
        rule (dataset-level and property-level) on the selected entity; each
        carries the original ``source`` (the single table under test), a unique
        ``name``, a resolved ``test_type`` + fields, and ``on_violation`` derived
        from the rule ``severity``. The ``contract`` field is dropped.

        :raises lhp.errors.LHPError: invalid/missing contract file, unknown
            ``type``, unresolved/unknown entity, a missing ``source``, or an
            explicit ``test_type`` (mutually exclusive with ``contract``).
        """
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

        if action.get("test_type") is not None:
            raise self._error(
                action_name,
                "'test_type' is mutually exclusive with a 'contract' on a test "
                "action.",
                "Remove 'test_type' to expand the contract's quality rules, or "
                "remove the 'contract' reference.",
            )

        if not action.get("source"):
            raise self._error(
                action_name,
                "A contract-bearing test action is missing a 'source'.",
                "Set 'source' to the single table the quality rules run against.",
            )

        # 2. Parse the contract.
        contract_dict = self._parse_contract(file_ref, action_name, project_root)

        # 3. Select the entity object.
        obj = self._select_entity(
            contract_dict, contract.get("object"), action_name, file_ref
        )

        # 4. Map the entity's quality rules to partial test dicts.
        tests = odcs_quality_to_tests(obj, source=action["source"])

        # 5. Finalise each emitted dict into a concrete test action.
        base = action["name"]
        used_names: set = set()
        expanded: List[Dict[str, Any]] = []
        for partial in tests:
            partial["type"] = "test"
            name = f"{base}_{partial['name']}"
            unique = name
            index = 1
            while unique in used_names:
                index += 1
                unique = f"{name}_{index}"
            used_names.add(unique)
            partial["name"] = unique
            partial.pop("contract", None)
            expanded.append(partial)

        return expanded

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
        if action_type == "test":
            # A contract on a test action expands 1→N into concrete test actions.
            return "test_expand"
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
        object_name: Optional[str],
        action_name: str,
        file_ref: str,
    ) -> Dict[str, Any]:
        objects = contract_dict.get("schema", []) or []

        if object_name is not None:
            for obj in objects:
                if obj.get("name") == object_name:
                    return obj
            raise self._error(
                action_name,
                f"Object {object_name!r} not found in contract {file_ref}.",
                "Set 'contract.object' to one of the contract's schema "
                "object names.",
            )

        if len(objects) == 1:
            return objects[0]

        raise self._error(
            action_name,
            f"Contract {file_ref} has {len(objects)} schema objects; "
            "'object' is required to select one.",
            "Set 'contract.object' to the schema object to resolve.",
        )

    # ------------------------------------------------------------------
    # Injection by action type
    # ------------------------------------------------------------------
    def _entity_ddl(
        self, obj: Dict[str, Any], contract_stem: str, *, physical: bool = False
    ) -> str:
        """Build the schema-hints DDL string for the selected entity object.

        When ``physical`` is set (the cloudfiles **read** schema, which reads raw
        source files), each column uses its ODCS ``physicalName`` where provided,
        falling back to the contract ``name``. Otherwise (write ``table_schema``)
        the contract ``name`` is always used.
        """
        artifacts = self._translator.translate_schemas(
            {"schema": [obj]}, contract_stem=contract_stem
        )
        schema_dict = artifacts[0].schema_dict
        if physical:
            schema_dict = {
                **schema_dict,
                "columns": [
                    {**col, "name": col.get("physical_name") or col["name"]}
                    for col in schema_dict.get("columns", [])
                ],
            }
        return self._schema_parser.to_schema_hints(schema_dict)

    def _inject_cloudfiles(
        self,
        action: Dict[str, Any],
        obj: Dict[str, Any],
        contract_stem: str,
        schema_hints: bool,
        action_name: str,
    ) -> None:
        source = action.setdefault("source", {})
        ddl = self._entity_ddl(obj, contract_stem, physical=True)

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

        # Emit the legacy ``column_mapping`` / ``type_casting`` dict form rather than
        # the arrow mini-language: source names go in as YAML keys, so any name —
        # including ones with spaces or other non-identifier characters — works.
        # Rename (column_mapping) only when a property's physicalName differs from
        # its contract name; cast (type_casting) every column to its contract type.
        column_mapping: Dict[str, str] = {}
        type_casting: Dict[str, str] = {}
        for col in schema_columns:
            name = col["name"]
            src = col.get("physical_name")
            if src and src != name:
                column_mapping[src] = name
            type_casting[name] = col["type"]

        inline: Dict[str, Any] = {}
        if column_mapping:
            inline["column_mapping"] = column_mapping
        inline["type_casting"] = type_casting
        action["schema_inline"] = yaml.safe_dump(inline, sort_keys=False).strip()

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
