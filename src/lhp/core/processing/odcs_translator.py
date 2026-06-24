"""Translate ODCS contracts into LHP artifacts.

Each object in an ODCS contract's ``schema`` array becomes one
:class:`SchemaArtifact` (LHP schema format consumed by
:class:`lhp.parsers.schema_parser.SchemaParser`) and, when it has row-level
property constraints, one :class:`ExpectationsArtifact` (the dict format
consumed by the ``data_quality`` transform).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List

from ...utils.odcs_mapper import odcs_property_to_constraints, odcs_type_to_spark

logger = logging.getLogger(__name__)


def _slug(name: str) -> str:
    """Replace filesystem-unsafe characters in an object name."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)


@dataclass
class SchemaArtifact:
    """A single translated LHP schema (consumed in-memory by the contract resolver).

    :param object_name: the ODCS schema object's ``name``.
    :param file_name: output filename ``<contract-stem>.<object>_schema.yaml``.
    :param schema_dict: LHP schema dict (``name``/``version``/``description``/
        ``columns``/``primary_key``).
    """

    object_name: str
    file_name: str
    schema_dict: Dict[str, Any]


@dataclass
class ExpectationsArtifact:
    """A single translated LHP expectations artifact (consumed in-memory).

    :param object_name: the ODCS schema object's ``name``.
    :param file_name: output filename ``<contract-stem>.<object>_expectations.yaml``.
    :param expectations_dict: LHP expectations in the new dict format —
        ``{<constraint sql>: {"action": <warn|drop|fail>, "name": <slug>}}``.
    """

    object_name: str
    file_name: str
    expectations_dict: Dict[str, Dict[str, str]]


class OdcsTranslator:
    """Translate a parsed ODCS contract into LHP schema and expectations artifacts."""

    def translate_schemas(
        self, contract: Dict[str, Any], *, contract_stem: str
    ) -> List[SchemaArtifact]:
        """Translate every object in ``contract['schema']`` to a SchemaArtifact.

        :param contract: a parsed (and ODCS-valid) contract dict.
        :param contract_stem: the source contract filename without extension,
            used to build the collision-safe ``<stem>.<object>_schema.yaml`` name.
        :raises lhp.errors.LHPError: ``LHP-CFG-063`` for unmappable column types.
        """
        version = contract.get("version")
        artifacts: List[SchemaArtifact] = []

        for obj in contract.get("schema", []) or []:
            object_name = obj["name"]
            properties = obj.get("properties", []) or []

            columns: List[Dict[str, Any]] = []
            for prop in properties:
                column: Dict[str, Any] = {
                    "name": prop["name"],
                    "type": odcs_type_to_spark(prop),
                    "nullable": not prop.get("required", False),
                }
                if "description" in prop:
                    column["comment"] = prop["description"]
                if prop.get("physicalName"):
                    column["physical_name"] = prop["physicalName"]
                columns.append(column)

            schema_dict: Dict[str, Any] = {
                "name": object_name,
                "version": version,
            }
            if "description" in obj:
                schema_dict["description"] = obj["description"]
            schema_dict["columns"] = columns

            pk_props = [p for p in properties if p.get("primaryKey") is True]
            if pk_props:
                pk_props.sort(key=lambda p: p.get("primaryKeyPosition", 0))
                schema_dict["primary_key"] = [p["name"] for p in pk_props]

            file_name = f"{contract_stem}.{_slug(object_name)}_schema.yaml"
            artifacts.append(
                SchemaArtifact(
                    object_name=object_name,
                    file_name=file_name,
                    schema_dict=schema_dict,
                )
            )

        return artifacts

    def translate_expectations(
        self, contract: Dict[str, Any], *, contract_stem: str
    ) -> List[ExpectationsArtifact]:
        """Translate row-level property constraints into expectations artifacts.

        One :class:`ExpectationsArtifact` per schema object that has at least one
        derivable constraint; objects yielding no constraints are skipped (no
        empty files). Each property's predicates (from
        :func:`lhp.utils.odcs_mapper.odcs_property_to_constraints`)
        become entries in the new dict format, with ``action`` resolved per
        property from ``criticalDataElement`` (``true`` → ``fail`` else ``warn``).
        ``file_name`` is ``<contract-stem>.<object>_expectations.yaml``.
        """
        artifacts: List[ExpectationsArtifact] = []

        for obj in contract.get("schema", []) or []:
            expectations_dict: Dict[str, Dict[str, str]] = {}
            for prop in obj.get("properties", []) or []:
                action = "fail" if prop.get("criticalDataElement") else "warn"
                for predicate, name in odcs_property_to_constraints(prop):
                    expectations_dict[predicate] = {"action": action, "name": name}

            if not expectations_dict:
                continue

            object_name = obj["name"]
            file_name = f"{contract_stem}.{_slug(object_name)}_expectations.yaml"
            artifacts.append(
                ExpectationsArtifact(
                    object_name=object_name,
                    file_name=file_name,
                    expectations_dict=expectations_dict,
                )
            )

        return artifacts
