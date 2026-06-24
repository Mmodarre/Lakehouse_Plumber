"""Tests for ODCS contract -> LHP schema translation (Slice 1).

Covers:
  * ``lhp.utils.odcs_mapper.odcs_type_to_spark`` (type-mapping table)
  * ``lhp.parsers.odcs_parser.OdcsParser`` (validate + parse)
  * ``lhp.core.processing.odcs_translator.OdcsTranslator`` (object -> artifact)
  * round-trip of a translated schema through ``SchemaParser.to_schema_hints``
"""

import textwrap
from pathlib import Path

import pytest
import yaml

from lhp.core.processing.odcs_translator import OdcsTranslator, SchemaArtifact
from lhp.errors import LHPError
from lhp.parsers.odcs_parser import OdcsParser
from lhp.parsers.schema_parser import SchemaParser
from lhp.utils.odcs_mapper import odcs_type_to_spark

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

# A fully-featured contract that is *valid* against the vendored ODCS JSON
# Schema. NOTE: the ODCS schema does NOT permit ``precision``/``scale`` inside
# ``logicalTypeOptions``, so a DECIMAL is expressed via ``physicalType`` here
# (precision/scale-from-logicalTypeOptions is exercised in the unit tests of
# ``odcs_type_to_spark`` directly, where no schema validation occurs).
VALID_CONTRACT_YAML = textwrap.dedent(
    """
    version: "1.0.0"
    apiVersion: v3.0.2
    kind: DataContract
    id: 11111111-1111-1111-1111-111111111111
    status: active
    name: sales-contract
    schema:
      - name: orders
        physicalType: table
        description: Order facts
        properties:
          - name: order_id
            logicalType: integer
            physicalType: BIGINT
            required: true
            primaryKey: true
            primaryKeyPosition: 1
            description: Unique order id
          - name: amount
            logicalType: number
            physicalType: DECIMAL(18,2)
          - name: status
            logicalType: string
      - name: customers
        properties:
          - name: customer_id
            logicalType: integer
            required: true
    """
).strip()


# A single-object contract used for file_name-convention / round-trip tests.
SINGLE_OBJECT_CONTRACT_YAML = textwrap.dedent(
    """
    version: "2.3.0"
    apiVersion: v3.0.2
    kind: DataContract
    id: 22222222-2222-2222-2222-222222222222
    status: active
    name: single-contract
    schema:
      - name: events
        description: Event stream
        properties:
          - name: event_id
            logicalType: integer
            physicalType: BIGINT
            required: true
            description: The event id
          - name: payload
            logicalType: string
    """
).strip()


# Deliberately INVALID: ``kind`` is not the allowed ``DataContract`` enum value
# (and ``status`` is also removed), so jsonschema validation must fail.
INVALID_CONTRACT_YAML = textwrap.dedent(
    """
    version: "1.0.0"
    apiVersion: v3.0.2
    kind: NotADataContract
    id: 33333333-3333-3333-3333-333333333333
    name: broken-contract
    schema:
      - name: orders
        properties:
          - name: order_id
            logicalType: integer
    """
).strip()


def _write_contract(directory: Path, name: str, content: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(content, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# odcs_type_to_spark
# ---------------------------------------------------------------------------


class TestOdcsTypeToSpark:
    def test_physical_type_passthrough_bigint(self):
        assert odcs_type_to_spark({"physicalType": "BIGINT"}) == "BIGINT"

    def test_physical_type_passthrough_decimal(self):
        assert odcs_type_to_spark({"physicalType": "DECIMAL(18,2)"}) == "DECIMAL(18,2)"

    def test_physical_type_passthrough_string(self):
        assert odcs_type_to_spark({"physicalType": "STRING"}) == "STRING"

    def test_physical_type_wins_over_logical_type(self):
        # When both are present, the explicit physical/DDL type is authoritative.
        assert (
            odcs_type_to_spark({"physicalType": "BIGINT", "logicalType": "string"})
            == "BIGINT"
        )

    def test_logical_string(self):
        assert odcs_type_to_spark({"logicalType": "string"}) == "STRING"

    def test_logical_integer_defaults_bigint(self):
        assert odcs_type_to_spark({"logicalType": "integer"}) == "BIGINT"

    def test_logical_integer_i32_is_int(self):
        assert (
            odcs_type_to_spark(
                {"logicalType": "integer", "logicalTypeOptions": {"format": "i32"}}
            )
            == "INT"
        )

    def test_logical_number_defaults_double(self):
        assert odcs_type_to_spark({"logicalType": "number"}) == "DOUBLE"

    def test_logical_number_f32_is_float(self):
        assert (
            odcs_type_to_spark(
                {"logicalType": "number", "logicalTypeOptions": {"format": "f32"}}
            )
            == "FLOAT"
        )

    def test_logical_number_precision_scale_is_decimal(self):
        assert (
            odcs_type_to_spark(
                {
                    "logicalType": "number",
                    "logicalTypeOptions": {"precision": 18, "scale": 2},
                }
            )
            == "DECIMAL(18,2)"
        )

    def test_logical_boolean(self):
        assert odcs_type_to_spark({"logicalType": "boolean"}) == "BOOLEAN"

    def test_logical_date(self):
        assert odcs_type_to_spark({"logicalType": "date"}) == "DATE"

    def test_logical_timestamp(self):
        assert odcs_type_to_spark({"logicalType": "timestamp"}) == "TIMESTAMP"

    def test_logical_time_is_string(self):
        assert odcs_type_to_spark({"logicalType": "time"}) == "STRING"

    def test_logical_object_becomes_struct(self):
        prop = {
            "logicalType": "object",
            "properties": [
                {"name": "id", "logicalType": "integer"},
                {"name": "label", "logicalType": "string"},
            ],
        }
        assert odcs_type_to_spark(prop) == "STRUCT<id:BIGINT,label:STRING>"

    def test_logical_object_nested_struct(self):
        prop = {
            "logicalType": "object",
            "properties": [
                {
                    "name": "inner",
                    "logicalType": "object",
                    "properties": [
                        {"name": "x", "logicalType": "string"},
                    ],
                },
            ],
        }
        assert odcs_type_to_spark(prop) == "STRUCT<inner:STRUCT<x:STRING>>"

    def test_logical_array_of_string(self):
        prop = {
            "logicalType": "array",
            "items": {"logicalType": "string"},
        }
        assert odcs_type_to_spark(prop) == "ARRAY<STRING>"

    def test_logical_array_of_integer(self):
        prop = {
            "logicalType": "array",
            "items": {"logicalType": "integer"},
        }
        assert odcs_type_to_spark(prop) == "ARRAY<BIGINT>"

    def test_array_of_struct(self):
        prop = {
            "logicalType": "array",
            "items": {
                "logicalType": "object",
                "properties": [
                    {"name": "k", "logicalType": "string"},
                ],
            },
        }
        assert odcs_type_to_spark(prop) == "ARRAY<STRUCT<k:STRING>>"

    def test_unknown_logical_type_raises_cfg_063(self):
        with pytest.raises(LHPError) as excinfo:
            odcs_type_to_spark({"logicalType": "geo_point"})
        assert excinfo.value.code == "LHP-CFG-063"

    def test_empty_property_raises_cfg_063(self):
        # Neither physicalType nor a (known) logicalType -> unmappable.
        with pytest.raises(LHPError) as excinfo:
            odcs_type_to_spark({})
        assert excinfo.value.code == "LHP-CFG-063"


# ---------------------------------------------------------------------------
# OdcsParser
# ---------------------------------------------------------------------------


class TestOdcsParser:
    def test_parse_valid_returns_dict(self, tmp_path):
        path = _write_contract(tmp_path, "sales.yaml", VALID_CONTRACT_YAML)
        result = OdcsParser().parse(path)
        assert isinstance(result, dict)
        assert result["kind"] == "DataContract"
        assert result["version"] == "1.0.0"
        # schema is the array of objects
        names = [obj["name"] for obj in result["schema"]]
        assert names == ["orders", "customers"]

    def test_parse_invalid_raises_cfg_062(self, tmp_path):
        path = _write_contract(tmp_path, "broken.yaml", INVALID_CONTRACT_YAML)
        with pytest.raises(LHPError) as excinfo:
            OdcsParser().parse(path)
        assert excinfo.value.code == "LHP-CFG-062"

    def test_parse_missing_required_top_level_field_raises_cfg_062(self, tmp_path):
        # Drop the required ``status`` field -> schema validation must fail.
        bad = yaml.safe_load(VALID_CONTRACT_YAML)
        del bad["status"]
        path = tmp_path / "no_status.yaml"
        path.write_text(yaml.safe_dump(bad), encoding="utf-8")
        with pytest.raises(LHPError) as excinfo:
            OdcsParser().parse(path)
        assert excinfo.value.code == "LHP-CFG-062"


# ---------------------------------------------------------------------------
# OdcsTranslator
# ---------------------------------------------------------------------------


class TestOdcsTranslator:
    def _translate(self, contract_yaml, stem):
        contract = yaml.safe_load(contract_yaml)
        return OdcsTranslator().translate_schemas(contract, contract_stem=stem)

    def test_multi_object_yields_one_artifact_per_object(self):
        artifacts = self._translate(VALID_CONTRACT_YAML, "sales")
        assert len(artifacts) == 2
        assert all(isinstance(a, SchemaArtifact) for a in artifacts)
        assert [a.object_name for a in artifacts] == ["orders", "customers"]

    def test_file_name_is_stem_prefixed_multi(self):
        artifacts = self._translate(VALID_CONTRACT_YAML, "sales")
        file_names = {a.file_name for a in artifacts}
        assert file_names == {
            "sales.orders_schema.yaml",
            "sales.customers_schema.yaml",
        }

    def test_file_name_is_stem_prefixed_even_for_single_object(self):
        artifacts = self._translate(SINGLE_OBJECT_CONTRACT_YAML, "single")
        assert len(artifacts) == 1
        assert artifacts[0].file_name == "single.events_schema.yaml"

    def test_schema_dict_basic_shape(self):
        artifacts = self._translate(VALID_CONTRACT_YAML, "sales")
        orders = next(a for a in artifacts if a.object_name == "orders")
        sd = orders.schema_dict
        assert sd["name"] == "orders"
        assert sd["version"] == "1.0.0"
        assert sd["description"] == "Order facts"
        assert isinstance(sd["columns"], list)

    def test_columns_carry_type_and_comment(self):
        artifacts = self._translate(VALID_CONTRACT_YAML, "sales")
        orders = next(a for a in artifacts if a.object_name == "orders")
        cols = {c["name"]: c for c in orders.schema_dict["columns"]}

        assert cols["order_id"]["type"] == "BIGINT"
        assert cols["order_id"]["comment"] == "Unique order id"
        assert cols["amount"]["type"] == "DECIMAL(18,2)"
        assert cols["status"]["type"] == "STRING"

    def test_nullable_derived_from_required(self):
        artifacts = self._translate(VALID_CONTRACT_YAML, "sales")
        orders = next(a for a in artifacts if a.object_name == "orders")
        cols = {c["name"]: c for c in orders.schema_dict["columns"]}

        # required: true => nullable: false
        assert cols["order_id"]["nullable"] is False
        # required absent => nullable: true
        assert cols["status"]["nullable"] is True

    def test_primary_key_present_for_object_with_pk(self):
        artifacts = self._translate(VALID_CONTRACT_YAML, "sales")
        orders = next(a for a in artifacts if a.object_name == "orders")
        assert orders.schema_dict["primary_key"] == ["order_id"]

    def test_primary_key_absent_when_no_pk(self):
        artifacts = self._translate(VALID_CONTRACT_YAML, "sales")
        customers = next(a for a in artifacts if a.object_name == "customers")
        assert "primary_key" not in customers.schema_dict

    def test_primary_key_ordered_by_position(self):
        contract = {
            "version": "1.0.0",
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "id": "id",
            "status": "active",
            "schema": [
                {
                    "name": "composite",
                    "properties": [
                        {
                            "name": "b",
                            "logicalType": "string",
                            "primaryKey": True,
                            "primaryKeyPosition": 2,
                        },
                        {
                            "name": "a",
                            "logicalType": "string",
                            "primaryKey": True,
                            "primaryKeyPosition": 1,
                        },
                    ],
                }
            ],
        }
        artifacts = OdcsTranslator().translate_schemas(contract, contract_stem="c")
        assert artifacts[0].schema_dict["primary_key"] == ["a", "b"]

    def test_description_omitted_when_absent(self):
        artifacts = self._translate(VALID_CONTRACT_YAML, "sales")
        customers = next(a for a in artifacts if a.object_name == "customers")
        # customers object has no description in the fixture
        assert "description" not in customers.schema_dict

    def test_comment_omitted_when_absent(self):
        artifacts = self._translate(VALID_CONTRACT_YAML, "sales")
        orders = next(a for a in artifacts if a.object_name == "orders")
        cols = {c["name"]: c for c in orders.schema_dict["columns"]}
        # status has no description -> no comment key
        assert "comment" not in cols["status"]

    def test_physical_name_captured_on_column_when_present(self):
        contract = {
            "version": "1.0.0",
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "id": "id",
            "status": "active",
            "schema": [
                {
                    "name": "orders",
                    "properties": [
                        {
                            "name": "order_id",
                            "physicalName": "ord_id",
                            "logicalType": "integer",
                            "physicalType": "BIGINT",
                        },
                        {"name": "status", "logicalType": "string"},
                    ],
                }
            ],
        }
        artifacts = OdcsTranslator().translate_schemas(contract, contract_stem="c")
        cols = {c["name"]: c for c in artifacts[0].schema_dict["columns"]}

        # physicalName present and differing -> captured under ``physical_name``.
        assert cols["order_id"]["physical_name"] == "ord_id"
        # No physicalName -> key omitted entirely.
        assert "physical_name" not in cols["status"]

    def test_unmappable_column_raises_cfg_063(self):
        contract = {
            "version": "1.0.0",
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "id": "id",
            "status": "active",
            "schema": [
                {
                    "name": "bad",
                    "properties": [
                        {"name": "weird", "logicalType": "unobtanium"},
                    ],
                }
            ],
        }
        with pytest.raises(LHPError) as excinfo:
            OdcsTranslator().translate_schemas(contract, contract_stem="c")
        assert excinfo.value.code == "LHP-CFG-063"


# ---------------------------------------------------------------------------
# Round-trip through SchemaParser.to_schema_hints (consumption compatibility)
# ---------------------------------------------------------------------------


class TestSchemaHintsRoundTrip:
    def test_translator_output_feeds_to_schema_hints(self):
        contract = yaml.safe_load(VALID_CONTRACT_YAML)
        artifacts = OdcsTranslator().translate_schemas(contract, contract_stem="sales")
        orders = next(a for a in artifacts if a.object_name == "orders")

        hints = SchemaParser().to_schema_hints(orders.schema_dict)

        # required: true -> NOT NULL; nullable columns have no constraint.
        assert hints == (
            "order_id BIGINT NOT NULL, amount DECIMAL(18,2), status STRING"
        )
