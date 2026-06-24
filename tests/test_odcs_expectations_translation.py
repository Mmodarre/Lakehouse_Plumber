"""Tests for ODCS contract -> LHP expectations translation (Slice 2).

Covers:
  * ``lhp.utils.odcs_mapper.odcs_property_to_constraints``
    (per-property ``(predicate, name)`` derivation)
  * ``lhp.core.processing.odcs_translator.OdcsTranslator.translate_expectations``
    (object -> ExpectationsArtifact, new dict format, action resolution)
"""

import textwrap

import yaml

from lhp.core.processing.odcs_translator import (
    ExpectationsArtifact,
    OdcsTranslator,
)
from lhp.utils.odcs_mapper import odcs_property_to_constraints

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _contract(schema_yaml: str) -> str:
    """Wrap a ``schema:`` block in the minimal valid-ODCS top-level envelope."""
    body = textwrap.indent(textwrap.dedent(schema_yaml).strip(), "  ")
    return (
        'version: "1.0.0"\n'
        "apiVersion: v3.0.2\n"
        "kind: DataContract\n"
        "id: 11111111-1111-1111-1111-111111111111\n"
        "status: active\n"
        "name: test-contract\n"
        "schema:\n"
        f"{body}\n"
    )


# ---------------------------------------------------------------------------
# odcs_property_to_constraints  (unit, one per mapping row)
# ---------------------------------------------------------------------------


class TestOdcsPropertyToConstraints:
    # -- property-level ----------------------------------------------------

    def test_required_is_not_translated(self):
        # The property-level ``required`` flag is enforced by the schema's
        # NOT NULL column constraint, not by an expectation.
        prop = {"name": "id", "logicalType": "integer", "required": True}
        assert odcs_property_to_constraints(prop) == []

    def test_no_constraints_yields_empty(self):
        prop = {"name": "status", "logicalType": "string"}
        assert odcs_property_to_constraints(prop) == []

    def test_required_false_yields_empty(self):
        prop = {"name": "status", "logicalType": "string", "required": False}
        assert odcs_property_to_constraints(prop) == []

    def test_critical_data_element_alone_yields_no_predicate(self):
        # criticalDataElement carries no predicate of its own.
        prop = {"name": "id", "logicalType": "integer", "criticalDataElement": True}
        assert odcs_property_to_constraints(prop) == []

    # -- string logicalTypeOptions ----------------------------------------

    def test_string_min_length(self):
        prop = {
            "name": "code",
            "logicalType": "string",
            "logicalTypeOptions": {"minLength": 2},
        }
        assert odcs_property_to_constraints(prop) == [
            ("length(code) >= 2", "code_min_length")
        ]

    def test_string_max_length(self):
        prop = {
            "name": "code",
            "logicalType": "string",
            "logicalTypeOptions": {"maxLength": 10},
        }
        assert odcs_property_to_constraints(prop) == [
            ("length(code) <= 10", "code_max_length")
        ]

    def test_string_pattern(self):
        prop = {
            "name": "code",
            "logicalType": "string",
            "logicalTypeOptions": {"pattern": "^[A-Z]+$"},
        }
        assert odcs_property_to_constraints(prop) == [
            ("code RLIKE '^[A-Z]+$'", "code_pattern")
        ]

    def test_string_pattern_single_quotes_doubled(self):
        prop = {
            "name": "code",
            "logicalType": "string",
            "logicalTypeOptions": {"pattern": "it's"},
        }
        assert odcs_property_to_constraints(prop) == [
            ("code RLIKE 'it''s'", "code_pattern")
        ]

    def test_string_format_not_translated(self):
        prop = {
            "name": "email",
            "logicalType": "string",
            "logicalTypeOptions": {"format": "email"},
        }
        assert odcs_property_to_constraints(prop) == []

    def test_string_multiple_options_preserve_order(self):
        prop = {
            "name": "code",
            "logicalType": "string",
            "logicalTypeOptions": {"minLength": 2, "maxLength": 10},
        }
        assert odcs_property_to_constraints(prop) == [
            ("length(code) >= 2", "code_min_length"),
            ("length(code) <= 10", "code_max_length"),
        ]

    # -- integer / number logicalTypeOptions (bare numeric literals) ------

    def test_number_minimum(self):
        prop = {
            "name": "amount",
            "logicalType": "number",
            "logicalTypeOptions": {"minimum": 0},
        }
        assert odcs_property_to_constraints(prop) == [
            ("amount >= 0", "amount_min")
        ]

    def test_number_maximum(self):
        prop = {
            "name": "amount",
            "logicalType": "number",
            "logicalTypeOptions": {"maximum": 100},
        }
        assert odcs_property_to_constraints(prop) == [
            ("amount <= 100", "amount_max")
        ]

    def test_integer_exclusive_minimum(self):
        prop = {
            "name": "qty",
            "logicalType": "integer",
            "logicalTypeOptions": {"exclusiveMinimum": 0},
        }
        assert odcs_property_to_constraints(prop) == [
            ("qty > 0", "qty_exclusive_min")
        ]

    def test_integer_exclusive_maximum(self):
        prop = {
            "name": "qty",
            "logicalType": "integer",
            "logicalTypeOptions": {"exclusiveMaximum": 100},
        }
        assert odcs_property_to_constraints(prop) == [
            ("qty < 100", "qty_exclusive_max")
        ]

    def test_integer_multiple_of(self):
        prop = {
            "name": "qty",
            "logicalType": "integer",
            "logicalTypeOptions": {"multipleOf": 5},
        }
        assert odcs_property_to_constraints(prop) == [
            ("qty % 5 = 0", "qty_multiple_of")
        ]

    def test_number_multiple_of_float(self):
        prop = {
            "name": "amount",
            "logicalType": "number",
            "logicalTypeOptions": {"multipleOf": 0.5},
        }
        assert odcs_property_to_constraints(prop) == [
            ("amount % 0.5 = 0", "amount_multiple_of")
        ]

    def test_integer_literal_renders_without_decimal(self):
        prop = {
            "name": "qty",
            "logicalType": "integer",
            "logicalTypeOptions": {"minimum": 1},
        }
        # int 1, not 1.0
        assert odcs_property_to_constraints(prop) == [
            ("qty >= 1", "qty_min")
        ]

    def test_number_format_precision_scale_not_translated(self):
        prop = {
            "name": "amount",
            "logicalType": "number",
            "logicalTypeOptions": {"format": "f32", "precision": 18, "scale": 2},
        }
        assert odcs_property_to_constraints(prop) == []

    # -- date / timestamp / time logicalTypeOptions (quoted literals) -----

    def test_date_minimum_quoted(self):
        prop = {
            "name": "d",
            "logicalType": "date",
            "logicalTypeOptions": {"minimum": "2020-01-01"},
        }
        assert odcs_property_to_constraints(prop) == [
            ("d >= '2020-01-01'", "d_min")
        ]

    def test_timestamp_maximum_quoted(self):
        prop = {
            "name": "ts",
            "logicalType": "timestamp",
            "logicalTypeOptions": {"maximum": "2030-12-31T23:59:59"},
        }
        assert odcs_property_to_constraints(prop) == [
            ("ts <= '2030-12-31T23:59:59'", "ts_max")
        ]

    def test_time_exclusive_bounds_quoted(self):
        prop = {
            "name": "t",
            "logicalType": "time",
            "logicalTypeOptions": {
                "exclusiveMinimum": "00:00:00",
                "exclusiveMaximum": "23:59:59",
            },
        }
        assert odcs_property_to_constraints(prop) == [
            ("t > '00:00:00'", "t_exclusive_min"),
            ("t < '23:59:59'", "t_exclusive_max"),
        ]

    def test_date_format_timezone_not_translated(self):
        prop = {
            "name": "d",
            "logicalType": "date",
            "logicalTypeOptions": {
                "format": "yyyy-MM-dd",
                "timezone": "UTC",
                "defaultTimezone": "UTC",
            },
        }
        assert odcs_property_to_constraints(prop) == []

    # -- array logicalTypeOptions -----------------------------------------

    def test_array_min_items(self):
        prop = {
            "name": "tags",
            "logicalType": "array",
            "logicalTypeOptions": {"minItems": 1},
        }
        assert odcs_property_to_constraints(prop) == [
            ("tags IS NULL OR (size(tags) >= 1)", "tags_min_items")
        ]

    def test_array_max_items(self):
        prop = {
            "name": "tags",
            "logicalType": "array",
            "logicalTypeOptions": {"maxItems": 5},
        }
        assert odcs_property_to_constraints(prop) == [
            ("tags IS NULL OR (size(tags) <= 5)", "tags_max_items")
        ]

    def test_array_unique_items_true(self):
        prop = {
            "name": "tags",
            "logicalType": "array",
            "logicalTypeOptions": {"uniqueItems": True},
        }
        assert odcs_property_to_constraints(prop) == [
            (
                "tags IS NULL OR (size(tags) = size(array_distinct(tags)))",
                "tags_unique_items",
            )
        ]

    def test_array_unique_items_false_not_translated(self):
        prop = {
            "name": "tags",
            "logicalType": "array",
            "logicalTypeOptions": {"uniqueItems": False},
        }
        assert odcs_property_to_constraints(prop) == []

    # -- object logicalTypeOptions (STRUCT) -------------------------------

    def test_object_required_fields_one_entry_each(self):
        prop = {
            "name": "addr",
            "logicalType": "object",
            "logicalTypeOptions": {"required": ["street", "city"]},
        }
        assert odcs_property_to_constraints(prop) == [
            ("addr IS NULL OR (addr.street IS NOT NULL)", "addr_street_not_null"),
            ("addr IS NULL OR (addr.city IS NOT NULL)", "addr_city_not_null"),
        ]

    def test_object_min_max_properties_not_translated(self):
        prop = {
            "name": "addr",
            "logicalType": "object",
            "logicalTypeOptions": {"minProperties": 1, "maxProperties": 5},
        }
        assert odcs_property_to_constraints(prop) == []

    # -- out of scope ------------------------------------------------------

    def test_unique_true_not_translated(self):
        prop = {"name": "id", "logicalType": "integer", "unique": True}
        assert odcs_property_to_constraints(prop) == []

    def test_primary_key_not_translated(self):
        prop = {
            "name": "id",
            "logicalType": "integer",
            "primaryKey": True,
            "primaryKeyPosition": 1,
        }
        assert odcs_property_to_constraints(prop) == []

    def test_quality_array_not_translated(self):
        prop = {
            "name": "amount",
            "logicalType": "number",
            "quality": [{"type": "sql", "query": "SELECT 1"}],
        }
        assert odcs_property_to_constraints(prop) == []

    def test_boolean_type_options_not_translated(self):
        prop = {
            "name": "flag",
            "logicalType": "boolean",
            "logicalTypeOptions": {"minLength": 1},
        }
        assert odcs_property_to_constraints(prop) == []

    # -- required is ignored even when value options are present -----------

    def test_required_ignored_value_option_still_emitted(self):
        prop = {
            "name": "full_name",
            "logicalType": "string",
            "required": True,
            "logicalTypeOptions": {"minLength": 1},
        }
        # ``required`` contributes nothing; only the logicalTypeOptions check.
        assert odcs_property_to_constraints(prop) == [
            ("length(full_name) >= 1", "full_name_min_length"),
        ]


# ---------------------------------------------------------------------------
# OdcsTranslator.translate_expectations
# ---------------------------------------------------------------------------


class TestTranslateExpectations:
    def _translate(self, contract_yaml, stem):
        contract = yaml.safe_load(contract_yaml)
        return OdcsTranslator().translate_expectations(contract, contract_stem=stem)

    def test_returns_expectations_artifacts(self):
        contract = _contract(
            """
            - name: orders
              properties:
                - name: order_id
                  logicalType: integer
                  logicalTypeOptions:
                    minimum: 1
            """
        )
        artifacts = self._translate(contract, "sales")
        assert len(artifacts) == 1
        assert all(isinstance(a, ExpectationsArtifact) for a in artifacts)
        assert artifacts[0].object_name == "orders"

    def test_file_name_convention(self):
        contract = _contract(
            """
            - name: orders
              properties:
                - name: order_id
                  logicalType: integer
                  logicalTypeOptions:
                    minimum: 1
            """
        )
        artifacts = self._translate(contract, "sales")
        assert artifacts[0].file_name == "sales.orders_expectations.yaml"

    def test_object_with_no_constraints_is_omitted(self):
        # 'orders' has a logicalTypeOptions constraint; 'lookups' has only a
        # type-only property (and a bare required, which is NOT translated).
        contract = _contract(
            """
            - name: orders
              properties:
                - name: order_id
                  logicalType: integer
                  logicalTypeOptions:
                    minimum: 1
            - name: lookups
              properties:
                - name: code
                  logicalType: string
                  required: true
            """
        )
        artifacts = self._translate(contract, "sales")
        assert [a.object_name for a in artifacts] == ["orders"]

    def test_expectations_dict_new_format_warn(self):
        contract = _contract(
            """
            - name: orders
              properties:
                - name: order_id
                  logicalType: integer
                  logicalTypeOptions:
                    minimum: 1
            """
        )
        artifacts = self._translate(contract, "sales")
        assert artifacts[0].expectations_dict == {
            "order_id >= 1": {
                "action": "warn",
                "name": "order_id_min",
            },
        }

    def test_critical_data_element_sets_fail_for_all_entries(self):
        contract = _contract(
            """
            - name: orders
              properties:
                - name: full_name
                  logicalType: string
                  criticalDataElement: true
                  logicalTypeOptions:
                    minLength: 1
            """
        )
        artifacts = self._translate(contract, "sales")
        assert artifacts[0].expectations_dict == {
            "length(full_name) >= 1": {
                "action": "fail",
                "name": "full_name_min_length",
            },
        }

    def test_non_critical_same_property_is_warn(self):
        contract = _contract(
            """
            - name: orders
              properties:
                - name: full_name
                  logicalType: string
                  logicalTypeOptions:
                    minLength: 1
            """
        )
        artifacts = self._translate(contract, "sales")
        assert artifacts[0].expectations_dict == {
            "length(full_name) >= 1": {
                "action": "warn",
                "name": "full_name_min_length",
            },
        }

    def test_multi_object_constrained_objects_only(self):
        # 'orders' has a CDE-constrained property; 'status' (type-only) and a
        # third object with no logicalTypeOptions would be omitted.
        contract = _contract(
            """
            - name: orders
              properties:
                - name: order_id
                  logicalType: integer
                  criticalDataElement: true
                  logicalTypeOptions:
                    minimum: 1
                - name: status
                  logicalType: string
            - name: customers
              properties:
                - name: age
                  logicalType: integer
                  logicalTypeOptions:
                    minimum: 0
            """
        )
        artifacts = self._translate(contract, "sales")
        by_name = {a.object_name: a for a in artifacts}
        assert set(by_name) == {"orders", "customers"}
        assert by_name["orders"].expectations_dict == {
            "order_id >= 1": {
                "action": "fail",
                "name": "order_id_min",
            },
        }
        assert by_name["customers"].expectations_dict == {
            "age >= 0": {"action": "warn", "name": "age_min"},
        }

    def test_multiple_properties_merge_into_one_object_dict(self):
        contract = _contract(
            """
            - name: orders
              properties:
                - name: order_id
                  logicalType: integer
                  logicalTypeOptions:
                    minimum: 1
                - name: amount
                  logicalType: number
                  logicalTypeOptions:
                    minimum: 0
            """
        )
        artifacts = self._translate(contract, "sales")
        assert artifacts[0].expectations_dict == {
            "order_id >= 1": {
                "action": "warn",
                "name": "order_id_min",
            },
            "amount >= 0": {"action": "warn", "name": "amount_min"},
        }
