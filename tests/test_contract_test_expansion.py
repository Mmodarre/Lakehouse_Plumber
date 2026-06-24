"""Tests for ODCS ``quality`` rule → LHP ``test`` action expansion (Slice 3).

A ``type: test`` action that carries a ``contract`` (and NO ``test_type``) is
expanded 1→N by ``ContractResolver`` into concrete test actions — one per
mappable ODCS ``quality`` rule on the selected entity. This cut covers
``quality`` array rules only (dataset-level ``schema[].quality`` AND
property-level ``schema[].properties[].quality``), against a single source
table (no referential_integrity / multi-table).

Two layers are pinned here:

1. The pure helper ``lhp.utils.odcs_mapper.odcs_quality_to_tests(obj, *, source)``
   maps one entity object's quality rules to a list of partial test-action dicts.
2. ``ContractResolver._expand_test_action`` (reached via ``resolve``) prefixes
   the base action name, echoes ``source``, drops ``contract``, and replaces the
   original action with the expansion.

HELPER OUTPUT SHAPE (Tester's choice — the Implementer must match this):
``odcs_quality_to_tests`` returns dicts WITHOUT ``type``, each carrying:

  * ``test_type``  — ``"uniqueness"`` / ``"completeness"`` / ``"custom_sql"``
  * ``source``     — echoed verbatim from the ``source=`` keyword
  * ``on_violation`` — from rule ``severity`` (error→fail, warning/info→warn,
    absent→fail)
  * ``test_id``    — the ODCS rule's ``name`` (only when the rule has a ``name``)
  * ``name``       — set to the BARE ``rule_slug`` (the rule's ``name``); the
    resolver later prefixes ``f"{base}_"`` to produce the final action name
  * type-specific fields:
      - uniqueness   → ``columns: [<col>]``
      - completeness → ``required_columns: [<col>]``
      - custom_sql   → ``sql`` + ``expectations``
        (``expectations = [{"name": <rule_slug>, "expression": ..., "on_violation": <ov>}]``)

Skipped rules (no usable mapping) emit NO dict. These helper tests call the
helper directly on hand-built ``obj`` dicts and assert the exact emitted fields.

RED until ``odcs_quality_to_tests`` and ``_expand_test_action`` are implemented.
"""

import textwrap
from pathlib import Path

import pytest

from lhp.api import collect_response
from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.core.processing.contract_resolver import ContractResolver
from lhp.errors import LHPError
from lhp.utils.odcs_mapper import odcs_quality_to_tests

SOURCE = "cat.bronze.orders"


# ---------------------------------------------------------------------------
# Helper builders (entity objects with quality rules)
# ---------------------------------------------------------------------------


def _entity(*, quality=None, properties=None) -> dict:
    """Build a minimal ODCS schema-object dict for the helper."""
    obj: dict = {"name": "orders"}
    if quality is not None:
        obj["quality"] = list(quality)
    if properties is not None:
        obj["properties"] = list(properties)
    return obj


def _prop(name: str, *, quality=None) -> dict:
    """Build an ODCS property dict, optionally with property-level quality rules."""
    prop: dict = {"name": name, "logicalType": "string"}
    if quality is not None:
        prop["quality"] = list(quality)
    return prop


def _only(tests: list) -> dict:
    """Assert exactly one emitted test and return it."""
    assert len(tests) == 1, f"expected exactly one test, got {tests!r}"
    return tests[0]


# ---------------------------------------------------------------------------
# Helper unit tests — one per mapping row + the operator/severity tables
# ---------------------------------------------------------------------------


class TestHelperLibraryMetrics:
    def test_duplicate_values_must_be_zero_is_uniqueness(self):
        # Row 1: property-level duplicateValues mustBe 0 → uniqueness on that col.
        obj = _entity(
            properties=[
                _prop(
                    "order_id",
                    quality=[
                        {
                            "name": "order_id_unique",
                            "type": "library",
                            "metric": "duplicateValues",
                            "mustBe": 0,
                            "severity": "error",
                        }
                    ],
                )
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "uniqueness"
        assert test["columns"] == ["order_id"]
        assert test["source"] == SOURCE
        assert test["on_violation"] == "fail"
        assert test["test_id"] == "order_id_unique"
        assert test["name"] == "order_id_unique"

    def test_dataset_level_duplicate_values_uses_arguments_column(self):
        # Dataset-level column binding via arguments.column.
        obj = _entity(
            quality=[
                {
                    "name": "ds_unique",
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "arguments": {"column": "order_id"},
                }
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "uniqueness"
        assert test["columns"] == ["order_id"]

    def test_dataset_level_duplicate_values_uses_arguments_columns(self):
        obj = _entity(
            quality=[
                {
                    "name": "ds_unique_multi",
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                    "arguments": {"columns": ["order_id", "line_no"]},
                }
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "uniqueness"
        assert test["columns"] == ["order_id", "line_no"]

    def test_null_values_must_be_zero_is_completeness(self):
        # Row 2: nullValues mustBe 0 → completeness.
        obj = _entity(
            properties=[
                _prop(
                    "order_id",
                    quality=[
                        {
                            "name": "order_id_present",
                            "type": "library",
                            "metric": "nullValues",
                            "mustBe": 0,
                        }
                    ],
                )
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "completeness"
        assert test["required_columns"] == ["order_id"]
        # No severity → fail.
        assert test["on_violation"] == "fail"
        assert test["test_id"] == "order_id_present"

    def test_missing_values_must_be_zero_is_completeness(self):
        obj = _entity(
            properties=[
                _prop(
                    "order_id",
                    quality=[
                        {
                            "name": "order_id_present2",
                            "type": "library",
                            "metric": "missingValues",
                            "mustBe": 0,
                        }
                    ],
                )
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "completeness"
        assert test["required_columns"] == ["order_id"]

    def test_row_count_maps_to_custom_sql_count_star(self):
        # Row 3: rowCount, any operator → custom_sql COUNT(*).
        obj = _entity(
            quality=[
                {
                    "name": "order_volume",
                    "type": "library",
                    "metric": "rowCount",
                    "mustBeGreaterThan": 1000,
                    "severity": "warning",
                }
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "custom_sql"
        assert test["sql"] == "SELECT COUNT(*) AS metric FROM cat.bronze.orders"
        assert test["on_violation"] == "warn"
        assert test["test_id"] == "order_volume"
        assert test["expectations"] == [
            {
                "name": "order_volume",
                "expression": "metric > 1000",
                "on_violation": "warn",
            }
        ]

    def test_null_values_non_zero_operator_is_custom_sql_where_null(self):
        # Row 4: nullValues with a non-(mustBe 0) operator → custom_sql.
        obj = _entity(
            properties=[
                _prop(
                    "status",
                    quality=[
                        {
                            "name": "status_nulls",
                            "type": "library",
                            "metric": "nullValues",
                            "mustBeLessThan": 5,
                        }
                    ],
                )
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "custom_sql"
        assert (
            test["sql"]
            == "SELECT COUNT(*) AS metric FROM cat.bronze.orders WHERE status IS NULL"
        )
        assert test["expectations"] == [
            {
                "name": "status_nulls",
                "expression": "metric < 5",
                "on_violation": "fail",
            }
        ]

    def test_missing_values_non_zero_operator_is_custom_sql_where_null(self):
        obj = _entity(
            properties=[
                _prop(
                    "status",
                    quality=[
                        {
                            "name": "status_missing",
                            "type": "library",
                            "metric": "missingValues",
                            "mustBeLessThan": 5,
                        }
                    ],
                )
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "custom_sql"
        assert (
            test["sql"]
            == "SELECT COUNT(*) AS metric FROM cat.bronze.orders WHERE status IS NULL"
        )

    def test_duplicate_values_non_zero_operator_is_custom_sql_group_by(self):
        # Row 5: duplicateValues with non-(mustBe 0) → custom_sql GROUP BY/HAVING.
        obj = _entity(
            properties=[
                _prop(
                    "order_id",
                    quality=[
                        {
                            "name": "dup_few",
                            "type": "library",
                            "metric": "duplicateValues",
                            "mustBeLessThan": 10,
                        }
                    ],
                )
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "custom_sql"
        assert test["sql"] == (
            "SELECT COUNT(*) AS metric FROM "
            "(SELECT order_id FROM cat.bronze.orders "
            "GROUP BY order_id HAVING COUNT(*) > 1)"
        )
        assert test["expectations"] == [
            {
                "name": "dup_few",
                "expression": "metric < 10",
                "on_violation": "fail",
            }
        ]

    def test_invalid_values_with_valid_values_is_custom_sql_not_in(self):
        # Row 7: invalidValues + arguments.validValues → custom_sql NOT IN.
        obj = _entity(
            properties=[
                _prop(
                    "status",
                    quality=[
                        {
                            "name": "status_valid",
                            "type": "library",
                            "metric": "invalidValues",
                            "mustBe": 0,
                            "arguments": {"validValues": ["open", "closed"]},
                        }
                    ],
                )
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "custom_sql"
        assert test["sql"] == (
            "SELECT COUNT(*) AS metric FROM cat.bronze.orders "
            "WHERE status NOT IN ('open', 'closed')"
        )
        assert test["expectations"] == [
            {
                "name": "status_valid",
                "expression": "metric = 0",
                "on_violation": "fail",
            }
        ]


class TestHelperSqlRule:
    def test_sql_rule_property_level_substitutes_table_and_column(self):
        # Row 6: sql rule, property-level → ${table}/${column} substitution.
        obj = _entity(
            properties=[
                _prop(
                    "status",
                    quality=[
                        {
                            "name": "status_present",
                            "type": "sql",
                            "query": "SELECT COUNT(*) FROM ${table} WHERE ${column} IS NOT NULL",
                            "mustBeGreaterThan": 0,
                        }
                    ],
                )
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "custom_sql"
        assert test["sql"] == (
            "SELECT (SELECT COUNT(*) FROM cat.bronze.orders "
            "WHERE status IS NOT NULL) AS metric"
        )
        assert test["expectations"] == [
            {
                "name": "status_present",
                "expression": "metric > 0",
                "on_violation": "fail",
            }
        ]

    def test_sql_rule_dataset_level_substitutes_table_only(self):
        obj = _entity(
            quality=[
                {
                    "name": "ds_sql",
                    "type": "sql",
                    "query": "SELECT COUNT(*) FROM ${table}",
                    "mustBeGreaterThan": 0,
                }
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["test_type"] == "custom_sql"
        assert (
            test["sql"]
            == "SELECT (SELECT COUNT(*) FROM cat.bronze.orders) AS metric"
        )


class TestHelperOperatorTable:
    """The operator → comparison-symbol table on the custom_sql expression."""

    @pytest.mark.parametrize(
        "operator,value,expected_expr",
        [
            ("mustBe", 0, "metric = 0"),
            ("mustNotBe", 0, "metric != 0"),
            ("mustBeGreaterThan", 1000, "metric > 1000"),
            ("mustBeGreaterOrEqualTo", 1000, "metric >= 1000"),
            ("mustBeLessThan", 5, "metric < 5"),
            ("mustBeLessOrEqualTo", 5, "metric <= 5"),
        ],
    )
    def test_scalar_operators_map_to_symbols(self, operator, value, expected_expr):
        obj = _entity(
            quality=[
                {
                    "name": "rc",
                    "type": "library",
                    "metric": "rowCount",
                    operator: value,
                }
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["expectations"][0]["expression"] == expected_expr

    def test_must_be_between_maps_to_between(self):
        obj = _entity(
            quality=[
                {
                    "name": "rc_between",
                    "type": "library",
                    "metric": "rowCount",
                    "mustBeBetween": [100, 200],
                }
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["expectations"][0]["expression"] == "metric BETWEEN 100 AND 200"

    def test_must_not_be_between_maps_to_not_between(self):
        obj = _entity(
            quality=[
                {
                    "name": "rc_not_between",
                    "type": "library",
                    "metric": "rowCount",
                    "mustNotBeBetween": [100, 200],
                }
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert (
            test["expectations"][0]["expression"] == "metric NOT BETWEEN 100 AND 200"
        )


class TestHelperSeverity:
    @pytest.mark.parametrize(
        "severity,expected",
        [
            ("error", "fail"),
            ("warning", "warn"),
            ("info", "warn"),
        ],
    )
    def test_severity_maps_to_on_violation(self, severity, expected):
        obj = _entity(
            quality=[
                {
                    "name": "rc",
                    "type": "library",
                    "metric": "rowCount",
                    "mustBeGreaterThan": 0,
                    "severity": severity,
                }
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["on_violation"] == expected
        # And custom_sql expectations carry the same on_violation.
        assert test["expectations"][0]["on_violation"] == expected

    def test_absent_severity_defaults_to_fail(self):
        obj = _entity(
            quality=[
                {
                    "name": "rc",
                    "type": "library",
                    "metric": "rowCount",
                    "mustBeGreaterThan": 0,
                }
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["on_violation"] == "fail"


class TestHelperSkips:
    """Row 8 + the dataset-level-no-column case → no test emitted."""

    def test_custom_rule_is_skipped(self):
        obj = _entity(
            quality=[{"name": "c", "type": "custom", "engine": "great_expectations"}]
        )
        assert odcs_quality_to_tests(obj, source=SOURCE) == []

    def test_text_rule_is_skipped(self):
        obj = _entity(
            properties=[
                _prop("notes", quality=[{"name": "note", "type": "text"}])
            ]
        )
        assert odcs_quality_to_tests(obj, source=SOURCE) == []

    def test_invalid_values_with_no_valid_values_is_skipped(self):
        obj = _entity(
            properties=[
                _prop(
                    "status",
                    quality=[
                        {
                            "name": "x",
                            "type": "library",
                            "metric": "invalidValues",
                            "mustBe": 0,
                        }
                    ],
                )
            ]
        )
        assert odcs_quality_to_tests(obj, source=SOURCE) == []

    def test_dataset_level_column_metric_without_column_is_skipped(self):
        # A column-bound metric at dataset level with no arguments.column(s).
        obj = _entity(
            quality=[
                {
                    "name": "x",
                    "type": "library",
                    "metric": "duplicateValues",
                    "mustBe": 0,
                }
            ]
        )
        assert odcs_quality_to_tests(obj, source=SOURCE) == []

    def test_entity_with_no_quality_rules_emits_nothing(self):
        obj = _entity(properties=[_prop("order_id"), _prop("status")])
        assert odcs_quality_to_tests(obj, source=SOURCE) == []


class TestHelperNamelessRules:
    """A rule with no ``name`` gets a derived slug (no ``base_None`` action name)."""

    def test_nameless_dataset_rule_uses_metric_slug(self):
        obj = _entity(
            quality=[{"type": "library", "metric": "rowCount", "mustBeGreaterThan": 0}]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["name"] == "rowcount"  # derived slug is lower-cased
        assert "test_id" not in test  # no rule name → no test_id
        assert test["expectations"][0]["name"] == "rowcount"

    def test_nameless_property_rule_uses_column_and_metric_slug(self):
        obj = _entity(
            properties=[
                _prop(
                    "order_id",
                    quality=[
                        {"type": "library", "metric": "duplicateValues", "mustBe": 0}
                    ],
                )
            ]
        )
        test = _only(odcs_quality_to_tests(obj, source=SOURCE))
        assert test["name"] == "order_id_duplicatevalues"
        assert "test_id" not in test

    def test_nameless_rule_yields_non_none_action_name_via_resolver(
        self, tmp_path, resolver
    ):
        contract = textwrap.dedent(
            """
            version: "1.0.0"
            apiVersion: v3.0.2
            kind: DataContract
            id: 55555555-5555-5555-5555-555555555555
            status: active
            name: nameless-contract
            schema:
              - name: orders
                quality:
                  - type: library
                    metric: rowCount
                    mustBeGreaterThan: 0
            """
        ).strip()
        _write_contract(tmp_path, contract)
        fg = _flowgroup(_test_action())

        result = resolver.resolve(fg, project_root=tmp_path)

        names = [a["name"] for a in result["actions"]]
        assert names == ["orders_contract_tests_rowcount"]
        assert "None" not in names[0]


# ---------------------------------------------------------------------------
# Resolver expansion (1 → N) — the worked example
# ---------------------------------------------------------------------------

# The worked-example contract: entity ``orders`` with 1 dataset-level rowCount
# rule + 3 property-level rules (order_id unique + present, status sql) + a
# skipped ``text`` rule on ``notes``.
WORKED_CONTRACT = textwrap.dedent(
    """
    version: "1.0.0"
    apiVersion: v3.0.2
    kind: DataContract
    id: 33333333-3333-3333-3333-333333333333
    status: active
    name: orders-contract
    schema:
      - name: orders
        physicalType: table
        quality:
          - name: order_volume
            type: library
            metric: rowCount
            mustBeGreaterThan: 1000
            severity: warning
        properties:
          - name: order_id
            logicalType: integer
            physicalType: BIGINT
            quality:
              - name: order_id_unique
                type: library
                metric: duplicateValues
                mustBe: 0
                severity: error
              - name: order_id_present
                type: library
                metric: nullValues
                mustBe: 0
          - name: status
            logicalType: string
            quality:
              - name: status_present
                type: sql
                query: "SELECT COUNT(*) FROM ${table} WHERE ${column} IS NOT NULL"
                mustBeGreaterThan: 0
          - name: notes
            logicalType: string
            quality:
              - name: note
                type: text
    """
).strip()

# A multi-object variant (orders + customers) — object required. The
# customers block is indented to sit under ``schema:`` as a sibling of orders.
WORKED_MULTI_CONTRACT = (
    WORKED_CONTRACT
    + "\n"
    + textwrap.indent(
        textwrap.dedent(
            """\
            - name: customers
              properties:
                - name: customer_id
                  logicalType: integer
                  quality:
                    - name: customer_id_unique
                      type: library
                      metric: duplicateValues
                      mustBe: 0
            """
        ).rstrip(),
        "  ",
    )
)

CONTRACT_FILE = "contracts/orders.odcs.yaml"


def _write_contract(root: Path, content: str = WORKED_CONTRACT) -> Path:
    contracts_dir = root / "contracts"
    contracts_dir.mkdir(parents=True, exist_ok=True)
    path = contracts_dir / "orders.odcs.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def _flowgroup(*actions) -> dict:
    return {
        "pipeline": "sales",
        "flowgroup": "orders_tests",
        "actions": list(actions),
    }


def _test_action(**contract_extra) -> dict:
    contract = {"file": CONTRACT_FILE, "type": "odcs"}
    contract.update(contract_extra)
    return {
        "name": "orders_contract_tests",
        "type": "test",
        "source": "cat.bronze.orders",
        "contract": contract,
    }


@pytest.fixture
def resolver() -> ContractResolver:
    return ContractResolver()


class TestResolverExpansion:
    def test_worked_example_expands_to_four_actions(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_test_action())

        result = resolver.resolve(fg, project_root=tmp_path)
        actions = result["actions"]

        assert len(actions) == 4
        # Every expanded action is a test action, echoes the source, drops contract.
        for action in actions:
            assert action["type"] == "test"
            assert action["source"] == "cat.bronze.orders"
            assert "contract" not in action

        by_name = {a["name"]: a for a in actions}

        order_volume = by_name["orders_contract_tests_order_volume"]
        assert order_volume["test_type"] == "custom_sql"
        assert order_volume["on_violation"] == "warn"
        assert order_volume["test_id"] == "order_volume"
        assert (
            order_volume["sql"]
            == "SELECT COUNT(*) AS metric FROM cat.bronze.orders"
        )
        assert order_volume["expectations"] == [
            {
                "name": "order_volume",
                "expression": "metric > 1000",
                "on_violation": "warn",
            }
        ]

        unique = by_name["orders_contract_tests_order_id_unique"]
        assert unique["test_type"] == "uniqueness"
        assert unique["on_violation"] == "fail"
        assert unique["test_id"] == "order_id_unique"
        assert unique["columns"] == ["order_id"]

        present = by_name["orders_contract_tests_order_id_present"]
        assert present["test_type"] == "completeness"
        assert present["on_violation"] == "fail"
        assert present["test_id"] == "order_id_present"
        assert present["required_columns"] == ["order_id"]

        status = by_name["orders_contract_tests_status_present"]
        assert status["test_type"] == "custom_sql"
        assert status["on_violation"] == "fail"
        assert status["test_id"] == "status_present"
        assert status["sql"] == (
            "SELECT (SELECT COUNT(*) FROM cat.bronze.orders "
            "WHERE status IS NOT NULL) AS metric"
        )
        assert status["expectations"] == [
            {
                "name": "status_present",
                "expression": "metric > 0",
                "on_violation": "fail",
            }
        ]

    def test_original_action_is_replaced_entirely(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_test_action())

        result = resolver.resolve(fg, project_root=tmp_path)

        # The original (no-test_type) action must not survive.
        assert all(a.get("test_type") for a in result["actions"])
        assert all("contract" not in a for a in result["actions"])

    def test_other_actions_are_preserved_around_expansion(self, tmp_path, resolver):
        _write_contract(tmp_path)
        plain = {
            "name": "load_plain",
            "type": "load",
            "target": "v_plain",
            "source": {"type": "delta", "database": "cat.raw", "table": "x"},
        }
        fg = _flowgroup(plain, _test_action())

        result = resolver.resolve(fg, project_root=tmp_path)

        assert result["actions"][0] == plain
        # The 4 expanded actions follow the plain one.
        assert len(result["actions"]) == 5

    def test_multi_object_with_object_selects_orders(self, tmp_path, resolver):
        _write_contract(tmp_path, WORKED_MULTI_CONTRACT)
        fg = _flowgroup(_test_action(object="orders"))

        result = resolver.resolve(fg, project_root=tmp_path)

        names = {a["name"] for a in result["actions"]}
        # 4 from orders — none from customers.
        assert len(result["actions"]) == 4
        assert "orders_contract_tests_order_volume" in names
        assert not any("customer" in n for n in names)


class TestResolverNoOpAndDrop:
    def test_test_action_without_contract_is_unchanged(self, tmp_path, resolver):
        # A plain test action (no contract) must be ignored by the resolver. We
        # still need at least one contract action present, or resolve short-circuits.
        _write_contract(tmp_path)
        plain_test = {
            "name": "manual_check",
            "type": "test",
            "test_type": "row_count",
            "source": ["a", "b"],
            "on_violation": "fail",
        }
        fg = _flowgroup(plain_test, _test_action())

        result = resolver.resolve(fg, project_root=tmp_path)

        assert result["actions"][0] == plain_test

    def test_entity_with_no_mappable_rules_drops_the_action(self, tmp_path, resolver):
        # A contract whose entity has zero mappable quality rules → action dropped.
        empty_contract = textwrap.dedent(
            """
            version: "1.0.0"
            apiVersion: v3.0.2
            kind: DataContract
            id: 44444444-4444-4444-4444-444444444444
            status: active
            name: empty-contract
            schema:
              - name: orders
                properties:
                  - name: notes
                    logicalType: string
                    quality:
                      - name: note
                        type: text
            """
        ).strip()
        _write_contract(tmp_path, empty_contract)
        fg = _flowgroup(_test_action())

        result = resolver.resolve(fg, project_root=tmp_path)

        assert result["actions"] == []


# ---------------------------------------------------------------------------
# Error cases — all LHP-CFG-064
# ---------------------------------------------------------------------------


class TestExpansionErrorCases:
    def test_explicit_test_type_with_contract_raises(self, tmp_path, resolver):
        # test_type is mutually exclusive with a contract on a test action.
        _write_contract(tmp_path)
        action = _test_action()
        action["test_type"] = "uniqueness"
        fg = _flowgroup(action)

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_missing_source_raises(self, tmp_path, resolver):
        _write_contract(tmp_path)
        action = _test_action()
        del action["source"]
        fg = _flowgroup(action)

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_missing_contract_file_raises(self, tmp_path, resolver):
        # No contract written; the referenced file does not exist.
        fg = _flowgroup(_test_action())

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_unknown_contract_type_raises(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_test_action(type="snowflake"))

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_multi_object_without_object_raises(self, tmp_path, resolver):
        _write_contract(tmp_path, WORKED_MULTI_CONTRACT)
        fg = _flowgroup(_test_action())  # ambiguous: two schema objects

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_unknown_object_raises(self, tmp_path, resolver):
        _write_contract(tmp_path, WORKED_MULTI_CONTRACT)
        fg = _flowgroup(_test_action(object="does_not_exist"))

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"


# ---------------------------------------------------------------------------
# Integration: full facade generation with a contract-bearing test action
# ---------------------------------------------------------------------------

INTEGRATION_FLOWGROUP = textwrap.dedent(
    """
    pipeline: sales_pipeline
    flowgroup: orders_tests
    actions:
      - name: orders_contract_tests
        type: test
        source: "{catalog}.bronze.orders"
        contract:
          file: contracts/orders.odcs.yaml
          type: odcs
          object: orders
    """
).strip()


class TestContractTestGenerationIntegration:
    """End-to-end: a contract-bearing test action expands into generated test code.

    Drives the facade with ``include_tests=True`` (the gate found on
    ``generate_pipelines``; tests are skipped by default). RED until
    ``_expand_test_action`` / ``odcs_quality_to_tests`` are implemented.
    """

    def _build_project(self, root: Path) -> None:
        (root / "lhp.yaml").write_text(
            'name: sales_project\nversion: "1.0"\n', encoding="utf-8"
        )
        subs = root / "substitutions"
        subs.mkdir()
        (subs / "dev.yaml").write_text(
            "dev:\n  catalog: test_catalog\n", encoding="utf-8"
        )
        _write_contract(root)
        pipes = root / "pipelines"
        pipes.mkdir()
        (pipes / "orders_tests.yaml").write_text(
            INTEGRATION_FLOWGROUP, encoding="utf-8"
        )

    def test_generated_python_contains_expanded_test_functions(self, tmp_path):
        self._build_project(tmp_path)

        facade = LakehousePlumberApplicationFacade.for_project(
            tmp_path, enforce_version=False
        )
        output_dir = tmp_path / "generated"
        batch = collect_response(
            facade.generation.generate_pipelines(
                pipeline_filter="sales_pipeline",
                env="dev",
                output_dir=output_dir,
                include_tests=True,
            )
        )

        response = batch.pipeline_responses["sales_pipeline"]
        generated = response.generated_filenames
        assert generated, "expected at least one generated file"

        code = "\n".join(
            (output_dir / "sales_pipeline" / name).read_text(encoding="utf-8")
            for name in generated
        )

        # rowCount → custom_sql (warn): SELECT COUNT(*) AS metric + metric > 1000.
        assert "SELECT COUNT(*) AS metric FROM test_catalog.bronze.orders" in code
        assert '"order_volume": "metric > 1000"' in code

        # duplicateValues mustBe 0 → uniqueness on order_id.
        assert "GROUP BY order_id" in code

        # nullValues mustBe 0 → completeness: order_id IS NOT NULL.
        assert "order_id IS NOT NULL" in code

        # sql rule → status sub-select substituted with the resolved table/column.
        assert (
            "SELECT (SELECT COUNT(*) FROM test_catalog.bronze.orders "
            "WHERE status IS NOT NULL) AS metric" in code
        )
