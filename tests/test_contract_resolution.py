"""Tests for the ``contract`` action field resolution pass (Slice 4).

A pre-generation pass (``lhp.core.processing.contract_resolver.ContractResolver``)
rewrites every action that carries a ``contract`` field into an inline artifact
(schema DDL / cast-only schema_inline / expectations list) and strips the
``contract`` key. Injection is implicit by action type.

These tests pin the exact resolution spec against a known ``orders`` ODCS
contract. The expected DDL / predicates were confirmed verbatim by importing
``SchemaParser.to_schema_hints`` and ``odcs_property_to_constraints``:

  * schema DDL          -> ``"order_id BIGINT NOT NULL, status STRING"``
  * order_id predicate  -> ``("order_id >= 1", "order_id_min")``  (CDE -> fail)
  * status predicate    -> ``("length(status) >= 1", "status_min_length")`` (warn)

An integration test (bottom) drives the full facade generation and is RED until
the resolver is implemented AND wired into the flowgroup pipeline.
"""

import textwrap
from pathlib import Path

import pytest

from lhp.api import collect_response
from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.core.processing.contract_resolver import ContractResolver
from lhp.errors import LHPError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Expected DDL string the resolver inlines (from SchemaParser.to_schema_hints).
EXPECTED_DDL = "order_id BIGINT NOT NULL, status STRING"

# A single-object ODCS contract: ``orders`` with a CDE integer + a string.
SINGLE_OBJECT_CONTRACT = textwrap.dedent(
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
        properties:
          - name: order_id
            logicalType: integer
            physicalType: BIGINT
            required: true
            criticalDataElement: true
            logicalTypeOptions:
              minimum: 1
          - name: status
            logicalType: string
            logicalTypeOptions:
              minLength: 1
    """
).strip()

# A multi-object contract (``orders`` + ``customers``) — entity_name required.
MULTI_OBJECT_CONTRACT = textwrap.dedent(
    """
    version: "1.0.0"
    apiVersion: v3.0.2
    kind: DataContract
    id: 22222222-2222-2222-2222-222222222222
    status: active
    name: multi-contract
    schema:
      - name: orders
        properties:
          - name: order_id
            logicalType: integer
            physicalType: BIGINT
            required: true
            criticalDataElement: true
            logicalTypeOptions:
              minimum: 1
          - name: status
            logicalType: string
            logicalTypeOptions:
              minLength: 1
      - name: customers
        properties:
          - name: customer_id
            logicalType: integer
            required: true
    """
).strip()


def _write_contract(root: Path, content: str = SINGLE_OBJECT_CONTRACT) -> Path:
    """Write the contract under ``<root>/contracts/sales.odcs.yaml`` and return it."""
    contracts_dir = root / "contracts"
    contracts_dir.mkdir(parents=True, exist_ok=True)
    path = contracts_dir / "sales.odcs.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def _flowgroup(*actions) -> dict:
    """Build a ``FlowGroup.model_dump()``-shaped dict around the given actions."""
    return {
        "pipeline": "sales",
        "flowgroup": "ingest",
        "actions": list(actions),
    }


# Per-action-type action dicts that carry a ``contract`` field.
CONTRACT_FILE = "contracts/sales.odcs.yaml"


def _cloudfiles_action(**contract_extra) -> dict:
    contract = {"file": CONTRACT_FILE, "type": "odcs"}
    contract.update(contract_extra)
    return {
        "name": "load_orders",
        "type": "load",
        "target": "v_orders_raw",
        "source": {
            "type": "cloudfiles",
            "path": "/data/orders",
            "format": "json",
        },
        "contract": contract,
    }


def _write_action(**contract_extra) -> dict:
    contract = {"file": CONTRACT_FILE, "type": "odcs"}
    contract.update(contract_extra)
    return {
        "name": "write_orders",
        "type": "write",
        "source": "v_orders_validated",
        "write_target": {
            "type": "streaming_table",
            "database": "cat.bronze",
            "table": "orders",
        },
        "contract": contract,
    }


def _schema_transform_action(**contract_extra) -> dict:
    contract = {"file": CONTRACT_FILE, "type": "odcs"}
    contract.update(contract_extra)
    return {
        "name": "apply_schema",
        "type": "transform",
        "transform_type": "schema",
        "source": "v_orders_raw",
        "target": "v_orders_typed",
        "contract": contract,
    }


def _data_quality_action(**contract_extra) -> dict:
    contract = {"file": CONTRACT_FILE, "type": "odcs"}
    contract.update(contract_extra)
    return {
        "name": "validate_orders",
        "type": "transform",
        "transform_type": "data_quality",
        "source": "v_orders_typed",
        "target": "v_orders_validated",
        "readMode": "stream",
        "contract": contract,
    }


@pytest.fixture
def resolver() -> ContractResolver:
    return ContractResolver()


# ---------------------------------------------------------------------------
# Per-action-type injection
# ---------------------------------------------------------------------------


class TestCloudfilesLoadResolution:
    def test_injects_inline_schema_into_source(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_cloudfiles_action())

        result = resolver.resolve(fg, project_root=tmp_path)

        action = result["actions"][0]
        assert action["source"]["schema"] == EXPECTED_DDL
        assert "contract" not in action

    def test_schema_hints_false_does_not_add_hints_option(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_cloudfiles_action())  # schema_hints defaults to False

        result = resolver.resolve(fg, project_root=tmp_path)

        action = result["actions"][0]
        options = action["source"].get("options", {})
        assert "cloudFiles.schemaHints" not in options

    def test_schema_hints_true_emits_hints_only_not_enforced_schema(
        self, tmp_path, resolver
    ):
        _write_contract(tmp_path)
        fg = _flowgroup(_cloudfiles_action(schema_hints=True))

        result = resolver.resolve(fg, project_root=tmp_path)

        action = result["actions"][0]
        # schema_hints: true emits ONLY the hints — never an enforced read schema.
        assert action["source"]["options"]["cloudFiles.schemaHints"] == EXPECTED_DDL
        assert "schema" not in action["source"]
        assert "contract" not in action


class TestWriteResolution:
    def test_injects_table_schema_into_write_target(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_write_action())

        result = resolver.resolve(fg, project_root=tmp_path)

        action = result["actions"][0]
        assert action["write_target"]["table_schema"] == EXPECTED_DDL
        assert "contract" not in action

    def test_materialized_view_target_also_resolved(self, tmp_path, resolver):
        _write_contract(tmp_path)
        action = _write_action()
        action["write_target"]["type"] = "materialized_view"
        fg = _flowgroup(action)

        result = resolver.resolve(fg, project_root=tmp_path)

        out = result["actions"][0]
        assert out["write_target"]["table_schema"] == EXPECTED_DDL
        assert "contract" not in out


class TestSchemaTransformResolution:
    def test_injects_cast_only_schema_inline(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_schema_transform_action())

        result = resolver.resolve(fg, project_root=tmp_path)

        action = result["actions"][0]
        # Cast-only: type only, NO ``NOT NULL``; newline-joined.
        assert action["schema_inline"] == "order_id: BIGINT\nstatus: STRING"
        assert "contract" not in action


class TestDataQualityResolution:
    def test_injects_expectations_list_with_per_property_action(
        self, tmp_path, resolver
    ):
        _write_contract(tmp_path)
        fg = _flowgroup(_data_quality_action())

        result = resolver.resolve(fg, project_root=tmp_path)

        action = result["actions"][0]
        # order_id is criticalDataElement -> fail; status -> warn.
        assert action["expectations"] == [
            {
                "name": "order_id_min",
                "expression": "order_id >= 1",
                "failureAction": "fail",
            },
            {
                "name": "status_min_length",
                "expression": "length(status) >= 1",
                "failureAction": "warn",
            },
        ]
        assert "contract" not in action

    def test_expectations_action_override_applies_to_all(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_data_quality_action(expectations_action="drop"))

        result = resolver.resolve(fg, project_root=tmp_path)

        action = result["actions"][0]
        assert action["expectations"] == [
            {
                "name": "order_id_min",
                "expression": "order_id >= 1",
                "failureAction": "drop",
            },
            {
                "name": "status_min_length",
                "expression": "length(status) >= 1",
                "failureAction": "drop",
            },
        ]
        assert "contract" not in action


# ---------------------------------------------------------------------------
# Entity selection
# ---------------------------------------------------------------------------


class TestEntitySelection:
    def test_single_object_contract_resolves_sole_object_without_entity_name(
        self, tmp_path, resolver
    ):
        _write_contract(tmp_path)  # single-object contract
        fg = _flowgroup(_cloudfiles_action())  # no entity_name

        result = resolver.resolve(fg, project_root=tmp_path)

        assert result["actions"][0]["source"]["schema"] == EXPECTED_DDL

    def test_explicit_entity_name_selects_named_object(self, tmp_path, resolver):
        _write_contract(tmp_path, MULTI_OBJECT_CONTRACT)
        fg = _flowgroup(_cloudfiles_action(entity_name="orders"))

        result = resolver.resolve(fg, project_root=tmp_path)

        assert result["actions"][0]["source"]["schema"] == EXPECTED_DDL

    def test_multi_object_without_entity_name_raises(self, tmp_path, resolver):
        _write_contract(tmp_path, MULTI_OBJECT_CONTRACT)
        fg = _flowgroup(_cloudfiles_action())  # ambiguous

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_unknown_entity_name_raises(self, tmp_path, resolver):
        _write_contract(tmp_path, MULTI_OBJECT_CONTRACT)
        fg = _flowgroup(_cloudfiles_action(entity_name="does_not_exist"))

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestErrorCases:
    def test_missing_contract_file_raises(self, tmp_path, resolver):
        # No contract written; the referenced file does not exist.
        fg = _flowgroup(_cloudfiles_action())

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_unknown_contract_type_raises(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_cloudfiles_action(type="snowflake"))

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_contract_on_sql_transform_unsupported_raises(self, tmp_path, resolver):
        _write_contract(tmp_path)
        action = {
            "name": "sql_step",
            "type": "transform",
            "transform_type": "sql",
            "source": "v_orders_raw",
            "target": "v_orders_sql",
            "sql": "SELECT * FROM v_orders_raw",
            "contract": {"file": CONTRACT_FILE, "type": "odcs"},
        }
        fg = _flowgroup(action)

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_contract_on_python_load_unsupported_raises(self, tmp_path, resolver):
        _write_contract(tmp_path)
        action = {
            "name": "py_load",
            "type": "load",
            "target": "v_py",
            "source": {"type": "python", "module_path": "loaders/x.py"},
            "contract": {"file": CONTRACT_FILE, "type": "odcs"},
        }
        fg = _flowgroup(action)

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_cloudfiles_conflict_with_explicit_source_schema_raises(
        self, tmp_path, resolver
    ):
        _write_contract(tmp_path)
        action = _cloudfiles_action()
        action["source"]["schema"] = "id BIGINT"  # already set -> conflict
        fg = _flowgroup(action)

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_data_quality_conflict_with_explicit_expectations_file_raises(
        self, tmp_path, resolver
    ):
        _write_contract(tmp_path)
        action = _data_quality_action()
        action["expectations_file"] = "expectations/explicit.yaml"  # conflict
        fg = _flowgroup(action)

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"


# ---------------------------------------------------------------------------
# No-op behaviour
# ---------------------------------------------------------------------------


class TestNoOp:
    def test_action_without_contract_is_returned_unchanged(self, tmp_path, resolver):
        _write_contract(tmp_path)
        plain = {
            "name": "load_plain",
            "type": "load",
            "target": "v_plain",
            "source": {"type": "cloudfiles", "path": "/data/x", "format": "json"},
        }
        contracted = _cloudfiles_action()
        fg = _flowgroup(plain, contracted)

        result = resolver.resolve(fg, project_root=tmp_path)

        # The plain action is untouched.
        assert result["actions"][0] == plain
        # The contracted action is resolved.
        assert result["actions"][1]["source"]["schema"] == EXPECTED_DDL

    def test_flowgroup_with_no_contracts_is_noop(self, tmp_path, resolver):
        plain = {
            "name": "load_plain",
            "type": "load",
            "target": "v_plain",
            "source": {"type": "delta", "database": "cat.raw", "table": "x"},
        }
        fg = _flowgroup(plain)

        result = resolver.resolve(fg, project_root=tmp_path)

        assert result == fg


# ---------------------------------------------------------------------------
# Integration: full facade generation with contract-bearing actions
# ---------------------------------------------------------------------------

INTEGRATION_FLOWGROUP = textwrap.dedent(
    """
    pipeline: sales_pipeline
    flowgroup: orders_ingest
    actions:
      - name: load_orders
        type: load
        target: v_orders_raw
        source:
          type: cloudfiles
          path: "/data/orders"
          format: json
        contract:
          file: contracts/sales.odcs.yaml
          type: odcs
          schema_hints: true
      - name: validate_orders
        type: transform
        transform_type: data_quality
        source: v_orders_raw
        target: v_orders_validated
        readMode: stream
        contract:
          file: contracts/sales.odcs.yaml
          type: odcs
      - name: write_orders
        type: write
        source: v_orders_validated
        write_target:
          type: streaming_table
          database: "{catalog}.bronze"
          table: orders
    """
).strip()


class TestContractGenerationIntegration:
    """End-to-end: contract-bearing actions generate inline schema + expectations.

    RED until the resolver is implemented AND wired into the flowgroup pipeline.
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
        (pipes / "orders.yaml").write_text(INTEGRATION_FLOWGROUP, encoding="utf-8")

    def test_generated_python_contains_inline_schema_and_expectations(self, tmp_path):
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
            )
        )

        response = batch.pipeline_responses["sales_pipeline"]
        generated = response.generated_filenames
        assert generated, "expected at least one generated file"

        code = "\n".join(
            (output_dir / "sales_pipeline" / name).read_text(encoding="utf-8")
            for name in generated
        )

        # Inline schema DDL columns flow into the cloudfiles load.
        assert "order_id BIGINT NOT NULL" in code
        assert "status STRING" in code

        # Contract-derived expectations flow into @dp.expect_all* decorators.
        assert "@dp.expect_all_or_fail" in code  # order_id (criticalDataElement)
        assert "@dp.expect_all" in code  # status (warn)
        assert '"order_id_min": "order_id >= 1"' in code
        assert '"status_min_length": "length(status) >= 1"' in code
