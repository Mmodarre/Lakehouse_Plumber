"""Flowgroup-level generation of CloudFiles explicit schemas (issues #287, #288).

Builds a throwaway project rather than extending the e2e fixture project: a
new fixture pipeline would change every all-pipelines aggregate baseline.
"""

import ast
import subprocess

import pytest
import yaml

from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.core.codegen.formatter import _ruff_exe
from tests.helpers import read_generated_pipeline

_PIPELINE = "schema_pipeline"

_LHP_YAML = """\
name: schema_project
version: "1.0"
operational_metadata:
  columns:
    _row_weight:
      expression: "F.lit(1).cast(LongType())"
      description: "Needs LongType without a BIGINT schema column"
      applies_to: ["view"]
"""

# Exercises #287 escaping end to end, including the write path's splitlines().
_HOSTILE_COMMENT = 'C:\\temp\\new "quoted" \u2028 end \\'

# Both files use the canonical ``table:`` key, which previously named both
# variables ``schema_schema``.
_PRICES_SCHEMA = {
    "table": "prices",
    "columns": [
        {
            "name": "sku",
            "type": "STRING",
            "nullable": False,
            "comment": _HOSTILE_COMMENT,
        },
        {"name": "price", "type": "DECIMAL(10,2)"},
    ],
}

_ORDERS_SCHEMA = {
    "table": "orders",
    "columns": [
        {"name": "order_id", "type": "INT"},
        {"name": "placed_on", "type": "DATE"},
    ],
}

_FLOWGROUP = f"""\
pipeline: {_PIPELINE}
flowgroup: two_schemas
operational_metadata: ["_row_weight"]

actions:
  - name: load_prices
    type: load
    source:
      type: cloudfiles
      path: "/data/prices/*.csv"
      format: csv
      schema: "schemas/prices.yaml"
    target: v_prices_raw

  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: "/data/orders/*.csv"
      format: csv
      schema: "schemas/orders.yaml"
    target: v_orders_raw

  - name: write_prices
    type: write
    source: v_prices_raw
    write_target:
      type: streaming_table
      catalog: cat
      schema: bronze
      table: prices

  - name: write_orders
    type: write
    source: v_orders_raw
    write_target:
      type: streaming_table
      catalog: cat
      schema: bronze
      table: orders
"""


@pytest.fixture
def generated(tmp_path):
    """Generate the two-schema flowgroup and return ``(path, source)``."""
    project = tmp_path / "project"
    for directory in ("pipelines", "substitutions", "schemas"):
        (project / directory).mkdir(parents=True)
    (project / "lhp.yaml").write_text(_LHP_YAML, encoding="utf-8")
    (project / "substitutions" / "dev.yaml").write_text(
        "dev:\n  env: dev\n", encoding="utf-8"
    )
    for name, schema in (("prices", _PRICES_SCHEMA), ("orders", _ORDERS_SCHEMA)):
        (project / "schemas" / f"{name}.yaml").write_text(
            yaml.safe_dump(schema, sort_keys=False), encoding="utf-8"
        )
    (project / "pipelines" / "two_schemas.yaml").write_text(
        _FLOWGROUP, encoding="utf-8"
    )

    output_dir = tmp_path / "generated"
    facade = LakehousePlumberApplicationFacade.for_project(
        project, enforce_version=False
    )
    files = read_generated_pipeline(
        facade, pipeline_field=_PIPELINE, env="dev", output_dir=output_dir
    )
    return output_dir / _PIPELINE / "two_schemas.py", files["two_schemas.py"]


def _schema_argument(tree, view_name):
    """Return the variable name passed to ``.schema(...)`` inside a view function."""
    view = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == view_name
    )
    (call,) = [
        node
        for node in ast.walk(view)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "schema"
    ]
    return call.args[0].id


def _struct_field_names(tree, variable_name):
    assign = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign) and node.targets[0].id == variable_name
    )
    return [
        ast.literal_eval(node.args[0])
        for node in ast.walk(assign.value)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "StructField"
    ]


def test_each_view_reads_its_own_schema_variable(generated):
    _, source = generated
    tree = ast.parse(source)

    assert _schema_argument(tree, "v_prices_raw") == "v_prices_raw_schema"
    assert _schema_argument(tree, "v_orders_raw") == "v_orders_raw_schema"
    assert _struct_field_names(tree, "v_prices_raw_schema") == ["sku", "price"]
    assert _struct_field_names(tree, "v_orders_raw_schema") == [
        "order_id",
        "placed_on",
    ]


def test_escaped_comment_survives_generation(generated):
    _, source = generated
    tree = ast.parse(source)
    assign = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign) and node.targets[0].id == "v_prices_raw_schema"
    )
    sku_field = assign.value.args[0].elts[0]

    assert ast.literal_eval(sku_field.args[3]) == {"comment": _HOSTILE_COMMENT}


def test_types_imports_are_one_per_used_name(generated):
    _, source = generated

    types_imports = sorted(
        line
        for line in source.splitlines()
        if line.startswith("from pyspark.sql.types")
    )

    assert types_imports == [
        f"from pyspark.sql.types import {name}"
        for name in (
            "DateType",
            "DecimalType",
            "IntegerType",
            "LongType",
            "StringType",
            "StructField",
            "StructType",
        )
    ]


def test_generated_module_has_no_undefined_unused_or_duplicate_names(generated):
    path, _ = generated

    result = subprocess.run(
        [
            _ruff_exe(),
            "check",
            "--isolated",
            "--select",
            "F401,F811,F821",
            "--config",
            "builtins=['spark']",
            "--output-format",
            "concise",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
