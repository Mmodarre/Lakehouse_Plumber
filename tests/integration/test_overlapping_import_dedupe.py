"""Flowgroup-level guard against duplicate ``StringType`` bindings (issue #299).

A quarantine data-quality action imports ``MapType, StringType`` on one line;
a cloudfiles schema load, an ``lhp.yaml`` operational-metadata expression that
calls ``StringType()``, or a metadata column's ``additional_imports`` line also
imports ``StringType``. Both lines
used to reach the generated module, which then bound ``StringType`` twice
(ruff F811).

Builds throwaway projects rather than extending the e2e fixture project: a new
fixture pipeline would change every all-pipelines aggregate baseline.
"""

import subprocess

import pytest
import yaml

from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.core.codegen.formatter import _ruff_exe
from tests.helpers import read_generated_pipeline

_PIPELINE = "dedupe_pipeline"

_ORDERS_SCHEMA = {
    "table": "orders",
    "columns": [
        {"name": "order_id", "type": "BIGINT"},
        {"name": "note", "type": "STRING"},
    ],
}

_EXPECTATIONS = {
    "order_id IS NOT NULL": {"action": "drop", "name": "valid_order_id"},
}

_LOAD_WITH_SCHEMA = """\
  - name: load_orders
    type: load
    readMode: stream
    source:
      type: cloudfiles
      path: "/Volumes/cat/raw/orders"
      format: json
      schema: "schemas/orders.yaml"
    target: v_orders_raw
"""

_LOAD_WITH_METADATA = """\
  - name: load_orders
    type: load
    readMode: stream
    operational_metadata: ["_null_note"]
    source:
      type: cloudfiles
      path: "/Volumes/cat/raw/orders"
      format: json
    target: v_orders_raw
"""

_QUARANTINE_AND_WRITE = """\
  - name: orders_quarantine
    type: transform
    transform_type: data_quality
    source: v_orders_raw
    target: v_orders_validated
    readMode: stream
    expectations_file: "expectations/orders.yaml"
    mode: quarantine
    quarantine:
      dlq_table: "cat.bronze.dlq"
      source_table: "cat.bronze.orders"

  - name: write_orders
    type: write
    source: v_orders_validated
    write_target:
      type: streaming_table
      catalog: cat
      schema: bronze
      table: orders
"""

_METADATA_LHP_YAML = """\
name: dedupe_project
version: "1.0"
operational_metadata:
  columns:
    _null_note:
      expression: "F.lit(None).cast(StringType())"
      description: "Calls StringType() without a STRING schema column"
      applies_to: ["view"]
"""

# ``additional_imports`` reach the generator verbatim and overlap only partly.
_ADDITIONAL_IMPORTS_LHP_YAML = """\
name: dedupe_project
version: "1.0"
operational_metadata:
  columns:
    _null_note:
      expression: "F.lit(None)"
      description: "Imports StringType through additional_imports"
      applies_to: ["view"]
      additional_imports:
        - "from pyspark.sql.types import LongType, StringType"
"""

_PLAIN_LHP_YAML = 'name: dedupe_project\nversion: "1.0"\n'

_CASES = {
    "cloudfiles_schema": (_PLAIN_LHP_YAML, _LOAD_WITH_SCHEMA),
    "operational_metadata_expression": (_METADATA_LHP_YAML, _LOAD_WITH_METADATA),
    "additional_imports": (_ADDITIONAL_IMPORTS_LHP_YAML, _LOAD_WITH_METADATA),
}


@pytest.fixture(params=sorted(_CASES))
def generated(request, tmp_path):
    """Generate the quarantine flowgroup and return ``(path, source)``."""
    lhp_yaml, load_action = _CASES[request.param]
    project = tmp_path / "project"
    for directory in ("pipelines", "substitutions", "schemas", "expectations"):
        (project / directory).mkdir(parents=True)
    (project / "lhp.yaml").write_text(lhp_yaml, encoding="utf-8")
    (project / "substitutions" / "dev.yaml").write_text(
        "dev:\n  env: dev\n", encoding="utf-8"
    )
    (project / "schemas" / "orders.yaml").write_text(
        yaml.safe_dump(_ORDERS_SCHEMA, sort_keys=False), encoding="utf-8"
    )
    (project / "expectations" / "orders.yaml").write_text(
        yaml.safe_dump(_EXPECTATIONS, sort_keys=False), encoding="utf-8"
    )
    flowgroup = (
        f"pipeline: {_PIPELINE}\nflowgroup: orders\n\nactions:\n"
        f"{load_action}\n{_QUARANTINE_AND_WRITE}"
    )
    (project / "pipelines" / "orders.yaml").write_text(flowgroup, encoding="utf-8")

    output_dir = tmp_path / "generated"
    facade = LakehousePlumberApplicationFacade.for_project(
        project, enforce_version=False
    )
    files = read_generated_pipeline(
        facade, pipeline_field=_PIPELINE, env="dev", output_dir=output_dir
    )
    return output_dir / _PIPELINE / "orders.py", files["orders.py"]


def test_string_type_is_imported_once(generated):
    _, source = generated

    types_imports = [
        line
        for line in source.splitlines()
        if line.startswith("from pyspark.sql.types")
    ]

    assert "from pyspark.sql.types import MapType" in types_imports
    assert types_imports.count("from pyspark.sql.types import StringType") == 1
    assert "from pyspark.sql.types import MapType, StringType" not in types_imports


def test_generated_module_has_no_duplicate_binding(generated):
    path, _ = generated

    result = subprocess.run(
        [
            _ruff_exe(),
            "check",
            "--isolated",
            "--select",
            "F811",
            "--output-format",
            "concise",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
