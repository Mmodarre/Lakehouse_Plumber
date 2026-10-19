"""End to end: a template with ``render_sql_files: true`` and a ``{{ }}`` SQL file.

A materialized-view template points at ``sql/gold/<table>.sql``; the SQL file
names its input with a template parameter (``{{ silver_schema }}``, defaulted
in the template) and the catalog with an environment token (``${catalog}``).
Generation through the public facade resolves both, and an undefined name
stops generation with LHP-CFG-071.
"""

from pathlib import Path

import pytest
import yaml

from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.errors import LHPError
from tests.helpers import read_generated_pipeline

pytestmark = pytest.mark.integration

_TEMPLATE = {
    "name": "gold_mv",
    "render_sql_files": True,
    "parameters": [
        {"name": "table", "required": True},
        {"name": "silver_schema", "default": "silver"},
        {"name": "gold_schema", "default": "gold"},
    ],
    "actions": [
        {
            "name": "write_{{ table }}",
            "type": "write",
            "write_target": {
                "type": "materialized_view",
                "catalog": "${catalog}",
                "schema": "{{ gold_schema }}",
                "table": "{{ table }}",
                "sql_path": "sql/gold/{{ table }}.sql",
            },
        }
    ],
}


def _project(root: Path, sql: str) -> None:
    for folder in ("templates", "pipelines/gold", "substitutions", "sql/gold"):
        (root / folder).mkdir(parents=True)
    (root / "templates" / "gold_mv.yaml").write_text(yaml.dump(_TEMPLATE))
    (root / "pipelines" / "gold" / "orders.yaml").write_text(
        yaml.dump(
            {
                "pipeline": "gold",
                "flowgroup": "orders",
                "use_template": "gold_mv",
                "template_parameters": {"table": "orders"},
            }
        )
    )
    (root / "substitutions" / "dev.yaml").write_text(
        yaml.dump({"dev": {"catalog": "main_dev"}})
    )
    (root / "sql" / "gold" / "orders.sql").write_text(sql)


def _generate(root: Path) -> dict:
    facade = LakehousePlumberApplicationFacade.for_project(root, enforce_version=False)
    return read_generated_pipeline(
        facade, pipeline_field="gold", env="dev", output_dir=root / "generated"
    )


def test_sql_file_uses_template_parameters_and_tokens(tmp_path):
    _project(
        tmp_path,
        "SELECT o.* FROM ${catalog}.{{ silver_schema }}.orders o\n"
        "WHERE o.__END_AT IS NULL",
    )

    code = _generate(tmp_path)["orders.py"]

    assert "main_dev.gold.orders" in code
    assert "FROM main_dev.silver.orders o" in code
    assert "{{" not in code


def test_undefined_name_in_sql_file_stops_generation(tmp_path):
    _project(tmp_path, "SELECT * FROM ${catalog}.{{ bronze_schema }}.orders")

    with pytest.raises(LHPError) as excinfo:
        _generate(tmp_path)

    assert excinfo.value.code == "LHP-CFG-071"
    assert "bronze_schema" in excinfo.value.details
