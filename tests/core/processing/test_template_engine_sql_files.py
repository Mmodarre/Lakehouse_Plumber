"""Template expansion renders a template's ``sql_path`` files with its parameters.

Built through ``TemplateEngine.for_project`` with the loader the composition
roots inject, so the engine reads files exactly as generation resolves them.
"""

import pickle

import pytest

from lhp.core.loaders import read_sql_file
from lhp.core.processing import TemplateEngine

pytestmark = pytest.mark.unit

_TEMPLATE = """
name: gold_mv
parameters:
  - name: table
    required: true
  - name: silver_schema
    default: silver
actions:
  - name: "write_{{ table }}"
    type: write
    write_target:
      type: materialized_view
      catalog: "${catalog}"
      schema: gold
      table: "{{ table }}"
      sql_path: "sql/gold/{{ table }}.sql"
"""


def _engine(root, sql):
    (root / "templates").mkdir()
    (root / "templates" / "gold_mv.yaml").write_text(_TEMPLATE)
    (root / "sql" / "gold").mkdir(parents=True)
    (root / "sql" / "gold" / "orders.sql").write_text(sql)
    return TemplateEngine.for_project(root, read_sql_file)


def test_sql_file_renders_with_defaults_and_keeps_its_path(tmp_path):
    engine = _engine(tmp_path, "SELECT * FROM ${catalog}.{{ silver_schema }}.orders\n")

    (action,) = engine.render_template("gold_mv", {"table": "orders"})

    assert action.write_target["sql"] == "SELECT * FROM ${catalog}.silver.orders"
    assert action.write_target["sql_path"] == "sql/gold/orders.sql"


def test_flowgroup_values_override_defaults(tmp_path):
    engine = _engine(tmp_path, "SELECT * FROM {{ silver_schema }}.orders")

    (action,) = engine.render_template(
        "gold_mv", {"table": "orders", "silver_schema": "silver_v2"}
    )

    assert action.write_target["sql"] == "SELECT * FROM silver_v2.orders"


def test_engine_without_a_reader_leaves_sql_files_to_generation(tmp_path):
    _engine(tmp_path, "SELECT * FROM {{ silver_schema }}.orders")
    engine = TemplateEngine(tmp_path / "templates")

    (action,) = engine.render_template("gold_mv", {"table": "orders"})

    assert "sql" not in action.write_target


def test_engine_with_reader_survives_the_worker_pickle(tmp_path):
    engine = _engine(tmp_path, "SELECT * FROM {{ silver_schema }}.orders")

    clone = pickle.loads(pickle.dumps(engine))
    (action,) = clone.render_template("gold_mv", {"table": "orders"})

    assert action.write_target["sql"] == "SELECT * FROM silver.orders"
