"""``render_sql_files: true`` passes the template parameters to its ``sql_path`` files.

The template engine sets ``sql_parameters`` (the parameters with defaults
applied) on every rendered action that references a ``sql_path`` file, at
action level, in ``source`` or in ``write_target``. Keys the action sets in
``sql_parameters`` itself win. Without the flag nothing changes.
"""

from pathlib import Path

import pytest

from lhp.core.processing import TemplateEngine

pytestmark = pytest.mark.unit

_TEMPLATE = """
name: gold_mv
render_sql_files: {flag}
parameters:
  - name: table
    required: true
  - name: silver_schema
    default: silver
actions:
  - name: load_{{{{ table }}}}
    type: load
    source:
      type: sql
      sql_path: "sql/{{{{ table }}}}_load.sql"
    target: v_{{{{ table }}}}
  - name: transform_{{{{ table }}}}
    type: transform
    transform_type: sql
    source: v_{{{{ table }}}}
    target: v_{{{{ table }}}}_t
    sql_path: "sql/{{{{ table }}}}_transform.sql"
    sql_parameters:
      silver_schema: override
      extra: x
  - name: write_{{{{ table }}}}
    type: write
    write_target:
      type: materialized_view
      catalog: c
      schema: gold
      table: "{{{{ table }}}}"
      sql_path: "sql/{{{{ table }}}}.sql"
  - name: write_{{{{ table }}}}_copy
    type: write
    source: v_{{{{ table }}}}_t
    write_target:
      type: streaming_table
      catalog: c
      schema: gold
      table: "{{{{ table }}}}_copy"
"""


def _engine(tmp_path: Path, flag: str) -> TemplateEngine:
    templates = tmp_path / "templates"
    templates.mkdir()
    (templates / "gold_mv.yaml").write_text(_TEMPLATE.format(flag=flag))
    return TemplateEngine(templates)


def _by_name(actions):
    return {a.name: a for a in actions}


def test_flag_sets_sql_parameters_on_every_sql_path_action(tmp_path):
    actions = _by_name(
        _engine(tmp_path, "true").render_template("gold_mv", {"table": "orders"})
    )

    assert actions["load_orders"].sql_parameters == {
        "table": "orders",
        "silver_schema": "silver",
    }
    assert actions["write_orders"].sql_parameters == {
        "table": "orders",
        "silver_schema": "silver",
    }


def test_explicit_sql_parameters_win_over_template_parameters(tmp_path):
    actions = _by_name(
        _engine(tmp_path, "true").render_template("gold_mv", {"table": "orders"})
    )

    assert actions["transform_orders"].sql_parameters == {
        "table": "orders",
        "silver_schema": "override",
        "extra": "x",
    }


def test_action_without_sql_path_is_unchanged(tmp_path):
    actions = _by_name(
        _engine(tmp_path, "true").render_template("gold_mv", {"table": "orders"})
    )

    assert actions["write_orders_copy"].sql_parameters is None


def test_flowgroup_values_override_defaults(tmp_path):
    actions = _by_name(
        _engine(tmp_path, "true").render_template(
            "gold_mv", {"table": "orders", "silver_schema": "silver_v2"}
        )
    )

    assert actions["write_orders"].sql_parameters["silver_schema"] == "silver_v2"


def test_without_the_flag_only_explicit_sql_parameters_are_set(tmp_path):
    actions = _by_name(
        _engine(tmp_path, "false").render_template("gold_mv", {"table": "orders"})
    )

    assert actions["load_orders"].sql_parameters is None
    assert actions["write_orders"].sql_parameters is None
    assert actions["transform_orders"].sql_parameters == {
        "silver_schema": "override",
        "extra": "x",
    }
