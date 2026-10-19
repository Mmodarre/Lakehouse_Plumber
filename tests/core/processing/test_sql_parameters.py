"""``render_sql_parameters``: render a ``sql_path`` file with an action's ``sql_parameters``.

Rendering is strict: an undefined name or invalid Jinja raises LHP-CFG-071
naming the file, the action and the parameters on offer. ``${token}`` and
``${secret:...}`` references are not Jinja syntax and pass through for the
substitution step that follows.
"""

import pytest

from lhp.core.processing import render_sql_parameters
from lhp.errors import LHPError

pytestmark = pytest.mark.unit


def test_names_are_rendered_and_dollar_tokens_pass_through():
    text = "SELECT * FROM ${catalog}.{{ silver_schema }}.orders -- ${secret:s/k}\n"

    rendered = render_sql_parameters(
        text, {"silver_schema": "silver"}, file="sql/q.sql"
    )

    assert rendered == "SELECT * FROM ${catalog}.silver.orders -- ${secret:s/k}\n"


def test_jinja_control_flow_and_non_string_values_render():
    text = (
        "SELECT {% for c in columns %}{{ c }}{% if not loop.last %}, {% endif %}"
        "{% endfor %} FROM t LIMIT {{ limit }}"
    )

    rendered = render_sql_parameters(
        text, {"columns": ["a", "b"], "limit": 10}, file="sql/q.sql"
    )

    assert rendered == "SELECT a, b FROM t LIMIT 10"


def test_raw_block_keeps_literal_braces():
    text = "SELECT '{% raw %}{{ not_a_name }}{% endraw %}' AS x FROM {{ t }}"

    assert (
        render_sql_parameters(text, {"t": "orders"}, file="sql/q.sql")
        == "SELECT '{{ not_a_name }}' AS x FROM orders"
    )


def test_undefined_name_raises_cfg_071_with_context():
    with pytest.raises(LHPError) as excinfo:
        render_sql_parameters(
            "SELECT * FROM {{ gold_schema }}.orders",
            {"silver_schema": "silver", "table": "orders"},
            file="sql/gold/orders.sql",
            pipeline="gold",
            flowgroup="orders_fg",
            action="write_orders",
        )

    error = excinfo.value
    assert error.code == "LHP-CFG-071"
    assert "gold_schema" in error.details
    assert "sql/gold/orders.sql" in error.title
    assert error.context["File"] == "sql/gold/orders.sql"
    assert error.context["Parameters"] == "silver_schema, table"
    assert error.context["Pipeline"] == "gold"
    assert error.context["FlowGroup"] == "orders_fg"
    assert error.context["Action"] == "write_orders"


def test_invalid_jinja_raises_cfg_071_with_line():
    with pytest.raises(LHPError) as excinfo:
        render_sql_parameters("SELECT 1\nFROM {{ t ", {"t": "orders"}, file="sql/q.sql")

    error = excinfo.value
    assert error.code == "LHP-CFG-071"
    assert error.context["Line"] == "2"
    assert "not a valid Jinja template" in error.details


def test_sandbox_refuses_python_attribute_access():
    with pytest.raises(LHPError) as excinfo:
        render_sql_parameters(
            "SELECT '{{ name.__class__.__mro__ }}'", {"name": "x"}, file="sql/q.sql"
        )

    assert excinfo.value.code == "LHP-CFG-071"


def test_empty_parameters_still_render_strictly():
    with pytest.raises(LHPError) as excinfo:
        render_sql_parameters("SELECT {{ x }}", {}, file="sql/q.sql")

    assert excinfo.value.context["Parameters"] == "(none)"
