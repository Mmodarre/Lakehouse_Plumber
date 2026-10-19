"""Template SQL files: rendered at expansion with the template parameters.

``render_template_sql_file`` renders strictly in the sandbox and raises
LHP-CFG-071 with the file, line, template, action and parameters on offer.
``inline_template_sql_files`` puts the rendered text in the inline ``sql`` of
the three places a generator reads a ``sql_path`` from, keeping ``sql_path``.
"""

import pytest

from lhp.core.processing.template_sql_files import (
    inline_template_sql_files,
    render_template_sql_file,
)
from lhp.errors import LHPError

pytestmark = pytest.mark.unit


def _reader(files):
    def read(path):
        return files.get(path)

    return read


def test_names_are_rendered_and_dollar_tokens_pass_through():
    text = "SELECT * FROM ${catalog}.{{ silver_schema }}.orders -- ${secret:s/k}\n"

    rendered = render_template_sql_file(
        text, {"silver_schema": "silver"}, file="sql/q.sql"
    )

    assert rendered == "SELECT * FROM ${catalog}.silver.orders -- ${secret:s/k}"


def test_jinja_control_flow_and_non_string_values_render():
    text = (
        "SELECT {% for c in columns %}{{ c }}{% if not loop.last %}, {% endif %}"
        "{% endfor %} FROM t LIMIT {{ limit }}"
    )

    rendered = render_template_sql_file(
        text, {"columns": ["a", "b"], "limit": 10}, file="sql/q.sql"
    )

    assert rendered == "SELECT a, b FROM t LIMIT 10"


def test_raw_block_keeps_literal_braces():
    text = "SELECT '{% raw %}{{ not_a_name }}{% endraw %}' AS x FROM {{ t }}"

    assert (
        render_template_sql_file(text, {"t": "orders"}, file="sql/q.sql")
        == "SELECT '{{ not_a_name }}' AS x FROM orders"
    )


def test_undefined_name_raises_cfg_071_with_context():
    with pytest.raises(LHPError) as excinfo:
        render_template_sql_file(
            "SELECT * FROM {{ gold_schema }}.orders",
            {"silver_schema": "silver", "table": "orders"},
            file="sql/gold/orders.sql",
            template="gold_mv",
            action="write_orders",
        )

    error = excinfo.value
    assert error.code == "LHP-CFG-071"
    assert "gold_schema" in error.details
    assert "sql/gold/orders.sql" in error.title
    assert error.context["File"] == "sql/gold/orders.sql"
    assert error.context["Parameters"] == "silver_schema, table"
    assert error.context["Template"] == "gold_mv"
    assert error.context["Action"] == "write_orders"


def test_invalid_jinja_raises_cfg_071_with_line():
    with pytest.raises(LHPError) as excinfo:
        render_template_sql_file(
            "SELECT 1\nFROM {{ t ", {"t": "orders"}, file="sql/q.sql"
        )

    error = excinfo.value
    assert error.code == "LHP-CFG-071"
    assert error.context["Line"] == "2"
    assert "not a valid Jinja template" in error.details


def test_sandbox_refuses_python_attribute_access():
    with pytest.raises(LHPError) as excinfo:
        render_template_sql_file(
            "SELECT '{{ name.__class__.__mro__ }}'", {"name": "x"}, file="sql/q.sql"
        )

    assert excinfo.value.code == "LHP-CFG-071"


_TRANSFORM = {"name": "t", "type": "transform", "transform_type": "sql"}


def test_each_sql_path_place_gets_its_rendered_sql_and_keeps_the_path():
    files = {
        "t.sql": "SELECT * FROM {{ s }}.a",
        "l.sql": "SELECT * FROM {{ s }}.b",
        "m.sql": "SELECT * FROM {{ s }}.c",
    }
    read = _reader(files)

    transform = inline_template_sql_files(
        {**_TRANSFORM, "sql_path": "t.sql"},
        {"s": "x"},
        read_sql_file=read,
        template="tp",
    )
    load = inline_template_sql_files(
        {"name": "l", "type": "load", "source": {"type": "sql", "sql_path": "l.sql"}},
        {"s": "x"},
        read_sql_file=read,
        template="tp",
    )
    view = inline_template_sql_files(
        {
            "name": "m",
            "type": "write",
            "write_target": {"type": "materialized_view", "sql_path": "m.sql"},
        },
        {"s": "x"},
        read_sql_file=read,
        template="tp",
    )

    assert transform == {**_TRANSFORM, "sql_path": "t.sql", "sql": "SELECT * FROM x.a"}
    assert load["source"] == {
        "type": "sql",
        "sql_path": "l.sql",
        "sql": "SELECT * FROM x.b",
    }
    assert view["write_target"]["sql"] == "SELECT * FROM x.c"
    assert view["write_target"]["sql_path"] == "m.sql"


@pytest.mark.parametrize(
    "files",
    [
        {"q.sql": "SELECT * FROM silver.orders -- no template syntax"},
        {},
    ],
    ids=["no-jinja-delimiters", "missing-file"],
)
def test_file_without_jinja_or_missing_is_left_to_generation(files):
    action = {**_TRANSFORM, "sql_path": "q.sql"}

    result = inline_template_sql_files(
        action, {}, read_sql_file=_reader(files), template="tp"
    )

    assert result == action


def test_inline_sql_wins_and_its_file_is_not_read():
    def read(path):
        raise AssertionError(f"read {path}")

    action = {**_TRANSFORM, "sql": "SELECT 1", "sql_path": "q.sql"}

    assert (
        inline_template_sql_files(action, {}, read_sql_file=read, template="tp")
        == action
    )


@pytest.mark.parametrize(
    "action",
    [
        {"name": "l", "type": "load", "source": {"type": "delta", "sql_path": "q.sql"}},
        {
            "name": "p",
            "type": "transform",
            "transform_type": "python",
            "sql_path": "q.sql",
        },
        {
            "name": "w",
            "type": "write",
            "write_target": {"type": "streaming_table", "sql_path": "q.sql"},
        },
    ],
    ids=["load-delta-source", "python-transform", "streaming-table-target"],
)
def test_only_the_three_generator_places_are_rendered(action):
    result = inline_template_sql_files(
        action, {}, read_sql_file=_reader({"q.sql": "{{ x }}"}), template="tp"
    )

    assert result == action


def test_backslash_path_reads_the_file_generation_reads():
    result = inline_template_sql_files(
        {**_TRANSFORM, "sql_path": "sql\\gold\\q.sql"},
        {"t": "orders"},
        read_sql_file=_reader({"sql/gold/q.sql": "SELECT * FROM {{ t }}"}),
        template="tp",
    )

    assert result["sql"] == "SELECT * FROM orders"


def test_file_that_renders_to_empty_sql_raises_cfg_071():
    with pytest.raises(LHPError) as excinfo:
        inline_template_sql_files(
            {**_TRANSFORM, "sql_path": "q.sql"},
            {"enabled": False},
            read_sql_file=_reader({"q.sql": "{% if enabled %}SELECT 1{% endif %}\n"}),
            template="tp",
        )

    assert excinfo.value.code == "LHP-CFG-071"
    assert "renders to empty SQL" in excinfo.value.details
