"""Generated schema-transform code refers to oddly named columns correctly (#324).

``F.col`` and ``DataFrame.select`` parse their argument, so a name containing
``.`` or a backtick must reach them backtick-quoted. ``withColumnRenamed`` and
the name argument of ``withColumn`` take names literally and must stay raw.
"""

import ast

import yaml

from lhp.generators.transform import SchemaTransformGenerator
from lhp.models import Action, ActionType, TransformType
from lhp.utils.file_header import normalize_content


def _generate_code(schema_inline: str, enforcement: str = "strict") -> str:
    action = Action(
        name="apply_schema",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.SCHEMA,
        source="v_raw",
        target="v_clean",
        schema_inline=schema_inline,
        enforcement=enforcement,
        readMode="batch",
    )
    return SchemaTransformGenerator().generate(action, {})


def _generate(schema_inline: str, enforcement: str = "strict") -> ast.Module:
    return ast.parse(normalize_content(_generate_code(schema_inline, enforcement)))


def _calls(tree: ast.Module, attr: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == attr
    ]


def _args(call: ast.Call) -> list[object]:
    return [ast.literal_eval(arg) for arg in call.args]


def _columns_to_select(tree: ast.Module) -> object:
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "columns_to_select"
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("columns_to_select was not generated")


def test_cast_of_dotted_name_quotes_only_the_column_reference():
    tree = _generate(yaml.safe_dump({"type_casting": {"a.b": "INT"}}))

    (with_column,) = _calls(tree, "withColumn")
    assert ast.literal_eval(with_column.args[0]) == "a.b"
    (col,) = _calls(tree, "col")
    assert _args(col) == ["`a.b`"]
    assert _columns_to_select(tree) == ["`a.b`"]


def test_backticks_inside_a_name_are_doubled():
    tree = _generate(yaml.safe_dump({"type_casting": {"tick`name": "STRING"}}))

    (col,) = _calls(tree, "col")
    assert _args(col) == ["`tick``name`"]
    assert _columns_to_select(tree) == ["`tick``name`"]


def test_rename_arguments_are_never_backtick_quoted():
    tree = _generate(
        yaml.safe_dump({"column_mapping": {"a.b": "ab", "tick`name": "tick"}})
    )

    renames = [_args(call) for call in _calls(tree, "withColumnRenamed")]
    assert renames == [["a.b", "ab"], ["tick`name", "tick"]]


def test_names_that_already_resolved_render_unchanged():
    code = _generate_code(
        yaml.safe_dump({"type_casting": {"amount": "INT", "customer name": "STRING"}})
    )

    assert 'F.col("amount").cast("INT")' in code
    assert 'F.col("customer name").cast("STRING")' in code


def test_issue_324_spaced_source_renames_literally():
    tree = _generate('columns:\n  - "`customer name`  -> CUSTOMER_NAME"')

    (rename,) = _calls(tree, "withColumnRenamed")
    assert _args(rename) == ["customer name", "CUSTOMER_NAME"]
    assert _columns_to_select(tree) == ["CUSTOMER_NAME"]


def test_quoted_dotted_pass_through_is_selected_quoted():
    tree = _generate('columns:\n  - "`order.id`"\n  - "`tick``name`: STRING"')

    (col,) = _calls(tree, "col")
    assert _args(col) == ["`tick``name`"]
    assert _columns_to_select(tree) == ["`tick``name`", "`order.id`"]
