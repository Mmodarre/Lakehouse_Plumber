"""Generated schema-transform code refers to oddly named columns correctly (#324).

``F.col`` and ``DataFrame.select`` parse their argument, so a name containing
``.`` or a backtick must reach them backtick-quoted. ``withColumnRenamed`` and
the name argument of ``withColumn`` take names literally and must stay raw.
"""

import ast

import pytest
import yaml

from lhp.generators.transform import SchemaTransformGenerator
from lhp.models import (
    Action,
    ActionType,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
    TransformType,
)
from lhp.utils.file_header import normalize_content

pytestmark = pytest.mark.unit


def _generate_code(
    schema_inline: str, enforcement: str = "strict", context: dict | None = None
) -> str:
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
    return SchemaTransformGenerator().generate(action, context or {})


def _generate(
    schema_inline: str, enforcement: str = "strict", context: dict | None = None
) -> ast.Module:
    code = _generate_code(schema_inline, enforcement, context)
    return ast.parse(normalize_content(code))


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


def _assigned(tree: ast.Module, variable: str) -> object:
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == variable
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{variable} was not generated")


def _columns_to_select(tree: ast.Module) -> object:
    return _assigned(tree, "columns_to_select")


def test_operational_metadata_names_stay_raw_for_the_membership_check():
    # The generated code tests ``meta_col in set(df.columns)`` against raw
    # names, so quoting this list would silently drop the metadata column.
    project_config = ProjectConfig(
        name="p",
        operational_metadata=ProjectOperationalMetadataConfig(
            columns={
                "_meta.ts": MetadataColumnConfig(
                    expression="F.current_timestamp()", applies_to=["view"]
                )
            }
        ),
    )

    tree = _generate(
        yaml.safe_dump({"type_casting": {"a.b": "INT"}}),
        context={"project_config": project_config},
    )

    assert _assigned(tree, "metadata_columns") == ["_meta.ts"]
    assert _columns_to_select(tree) == ["`a.b`"]


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
