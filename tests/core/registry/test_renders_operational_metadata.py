"""``renders_operational_metadata``: which generators emit metadata columns (#289).

The flag on each generator class is the one declaration both paths read: the
render path (``BaseActionGenerator._get_operational_metadata``) refuses to run
on a generator that does not set it, and the validate path checks
operational-metadata expressions only for actions whose generator sets it
(``ActionRegistry.renders_operational_metadata``). So ``lhp validate`` and
``lhp generate`` agree on which selected columns reach generated code.
"""

import ast
import inspect
import textwrap

import pytest

from lhp.core.registry import ActionRegistry, BaseActionGenerator
from lhp.core.registry.action_registry import _REGISTERED_GENERATORS
from lhp.generators.write import SinkWriteGenerator
from lhp.models import Action, ActionType, TransformType


def _calls_render_path(generator_class: type) -> bool:
    """Whether the class, or an LHP class it inherits from, calls the render path.

    Walks the AST of every LHP class in the MRO (except the base that defines
    the method), so an inherited ``generate`` counts and comments do not.
    """
    for cls in generator_class.__mro__:
        if cls is BaseActionGenerator or not cls.__module__.startswith("lhp."):
            continue
        tree = ast.parse(textwrap.dedent(inspect.getsource(cls)))
        if any(
            isinstance(node, ast.Attribute) and node.attr == "_get_operational_metadata"
            for node in ast.walk(tree)
        ):
            return True
    return False


def _load(source_type: str) -> Action:
    return Action(
        name="l", type=ActionType.LOAD, source={"type": source_type}, target="v"
    )


def _write(write_target: dict) -> Action:
    return Action(
        name="w", type=ActionType.WRITE, source="v", write_target=write_target
    )


@pytest.mark.unit
class TestActionRegistryRendersOperationalMetadata:
    @pytest.mark.parametrize(
        ("action", "expected"),
        [
            (_load("cloudfiles"), True),
            (_load("delta"), True),
            (_load("jdbc"), True),
            (
                Action(name="l", type=ActionType.LOAD, source="SELECT 1", target="v"),
                True,
            ),
            (
                Action(
                    name="t",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.SQL,
                    source="v",
                    target="v2",
                ),
                True,
            ),
            (
                Action(
                    name="t",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.SCHEMA,
                    source="v",
                    target="v2",
                ),
                False,
            ),
            (_write({"type": "streaming_table", "table": "t"}), False),
            (_write({"type": "materialized_view", "table": "t"}), False),
            (_write({"type": "sink", "sink_type": "delta", "sink_name": "s"}), True),
            (
                Action(
                    name="c",
                    type=ActionType.TEST,
                    test_type="row_count",
                    source=["a", "b"],
                ),
                False,
            ),
        ],
        ids=[
            "load_cloudfiles",
            "load_delta",
            "load_jdbc",
            "load_string_sql",
            "transform_sql",
            "transform_schema",
            "write_streaming_table",
            "write_materialized_view",
            "write_sink",
            "test_row_count",
        ],
    )
    def test_reads_the_generator_declaration(self, action, expected):
        assert ActionRegistry().renders_operational_metadata(action) is expected


@pytest.mark.unit
class TestDeclarationMatchesRenderPath:
    def test_undeclared_generator_cannot_render_metadata(self):
        """The render path refuses a generator that renders without declaring,
        so validation can never skip a column that reaches generated code."""

        class _Undeclared(BaseActionGenerator):
            def generate(self, action, context):
                self._get_operational_metadata(action, context)
                return ""

        with pytest.raises(TypeError, match="renders_operational_metadata"):
            _Undeclared().generate(_load("sql"), {})

    @pytest.mark.parametrize(
        "generator_class",
        [
            cls
            for family in _REGISTERED_GENERATORS.values()
            for cls in family.values()
            if cls is not SinkWriteGenerator
        ],
        ids=lambda cls: cls.__name__,
    )
    def test_declaration_matches_whether_generate_renders(self, generator_class):
        """A generator that declares the flag without rendering would make
        validation check columns that never reach generated code."""
        assert generator_class.renders_operational_metadata is _calls_render_path(
            generator_class
        )

    def test_sink_dispatcher_declares_what_its_sink_generators_render(self):
        """The registry maps every sink to the dispatcher, which renders
        through one of its per-``sink_type`` generators."""
        sink_generators = [type(g) for g in SinkWriteGenerator().generators.values()]
        for cls in sink_generators:
            assert cls.renders_operational_metadata is _calls_render_path(cls)
        assert SinkWriteGenerator.renders_operational_metadata is all(
            cls.renders_operational_metadata for cls in sink_generators
        )
