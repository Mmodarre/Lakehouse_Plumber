"""Regression tests for issue #189 — python transform ``parameters`` default.

``Action.parameters`` is ``Optional`` and defaults to ``None``, so the
attribute exists with value ``None`` whenever YAML omits it. The template
renders ``parameters = {{ parameters | tojson }}`` unconditionally and
``tojson`` is bound to :func:`json.dumps`, so a ``None`` used to emit
``parameters = null`` — an unbound name that raises ``NameError`` the first
time Lakeflow evaluates the view. The documented default is ``{}``
(``docs/reference/actions/transform.rst``), which is also what the dependency
analyser already assumes (``core/dependencies/_binding_rules.py``).
"""

import tempfile
from pathlib import Path

import pytest

from lhp.generators.transform import PythonTransformGenerator
from lhp.models import Action, ActionType, FlowGroup, TransformType

TRANSFORM_MODULE_SOURCE = """
def enrich_customers(df, spark, parameters):
    return df
"""


def _generate(tmp_path: Path, **action_fields) -> str:
    """Render the python-transform action, writing its module into ``tmp_path``."""
    transformations = tmp_path / "transformations"
    transformations.mkdir(parents=True, exist_ok=True)
    (transformations / "enrich_customers.py").write_text(TRANSFORM_MODULE_SOURCE)

    action = Action(
        name="enrich_customers",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.PYTHON,
        source="v_customers_validated",
        target="v_customers_enriched",
        module_path="transformations/enrich_customers.py",
        function_name="enrich_customers",
        **action_fields,
    )
    return PythonTransformGenerator().generate(
        action,
        {
            "output_dir": tmp_path / "generated",
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(
                pipeline="test_pipeline", flowgroup="test_flowgroup", actions=[]
            ),
        },
    )


def _parameters_assignments(code: str) -> list:
    """Every ``parameters = ...`` right-hand side in the generated code."""
    return [
        line.split("=", 1)[1].strip()
        for line in code.splitlines()
        if line.strip().startswith("parameters =")
    ]


@pytest.mark.unit
def test_omitted_parameters_render_empty_dict():
    """No ``parameters:`` in YAML still emits a usable dict, never ``null``."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = _generate(Path(tmpdir))

    assert _parameters_assignments(code) == ["{}"]
    assert "null" not in code


@pytest.mark.unit
def test_yaml_null_parameters_render_empty_dict():
    """A bare ``parameters:`` key (YAML null) behaves like an omitted one."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = _generate(Path(tmpdir), parameters=None)

    assert _parameters_assignments(code) == ["{}"]
    assert "null" not in code


@pytest.mark.unit
def test_declared_parameters_are_passed_through_unchanged():
    """The coalesce must not disturb a populated dict."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = _generate(Path(tmpdir), parameters={"enrichment_type": "full"})

    assert _parameters_assignments(code) == ['{"enrichment_type": "full"}']
