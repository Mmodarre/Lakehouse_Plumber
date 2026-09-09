"""Regression tests for issue #189 — python load ``source.parameters`` default.

The sibling of the python-transform defect. ``source_config.get("parameters")``
reads a plain YAML dict, so an *absent* key already yielded ``{}``; only a
present-but-null ``parameters:`` leaked ``None`` into
``parameters = {{ parameters | tojson }}`` and emitted the unbound name
``null``. The documented default is ``{}``
(``docs/reference/actions/load.rst``).
"""

import tempfile
from pathlib import Path

import pytest

from lhp.generators.load.python import PythonLoadGenerator
from lhp.models import Action, ActionType, FlowGroup

LOADER_MODULE_SOURCE = """
def load_data(spark, parameters):
    return None
"""


def _generate(tmp_path: Path, source: dict) -> str:
    """Render the python-load action, writing its module into ``tmp_path``."""
    loaders = tmp_path / "loaders"
    loaders.mkdir(parents=True, exist_ok=True)
    (loaders / "my_loader.py").write_text(LOADER_MODULE_SOURCE)

    action = Action(
        name="load_custom_data",
        type=ActionType.LOAD,
        source=source,
        target="v_custom_data",
    )
    return PythonLoadGenerator().generate(
        action,
        {
            "output_dir": tmp_path / "generated",
            "spec_dir": tmp_path,
            "flowgroup": FlowGroup(
                pipeline="test_pipeline", flowgroup="test_flowgroup", actions=[]
            ),
        },
    )


def _source(**overrides) -> dict:
    source = {
        "type": "python",
        "module_path": "loaders/my_loader.py",
        "function_name": "load_data",
    }
    source.update(overrides)
    return source


def _parameters_assignments(code: str) -> list:
    """Every ``parameters = ...`` right-hand side in the generated code."""
    return [
        line.split("=", 1)[1].strip()
        for line in code.splitlines()
        if line.strip().startswith("parameters =")
    ]


@pytest.mark.unit
def test_yaml_null_parameters_render_empty_dict():
    """A bare ``parameters:`` key (YAML null) emits ``{}``, never ``null``."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = _generate(Path(tmpdir), _source(parameters=None))

    assert _parameters_assignments(code) == ["{}"]
    assert "null" not in code


@pytest.mark.unit
def test_omitted_parameters_render_empty_dict():
    """An absent key was already correct; pin it so it stays that way."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = _generate(Path(tmpdir), _source())

    assert _parameters_assignments(code) == ["{}"]
    assert "null" not in code


@pytest.mark.unit
def test_declared_parameters_are_passed_through_unchanged():
    """The coalesce must not disturb a populated dict."""
    with tempfile.TemporaryDirectory() as tmpdir:
        code = _generate(Path(tmpdir), _source(parameters={"region": "us"}))

    assert _parameters_assignments(code) == ['{"region": "us"}']
