"""Python-literal filter registration across every ``.py.j2`` Jinja environment.

Templates render user strings into generated Python through ``py_str`` /
``py_str_single`` / ``py_block`` / ``py_literal`` / ``py_comment``; an environment
missing one of them fails at template-compile time, so every environment that
renders a ``.py.j2`` template must carry all five.
"""

import json

import pytest
from jinja2 import Environment

from lhp.core.codegen.template_renderer import (
    TemplateRenderer,
    get_shared_generator_environment,
    register_python_literal_filters,
)
from lhp.core.codegen.tst_reporting_hook_generator import TestReportingHookGenerator
from lhp.core.codegen.uc_tagging_hook_generator import UCTaggingHookGenerator
from lhp.core.sandbox._runtime_shim import _environment as sandbox_environment
from lhp.models import ProjectConfig
from lhp.utils.python_literals import PYTHON_LITERAL_FILTERS

_FILTER_NAMES = ("py_str", "py_str_single", "py_block", "py_literal", "py_comment")


def _assert_has_python_literal_filters(env: Environment) -> None:
    for name in _FILTER_NAMES:
        assert env.filters[name] is PYTHON_LITERAL_FILTERS[name], name


@pytest.mark.unit
def test_register_python_literal_filters_adds_the_five_filters():
    env = Environment()  # nosec B701 — test-only, renders no HTML

    register_python_literal_filters(env)

    _assert_has_python_literal_filters(env)


@pytest.mark.unit
def test_registration_leaves_tojson_bound_to_json_dumps():
    env = get_shared_generator_environment()

    assert env.filters["tojson"] is json.dumps
    assert TemplateRenderer().env.filters["tojson"] is json.dumps


@pytest.mark.unit
def test_shared_generator_environment_has_the_filters():
    _assert_has_python_literal_filters(get_shared_generator_environment())


@pytest.mark.unit
def test_template_renderer_environment_has_the_filters():
    _assert_has_python_literal_filters(TemplateRenderer.from_package().env)


@pytest.mark.unit
def test_test_reporting_hook_environment_has_the_filters(tmp_path):
    generator = TestReportingHookGenerator(ProjectConfig(name="p"), tmp_path)

    _assert_has_python_literal_filters(generator._jinja_env)


@pytest.mark.unit
def test_uc_tagging_hook_environment_has_the_filters(tmp_path):
    generator = UCTaggingHookGenerator(ProjectConfig(name="p"), tmp_path)

    _assert_has_python_literal_filters(generator._jinja_env)


@pytest.mark.unit
def test_sandbox_runtime_shim_environment_has_the_filters():
    _assert_has_python_literal_filters(sandbox_environment())


@pytest.mark.unit
def test_filters_render_in_a_template():
    env = Environment()  # nosec B701 — test-only, renders no HTML
    register_python_literal_filters(env)

    rendered = env.from_string(
        'x = {{ s|py_str }}\ny = """{{ s|py_block }}"""\n'
        "z = {{ d|py_literal }}\n# {{ s|py_comment }}\n"
    ).render(s='a"b\\c\nd', d={"flag": True, "nothing": None})

    assert rendered == (
        'x = "a\\"b\\\\c\\nd"\n'
        'y = """a"b\\\\c\nd"""\n'
        'z = {"flag": True, "nothing": None}\n'
        '# a"b\\c\n# d'
    )
