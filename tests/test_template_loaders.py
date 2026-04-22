"""Unit tests for the PackageLoader-based template loading paths."""

from datetime import date

import pytest
from jinja2 import Environment
from jinja2.exceptions import TemplateNotFound

from lhp.core.base_generator import BaseActionGenerator
from lhp.core.init_template_context import InitTemplateContext
from lhp.core.init_template_loader import InitTemplateLoader
from lhp.utils.template_renderer import (
    TemplateRenderer,
    get_lhp_template_loader,
)


def test_get_lhp_template_loader_resolves_known_template() -> None:
    """The helper returns a loader that can resolve a real package template."""
    env = Environment(loader=get_lhp_template_loader())
    # The monitoring template is the v0.8.2 regression trigger — assert it loads.
    template = env.get_template("monitoring/union_event_logs.py.j2")
    assert template is not None


def test_get_lhp_template_loader_raises_template_not_found_for_missing() -> None:
    """Missing templates still raise a proper Jinja2 exception."""
    env = Environment(loader=get_lhp_template_loader())
    with pytest.raises(TemplateNotFound):
        env.get_template("does/not/exist.j2")


def test_template_renderer_from_package_resolves_bundle_template() -> None:
    """TemplateRenderer.from_package() wires the package loader so real
    templates under lhp/templates/ resolve."""
    renderer = TemplateRenderer.from_package()
    template = renderer.env.get_template("bundle/pipeline_resource.yml.j2")
    assert template is not None


def test_init_template_loader_renders_lhp_yaml() -> None:
    """InitTemplateLoader renders the lhp.yaml.j2 template with a context."""
    loader = InitTemplateLoader()
    context = InitTemplateContext(
        project_name="demo_project",
        current_date=date(2026, 4, 22).isoformat(),
        author="Test User",
        bundle_enabled=False,
        bundle_uuid="",
    )
    rendered = loader.render_template("lhp.yaml.j2", context)
    assert "demo_project" in rendered


def test_base_generator_subclass_can_render() -> None:
    """A concrete BaseActionGenerator subclass can resolve a real action template."""

    class _ConcreteGen(BaseActionGenerator):
        def generate(self, action, context):  # type: ignore[override]
            return ""

    gen = _ConcreteGen()
    # cloudfiles.py.j2 is one of the standard load action templates.
    template = gen.env.get_template("load/cloudfiles.py.j2")
    assert template is not None
