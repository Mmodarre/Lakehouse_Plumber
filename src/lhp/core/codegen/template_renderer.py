"""Template rendering utility for LakehousePlumber."""

import functools
import json
import logging
from typing import Any, Dict, Optional

import yaml
from jinja2 import (
    BaseLoader,
    Environment,
    PackageLoader,
    TemplateSyntaxError,
    UndefinedError,
)
from jinja2.exceptions import TemplateNotFound

from ...errors import ErrorFactory, codes
from ...utils.yaml_filters import dict_to_yaml

logger = logging.getLogger(__name__)


def get_lhp_template_loader() -> PackageLoader:
    """Single source of truth for LHP package-template location; renames/moves are a one-line change here."""
    return PackageLoader("lhp", "templates")


@functools.lru_cache(maxsize=1)
def get_shared_generator_environment() -> Environment:
    """Return the process-local Jinja2 ``Environment`` shared by all action generators.

    Building a fresh ``Environment`` per action (a generator is returned per
    action by the registry) means every render recompiles its template against
    an empty per-Environment cache. A single shared Environment amortizes
    template compilation and lets subsequent renders hit Jinja's in-Environment
    template cache.

    BYTE-IDENTITY REQUIREMENT: this MUST replicate ``BaseActionGenerator``'s
    historical per-instance Environment exactly. The filters in particular must
    match the *generator* environment — ``toyaml`` is :func:`yaml.dump` here,
    NOT :func:`~lhp.utils.yaml_filters.dict_to_yaml` (which is the distinct
    filter used by :class:`TemplateRenderer`; see Decision §1.4). Do not unify
    the two.

    ``auto_reload=False`` is an output-neutral addition (not part of the
    replication): templates are immutable within a run, so it only skips the
    per-fetch mtime stat.
    """
    env = Environment(  # nosec B701 — generates Python, not HTML
        loader=get_lhp_template_loader(),
        trim_blocks=True,
        lstrip_blocks=True,
        auto_reload=False,
    )
    env.filters["tojson"] = json.dumps
    env.filters["toyaml"] = yaml.dump
    return env


class TemplateRenderer:
    def __init__(self, loader: Optional[BaseLoader] = None):
        if loader is None:
            loader = get_lhp_template_loader()
        self.env = Environment(  # nosec B701 — generates Python, not HTML
            loader=loader, trim_blocks=True, lstrip_blocks=True
        )

        self.env.filters["tojson"] = json.dumps
        self.env.filters["toyaml"] = dict_to_yaml

    @classmethod
    def from_package(cls) -> "TemplateRenderer":
        """Construct a renderer backed by the LHP package template tree."""
        return cls(get_lhp_template_loader())

    def render_template(self, template_name: str, context: Dict[str, Any]) -> str:
        try:
            template = self.env.get_template(template_name)
        except TemplateNotFound as e:
            raise ErrorFactory.template_not_found(
                template_name=template_name,
                available_templates=[],
                templates_dir="lhp.templates package",
            ) from e

        try:
            return template.render(**context)
        except UndefinedError as e:
            raise ErrorFactory.config_error(
                codes.CFG_029,
                title="Template rendering error: undefined variable",
                details=f"Template '{template_name}' references an undefined variable: {e}",
                suggestions=[
                    "Check that all required template variables are provided in context",
                    "Review the template file for typos in variable names",
                ],
                context={"Template": template_name, "Error": str(e)},
            ) from e
        except TemplateSyntaxError as e:
            raise ErrorFactory.config_error(
                codes.CFG_030,
                title="Template syntax error",
                details=f"Template '{template_name}' has a syntax error: {e}",
                suggestions=[
                    "Check the Jinja2 syntax in the template file",
                    f"Review the template at: lhp/templates/{template_name}",
                ],
                context={"Template": template_name, "Line": str(e.lineno or "unknown")},
            ) from e
