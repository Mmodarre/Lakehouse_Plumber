"""Unit tests for the process-local shared generator Jinja2 Environment.

Covers ``get_shared_generator_environment`` (the lru_cache-memoized factory)
and its byte-identity contract with the historical per-instance Environment
that ``BaseActionGenerator`` used to build. The shared Environment is the
single biggest hot-path lever: it amortizes template compilation across the
per-action generators returned by the registry.
"""

import json

import yaml

from lhp.core.codegen.template_renderer import get_shared_generator_environment
from lhp.core.registry import BaseActionGenerator


class _ConcreteGenA(BaseActionGenerator):
    """Minimal concrete generator (abstract ``generate`` satisfied)."""

    def generate(self, action, context):  # type: ignore[override]
        return ""


class _ConcreteGenB(BaseActionGenerator):
    """A second, distinct concrete generator subclass."""

    def generate(self, action, context):  # type: ignore[override]
        return ""


def test_shared_environment_is_memoized_singleton() -> None:
    """Repeated calls return the SAME object and the lru_cache records a hit.

    The cache-hit on the second call is the construction-count proof: the
    Environment (and therefore template compilation against its cache) is built
    once, not per call.
    """
    get_shared_generator_environment.cache_clear()

    first = get_shared_generator_environment()
    hits_before = get_shared_generator_environment.cache_info().hits

    second = get_shared_generator_environment()
    hits_after = get_shared_generator_environment.cache_info().hits

    assert first is second
    assert hits_after == hits_before + 1


def test_generator_subclasses_share_one_environment() -> None:
    """Two different BaseActionGenerator subclasses share the same env object."""
    gen_a = _ConcreteGenA()
    gen_b = _ConcreteGenB()

    assert gen_a.env is gen_b.env
    assert gen_a.env is get_shared_generator_environment()


def test_shared_environment_filter_and_flag_parity() -> None:
    """The shared env replicates BaseActionGenerator's historical construction.

    Filters must match the *generator* environment: ``toyaml`` is
    ``yaml.dump`` (NOT ``dict_to_yaml`` — that is TemplateRenderer's distinct
    filter, Decision §1.4), and ``tojson`` is ``json.dumps``. ``trim_blocks``
    and ``lstrip_blocks`` must both be True.
    """
    env = get_shared_generator_environment()

    assert env.filters["toyaml"] is yaml.dump
    assert env.filters["tojson"] is json.dumps
    assert env.trim_blocks is True
    assert env.lstrip_blocks is True
