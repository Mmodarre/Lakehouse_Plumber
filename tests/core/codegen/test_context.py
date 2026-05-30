"""Unit tests for :class:`GenerationContextBuilder`.

Covers the per-builder source-function signature cache: every flowgroup
context produced by a single builder must receive the *same* cache dict
object (identity), so a parse-once-per-source-file optimization can be
shared across all flowgroups in a pipeline run.
"""

import pytest

from lhp.core.codegen.context import GenerationContextBuilder
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.models import FlowGroup


@pytest.mark.unit
def test_signature_cache_is_same_object_across_flowgroup_contexts():
    """Two contexts from one builder share the identical cache dict."""
    builder = GenerationContextBuilder()
    sub_mgr = EnhancedSubstitutionManager()

    ctx_a = builder.build(
        flowgroup=FlowGroup(pipeline="p", flowgroup="fg_a"),
        substitution_mgr=sub_mgr,
        preset_config={},
        output_dir=None,
        source_yaml=None,
        env="dev",
    )
    ctx_b = builder.build(
        flowgroup=FlowGroup(pipeline="p", flowgroup="fg_b"),
        substitution_mgr=sub_mgr,
        preset_config={},
        output_dir=None,
        source_yaml=None,
        env="dev",
    )

    assert "source_function_signature_cache" in ctx_a
    assert "source_function_signature_cache" in ctx_b

    # Same dict object (identity), so writes by one flowgroup are visible to
    # the next within the same builder/pipeline.
    assert ctx_a["source_function_signature_cache"] is ctx_b["source_function_signature_cache"]
    assert ctx_a["source_function_signature_cache"] is builder._source_function_signature_cache

    # Starts empty and is a dict keyed by resolved absolute source path (str).
    assert ctx_a["source_function_signature_cache"] == {}

    ctx_a["source_function_signature_cache"]["/abs/path/src.py"] = object()
    assert "/abs/path/src.py" in ctx_b["source_function_signature_cache"]


@pytest.mark.unit
def test_signature_cache_is_isolated_between_builders():
    """Distinct builders own distinct caches (per-builder / per-worker scope)."""
    builder_one = GenerationContextBuilder()
    builder_two = GenerationContextBuilder()

    assert (
        builder_one._source_function_signature_cache
        is not builder_two._source_function_signature_cache
    )
