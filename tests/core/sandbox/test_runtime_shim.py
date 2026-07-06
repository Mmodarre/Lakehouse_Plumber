"""Tests for the sandbox runtime table-name shim (``_runtime_shim.py``).

The emitted ``__lhp_sandbox_table`` helper must mirror the static matcher
exactly: canonicalize, match the produced set (3-part exact + the 2<->3-part
short-key rule with the catalog-uniqueness guard), and rename only the LEAF of
the caller's spelling. These tests EXEC the rendered helper source and probe it
directly, plus cover the wrap / idempotency / placement text primitives.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import pytest

from lhp.core.sandbox import (
    SandboxTableRenames,
    TableRenameStrategy,
    build_sandbox_table_renames,
)
from lhp.core.sandbox._runtime_shim import (
    HELPER_NAME,
    is_shim_call,
    module_defines_helper,
    render_helper,
    wrap_arg,
)
from lhp.models import Action, ActionType, FlowGroup


def _write(name: str, catalog: str, schema: str, table: str) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=f"v_{name}",
        write_target={
            "type": "streaming_table",
            "catalog": catalog,
            "schema": schema,
            "table": table,
        },
    )


def _sink(name: str, table_name: str) -> Action:
    return Action(
        name=name,
        type=ActionType.WRITE,
        source=f"v_{name}",
        write_target={
            "type": "sink",
            "sink_type": "delta",
            "options": {"tableName": table_name},
        },
    )


def _renames(
    actions: Sequence[Action],
    namespace: str = "alice",
    pattern: str = "{namespace}_{table}",
) -> SandboxTableRenames:
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=list(actions))
    strategy = TableRenameStrategy(namespace=namespace, table_pattern=pattern)
    return build_sandbox_table_renames([flowgroup], strategy)


def _compile_helper(renames: SandboxTableRenames) -> Callable[[str], str]:
    """Render + exec the helper source; return the callable it defines."""
    namespace: dict = {}
    exec(render_helper(renames), namespace)
    return namespace[HELPER_NAME]


@pytest.fixture
def helper() -> Callable[[str], str]:
    renames = _renames(
        [
            _write("w0", "dev", "silver", "orders"),
            _write("w1", "dev", "bronze", "RawOrders"),
            _sink("s0", "stg.events"),
        ]
    )
    return _compile_helper(renames)


@pytest.mark.unit
class TestHelperRuntime:
    """The exec'd helper mirrors the static matcher's decisions."""

    def test_exact_three_part_match_renames_leaf(self, helper):
        assert helper("dev.silver.orders") == "dev.silver.alice_orders"

    def test_case_variant_preserves_caller_casing(self, helper):
        # Matching is case-insensitive; the caller's casing survives.
        assert helper("DEV.SILVER.Orders") == "DEV.SILVER.alice_Orders"

    def test_backticked_parts_preserved(self, helper):
        assert helper("dev.silver.`orders`") == "dev.silver.`alice_orders`"

    def test_two_part_input_matches_unique_three_part_producer(self, helper):
        assert helper("silver.orders") == "silver.alice_orders"

    def test_two_part_producer_matched(self, helper):
        # stg.events is registered 2-part (a delta sink without a catalog).
        assert helper("stg.events") == "stg.alice_events"

    def test_non_match_is_identity(self, helper):
        assert helper("dev.gold.summary") == "dev.gold.summary"

    def test_one_part_is_identity(self, helper):
        assert helper("orders") == "orders"

    def test_already_renamed_is_identity(self, helper):
        # Idempotent: a renamed leaf no longer matches the produced set.
        assert helper("dev.silver.alice_orders") == "dev.silver.alice_orders"

    def test_ambiguous_short_key_not_renamed(self):
        # Two catalogs produce sch.dup → a 2-part input is ambiguous → identity,
        # but the fully-qualified 3-part spellings still rename.
        renames = _renames(
            [_write("a", "cat1", "sch", "dup"), _write("b", "cat2", "sch", "dup")]
        )
        helper = _compile_helper(renames)
        assert helper("sch.dup") == "sch.dup"
        assert helper("cat1.sch.dup") == "cat1.sch.alice_dup"
        assert helper("cat2.sch.dup") == "cat2.sch.alice_dup"

    def test_custom_pattern_prefix_and_suffix(self):
        renames = _renames(
            [_write("w", "dev", "silver", "orders")],
            namespace="bob",
            pattern="sbx_{table}_{namespace}",
        )
        helper = _compile_helper(renames)
        assert helper("dev.silver.orders") == "dev.silver.sbx_orders_bob"


@pytest.mark.unit
class TestHelperRendering:
    """Rendering determinism and the multi-{table} guard."""

    def test_render_is_deterministic(self):
        renames = _renames(
            [
                _write("w0", "dev", "silver", "orders"),
                _write("w1", "dev", "bronze", "raw"),
                _sink("s0", "stg.events"),
            ]
        )
        assert render_helper(renames) == render_helper(renames)

    def test_multiple_table_placeholder_pattern_raises(self):
        renames = _renames(
            [_write("w", "dev", "silver", "orders")],
            pattern="{namespace}_{table}_{table}",
        )
        with pytest.raises(ValueError, match="exactly once"):
            render_helper(renames)


@pytest.mark.unit
class TestShimTextPrimitives:
    """The wrap / idempotency-marker helpers used by the rewriter."""

    def test_wrap_arg(self):
        assert wrap_arg("name") == "__lhp_sandbox_table(name)"

    def test_is_shim_call_true(self):
        assert is_shim_call("__lhp_sandbox_table(name)")
        assert is_shim_call("__lhp_sandbox_table( x )")

    def test_is_shim_call_false(self):
        assert not is_shim_call("name")
        assert not is_shim_call("get_name()")
        assert not is_shim_call("__lhp_sandbox_table_other(x)")

    def test_module_defines_helper(self):
        assert module_defines_helper("def __lhp_sandbox_table(x):\n    return x\n")
        assert not module_defines_helper("df = spark.table('a.b.c')\n")
