"""Unit tests for the Claude SDK turn registry (approvals + interrupts).

Async paths run under ``asyncio.run`` per repo convention (no pytest-asyncio).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from lhp.webapp.services.claude_sdk_bridge import (
    INTERRUPT_DENIED,
    ClaudeTurnRegistry,
    get_claude_turns,
)

pytestmark = pytest.mark.webapp


def test_begin_get_end_turn_lifecycle() -> None:
    registry = ClaudeTurnRegistry()
    assert registry.get("s1") is None

    handle = registry.begin_turn("s1")
    assert registry.get("s1") is handle
    assert handle.interrupt_requested is False

    registry.end_turn("s1")
    assert registry.get("s1") is None
    # Ending an unknown turn is a no-op, not an error.
    registry.end_turn("s1")


def test_create_and_resolve_approval() -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        registry.begin_turn("s1")
        elicitation_id, future = registry.create_approval("s1")

        assert elicitation_id.startswith("elic_")
        assert registry.resolve_approval("s1", elicitation_id, "accept") is True
        assert await future == "accept"
        # A second resolve of the same elicitation is unknown -> False (404).
        assert registry.resolve_approval("s1", elicitation_id, "accept") is False

    asyncio.run(run())


def test_resolve_approval_unknown_session_or_id_returns_false() -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        assert registry.resolve_approval("nope", "elic_x", "accept") is False
        registry.begin_turn("s1")
        assert registry.resolve_approval("s1", "elic_x", "accept") is False

    asyncio.run(run())


def test_create_approval_without_live_turn_raises() -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        with pytest.raises(KeyError):
            registry.create_approval("s1")

    asyncio.run(run())


def test_request_interrupt_unblocks_pending_approvals_then_interrupts() -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        handle = registry.begin_turn("s1")
        _, future = registry.create_approval("s1")

        calls: list[str] = []

        async def fake_interrupt() -> None:
            # The approval future MUST already be resolved when the SDK
            # interrupt runs, or the control loop would deadlock.
            assert future.done()
            assert future.result() == INTERRUPT_DENIED
            calls.append("interrupt")

        handle.interrupt = fake_interrupt

        assert await registry.request_interrupt("s1") is True
        assert calls == ["interrupt"]
        assert handle.interrupt_requested is True
        assert handle.approvals == {}

    asyncio.run(run())


def test_request_interrupt_before_client_connects() -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        handle = registry.begin_turn("s1")
        # No interrupt callable registered yet: the flag alone is the signal.
        assert await registry.request_interrupt("s1") is True
        assert handle.interrupt_requested is True

    asyncio.run(run())


def test_request_interrupt_without_live_turn_returns_false() -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        assert await registry.request_interrupt("s1") is False

    asyncio.run(run())


def test_end_turn_cancels_stray_futures() -> None:
    async def run() -> None:
        registry = ClaudeTurnRegistry()
        registry.begin_turn("s1")
        _, future = registry.create_approval("s1")

        registry.end_turn("s1")
        assert future.cancelled()

    asyncio.run(run())


def test_get_claude_turns_caches_on_app_state() -> None:
    app = SimpleNamespace(state=SimpleNamespace())
    registry = get_claude_turns(app)
    assert isinstance(registry, ClaudeTurnRegistry)
    assert get_claude_turns(app) is registry
    assert app.state.claude_turns is registry
