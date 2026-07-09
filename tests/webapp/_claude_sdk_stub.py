"""Scripted stand-in for ``ClaudeSDKClient`` (the ``client_factory`` seam).

Implements :class:`~lhp.webapp.services.claude_sdk_chat.ClaudeClientProtocol`
structurally — tests never spawn the SDK subprocess. The script is a list of
entries consumed by ``receive_messages``:

* a real SDK message dataclass — yielded as-is;
* :data:`RAISE` — raise ``ProcessError`` (subprocess death mid-stream);
* :data:`HANG` — park until ``interrupt()`` is called, then continue;
* :class:`Sleep` — idle for ``seconds`` (heartbeat tests);
* :class:`Approval` — invoke the engine's ``can_use_tool`` callback with the
  given tool call (mirroring the SDK control loop: the message stream stalls
  until the callback returns) and record its ``PermissionResult``.

Mirrors the conventions of ``_omnigent_stub.py``: records everything the
tests assert on (received options, queries, interrupt calls, permission
results) and asserts nothing itself.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncIterator, Optional

from claude_agent_sdk import ClaudeAgentOptions, ProcessError
from claude_agent_sdk.types import ToolPermissionContext

#: Script sentinel: raise ``ProcessError`` from the message stream.
RAISE = object()


@dataclass
class Hang:
    """Script sentinel: park the stream until ``interrupt()`` fires."""


HANG = Hang()


@dataclass
class Sleep:
    """Script sentinel: idle the stream for ``seconds``."""

    seconds: float


@dataclass
class Wait:
    """Script sentinel: park the stream until the test sets ``event``
    (deterministic cross-turn coordination for the locking tests)."""

    event: asyncio.Event


@dataclass
class Approval:
    """Script sentinel: drive one ``can_use_tool`` round-trip."""

    tool: str
    input: dict[str, Any]


class FakeClaudeClient:
    """Protocol-satisfying double; behavior comes entirely from the script."""

    def __init__(self, options: ClaudeAgentOptions, script: list[Any]) -> None:
        self.options = options
        self.script = list(script)
        self.connect_error: Optional[Exception] = None
        self.connected = False
        self.disconnected = False
        self.queries: list[str] = []
        self.interrupt_calls = 0
        self.permission_results: list[Any] = []
        self._interrupted = asyncio.Event()

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True

    async def query(self, prompt: str, session_id: str = "default") -> None:
        self.queries.append(prompt)

    async def interrupt(self) -> None:
        self.interrupt_calls += 1
        self._interrupted.set()

    async def disconnect(self) -> None:
        self.disconnected = True

    async def receive_messages(self) -> AsyncIterator[Any]:
        for entry in self.script:
            if entry is RAISE:
                raise ProcessError("scripted subprocess death", exit_code=1)
            if isinstance(entry, Hang):
                await self._interrupted.wait()
                continue
            if isinstance(entry, Sleep):
                await asyncio.sleep(entry.seconds)
                continue
            if isinstance(entry, Wait):
                await entry.event.wait()
                continue
            if isinstance(entry, Approval):
                assert self.options.can_use_tool is not None
                result = await self.options.can_use_tool(
                    entry.tool, entry.input, ToolPermissionContext(tool_use_id="tu_x")
                )
                self.permission_results.append(result)
                continue
            yield entry


def make_factory(
    script: list[Any],
    holder: list[FakeClaudeClient],
    connect_error: Optional[Exception] = None,
):
    """A ``client_factory`` that scripts every client and records it in ``holder``."""

    def factory(options: ClaudeAgentOptions) -> FakeClaudeClient:
        client = FakeClaudeClient(options, script)
        client.connect_error = connect_error
        holder.append(client)
        return client

    return factory
