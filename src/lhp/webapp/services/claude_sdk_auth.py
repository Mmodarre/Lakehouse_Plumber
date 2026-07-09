"""Per-turn auth environment construction for the Claude SDK provider.

:func:`build_turn_env` turns the stored executor config into the environment
overrides handed to ``ClaudeAgentOptions(env=...)`` for ONE turn. Everything
here is SYNCHRONOUS (the ``databricks`` mode does blocking I/O — token cache,
possibly a browser round-trip); the async turn engine bridges via
``asyncio.to_thread`` under a timeout.

Auth modes (token-free from the user's perspective; the stored config only
ever names env vars / profiles — never key material):

* ``claude_subscription`` — ``env={}``: the SDK's bundled binary reads the
  user's ambient Claude Code credentials (macOS keychain /
  ``~/.claude/.credentials.json``). When the config names an
  ``oauth_token_env``, the value of THAT env var (a ``claude setup-token``
  token) is forwarded as ``CLAUDE_CODE_OAUTH_TOKEN``.
* ``databricks`` — a Databricks bearer is minted in pure Python via
  ``databricks-sdk`` (``profile`` from ``~/.databrickscfg``, or ``host`` +
  ``external-browser`` OAuth) and handed to the SDK as ``ANTHROPIC_AUTH_TOKEN``
  against the workspace's Anthropic-compatible ``/ai-gateway/anthropic``
  endpoint. Minted FRESH every turn (the bearer's ~1h lifetime vs
  minutes-long turns), never cached, never persisted, never logged.

Failures raise :class:`ClaudeAuthError` carrying a curated ``detail`` and the
``session.failed`` frame ``hint`` (``claude_auth`` / ``databricks_auth``);
raw exception text stays in the server log only.

Known constraint (spike-verified): ``ClaudeAgentOptions.env`` is a plain
dict-merge over the inherited process env — a key CANNOT be unset, and an
empty-string override still counts as set. ``ANTHROPIC_BASE_URL`` /
``ANTHROPIC_AUTH_TOKEN`` / ``ANTHROPIC_API_KEY`` exported in the ``lhp web``
server's own environment therefore leak into ``claude_subscription`` turns
and take precedence over ambient credentials. Documented in the user docs;
run ``lhp web`` from a shell without those vars for subscription auth.

:stability: internal
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from lhp.webapp.services.assistant_provision import DATABRICKS_DEFAULT_MODEL

logger = logging.getLogger(__name__)

#: Path suffix of the Databricks Anthropic-compatible Messages endpoint.
_AI_GATEWAY_PATH = "/ai-gateway/anthropic"

#: Ambient Claude Code credential file (the keychain is the other location;
#: it cannot be probed portably, so this check is best-effort display only).
_CLAUDE_CREDENTIALS = Path(".claude") / ".credentials.json"


class ClaudeAuthError(Exception):
    """Auth construction failed; ``detail``/``hint`` feed ``session.failed``."""

    def __init__(self, detail: str, hint: str) -> None:
        super().__init__(detail)
        self.detail = detail
        self.hint = hint


@dataclass
class ClaudeAuthEnv:
    """One turn's env overrides + model. The repr NEVER shows env values."""

    env: dict[str, str] = field(default_factory=dict)
    model: Optional[str] = None

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"ClaudeAuthEnv(env_keys={sorted(self.env)}, model={self.model!r})"


def _subscription_env(executor_cfg: dict[str, Any]) -> dict[str, str]:
    oauth_token_env = executor_cfg.get("oauth_token_env")
    if not oauth_token_env:
        # Ambient credentials: the SDK subprocess resolves them itself.
        return {}
    token = os.environ.get(oauth_token_env)
    if not token:
        raise ClaudeAuthError(
            detail=(
                f"The configured token environment variable "
                f"{oauth_token_env!r} is not set in the server's environment. "
                f"Export it (see `claude setup-token`) and restart `lhp web`."
            ),
            hint="claude_auth",
        )
    return {"CLAUDE_CODE_OAUTH_TOKEN": token}


def _databricks_env(executor_cfg: dict[str, Any]) -> dict[str, str]:
    # Imported lazily so the Omnigent-only deployment path never pays for it.
    from databricks.sdk.core import Config

    profile = executor_cfg.get("profile")
    host = executor_cfg.get("host")
    target = f"profile {profile!r}" if profile else f"host {host!r}"
    try:
        if profile:
            config = Config(profile=profile)
        else:
            config = Config(host=host, auth_type="external-browser")
        bearer = config.authenticate().get("Authorization", "")
    except Exception as exc:
        logger.exception("assistant auth: Databricks token mint failed")
        raise ClaudeAuthError(
            detail=(
                f"Could not authenticate to the Databricks workspace "
                f"{target}. Check the profile with `databricks auth login` "
                f"or the workspace host, then retry."
            ),
            hint="databricks_auth",
        ) from exc
    if not bearer.startswith("Bearer "):
        raise ClaudeAuthError(
            detail=(
                "The Databricks credential did not produce a bearer token; "
                "the configured auth type is not usable for the AI gateway."
            ),
            hint="databricks_auth",
        )
    return {
        "ANTHROPIC_BASE_URL": str(config.host).rstrip("/") + _AI_GATEWAY_PATH,
        "ANTHROPIC_AUTH_TOKEN": bearer.removeprefix("Bearer "),
        # Spike-verified (2026-07-10): the Databricks gateway 400s
        # ("invalid beta flag") on the CLI's newest experimental
        # anthropic-beta flags; this trims the set to ones it accepts.
        "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS": "1",
    }


def build_turn_env(executor_cfg: dict[str, Any]) -> ClaudeAuthEnv:
    """Build one turn's :class:`ClaudeAuthEnv` from the stored executor config.

    SYNC — callers bridge via ``asyncio.to_thread`` with a timeout (the
    ``external-browser`` path can block on a user's browser).

    :raises ClaudeAuthError: with a curated detail + ``session.failed`` hint.
    """
    mode = executor_cfg.get("mode", "claude_subscription")
    model: Optional[str] = executor_cfg.get("model") or None
    if mode == "claude_subscription":
        return ClaudeAuthEnv(env=_subscription_env(executor_cfg), model=model)
    if mode == "databricks":
        env = _databricks_env(executor_cfg)
        return ClaudeAuthEnv(env=env, model=model or DATABRICKS_DEFAULT_MODEL)
    raise ClaudeAuthError(
        detail=f"Unknown Claude provider auth mode {mode!r}.",
        hint="claude_auth",
    )


def subscription_credentials_present() -> bool:
    """Best-effort ambient-credential heuristic for ``/status`` display.

    True when the Claude Code credential file exists or a
    ``CLAUDE_CODE_OAUTH_TOKEN`` is already in the server env. macOS keychain
    credentials are invisible to this check, so a False NEVER gates anything
    — the turn itself is the authoritative probe.
    """
    if os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"):
        return True
    return (Path.home() / _CLAUDE_CREDENTIALS).is_file()


def sdk_available() -> bool:
    """True when the SDK's bundled ``claude`` binary is present (display only)."""
    try:
        import claude_agent_sdk
    except ImportError:
        return False
    bundled = Path(claude_agent_sdk.__file__).parent / "_bundled"
    # The binary is `claude` in POSIX wheels, `claude.exe` on Windows.
    return bundled.is_dir() and any(bundled.glob("claude*"))
