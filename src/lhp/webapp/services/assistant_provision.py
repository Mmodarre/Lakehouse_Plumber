"""Ensure a live, current omnigent session for the assistant panel.

Omnigent v0.4.0 has NO standalone agent registry (live-spike drift finding):
the multipart ``POST /v1/sessions`` creates the conversation and its
session-scoped agent together in one transaction. "Provisioning" therefore
means ensuring ONE live session whose agent bundle matches the current
desired state — the project-scoped agent config plus the packaged LHP skill
version. Drift in either produces a NEW bundled session; the old one is
marked stale in the local store (omnigent keeps its history server-side).

The desired state is captured by :func:`bundle_hash` over the canonical
agent config (:func:`build_agent_config`) and the installed skill version,
and persisted per session as ``agent_bundle_hash`` in
:mod:`~lhp.webapp.services.assistant_store`. :func:`ensure_session` is the
single entry point: reuse when the stored hash matches and omnigent still
knows the session; otherwise force-refresh the project skill (R4 —
unconditional on every (re)provision) and create session + runner anew.

Auth modes for ``executor_cfg`` (never any key material — the ``api_key``
value is a ``${VAR}`` environment-variable NAME interpolation expanded by
the omnigent server from ITS process env at config-parse time):

* ``omnigent_defaults`` — no ``auth`` block at all (spike-verified valid).
* ``databricks`` — ``auth: {type: databricks, profile: ...}``; the model
  defaults to ``databricks-claude-opus-4-8`` when unset.
* ``api_key_env`` — ``auth: {type: api_key, api_key: "${<VAR>}"}``.

The generated config NEVER contains a ``git`` key anywhere, and the
sandbox is ``{type: none}``: the spike showed ``darwin_seatbelt`` denies
exec of uv-venv interpreters, and the trusted-local-dev shortcut matches
the omnigent examples' posture for a loopback single-user daemon.

:stability: internal
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import tarfile
from pathlib import Path
from typing import Any, Optional

import yaml

from lhp.api import SkillFacade
from lhp.webapp.services import assistant_store
from lhp.webapp.services.omnigent_client import OmnigentClient

logger = logging.getLogger(__name__)

#: Fallback model for the ``databricks`` auth mode when the executor config
#: does not pin one (spike-verified working default).
_DATABRICKS_DEFAULT_MODEL = "databricks-claude-opus-4-8"

#: Short agent prompt; LHP-specific knowledge comes from the installed skill,
#: not from this prompt.
_ASSISTANT_PROMPT = (
    "You are working in a Lakehouse Plumber (LHP) project; prefer the lhp "
    "skill for LHP-specific questions about flowgroups, actions, presets, "
    "templates, substitutions, and code generation."
)

#: Where the skill installer records the installed version. Read here as
#: plain bytes-on-disk file I/O (the webapp's remit); the install itself
#: always goes through :class:`lhp.api.SkillFacade`.
_SKILL_MARKER_RELPATH = Path(".claude") / "skills" / "lhp" / ".lhp_skill_version"


def _sha8(text: str) -> str:
    """First 8 hex chars of the SHA-256 of ``text`` (agent-name uniquifier)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def build_agent_config(
    executor_cfg: dict[str, Any], project_root: Path
) -> dict[str, Any]:
    """Build the omnigent ``config.yaml`` dict for this project's agent.

    ``executor_cfg`` carries ``mode`` (one of the three auth modes in the
    module docstring; missing/unknown modes fall back to
    ``omnigent_defaults``) plus the mode's fields: an optional ``model``,
    ``profile`` for ``databricks``, and ``api_key_env`` for ``api_key_env``
    (both are hard-keyed — a missing required field raises ``KeyError``
    loudly rather than emitting a half-built bundle).
    """
    executor: dict[str, Any] = {
        "type": "omnigent",
        "config": {"harness": "claude-sdk"},
    }
    mode = executor_cfg.get("mode", "omnigent_defaults")
    model: Optional[str] = executor_cfg.get("model") or None
    if mode == "databricks":
        executor["auth"] = {"type": "databricks", "profile": executor_cfg["profile"]}
        if model is None:
            model = _DATABRICKS_DEFAULT_MODEL
    elif mode == "api_key_env":
        # ${VAR} is an env-var NAME interpolation expanded by the omnigent
        # server from ITS process env at parse time — never a key value.
        executor["auth"] = {
            "type": "api_key",
            "api_key": f"${{{executor_cfg['api_key_env']}}}",
        }
    elif mode != "omnigent_defaults":
        logger.debug(f"Unknown executor mode {mode!r}; emitting no auth block")
    if model is not None:
        executor["model"] = model

    resolved_root = project_root.resolve()
    return {
        "spec_version": 1,
        "name": f"lhp-{resolved_root.name}-{_sha8(str(resolved_root))}",
        "prompt": _ASSISTANT_PROMPT,
        "executor": executor,
        "os_env": {
            "type": "caller_process",
            "cwd": str(project_root),
            "sandbox": {"type": "none"},
        },
        "skills": "all",
    }


def bundle_hash(config: dict[str, Any], skill_version: str) -> str:
    """SHA-256 hex over the canonical config JSON plus the skill version.

    Canonical = ``json.dumps(sort_keys=True)`` with compact separators, so
    semantically equal configs hash identically regardless of key order.
    The skill version is appended so a skill upgrade alone re-provisions.
    """
    canonical = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256((canonical + skill_version).encode("utf-8")).hexdigest()


def make_tarball(config: dict[str, Any]) -> bytes:
    """Gzip tarball with exactly one member: ``config.yaml`` at the root."""
    payload = yaml.safe_dump(config, sort_keys=False).encode("utf-8")
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        member = tarfile.TarInfo(name="config.yaml")
        member.size = len(payload)
        tar.addfile(member, io.BytesIO(payload))
    return buffer.getvalue()


def installed_skill_version(project_root: Path) -> Optional[str]:
    """Version in the project's skill marker file, or ``None`` when absent.

    Public within the webapp: the assistant router reads this for the
    ``/assistant/status`` ``skill_installed`` / ``skill_version`` fields and
    as the chat gate's skill check (plain bytes-on-disk marker read — the
    install itself always goes through :class:`lhp.api.SkillFacade`).
    """
    marker = project_root / _SKILL_MARKER_RELPATH
    if not marker.is_file():
        return None
    return marker.read_text(encoding="utf-8").strip() or None


def _install_skill(project_root: Path) -> str:
    """Force-install the packaged LHP skill; return the installed version."""
    result = SkillFacade(project_root).install_project_skill(force=True)
    return result.skill_version


def _session_title(project_root: Path) -> str:
    return f"LHP Assistant ({project_root.name})"


async def ensure_session(
    client: OmnigentClient,
    project_root: Path,
    executor_cfg: dict[str, Any],
    host_id: str,
) -> tuple[str, bool]:
    """Return ``(session_id, created)`` for a live, current assistant session.

    Reuse path: the store has an active session AND its stored
    ``agent_bundle_hash`` matches the hash of the current config + the
    currently installed skill marker version AND omnigent still answers for
    that session — return it with ``created=False``.

    Otherwise (no active session, bundle drift, missing skill marker, or a
    dead session): mark any active session stale, force-refresh the project
    skill via :class:`lhp.api.SkillFacade` (R4 — unconditional, no
    version-compare logic), create a new bundled session, launch its runner
    on ``host_id``, record it as the active session with the new hash, and
    return ``created=True``.

    Store calls are synchronous and bridged via ``asyncio.to_thread`` (the
    store derives its database path from ``project_root``).

    :raises lhp.webapp.services.omnigent_client.OmnigentUnavailable: when
        the daemon cannot be reached.
    :raises httpx.HTTPStatusError: on non-404 HTTP errors from the daemon.
    :raises lhp.errors.LHPError: ``LHP-CFG-011`` when ``project_root`` is
        not an LHP project (raised by the skill install step).
    """
    config = build_agent_config(executor_cfg, project_root)
    active = await asyncio.to_thread(assistant_store.get_active_session, project_root)

    installed_version = await asyncio.to_thread(installed_skill_version, project_root)
    if active is not None and installed_version is not None:
        current_hash = bundle_hash(config, installed_version)
        session_id = str(active["session_id"])
        if active["agent_bundle_hash"] == current_hash and await client.session_alive(
            session_id
        ):
            logger.debug(f"Reusing live assistant session {session_id}")
            return session_id, False

    if active is not None:
        await asyncio.to_thread(
            assistant_store.mark_stale, project_root, active["session_id"]
        )

    skill_version = await asyncio.to_thread(_install_skill, project_root)
    new_hash = bundle_hash(config, skill_version)
    created = await client.create_bundled_session(
        make_tarball(config),
        title=_session_title(project_root),
        workspace=str(project_root),
    )
    session_id = str(created["session_id"])
    await client.launch_runner(host_id, session_id, str(project_root))
    await asyncio.to_thread(
        assistant_store.insert_session,
        project_root,
        session_id,
        str(created.get("agent_id", "")),
        host_id,
        new_hash,
        _session_title(project_root),
    )
    logger.info(
        f"Provisioned assistant session {session_id} on host {host_id} "
        f"(skill v{skill_version})"
    )
    return session_id, True
