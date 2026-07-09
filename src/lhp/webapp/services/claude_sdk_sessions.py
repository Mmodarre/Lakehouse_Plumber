"""Ensure a current Claude SDK assistant session (store-side provisioning).

The Claude provider has no daemon to provision against: a "session" is an
LHP-minted id (``claude_<uuid>``) rowed in ``assistant_sessions`` plus the
SDK's own resume handle (``runtime_session_id``, refreshed per turn by the
engine). "Ensuring" therefore means the same drift posture as the Omnigent
path (:mod:`~lhp.webapp.services.assistant_provision`), minus the liveness
probe — the SDK spawns per turn, so there is no daemon-side session to die.

Drift is captured by :func:`assistant_provision.bundle_hash` over a canonical
config of everything that shapes a turn — provider, auth mode, model,
profile / host / ``oauth_token_env`` NAMES (never values), system prompt,
cwd — plus the installed skill version. A mismatch marks the active session
stale, force-installs the project skill (same R4 posture: unconditional on
every (re)provision), and mints a fresh session id.

:stability: internal
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from pathlib import Path
from typing import Any, Optional

from lhp.api import SkillFacade
from lhp.webapp.services import assistant_store
from lhp.webapp.services.assistant_provision import (
    ASSISTANT_PROMPT,
    bundle_hash,
    installed_skill_version,
)

logger = logging.getLogger(__name__)


def claude_bundle_config(
    executor_cfg: dict[str, Any], project_root: Path
) -> dict[str, Any]:
    """Canonical drift-detection config for the Claude provider.

    Only turn-shaping NAMES go in — never key material (the same rule the
    store enforces): ``oauth_token_env`` is an env-var name, ``profile`` a
    ``~/.databrickscfg`` section name.
    """
    return {
        "provider": "claude_sdk",
        "mode": executor_cfg.get("mode", "claude_subscription"),
        "model": executor_cfg.get("model") or None,
        "profile": executor_cfg.get("profile") or None,
        "host": executor_cfg.get("host") or None,
        "oauth_token_env": executor_cfg.get("oauth_token_env") or None,
        "system_prompt": ASSISTANT_PROMPT,
        "cwd": str(project_root.resolve()),
    }


def _install_skill(project_root: Path) -> str:
    """Force-install the packaged LHP skill; return the installed version."""
    result = SkillFacade(project_root).install_project_skill(force=True)
    return result.skill_version


def _hash_matches(
    row: Optional[dict[str, Any]],
    config: dict[str, Any],
    installed_version: Optional[str],
) -> bool:
    """True when ``row`` is a Claude row whose bundle hash is still current."""
    return (
        row is not None
        and row.get("provider") == "claude_sdk"
        and installed_version is not None
        and row["agent_bundle_hash"] == bundle_hash(config, installed_version)
    )


async def _mint_fresh(
    project_root: Path, config: dict[str, Any]
) -> tuple[str, bool, Optional[str]]:
    """Force-refresh the skill, mint a fresh active session, return its id.

    The title is left ``NULL`` (the placeholder): the first user message
    claims it via :func:`assistant_store.set_title_if_default`.
    """
    skill_version = await asyncio.to_thread(_install_skill, project_root)
    new_hash = bundle_hash(config, skill_version)
    session_id = f"claude_{uuid.uuid4().hex}"
    await asyncio.to_thread(
        assistant_store.insert_claude_session, project_root, session_id, new_hash
    )
    logger.info(f"Provisioned assistant session {session_id} (skill v{skill_version})")
    return session_id, True, None


async def ensure_claude_session(
    project_root: Path,
    executor_cfg: dict[str, Any],
    session_id: Optional[str] = None,
) -> tuple[str, bool, Optional[str]]:
    """Return ``(session_id, created, resume_handle)`` for a current session.

    Explicit ``session_id`` (multi-tab / historical resume): the row is
    reused iff it is a ``claude_sdk`` row in ``active`` / ``archived``
    status whose ``agent_bundle_hash`` matches the current config +
    installed skill version; an archived match is reopened (tab restored
    from history). On hash drift the row is marked stale and a fresh session
    is minted; an unknown id also mints fresh (this is how a draft tab's
    first message creates its session).

    ``session_id=None`` (legacy clients / omnigent parity): the MRU active
    row is the candidate — reused on hash match, otherwise marked stale and
    replaced, exactly as before multi-tab.

    The reuse path returns ``created=False`` and the stored SDK resume
    handle (``None`` when no turn has completed yet); a fresh mint returns
    ``created=True`` with no resume handle.

    Store calls are synchronous and bridged via ``asyncio.to_thread``.

    :raises lhp.errors.LHPError: ``LHP-CFG-011`` when ``project_root`` is
        not an LHP project (raised by the skill install step).
    """
    config = claude_bundle_config(executor_cfg, project_root)
    installed_version = await asyncio.to_thread(installed_skill_version, project_root)

    if session_id is not None:
        row = await asyncio.to_thread(
            assistant_store.get_session, project_root, session_id
        )
        if _hash_matches(row, config, installed_version) and row is not None:
            status = str(row["status"])
            if status == "archived":
                await asyncio.to_thread(
                    assistant_store.reopen_session, project_root, session_id
                )
            if status in ("active", "archived"):
                logger.debug(f"Reusing assistant session {session_id}")
                resume = row.get("runtime_session_id")
                return session_id, False, str(resume) if resume else None
        if row is not None and row.get("status") != "stale":
            await asyncio.to_thread(
                assistant_store.mark_stale, project_root, str(row["session_id"])
            )
        return await _mint_fresh(project_root, config)

    active = await asyncio.to_thread(assistant_store.get_active_session, project_root)
    if _hash_matches(active, config, installed_version) and active is not None:
        reused_id = str(active["session_id"])
        logger.debug(f"Reusing assistant session {reused_id}")
        resume = active.get("runtime_session_id")
        return reused_id, False, str(resume) if resume else None

    if active is not None:
        await asyncio.to_thread(
            assistant_store.mark_stale, project_root, active["session_id"]
        )
    return await _mint_fresh(project_root, config)


def snapshot_items(project_root: Path, session_id: str) -> list[dict[str, Any]]:
    """The session transcript's envelopes for ``GET /session`` rehydration."""
    return assistant_store.list_items(project_root, session_id)
