"""The LHP routing block written into a project's ``CLAUDE.md``.

``lhp skill install`` (and ``update``) drop a small, fenced "Lakehouse Plumber
project" block into ``<project>/CLAUDE.md`` alongside the skill files. The
reason is mechanical: a skill ``description`` can only key off the user's query
text — it cannot see that ``lhp.yaml`` is present. So a request with no LHP
vocabulary ("ingest this volume and write to bronze") will not reliably route
to the ``lhp`` skill on description alone. The routing block is project context
loaded into every Claude Code session here, and it makes that in-project intent
stick.

Standard-library I/O only — no Rich, no ``lhp.errors``, no domain imports.
:class:`lhp.api.SkillFacade` (and, over the sanctioned ``cli -> api`` bridge,
the ``lhp skill`` command) decides what to do with the reported status; this
module only edits the file and reports what it did. The block is written only
for project installs, never ``--user`` (a global skill has no single project
``CLAUDE.md`` to own).

:stability: internal
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

CLAUDE_MD = "CLAUDE.md"
ROUTING_START = "<!-- lhp:routing:start -->"
ROUTING_END = "<!-- lhp:routing:end -->"

WriteStatus = Literal["created", "updated", "unchanged"]
RemoveStatus = Literal["removed", "absent"]

# Kept terse on purpose: it is loaded into every Claude Code session opened in
# the project, so it states the rule and stops.
_ROUTING_BODY = """## Lakehouse Plumber project

This directory is a Lakehouse Plumber (LHP) project — see `lhp.yaml`. All data
ingest / transform / write work should be done by authoring LHP YAML flowgroups
under `pipelines/` and running the `lhp` CLI, NOT by hand-writing DLT / notebook
/ PySpark code. Prefer the `lhp` skill for any pipeline, ingestion, flowgroup,
preset, template, blueprint, or substitution task in this project."""


def routing_block() -> str:
    """The fenced routing block, delimiting markers included.

    :stability: provisional
    """
    return f"{ROUTING_START}\n{_ROUTING_BODY}\n{ROUTING_END}"


def write_routing_block(project_dir: Path) -> WriteStatus:
    """Create or refresh the LHP routing block in ``<project_dir>/CLAUDE.md``.

    Idempotent and non-destructive to the rest of the file:

    - no ``CLAUDE.md`` → create it holding just the block (``"created"``);
    - existing file with the block → replace it in place, leaving everything
      around it untouched (``"updated"``, or ``"unchanged"`` if already
      current);
    - existing file without the block → append the block (``"updated"``).

    :stability: provisional
    """
    path = project_dir / CLAUDE_MD
    block = routing_block()

    if not path.exists():
        path.write_text(block + "\n", encoding="utf-8")
        logger.debug(f"Created {path} with LHP routing block")
        return "created"

    text = path.read_text(encoding="utf-8")
    start = text.find(ROUTING_START)
    end = text.find(ROUTING_END)
    if start != -1 and end > start:
        end += len(ROUTING_END)
        if text[start:end] == block:
            logger.debug(f"LHP routing block in {path} already current")
            return "unchanged"
        path.write_text(text[:start] + block + text[end:], encoding="utf-8")
        logger.debug(f"Replaced LHP routing block in {path}")
        return "updated"

    separator = "\n" if text.endswith("\n") else "\n\n"
    path.write_text(text + separator + block + "\n", encoding="utf-8")
    logger.debug(f"Appended LHP routing block to {path}")
    return "updated"


def remove_routing_block(project_dir: Path) -> RemoveStatus:
    """Strip the LHP routing block from ``<project_dir>/CLAUDE.md``.

    Removes only the fenced block; any surrounding content the user wrote is
    preserved. If removing the block empties the file (LHP created it), the
    file is deleted so ``uninstall`` leaves no trace. Returns ``"absent"`` when
    there is no file or no block to remove.

    :stability: provisional
    """
    path = project_dir / CLAUDE_MD
    if not path.exists():
        return "absent"

    text = path.read_text(encoding="utf-8")
    start = text.find(ROUTING_START)
    end = text.find(ROUTING_END)
    if start == -1 or end <= start:
        return "absent"
    end += len(ROUTING_END)

    remainder = (text[:start] + text[end:]).strip()
    if remainder:
        path.write_text(remainder + "\n", encoding="utf-8")
        logger.debug(f"Removed LHP routing block from {path}")
    else:
        path.unlink()
        logger.debug(f"Removed LHP routing block and deleted now-empty {path}")
    return "removed"
