"""The kind of view each action produces: streaming, batch or unknown.

A reader must read a view the way it was made: a streaming view only through
``spark.readStream``, a batch view only through ``spark.read``. An action's
view takes the kind of the read its generated code makes, which the action's
generator declares (``ActionRegistry.consumer_read_mode``), except:

- a SQL load or SQL transform is a ``spark.sql`` view, streaming exactly when
  its SQL reads a stream (``STREAM(x)`` or ``STREAM x``);
- a temp table, a write and a test produce no view, so their kind is
  ``"unknown"`` (a temp table is a table, not a view; a check over view reads
  skips it).

Validators and code generation import this module lazily, inside functions:
``lhp.core.dependencies`` imports a service that imports the validators, so a
module-level import from them would close a cycle (see
``core/validators/config_validator.py``). Generators never import it; they
read their own declarations.
"""

from __future__ import annotations

import copy
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional, Sequence

from lhp.models import Action, ActionType

from ...errors import LHPError
from ..loaders import load_sql_file
from ..registry import ActionRegistry, determine_action_subtype
from .sql_extraction import sql_reads_stream

if TYPE_CHECKING:
    from ..processing import EnhancedSubstitutionManager
    from ._parse_cache import ParseCache

logger = logging.getLogger(__name__)

#: The kind of view an action produces. ``"unknown"`` covers an action that
#: produces no view as well as one whose kind is not certain.
ProducerKind = Literal["stream", "batch", "unknown"]

# The documented `$source` placeholder of SQL transforms is never replaced in
# generated SQL, so the read it stands for has no known kind.
_SOURCE_PLACEHOLDER = re.compile(r"\$source\b")


def producer_kind(
    action: Action,
    flowgroup_actions: Sequence[Action],
    *,
    registry: Optional[ActionRegistry] = None,
    project_root: Optional[Path] = None,
    substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
    parse_cache: Optional[ParseCache] = None,
) -> ProducerKind:
    """The kind of the view ``action`` produces.

    ``flowgroup_actions`` are the actions of ``action``'s flowgroup, for a
    producer whose kind follows its own input; none does yet.

    A SQL action's body is read as its generator reads it: inline ``sql``, or
    the ``sql_path`` file under ``project_root``, substituted through
    ``substitution_mgr`` when one is given (without recording its secret
    references). SQL that cannot be read, or that holds the literal
    ``$source`` placeholder, is ``"unknown"``. ``parse_cache`` memoizes the
    STREAM probe across calls.

    ``registry`` defaults to a new :class:`ActionRegistry`. An action whose
    type has no registered generator is ``"unknown"``.
    """
    if action.type in (ActionType.WRITE, ActionType.TEST):
        return "unknown"
    try:
        sub_type = determine_action_subtype(action)
    except LHPError:
        logger.debug(f"No sub-type for action '{action.name}': kind unknown")
        return "unknown"
    if sub_type == "sql":
        return _sql_kind(action, project_root, substitution_mgr, parse_cache)
    if action.type == ActionType.TRANSFORM and sub_type == "temp_table":
        return "unknown"
    try:
        read = (registry or ActionRegistry()).consumer_read_mode(action)
    except LHPError:
        logger.debug(f"No generator for action '{action.name}': kind unknown")
        return "unknown"
    return read or "unknown"


def _sql_kind(
    action: Action,
    project_root: Optional[Path],
    substitution_mgr: Optional[EnhancedSubstitutionManager],
    parse_cache: Optional[ParseCache],
) -> ProducerKind:
    text = _sql_text(action, project_root, substitution_mgr)
    if text is None or _SOURCE_PLACEHOLDER.search(text):
        return "unknown"
    if parse_cache is not None:
        reads_stream = parse_cache.sql_reads_stream(text)
    else:
        reads_stream = sql_reads_stream(text)
    if reads_stream is None:
        return "unknown"
    return "stream" if reads_stream else "batch"


def _sql_text(
    action: Action,
    project_root: Optional[Path],
    substitution_mgr: Optional[EnhancedSubstitutionManager],
) -> Optional[str]:
    """The SQL a SQL load or transform renders, or ``None`` if it cannot be read.

    Mirrors ``generators/load/sql.py`` and ``generators/transform/sql.py``: a
    load's string source is its SQL as written, its ``source.sql`` key wins
    over ``source.sql_path``; a transform's non-empty ``sql`` (stripped) wins
    over ``sql_path``.
    """
    probe = _without_secret_recording(substitution_mgr)
    if action.type == ActionType.LOAD:
        source = action.source
        if isinstance(source, str):
            return source
        if not isinstance(source, dict):
            return None
        if "sql" in source:
            return _substituted(source["sql"], probe)
        return _file_text(source.get("sql_path"), action, project_root, probe)
    if action.sql:
        return _substituted(action.sql.strip(), probe)
    return _file_text(action.sql_path, action, project_root, probe)


def _substituted(
    text: object, substitution_mgr: Optional[EnhancedSubstitutionManager]
) -> Optional[str]:
    if not isinstance(text, str):
        return None
    if substitution_mgr is None:
        return text
    try:
        return substitution_mgr._process_string(text)
    except LHPError:
        logger.debug("Inline SQL does not substitute: kind unknown")
        return None


def _file_text(
    sql_path: object,
    action: Action,
    project_root: Optional[Path],
    substitution_mgr: Optional[EnhancedSubstitutionManager],
) -> Optional[str]:
    if not isinstance(sql_path, str) or not sql_path or project_root is None:
        return None
    try:
        return load_sql_file(
            sql_path, project_root, substitution_mgr, action=action.name
        )
    except (LHPError, OSError):
        logger.debug(f"sql_path '{sql_path}' of '{action.name}' unreadable")
        return None


def _without_secret_recording(
    substitution_mgr: Optional[EnhancedSubstitutionManager],
) -> Optional[EnhancedSubstitutionManager]:
    """A copy of ``substitution_mgr`` whose secret references are its own.

    Substituting records ``${secret:...}`` references on the manager; code
    generation records them when it renders the same SQL, so classifying the
    SQL must not.
    """
    if substitution_mgr is None:
        return None
    probe = copy.copy(substitution_mgr)
    probe.secret_references = set()
    return probe
