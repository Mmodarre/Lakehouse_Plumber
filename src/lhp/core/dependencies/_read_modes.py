"""How each action reads its source, and the kind of view it produces.

A reader must read a view the way it was made: a streaming view only through
``spark.readStream``, a batch view only through ``spark.read``.

:func:`effective_read_mode` is the read an action's generated code makes on
its source. It is the read the action's generator declares
(``ActionRegistry.consumer_read_mode``), except:

- a SQL load or SQL transform reads through its SQL body: a body with no
  ``STREAM`` reads every reference as a batch; a body with one reads only the
  ``STREAM`` arguments as streams, so it has no single read;
- a data_quality action in ``dqe`` mode with no ``readMode`` reads the way the
  view it reads was made, when that view is produced in the same flowgroup;
- a streaming-table flow with no ``readMode`` that code generation merges into
  a combined write reads with the table creator's ``readMode``.

:func:`producer_kind` is the kind of the view an action produces: the kind of
its read, except:

- a SQL load or SQL transform is a ``spark.sql`` view, streaming exactly when
  its SQL reads a stream (``STREAM(x)`` or ``STREAM x``);
- a temp table, a write and a test produce no view, so their kind is
  ``"unknown"`` (a temp table is a table, not a view; a check over view reads
  skips it).

Validators and code generation import this module lazily, inside functions:
``lhp.core.dependencies`` imports a service that imports the validators, so a
module-level import from them would close a cycle (see
``core/validators/config_validator.py``). Generators never import it; they
read their own declarations, and code generation passes a data_quality
generator its effective read mode in the generation context.
"""

from __future__ import annotations

import copy
import logging
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, FrozenSet, Literal, Optional, Sequence

from lhp.models import Action, ActionType

from ...errors import LHPError
from ..loaders import load_sql_file
from ..registry import ActionRegistry, ReadMode, determine_action_subtype
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


@dataclass(frozen=True)
class _Scope:
    """What resolving one action's read or view kind may consult."""

    actions: Sequence[Action]
    registry: ActionRegistry
    project_root: Optional[Path]
    substitution_mgr: Optional[EnhancedSubstitutionManager]
    parse_cache: Optional[ParseCache]
    # ids of the data_quality actions whose input is being resolved, so a
    # cycle of data_quality views ends instead of recursing.
    visiting: FrozenSet[int] = frozenset()


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

    ``flowgroup_actions`` are the actions of ``action``'s flowgroup: a
    data_quality view takes the kind of its read, which can follow the view
    it reads (see :func:`effective_read_mode`).

    A SQL action's body is read as its generator reads it: inline ``sql``, or
    the ``sql_path`` file under ``project_root``, substituted through
    ``substitution_mgr`` when one is given (without recording its secret
    references). SQL that cannot be read, or that holds the literal
    ``$source`` placeholder, is ``"unknown"``. ``parse_cache`` memoizes the
    STREAM probe across calls.

    ``registry`` defaults to a new :class:`ActionRegistry`. An action whose
    type has no registered generator is ``"unknown"``.
    """
    return _kind(
        action,
        _Scope(
            flowgroup_actions,
            registry or ActionRegistry(),
            project_root,
            substitution_mgr,
            parse_cache,
        ),
    )


def effective_read_mode(
    action: Action,
    flowgroup_actions: Sequence[Action],
    *,
    registry: Optional[ActionRegistry] = None,
    project_root: Optional[Path] = None,
    substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
    parse_cache: Optional[ParseCache] = None,
) -> Optional[ReadMode]:
    """The read ``action``'s generated code makes on its source.

    ``"stream"`` is ``spark.readStream``, ``"batch"`` is ``spark.read``, and
    ``None`` means no single certain read: a SQL body that reads a
    ``STREAM`` or cannot be read, a Python load, a ``snapshot_cdc`` write, a
    ``once`` CDC flow, an unrecognised ``readMode`` value, or an action type
    with no registered generator.

    - A data_quality action in ``dqe`` mode with an explicit ``readMode``
      reads that mode. With none, it reads the way its source view was made
      when one action of ``flowgroup_actions`` produces that view and its
      kind (:func:`producer_kind`) is known, and a stream otherwise (an
      external table, a Python load, an unreadable SQL body). In
      ``quarantine`` mode it always reads a stream.
    - A SQL load or SQL transform reads ``"batch"`` when its body reads no
      ``STREAM``, and ``None`` when it does: ``STREAM(x)`` reads ``x`` as a
      stream and every other reference as a batch.
    - A write action with no ``readMode`` reads with the ``readMode`` of its
      table creator in ``flowgroup_actions``: code generation combines the
      writes of one table into a write built from the one that creates the
      table (else the first of them). Its generator then declares the read.
    - Every other action reads what its generator declares
      (``ActionRegistry.consumer_read_mode``).

    The keyword arguments are those of :func:`producer_kind`, used for the SQL
    bodies this reads. Code generation, the read-mode lineage check and the
    data_quality to materialized-view fusion all take an action's read from
    here.
    """
    return _read(
        action,
        _Scope(
            flowgroup_actions,
            registry or ActionRegistry(),
            project_root,
            substitution_mgr,
            parse_cache,
        ),
    )


def _kind(action: Action, scope: _Scope) -> ProducerKind:
    if action.type in (ActionType.WRITE, ActionType.TEST):
        return "unknown"
    sub_type = _sub_type(action)
    if sub_type is None:
        return "unknown"
    if sub_type == "sql":
        return _sql_kind(
            action, scope.project_root, scope.substitution_mgr, scope.parse_cache
        )
    if action.type == ActionType.TRANSFORM and sub_type == "temp_table":
        return "unknown"
    return _read(action, scope) or "unknown"


def _read(action: Action, scope: _Scope) -> Optional[ReadMode]:
    sub_type = _sub_type(action)
    if sub_type is None:
        return None
    if action.type in (ActionType.LOAD, ActionType.TRANSFORM) and sub_type == "sql":
        kind = _sql_kind(
            action, scope.project_root, scope.substitution_mgr, scope.parse_cache
        )
        return "batch" if kind == "batch" else None
    if action.type == ActionType.TRANSFORM and sub_type == "data_quality":
        return _data_quality_read(action, scope)
    if action.type == ActionType.WRITE:
        return _declared_read(_as_combined_flow(action, scope.actions), scope)
    return _declared_read(action, scope)


def _sub_type(action: Action) -> Optional[str]:
    try:
        return determine_action_subtype(action)
    except LHPError:
        logger.debug(f"No sub-type for action '{action.name}': read unknown")
        return None


def _declared_read(action: Action, scope: _Scope) -> Optional[ReadMode]:
    try:
        return scope.registry.consumer_read_mode(action)
    except LHPError:
        logger.debug(f"No generator for action '{action.name}': read unknown")
        return None


def _data_quality_read(action: Action, scope: _Scope) -> Optional[ReadMode]:
    """A ``dqe`` action with no ``readMode`` reads its source view's kind.

    The generator declares the rest: an explicit ``readMode``, ``stream`` when
    the source's kind is not known, and ``stream`` in ``quarantine`` mode,
    whose dead-letter recycling is a streaming flow.
    """
    if action.mode != "quarantine" and not action.readMode:
        inherited = _source_view_kind(action, scope)
        if inherited != "unknown":
            return inherited
    return _declared_read(action, scope)


def _source_view_kind(action: Action, scope: _Scope) -> ProducerKind:
    """The kind of the in-flowgroup view a data_quality action reads.

    The view is the one the data_quality generator reads (``source``, or a
    ``source`` mapping's ``view`` / ``source`` key). ``"unknown"`` when no
    single load or transform of the flowgroup produces it, so an external
    table keeps the generator's default, and when the producer is already
    being resolved (a cycle of views).
    """
    source = action.source
    view = (
        source.get("view", source.get("source")) if isinstance(source, dict) else source
    )
    if not isinstance(view, str) or not view:
        return "unknown"
    producers = [
        a
        for a in scope.actions
        if a.target == view and a.type in (ActionType.LOAD, ActionType.TRANSFORM)
    ]
    visiting = scope.visiting | {id(action)}
    if len(producers) != 1 or id(producers[0]) in visiting:
        return "unknown"
    return _kind(producers[0], replace(scope, visiting=visiting))


def _as_combined_flow(action: Action, actions: Sequence[Action]) -> Action:
    """``action`` with the ``readMode`` its flow reads with in a combined write.

    Code generation renders the write actions of a flowgroup that target one
    table as one write built from the table creator: the first that creates
    the table, else the first of them. A flow with no ``readMode`` of its own
    reads with the creator's (``core/codegen/grouping.py`` and the
    ``streaming_table.py.j2`` flow loop). Code generation takes "the first"
    in dependency order; this takes it in ``actions`` order, which differs
    only when the creator is in another flowgroup.
    """
    if action.readMode:
        return action
    from ..validators import action_creates_table  # lazy, as in codegen/grouping.py

    key = _write_table_key(action)
    group = [
        a for a in actions if a.type == ActionType.WRITE and _write_table_key(a) == key
    ]
    creator = next((a for a in group if action_creates_table(a)), None)
    if creator is None and group:
        creator = group[0]
    if creator is None or not creator.readMode:
        return action
    return action.model_copy(update={"readMode": creator.readMode})


def _write_table_key(action: Action) -> str:
    """The table a write action's flow is grouped by in code generation.

    Mirrors ``WriteActionGrouper.group_write_actions_by_target``.
    """
    target: Any = action.write_target
    if not target:
        target = action.source if isinstance(action.source, dict) else {}
    config: Dict[str, Any] = target if isinstance(target, dict) else {}
    catalog = config.get("catalog", "")
    schema = config.get("schema", "")
    table = config.get("table") or config.get("name", "")
    if catalog and schema and table:
        return f"{catalog}.{schema}.{table}"
    return table or action.name


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
