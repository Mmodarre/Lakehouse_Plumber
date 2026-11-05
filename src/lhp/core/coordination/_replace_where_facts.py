"""Per-flowgroup facts for the project-wide REPLACE WHERE stream-read check.

The worker calls :func:`collect_flowgroup_facts` on each RESOLVED flowgroup,
after the ``--sandbox`` structured rewrite, and ships the facts back on its
:class:`~lhp.models.processing.FlowgroupOutcome`; the project stage
(:mod:`._project_cross_checks`) joins them once across every pipeline for
``LHP-VAL-072``.

- Producers: every table a write produces, in any mode, marked when the write
  is a ``mode: replace_where`` streaming table.
- Delta readers: a delta load whose effective read mode
  (:func:`lhp.core.dependencies.effective_read_mode`, with the worker's
  generator registry) is a stream. A change-feed read is not recorded;
  ``skipChangeCommits`` is.
- SQL readers: the ``STREAM(<table>)`` arguments of a SQL load or SQL
  transform, read as its generator renders it
  (:func:`lhp.core.dependencies.rendered_sql`: inline SQL, or the substituted
  ``sql_path`` file), and of a ``custom_sql`` test's SQL, parsed by the shared
  SQL reader (``$source`` names are dropped). A body without the word STREAM
  reads no stream and is not parsed. Only when the body does not parse, its
  STREAM arguments are found lexically
  (:func:`lhp.core.dependencies.scan_stream_reads`): placeholders, 1-part
  names and table-valued functions are not table reads, 2- and 3-part names
  are. A read the scan cannot classify, or a body that cannot be read, is
  recorded with ``table_ref=None``. Under ``--sandbox`` each read is renamed
  as generate's text pass renames the SQL; the structured rewrite has already
  renamed writes and delta loads.

An exception propagates: the worker fails the flowgroup (fail-closed).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

from lhp.core.dependencies import (
    effective_read_mode,
    extract_action_sources,
    is_change_feed_read,
    is_replace_where_write,
    produced_tables,
    rendered_sql,
    scan_stream_reads,
    sql_reads_stream,
    sql_table_reads,
)
from lhp.core.registry import determine_action_subtype
from lhp.core.sandbox import match_renamed_table, rename_parts
from lhp.models import Action, ActionType, FlowGroup, LoadSourceType, TestActionType
from lhp.models.processing import StreamReadFact, TableProducerFact

if TYPE_CHECKING:
    from lhp.core.processing import EnhancedSubstitutionManager
    from lhp.core.registry import ActionRegistry
    from lhp.core.sandbox import SandboxTableRenames

#: A SQL body's stream reads; ``None`` is a read whose table is unknown.
_SqlStreamReads = Tuple[Optional[str], ...]


def collect_flowgroup_facts(
    flowgroup: FlowGroup,
    pipeline: str,
    *,
    registry: ActionRegistry,
    substitution_mgr: Optional[EnhancedSubstitutionManager],
    project_root: Optional[Path],
    table_renames: Optional[SandboxTableRenames],
) -> Tuple[Tuple[TableProducerFact, ...], Tuple[StreamReadFact, ...]]:
    """The tables ``flowgroup`` produces and the tables it reads as a stream.

    ``registry`` must be the worker's populated generator registry: it
    declares each delta load's read mode.
    """
    producers: List[TableProducerFact] = []
    readers: List[StreamReadFact] = []
    actions = flowgroup.actions
    for action in actions:
        if action.type == ActionType.WRITE:
            replace_where = is_replace_where_write(action)
            producers.extend(
                TableProducerFact(
                    pipeline, flowgroup.flowgroup, action.name, table, replace_where
                )
                for table in produced_tables(action)
            )
        elif _is_delta_load(action):
            if _delta_reads_stream(action, actions, registry):
                readers.extend(
                    StreamReadFact(
                        pipeline, flowgroup.flowgroup, action.name, ref, "delta"
                    )
                    for ref in extract_action_sources(action)
                )
        else:
            sql_reads = _sql_stream_reads(action, project_root, substitution_mgr)
            readers.extend(
                StreamReadFact(
                    pipeline,
                    flowgroup.flowgroup,
                    action.name,
                    _sandbox_name(ref, table_renames),
                    "sql",
                )
                for ref in sql_reads
            )
    return tuple(producers), tuple(readers)


def _is_delta_load(action: Action) -> bool:
    return (
        action.type == ActionType.LOAD
        and isinstance(action.source, dict)
        and action.source.get("type") == LoadSourceType.DELTA
    )


def _delta_reads_stream(
    action: Action, actions: Sequence[Action], registry: ActionRegistry
) -> bool:
    """Whether the delta load streams its table's rows (not its change feed)."""
    source = action.source if isinstance(action.source, dict) else {}
    options = source.get("options")
    if isinstance(options, dict) and is_change_feed_read(options):
        return False
    return effective_read_mode(action, actions, registry=registry) == "stream"


def _sql_stream_reads(
    action: Action,
    project_root: Optional[Path],
    substitution_mgr: Optional[EnhancedSubstitutionManager],
) -> _SqlStreamReads:
    if action.type == ActionType.TEST:
        # A custom_sql test renders its resolved ``sql`` verbatim.
        if action.test_type != TestActionType.CUSTOM_SQL or not action.sql:
            return ()
        return _stream_reads_of(action.sql)
    if action.type not in (ActionType.LOAD, ActionType.TRANSFORM):
        return ()
    if determine_action_subtype(action) != "sql":
        return ()
    text = rendered_sql(
        action, project_root=project_root, substitution_mgr=substitution_mgr
    )
    return (None,) if text is None else _stream_reads_of(text)


def _stream_reads_of(sql: str) -> _SqlStreamReads:
    if sql_reads_stream(sql) is False:
        return ()
    reads = sql_table_reads(sql)
    if reads is None:
        return scan_stream_reads(sql)
    return tuple(sorted(name for name, stream in reads if stream))


def _sandbox_name(
    ref: Optional[str], renames: Optional[SandboxTableRenames]
) -> Optional[str]:
    """``ref`` with its table leaf renamed when the sandbox renames the table."""
    if ref is None or renames is None or match_renamed_table(ref, renames) is None:
        return ref
    *head, leaf = ref.split(".")
    renamed = rename_parts(renames.strategy, None, None, leaf.replace("`", "").strip())
    return ".".join([*head, renamed[2]])
