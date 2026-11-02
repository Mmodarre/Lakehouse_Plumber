"""Read-mode lineage validation (``LHP-VAL-070``).

Databricks checks every read of a pipeline view against the view's kind when
it analyzes the pipeline: a streaming view must be read with
``spark.readStream`` (``STREAM(view)`` in SQL), a batch view with
``spark.read`` (a plain reference in SQL). A streaming-table append flow that
reads a batch query must also run ``once``. Any other read fails the update.

This checks one flowgroup: every read of a view that a load or transform of
the same flowgroup produces, where the view's kind (``producer_kind``) and
the read are both certain. The read is ``effective_read_mode``; a SQL load or
SQL transform is checked per table its SQL names (``sql_source_reads``),
since a SQL body that reads a ``STREAM`` has no single read. Reads of
external tables and of views from other flowgroups are not checked.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Set, Tuple

from lhp.models import Action, ActionType, FlowGroup

from ....errors import LHPError
from ...registry import determine_action_subtype
from ._read_mode_lineage_messages import LineageRead, lineage_error

if TYPE_CHECKING:
    from ...registry import ActionRegistry

logger = logging.getLogger(__name__)


class ReadModeLineageValidator:
    """Check each read of an in-flowgroup view against the view's kind."""

    def __init__(
        self,
        registry: Optional[ActionRegistry] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        self.registry = registry
        self.project_root = project_root

    def validate(self, flowgroup: FlowGroup) -> List[LHPError]:
        actions = flowgroup.actions
        producers = _view_producers(actions)
        if not producers:
            return []
        logger.debug(
            f"Checking reads of {len(producers)} view(s) in '{flowgroup.flowgroup}'"
        )
        kinds: Dict[int, str] = {}
        errors: List[LHPError] = []
        for consumer in actions:
            seen: Set[Tuple[int, str]] = set()
            for name, read, sql in self._reads(consumer, actions):
                producer = producers.get(name.casefold())
                if producer is None or producer is consumer:
                    continue
                if (id(producer), read) in seen:
                    continue
                seen.add((id(producer), read))
                if id(producer) not in kinds:
                    kinds[id(producer)] = self._kind(producer, actions)
                kind = kinds[id(producer)]
                # A batch read in a streaming-table append flow must run once.
                once = kind == read == "batch" and not sql and _needs_once(consumer)
                if kind != "unknown" and (kind != read or once):
                    errors.append(
                        self._error(flowgroup, producer, consumer, kind, read, sql)
                    )
        return errors

    def _reads(
        self, consumer: Action, actions: Sequence[Action]
    ) -> List[Tuple[str, str, bool]]:
        """``(view name, read, read in SQL)`` for each certain read."""
        # Lazy: lhp.core.dependencies imports a service that imports validators.
        from ...dependencies import (
            effective_read_mode,
            extract_action_sources,
            sql_source_reads,
        )

        if _is_sql(consumer):
            reads = sql_source_reads(consumer, project_root=self.project_root)
            return [(name, read, True) for name, read in sorted(reads or ())]
        read = effective_read_mode(
            consumer, actions, registry=self.registry, project_root=self.project_root
        )
        if read is None:
            return []
        sources = extract_action_sources(consumer)
        return [(name, read, False) for name in sources if isinstance(name, str)]

    def _kind(self, producer: Action, actions: Sequence[Action]) -> str:
        from ...dependencies import producer_kind  # lazy, as in _reads

        return producer_kind(
            producer, actions, registry=self.registry, project_root=self.project_root
        )

    def _read(self, consumer: Action, actions: Sequence[Action]) -> Optional[str]:
        from ...dependencies import effective_read_mode  # lazy, as in _reads

        return effective_read_mode(
            consumer, actions, registry=self.registry, project_root=self.project_root
        )

    def _error(
        self,
        flowgroup: FlowGroup,
        producer: Action,
        consumer: Action,
        kind: str,
        read: str,
        sql: bool,
    ) -> LHPError:
        """The VAL-070 error, with the ``readMode`` fixes that would work."""
        actions = flowgroup.actions
        wanted = "stream" if kind == read else read
        producer_sql = _is_sql(producer)
        producer_can = not producer_sql and wanted == self._kind(
            producer.model_copy(update={"readMode": wanted}), actions
        )
        consumer_can = not sql and kind == self._read(
            consumer.model_copy(update={"readMode": kind}), actions
        )
        return lineage_error(
            LineageRead(
                flowgroup=flowgroup.flowgroup,
                producer=producer,
                consumer=consumer,
                kind=kind,
                read=read,
                sql=sql,
                producer_sql=producer_sql,
                needs_once=not sql and _needs_once(consumer),
                producer_can=producer_can,
                consumer_can=consumer_can,
            )
        )


def _view_producers(actions: Sequence[Action]) -> Dict[str, Action]:
    """Each view target of a single load or transform, by case-folded name."""
    found: Dict[str, List[Action]] = {}
    for action in actions:
        if action.type in (ActionType.LOAD, ActionType.TRANSFORM) and isinstance(
            action.target, str
        ):
            found.setdefault(action.target.casefold(), []).append(action)
    return {name: group[0] for name, group in found.items() if len(group) == 1}


def _is_sql(action: Action) -> bool:
    if action.type not in (ActionType.LOAD, ActionType.TRANSFORM):
        return False
    try:
        return determine_action_subtype(action) == "sql"
    except LHPError:
        return False


def _needs_once(action: Action) -> bool:
    """A streaming-table append flow that does not run ``once``."""
    if action.type != ActionType.WRITE or action.once:
        return False
    target = action.write_target if isinstance(action.write_target, dict) else {}
    return (
        determine_action_subtype(action) == "streaming_table"
        and target.get("mode", "standard") == "standard"
    )
