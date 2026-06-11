"""Producer-index construction and table-producer matching.

This module owns the *producer side* of action-graph dependency resolution:
turning a flowgroup set into the two canonical producer indexes and resolving a
consumer's read ``source`` against them.

Two producer indexes are built (both keyed on CANONICAL refs — lowercased,
backtick/whitespace-stripped — via :mod:`._canonical`, so a consumer matches a
producer by VALUE rather than surface syntax):

- ``table_producers``: canonical 3-part ``catalog.schema.table`` -> producing
  action ids.
- ``table_short_to_catalogs``: canonical ``schema.table`` suffix ->
  ``{catalog: action_ids}``, which drives the uniqueness check in the
  2-part<->3-part reconciliation rule (see :func:`match_table_producers`).

A WRITE action registers a producer for whichever destination it actually
writes:

- streaming_table / materialized_view -> ``write_target.catalog/schema/table``;
- a ``type: sink`` with ``sink_type: delta`` -> ``options.tableName`` (a 3-part
  ref), because a delta sink stores its destination there, NOT in
  catalog/schema/table. Without this, every table written by a delta sink had
  no registered producer and every downstream read of it was wrongly external.
- kafka / custom / foreachbatch sinks have no table destination -> NO producer
  (they are genuine terminal/external sinks).
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from lhp.models import ActionType, FlowGroup

from ._canonical import canonicalize_table_ref, split_canonical_parts


def build_producer_indexes(
    flowgroups: List[FlowGroup],
) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, List[str]]]]:
    """Build the canonical producer indexes from a flowgroup set.

    CHOKE POINT (i) PRODUCER REGISTRATION: every produced-table key is
    canonicalized before indexing so the consumer-side lookup (choke point
    (ii), :func:`match_table_producers`) compares identical normalized forms.

    Returns:
        A ``(table_producers, table_short_to_catalogs)`` pair (see the module
        docstring for the shape of each index).
    """
    table_producers: Dict[str, List[str]] = defaultdict(list)
    table_short_to_catalogs: Dict[str, Dict[str, List[str]]] = defaultdict(dict)

    for flowgroup in flowgroups:
        for action in flowgroup.actions:
            if action.type != ActionType.WRITE or not action.write_target:
                continue
            action_id = f"{flowgroup.flowgroup}.{action.name}"
            wt = _write_target_as_dict(action.write_target)

            catalog, schema, table = _read_write_target(wt)
            if catalog and schema and table:
                _register_table_key(
                    f"{catalog}.{schema}.{table}",
                    action_id,
                    table_producers,
                    table_short_to_catalogs,
                )

            sink_ref = _delta_sink_table(wt)
            if sink_ref:
                _register_table_key(
                    sink_ref,
                    action_id,
                    table_producers,
                    table_short_to_catalogs,
                )

    return table_producers, table_short_to_catalogs


def _write_target_as_dict(write_target: Any) -> Dict[str, Any]:
    """Normalize a ``write_target`` (Pydantic model or raw dict) to a dict."""
    if isinstance(write_target, dict):
        return write_target
    if hasattr(write_target, "model_dump"):
        return write_target.model_dump()
    return {}


def _read_write_target(wt: Dict[str, Any]) -> Tuple[str, str, str]:
    """Pull (catalog, schema, table) out of a write_target dict.

    Returns empty strings for absent parts. The caller only registers a
    producer when all three parts are present, so this never yields a 2-part
    producer; the empty-catalog (2-part) path is reached solely via a
    delta-sink ``tableName`` (see :func:`_delta_sink_table`).
    """
    return (
        wt.get("catalog") or wt.get("database") or "",
        wt.get("schema") or "",
        wt.get("table") or "",
    )


def _delta_sink_table(wt: Dict[str, Any]) -> str:
    """Return a delta sink's ``options.tableName`` destination, else ``""``.

    Only a ``type: sink`` with ``sink_type: delta`` writing to a table has a
    producible destination; kafka / custom / foreachbatch sinks (and delta
    sinks writing to ``path``) have no ``tableName`` and register no producer.
    """
    if wt.get("type") != "sink" or wt.get("sink_type") != "delta":
        return ""
    options = wt.get("options")
    if not isinstance(options, dict):
        return ""
    table_name = options.get("tableName")
    return table_name if isinstance(table_name, str) and table_name else ""


def _register_table_key(
    ref: str,
    action_id: str,
    table_producers: Dict[str, List[str]],
    table_short_to_catalogs: Dict[str, Dict[str, List[str]]],
) -> None:
    """Register ``action_id`` as a producer of ``ref`` in both indexes.

    ``ref`` is canonicalized here; the ``schema.table`` suffix (the last two
    canonical parts) is indexed under its producing catalog for the
    2-part<->3-part reconciliation. A 2-part ``ref`` registers an empty
    catalog.
    """
    canonical = canonicalize_table_ref(ref)
    table_producers[canonical].append(action_id)

    parts = canonical.split(".")
    if len(parts) >= 2:
        short = ".".join(parts[-2:])
        cat = ".".join(parts[:-2])  # empty for a 2-part ref
        table_short_to_catalogs[short].setdefault(cat, []).append(action_id)


def match_table_producers(
    source: str,
    table_producers: Dict[str, List[str]],
    table_short_to_catalogs: Dict[str, Dict[str, List[str]]],
) -> List[str]:
    """Resolve a read ``source`` to its producing action ids.

    CHOKE POINT (ii) EDGE-MATCH LOOKUP (table path). The caller tries the
    pipeline-scoped view index first and falls back here. Thin delegate: the
    LOCKED 2-part<->3-part reconciliation rule lives in exactly one place,
    :func:`match_produced_table`.
    """
    key = match_produced_table(source, table_producers, table_short_to_catalogs)
    return table_producers.get(key, []) if key is not None else []


def match_produced_table(
    source: str,
    table_producers: Dict[str, List[str]],
    table_short_to_catalogs: Dict[str, Dict[str, List[str]]],
) -> Optional[str]:
    """Resolve a read ``source`` to its canonical producer-index key.

    Returns the ``table_producers`` key that ``source`` matches, or ``None``
    when no producer matches (the read is external).

    LOCKED 2-part<->3-part reconciliation rule (single implementation):

    - A 3-part ``catalog.schema.table`` source matches a 3-part producer by
      exact canonical equality.
    - A 2-part ``schema.table`` source matches a 3-part producer only when
      ``schema`` and ``table`` align AND the producing catalog is UNIQUE among
      registered producers. If MULTIPLE catalogs registered a producer for that
      ``schema.table``, the match is AMBIGUOUS and yields NO edge (treated as
      external). This prevents phantom cross-catalog edges: tables are global,
      unlike pipeline-scoped views.
    - A 3-part source can also match a 2-part producer on the same
      ``schema.table`` + uniqueness guard. In practice 2-part producers are
      rare: a normal write registers a producer only with all three of
      catalog/schema/table present (see :func:`build_producer_indexes`), so the
      only way an empty-catalog (2-part) producer enters the index is a
      delta-sink ``tableName`` that itself omits the catalog.
    """
    parts = split_canonical_parts(source)

    if len(parts) == 3:
        canonical = ".".join(parts)
        if table_producers.get(canonical):
            return canonical
        # 3-part source, but the producer may have been registered 2-part
        # (empty catalog). Reconcile on the schema.table suffix + uniqueness.
        return _unique_short_key(".".join(parts[1:]), table_short_to_catalogs)

    if len(parts) == 2:
        return _unique_short_key(".".join(parts), table_short_to_catalogs)

    return None


def _unique_short_key(
    short: str,
    table_short_to_catalogs: Dict[str, Dict[str, List[str]]],
) -> Optional[str]:
    """Return the producer-index key for ``short`` when a single catalog claims it.

    Multiple distinct catalogs => ambiguous => no match (external). A unique
    EMPTY catalog means the producer itself was registered 2-part, so the
    ``table_producers`` key is ``short`` itself.
    """
    by_catalog = table_short_to_catalogs.get(short)
    if not by_catalog or len(by_catalog) != 1:
        return None
    (catalog,) = by_catalog
    return f"{catalog}.{short}" if catalog else short
