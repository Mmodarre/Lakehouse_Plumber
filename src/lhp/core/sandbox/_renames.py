"""Sandbox table-rename engine: strategy, rename set, and rewrite plan.

The developer-sandbox mode (``--sandbox``) rewrites every read/write of a
table *produced by an in-scope pipeline* to a per-developer name. This module
owns the three value objects and three functions that define that rewrite:

- The rename SET is exactly the producer index built by
  :func:`lhp.core.dependencies.build_producer_indexes` over the IN-SCOPE
  flowgroups: MV/ST ``write_target`` destinations plus delta-sink
  ``options.tableName``. Producer detection is NOT reimplemented here.
- Membership lookups delegate to
  :func:`lhp.core.dependencies.match_produced_table`, so the 2-part<->3-part
  reconciliation rule keeps its single implementation in ``core/dependencies``.
- The pattern itself is applied in exactly ONE place,
  :func:`rename_parts` — the choke point. v1 ships the TABLE strategy only;
  catalog/schema strategies are additive follow-ups that extend that one
  function.

Matching is canonical (lowercased, backtick-stripped — Unity Catalog name
matching is case-insensitive), but rewriting formats the per-site ORIGINAL
spelling of the table leaf, so each rewritten site preserves its author's
casing.
"""

from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Tuple

from lhp.core.dependencies import build_producer_indexes, match_produced_table
from lhp.models import FlowGroup
from lhp.models.processing import SandboxWarningRecord


@dataclass(frozen=True, slots=True)
class TableRenameStrategy:
    """How produced-table names are rewritten for one sandbox run.

    v1 ships the TABLE strategy only: ``table_pattern`` (placeholders
    ``{namespace}`` and ``{table}``, validated upstream by
    :class:`lhp.models.SandboxConfig`) is applied to the table LEAF via
    :func:`rename_parts`. ``kind`` is carried explicitly so future
    catalog/schema strategies are additive — new ``Literal`` members plus a
    new branch in :func:`rename_parts` — rather than a redesign.
    """

    namespace: str
    table_pattern: str
    kind: Literal["table"] = "table"


@dataclass(frozen=True, slots=True)
class SandboxTableRenames:
    """The sandbox rename set: producer indexes over IN-SCOPE flowgroups.

    ``table_producers`` / ``table_short_to_catalogs`` are the two canonical
    indexes from :func:`lhp.core.dependencies.build_producer_indexes` (keys
    lowercased and backtick-stripped). Frozen and picklable — instances cross
    the spawn boundary into pool workers. The dict fields make it unhashable,
    which is fine: workers need pickle, not hash.
    """

    strategy: TableRenameStrategy
    table_producers: Dict[str, List[str]]
    table_short_to_catalogs: Dict[str, Dict[str, List[str]]]


@dataclass(frozen=True, slots=True)
class SandboxRewritePlan:
    """One sandbox run's rewrite decisions: the rename set + carried warnings.

    ``warnings`` are stamped by the scope/rewrite prepass (e.g. the
    mixed-producer sink warning) and merely CARRIED here for re-emission on the
    main thread — this module never stamps warnings itself.
    """

    renames: SandboxTableRenames
    warnings: Tuple[SandboxWarningRecord, ...]


def build_sandbox_table_renames(
    in_scope_flowgroups: List[FlowGroup],
    strategy: TableRenameStrategy,
) -> SandboxTableRenames:
    """Build the rename set for ``in_scope_flowgroups``.

    Wraps :func:`lhp.core.dependencies.build_producer_indexes`, so the rename
    set is — by construction — exactly the tables those flowgroups produce
    (MV/ST write targets + delta-sink ``tableName``), canonicalized through
    the same choke point as dependency analysis.
    """
    table_producers, table_short_to_catalogs = build_producer_indexes(
        in_scope_flowgroups
    )
    # Plain dicts: shed the builder's defaultdict wrappers so the frozen value
    # object cannot grow keys through accidental defaulting lookups.
    return SandboxTableRenames(
        strategy=strategy,
        table_producers=dict(table_producers),
        table_short_to_catalogs=dict(table_short_to_catalogs),
    )


def match_renamed_table(ref: str, renames: SandboxTableRenames) -> Optional[str]:
    """Resolve ``ref`` to its canonical rename-set key, or ``None`` if exempt.

    Thin delegate over :func:`lhp.core.dependencies.match_produced_table`
    against the held indexes — the 2-part<->3-part rule stays in its single
    ``core/dependencies`` implementation. Rewriters call this to decide
    IF a site is in the rename set, then call :func:`rename_parts` on the
    site's ORIGINAL spelling parts to compute the replacement.
    """
    return match_produced_table(
        ref, renames.table_producers, renames.table_short_to_catalogs
    )


def rename_parts(
    strategy: TableRenameStrategy,
    catalog: Optional[str],
    schema: Optional[str],
    table: str,
) -> Tuple[Optional[str], Optional[str], str]:
    """THE single pattern-application choke point.

    v1 (``kind == "table"``) rewrites the table LEAF only — ``table_pattern``
    is formatted with the strategy's namespace and the per-site ORIGINAL
    spelling of ``table`` (matching is case-insensitive, so the author's
    casing survives inside the new name). ``catalog`` and ``schema`` pass
    through unchanged. Future catalog/schema strategies branch HERE and
    nowhere else.
    """
    renamed_leaf = strategy.table_pattern.format(
        namespace=strategy.namespace, table=table
    )
    return catalog, schema, renamed_leaf
