"""The ``LHP-VAL-070`` error for one read of an in-flowgroup view.

Split out of ``ReadModeLineageValidator`` so ``read_mode_lineage.py`` stays
within the validator file-size budget. Stateless: the validator works out
which fixes apply and passes them in as flags.
"""

from dataclasses import dataclass

from lhp.models import Action

from ....errors import ErrorFactory, LHPError, codes

_CALL = {"stream": "spark.readStream", "batch": "spark.read"}
_KIND = {"stream": "streaming", "batch": "batch"}


@dataclass(frozen=True)
class LineageRead:
    """One read of ``producer``'s view by ``consumer`` that Databricks rejects.

    ``kind`` is the view's kind and ``read`` the consumer's read (``stream`` or
    ``batch``); they are equal for a batch view appended by a streaming-table
    flow that is not ``once``. ``sql`` marks a read inside a SQL body.
    ``producer_sql`` marks a SQL producer, whose kind its SQL sets.
    ``needs_once`` marks a streaming-table append flow without ``once``.
    ``producer_can`` / ``consumer_can``: a ``readMode`` on that action fixes
    the read (the producer making the view the kind needed, the consumer
    reading the view's kind).
    """

    flowgroup: str
    producer: Action
    consumer: Action
    kind: str
    read: str
    sql: bool
    producer_sql: bool
    needs_once: bool
    producer_can: bool
    consumer_can: bool


def lineage_error(r: LineageRead) -> LHPError:
    view = str(r.producer.target)
    p, c = f"'{r.producer.name}'", f"'{r.consumer.name}'"
    if r.sql:
        how = f"as STREAM({view})" if r.read == "stream" else "without STREAM()"
    else:
        how = f"with {_CALL[r.read]}"
    if r.kind == r.read:
        title = f"Batch view '{view}' appended to a streaming table without once"
        details = (
            f"Action {c} appends batch view '{view}' (produced by {p}) to a "
            f"streaming table with spark.read in a flow that is not once. "
            f"Databricks rejects that (CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_"
            f"NOT_ALLOWED) after it has created the empty table."
        )
        tips = [f"Set `once: true` on {c} to append '{view}' once, as a backfill"]
        tips += [
            f"Or append it incrementally: {tip[:1].lower()}{tip[1:]}, and "
            f"`readMode: stream` on {c}"
            for tip in _producer_tips(r, "stream", view)
        ]
    else:
        title = f"Read of view '{view}' does not match its kind"
        details = (
            f"Action {c} reads view '{view}' {how}, but action {p} produces it "
            f"as a {_KIND[r.kind]} view. Databricks fails the update: a "
            f"streaming view must be read with spark.readStream (STREAM() in "
            f"SQL), a batch view with spark.read (no STREAM() in SQL)."
        )
        tips = _producer_tips(r, r.read, view)
        if r.read == "batch" and r.needs_once:
            tips = [f"{tip}, and `once: true` on {c}" for tip in tips]
        tips += _consumer_tips(r, view)
    if not tips:
        tips = [f"Write '{view}' to a table and read that table instead"]
    return ErrorFactory.validation_error(
        codes.VAL_070,
        title=title,
        details=details,
        suggestions=tips,
        context={
            "FlowGroup": r.flowgroup,
            "Producer": r.producer.name,
            "Consumer": r.consumer.name,
            "View": view,
            "View kind": r.kind,
            "Read": f"{r.read} ({how})",
        },
    )


def _producer_tips(r: LineageRead, kind: str, view: str) -> list[str]:
    """How the producer makes ``view`` a ``kind`` view, if it can."""
    p = f"'{r.producer.name}'"
    if r.producer_sql:
        if kind == "stream":
            return [f"Read its source as STREAM(...) in the SQL of {p}"]
        return [f"Remove STREAM() from the SQL of {p}"]
    if r.producer_can:
        return [f"Set `readMode: {kind}` on {p} so '{view}' is a {_KIND[kind]} view"]
    return []


def _consumer_tips(r: LineageRead, view: str) -> list[str]:
    """How the consumer reads the view's kind, if it can."""
    c = f"'{r.consumer.name}'"
    if r.sql:
        if r.kind == "stream":
            return [f"Read it as STREAM({view}) in the SQL of {c}"]
        return [f"Read {view} without STREAM() in the SQL of {c}"]
    if not r.consumer_can:
        return []
    if r.kind == "batch" and r.needs_once:
        return [f"Set `readMode: batch` and `once: true` on {c} (a backfill)"]
    return [f"Set `readMode: {r.kind}` on {c}"]
