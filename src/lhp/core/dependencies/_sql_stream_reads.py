"""Stream-aware reads of a SQL body: which tables it reads, and how.

Split from :mod:`.sql_extraction` (whose extraction these build on) so that
module stays within its size budget. Both functions parse as
:func:`~.sql_extraction.extract_tables_from_sql` does, with substitution tokens
masked; they are exported from :mod:`lhp.core.dependencies`.
"""

from __future__ import annotations

import logging
import re
from typing import List, Optional, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from .sql_extraction import _dlt_wrapper_call, _function_name, _mask_tokens, _reads

logger = logging.getLogger(__name__)

__all__ = ["sql_reads_stream", "sql_table_reads"]

# A stream read needs the word STREAM: `STREAM(x)` or `STREAM x`. Its absence
# answers sql_reads_stream without parsing.
_STREAM_WORD = re.compile(r"\bstream\b", re.IGNORECASE)


def sql_table_reads(sql: str) -> Optional[List[Tuple[str, bool]]]:
    """Each table read of a SQL body, with whether it reads a stream.

    The names are those :func:`extract_tables_from_sql` returns. The flag is
    ``True`` for the argument of ``STREAM(x)`` / ``STREAM x`` and ``False``
    for a plain reference, so a name read both ways appears twice. ``None``
    when the body does not parse.
    """
    if not sql or not isinstance(sql, str) or not sql.strip():
        return []
    try:
        return sorted(_reads(sql))
    except SqlglotError as e:
        logger.debug(f"sqlglot could not parse SQL body for its reads: {e}")
        return None


def sql_reads_stream(sql: str) -> Optional[bool]:
    """Whether a SQL body reads a stream: ``STREAM(x)`` or ``STREAM x``.

    A ``spark.sql`` view is a streaming view exactly when its SQL makes such
    a read. ``False`` without parsing when the word ``STREAM`` does not occur
    in any case. Otherwise the body is parsed as :func:`extract_tables_from_sql`
    parses it (substitution tokens masked) and the answer is whether any
    statement holds a :class:`sqlglot.exp.Stream` read; ``None`` when the body
    does not parse.
    """
    if not sql or not isinstance(sql, str) or not _STREAM_WORD.search(sql):
        return False
    masked_sql, _replacements, _dropped = _mask_tokens(sql)
    try:
        statements = sqlglot.parse(masked_sql, read="databricks")
    except SqlglotError as e:
        logger.debug(f"sqlglot could not parse SQL body for a STREAM read: {e}")
        return None
    for statement in statements:
        if statement is None:
            continue
        # The Stream node is the shape on the pinned sqlglot floor; a
        # `stream(...)` table function is the fallback shape
        # _dlt_wrapper_call also guards. Subqueries and CTEs are searched too.
        if next(statement.find_all(exp.Stream), None) is not None:
            return True
        wrappers = map(_dlt_wrapper_call, statement.find_all(exp.Table))
        if any(
            wrapper is not None and _function_name(wrapper).lower() == "stream"
            for wrapper in wrappers
        ):
            return True
    return False
