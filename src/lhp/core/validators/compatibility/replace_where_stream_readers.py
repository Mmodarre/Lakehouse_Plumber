"""Streaming reads of REPLACE WHERE tables, project-wide (``LHP-VAL-072``).

Each refresh of a ``mode: replace_where`` streaming table rewrites the rows its
predicate selects. A streaming read of such a table cannot follow rewritten
rows: Databricks fails it with ``DELTA_SOURCE_TABLE_IGNORE_CHANGES``, and with
``skipChangeCommits`` it silently drops the rewritten rows. Only a change-feed
read follows them (the worker never records one as a stream read).

The check runs once over the facts every flowgroup's worker recorded
(``TableProducerFact`` / ``StreamReadFact``), so a reader in any pipeline is
checked against every producer. Readers are matched with the shared producer
indexes built from ALL producers, so the 2-part uniqueness rule sees every
catalog; only a match whose producer is a REPLACE WHERE write is reported. A
SQL reader whose stream reads are unknown (``table_ref is None``) is reported
whenever the project has a REPLACE WHERE table, since it may stream from one.

When the run covers only some pipelines (``--pipeline``, a sandbox profile's
scope) the caller passes ``match_two_part=False``: a 2-part reader ref is then
not matched at all, because a ``schema.table`` that looks unique among the
selected producers may be ambiguous across the project. 3-part refs match as
always.

Composed only by ``ValidationService.validate_project_stream_reads`` (§9.24);
the errors form their own ``LHP-VAL-072`` family, keyed by reading pipeline.
"""

import logging
from typing import Dict, List, Sequence, Set, Tuple

from lhp.models.processing import StreamReadFact, TableProducerFact

logger = logging.getLogger(__name__)

_CONSEQUENCE = (
    "Each refresh rewrites rows, which a streaming read cannot follow "
    "(skipChangeCommits silently drops data)."
)


def _action_id(pipeline: str, flowgroup: str, action: str) -> str:
    return f"{pipeline}.{flowgroup}.{action}"


def _stream_read_message(reader: StreamReadFact, producer: str) -> str:
    return (
        f"{_action_id(reader.pipeline, reader.flowgroup, reader.action)} streams "
        f"from '{reader.table_ref}', a REPLACE WHERE table written by {producer}. "
        f"{_CONSEQUENCE} Use readMode: batch, or stream its change feed with "
        f'options.readChangeFeed: "true" (see docs).'
    )


def _unverified_read_message(reader: StreamReadFact, tables: Sequence[str]) -> str:
    return (
        f"{_action_id(reader.pipeline, reader.flowgroup, reader.action)} may "
        f"stream from a REPLACE WHERE table: LHP cannot tell which table a STREAM "
        f"read in its SQL names (the SQL does not parse and the STREAM argument "
        f"is not a plain table name, or the SQL cannot be read), and the project "
        f"writes REPLACE WHERE tables ({', '.join(tables)}). {_CONSEQUENCE} Name "
        f"the streamed table with its full catalog.schema.table, as "
        f"STREAM(catalog.schema.table), or read it through a delta load action "
        f"(see docs)."
    )


class ReplaceWhereStreamReadValidator:
    def validate(
        self,
        producers: Sequence[TableProducerFact],
        readers: Sequence[StreamReadFact],
        *,
        match_two_part: bool = True,
    ) -> Dict[str, List[str]]:
        """Return the VAL-072 messages for ``readers``, keyed by reading pipeline.

        Pipelines without a finding are absent. Messages keep the order of
        ``readers``; one reader naming the same table twice is reported once.
        ``match_two_part=False`` leaves 2-part reader refs unmatched (see the
        module docstring).
        """
        # Lazy: lhp.core.dependencies imports a service that imports the
        # validators (see config_validator.py).
        from ...dependencies import (
            index_produced_tables,
            match_produced_table,
            split_canonical_parts,
        )

        logger.debug(
            f"Checking {len(readers)} stream read(s) against "
            f"{len(producers)} produced table(s) for REPLACE WHERE targets"
        )
        replace_where_ids = {
            _action_id(p.pipeline, p.flowgroup, p.action)
            for p in producers
            if p.is_replace_where
        }
        if not replace_where_ids:
            return {}
        table_producers, table_short_to_catalogs = index_produced_tables(
            (p.table, _action_id(p.pipeline, p.flowgroup, p.action)) for p in producers
        )
        replace_where_tables = sorted(
            {p.table for p in producers if p.is_replace_where}
        )

        errors: Dict[str, List[str]] = {}
        seen: Set[Tuple[str, str, str, str]] = set()
        for reader in readers:
            if reader.table_ref is None:
                message = _unverified_read_message(reader, replace_where_tables)
                errors.setdefault(reader.pipeline, []).append(message)
                continue
            if not match_two_part and len(split_canonical_parts(reader.table_ref)) == 2:
                continue
            key = match_produced_table(
                reader.table_ref, table_producers, table_short_to_catalogs
            )
            if key is None:
                continue
            writers = [w for w in table_producers[key] if w in replace_where_ids]
            reader_key = (reader.pipeline, reader.flowgroup, reader.action, key)
            if not writers or reader_key in seen:
                continue
            seen.add(reader_key)
            message = _stream_read_message(reader, writers[0])
            errors.setdefault(reader.pipeline, []).append(message)

        logger.debug(
            f"Found {sum(map(len, errors.values()))} streaming read(s) of "
            f"REPLACE WHERE tables"
        )
        return errors
