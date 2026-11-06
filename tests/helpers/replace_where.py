"""Inject a cross-pipeline streaming read of a REPLACE WHERE table (#346).

:func:`inject_replace_where_stream_reader` writes two pipelines into a copy of
the e2e fixture project: one writes a ``mode: replace_where`` table, the other
streams from it with ``skipChangeCommits``. ``lhp validate`` and ``lhp generate``
must reject that read with LHP-VAL-072, whichever pipeline it lives in.

:func:`inject_two_part_stream_readers` writes a pipeline that streams from its
own REPLACE WHERE table by a 2-part and a 3-part name, and another pipeline
that writes the same ``schema.table`` in a different catalog: the 2-part name
is ambiguous project-wide, and must stay unmatched when ``-p`` hides the other
pipeline.

:func:`inject_nondeterministic_replace_where` writes the real LHP-VAL-073
producer: a pipeline whose batch load computes ``_processing_timestamp``
upstream of a ``mode: replace_where`` write. The spawn workers record the
warning, so CLI and API tests see it exactly as a user would.
"""

from __future__ import annotations

from pathlib import Path

RW_PRODUCER_PIPELINE = "rw_stream_silver"
RW_READER_PIPELINE = "rw_stream_gold"

#: The VAL-072 message the reader gets (the dev env resolves the tokens).
RW_READER_MESSAGE = (
    "rw_stream_gold.orders_rw_reader.load_orders_recent_stream streams from "
    "'acme_edw_dev.edw_silver.orders_recent_rw', a REPLACE WHERE table written "
    "by rw_stream_silver.orders_rw.write_orders_recent."
)

_PRODUCER_YAML = """\
pipeline: rw_stream_silver
flowgroup: orders_rw

actions:
  - name: load_orders_batch
    type: load
    readMode: batch
    source:
      type: delta
      catalog: "${catalog}"
      schema: "${bronze_schema}"
      table: customer
    target: v_orders_source

  - name: write_orders_recent
    type: write
    source: v_orders_source
    write_target:
      type: streaming_table
      mode: replace_where
      catalog: "${catalog}"
      schema: "${silver_schema}"
      table: orders_recent_rw
      replace_where_config:
        replace_where: "order_date >= date_add(current_date(), -7)"
"""

_READER_YAML = """\
pipeline: rw_stream_gold
flowgroup: orders_rw_reader

actions:
  - name: load_orders_recent_stream
    type: load
    readMode: stream
    source:
      type: delta
      catalog: "${catalog}"
      schema: "${silver_schema}"
      table: orders_recent_rw
      options:
        skipChangeCommits: "true"
    target: v_orders_recent

  - name: write_orders_recent_copy
    type: write
    source: v_orders_recent
    write_target:
      type: streaming_table
      catalog: "${catalog}"
      schema: "${gold_schema}"
      table: orders_recent_copy
"""


def inject_replace_where_stream_reader(project_root: Path) -> None:
    """Write the producer and reader pipelines under ``project_root/pipelines``."""
    for directory, name, text in (
        ("97_rw_stream_silver", "orders_rw.yaml", _PRODUCER_YAML),
        ("97_rw_stream_gold", "orders_rw_reader.yaml", _READER_YAML),
    ):
        path = project_root / "pipelines" / directory / name
        path.parent.mkdir(parents=True)
        path.write_text(text, encoding="utf-8")


TWO_PART_PIPELINE = "rw_2p_gold"
OTHER_CATALOG_PIPELINE_FILE = Path("pipelines/96_rw_2p_other/other_catalog.yaml")

_TWO_PART_READERS_YAML = """\
pipeline: rw_2p_gold
flowgroup: orders_2p

actions:
  - name: load_orders_batch
    type: load
    readMode: batch
    source:
      type: delta
      catalog: "${catalog}"
      schema: "${bronze_schema}"
      table: customer
    target: v_orders_source

  - name: write_orders_rw
    type: write
    source: v_orders_source
    write_target:
      type: streaming_table
      mode: replace_where
      catalog: "${catalog}"
      schema: "${silver_schema}"
      table: orders_rw_2p
      replace_where_config:
        replace_where: "order_date >= date_add(current_date(), -7)"

  - name: stream_two_part
    type: transform
    transform_type: sql
    source: v_orders_source
    sql: "SELECT * FROM STREAM(${silver_schema}.orders_rw_2p)"
    target: v_two_part

  - name: write_two_part
    type: write
    source: v_two_part
    write_target:
      type: streaming_table
      catalog: "${catalog}"
      schema: "${gold_schema}"
      table: orders_two_part_copy

  - name: stream_three_part
    type: transform
    transform_type: sql
    source: v_orders_source
    sql: "SELECT * FROM STREAM(${catalog}.${silver_schema}.orders_rw_2p)"
    target: v_three_part

  - name: write_three_part
    type: write
    source: v_three_part
    write_target:
      type: streaming_table
      catalog: "${catalog}"
      schema: "${gold_schema}"
      table: orders_three_part_copy
"""

_OTHER_CATALOG_YAML = """\
pipeline: rw_2p_other
flowgroup: other_catalog

actions:
  - name: load_other_batch
    type: load
    readMode: batch
    source:
      type: delta
      catalog: "${catalog}"
      schema: "${bronze_schema}"
      table: customer
    target: v_other_source

  - name: write_other
    type: write
    source: v_other_source
    write_target:
      type: materialized_view
      catalog: other_catalog
      schema: "${silver_schema}"
      table: orders_rw_2p
"""


def inject_two_part_stream_readers(project_root: Path) -> None:
    """Write the 2-part/3-part reader pipeline and the other-catalog producer."""
    for path, text in (
        (Path("pipelines/96_rw_2p_gold/orders_2p.yaml"), _TWO_PART_READERS_YAML),
        (OTHER_CATALOG_PIPELINE_FILE, _OTHER_CATALOG_YAML),
    ):
        target = project_root / path
        target.parent.mkdir(parents=True)
        target.write_text(text, encoding="utf-8")


NONDETERMINISTIC_PIPELINE = "rw_metadata_warn"
NONDETERMINISTIC_FLOWGROUP = "orders_rw_metadata"
NONDETERMINISTIC_FILE = Path("pipelines/95_rw_metadata_warn/orders_rw_metadata.yaml")

#: What the VAL-073 message lists for the injected flowgroup.
NONDETERMINISTIC_FINDING = (
    "write 'write_orders_recent' <- action 'load_orders_batch': "
    "column '_processing_timestamp'"
)

# ``{metadata}`` is the load's ``operational_metadata`` selection. The fixture's
# lhp.yaml defines ``_processing_timestamp`` as ``F.current_timestamp()``.
_NONDETERMINISTIC_YAML = """\
pipeline: rw_metadata_warn
flowgroup: orders_rw_metadata

actions:
  - name: load_orders_batch
    type: load
    readMode: batch
    operational_metadata: {metadata}
    source:
      type: delta
      catalog: "${{catalog}}"
      schema: "${{bronze_schema}}"
      table: customer
    target: v_orders_source

  - name: write_orders_recent
    type: write
    source: v_orders_source
    write_target:
      type: streaming_table
      mode: replace_where
      catalog: "${{catalog}}"
      schema: "${{silver_schema}}"
      table: orders_recent_metadata
      replace_where_config:
        replace_where: "order_date >= date_add(current_date(), -7)"
"""


def inject_nondeterministic_replace_where(
    project_root: Path, *, metadata: str = '["_processing_timestamp"]'
) -> Path:
    """Write (or overwrite) the LHP-VAL-073 flowgroup; return its path.

    ``metadata="false"`` writes the same flowgroup without the timestamp
    column: warning-free, so ``--strict`` passes on it.
    """
    path = project_root / NONDETERMINISTIC_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_NONDETERMINISTIC_YAML.format(metadata=metadata), encoding="utf-8")
    return path
