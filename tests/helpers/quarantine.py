"""On-disk quarantine projects for the ``lhp validate`` / ``lhp generate`` CLI tests."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence, Tuple

_EXPECTATIONS = """order_id IS NOT NULL:
  action: drop
  name: valid_order_id
"""

# Every flowgroup quarantines into the same DLQ. ``source_table`` is substituted
# in verbatim, so a caller can spell the same table with and without tokens.
_FLOWGROUP = """pipeline: {pipeline}
flowgroup: {flowgroup}
actions:
  - name: load_{flowgroup}
    type: load
    source:
      type: cloudfiles
      path: "/Volumes/c/s/landing/{flowgroup}/*.csv"
      format: csv
    target: v_{flowgroup}_raw
  - name: dq_{flowgroup}
    type: transform
    transform_type: data_quality
    source: v_{flowgroup}_raw
    expectations_file: expectations/rules.yaml
    mode: quarantine
    quarantine:
      dlq_table: "${{catalog}}.${{schema}}.orders_dlq"
      source_table: "{source_table}"
    target: v_{flowgroup}_validated
  - name: write_{flowgroup}
    type: write
    source: v_{flowgroup}_validated
    write_target:
      type: streaming_table
      catalog: "${{catalog}}"
      schema: "${{schema}}"
      table: {flowgroup}
"""


def write_quarantine_project(
    root: Path, flowgroups: Sequence[Tuple[str, str, str]]
) -> None:
    """Write a project of load -> quarantine -> write flowgroups sharing one DLQ.

    ``flowgroups`` holds ``(pipeline, flowgroup, source_table)`` triples; the
    ``dev`` substitutions resolve ``${catalog}.${schema}`` to ``c.s``.
    """
    (root / "lhp.yaml").write_text('name: quarantine_cli_test\nversion: "1.0"\n')
    (root / "substitutions").mkdir()
    (root / "substitutions" / "dev.yaml").write_text(
        "dev:\n  catalog: c\n  schema: s\n"
    )
    (root / "expectations").mkdir()
    (root / "expectations" / "rules.yaml").write_text(_EXPECTATIONS)
    for pipeline, flowgroup, source_table in flowgroups:
        fg_dir = root / "pipelines" / pipeline
        fg_dir.mkdir(parents=True, exist_ok=True)
        (fg_dir / f"{flowgroup}.yaml").write_text(
            _FLOWGROUP.format(
                pipeline=pipeline, flowgroup=flowgroup, source_table=source_table
            )
        )
