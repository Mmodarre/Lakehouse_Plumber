"""On-disk quarantine projects for the ``lhp validate`` / ``lhp generate`` CLI tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Sequence, Tuple

import yaml

_EXPECTATIONS: Dict[str, Any] = {
    "order_id IS NOT NULL": {"action": "drop", "name": "valid_order_id"},
}


def _flowgroup(pipeline: str, flowgroup: str, source_table: str) -> Dict[str, Any]:
    """Build a load -> quarantine -> write flowgroup.

    Every flowgroup quarantines into the same DLQ. ``source_table`` is used
    verbatim, so a caller can spell the same table with and without tokens.
    """
    return {
        "pipeline": pipeline,
        "flowgroup": flowgroup,
        "actions": [
            {
                "name": f"load_{flowgroup}",
                "type": "load",
                "source": {
                    "type": "cloudfiles",
                    "path": f"/Volumes/c/s/landing/{flowgroup}/*.csv",
                    "format": "csv",
                },
                "target": f"v_{flowgroup}_raw",
            },
            {
                "name": f"dq_{flowgroup}",
                "type": "transform",
                "transform_type": "data_quality",
                "source": f"v_{flowgroup}_raw",
                "expectations_file": "expectations/rules.yaml",
                "mode": "quarantine",
                "quarantine": {
                    "dlq_table": "${catalog}.${schema}.orders_dlq",
                    "source_table": source_table,
                },
                "target": f"v_{flowgroup}_validated",
            },
            {
                "name": f"write_{flowgroup}",
                "type": "write",
                "source": f"v_{flowgroup}_validated",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${schema}",
                    "table": flowgroup,
                },
            },
        ],
    }


def write_quarantine_project(
    root: Path, flowgroups: Sequence[Tuple[str, str, str]]
) -> None:
    """Write a project of load -> quarantine -> write flowgroups sharing one DLQ.

    ``flowgroups`` holds ``(pipeline, flowgroup, source_table)`` triples; the
    ``dev`` substitutions resolve ``${catalog}.${schema}`` to ``c.s``.
    """
    (root / "lhp.yaml").write_text(
        yaml.safe_dump({"name": "quarantine_cli_test", "version": "1.0"})
    )
    (root / "substitutions").mkdir()
    (root / "substitutions" / "dev.yaml").write_text(
        yaml.safe_dump({"dev": {"catalog": "c", "schema": "s"}})
    )
    (root / "expectations").mkdir()
    (root / "expectations" / "rules.yaml").write_text(
        yaml.safe_dump(_EXPECTATIONS, sort_keys=False)
    )
    for pipeline, flowgroup, source_table in flowgroups:
        fg_dir = root / "pipelines" / pipeline
        fg_dir.mkdir(parents=True, exist_ok=True)
        (fg_dir / f"{flowgroup}.yaml").write_text(
            yaml.safe_dump(
                _flowgroup(pipeline, flowgroup, source_table), sort_keys=False
            )
        )
