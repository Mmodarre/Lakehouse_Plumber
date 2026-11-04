"""Drive a structured validation warning through a real CLI run.

The worker pool runs each flowgroup in a ``spawn`` subprocess, so a test cannot
monkeypatch a validator inside it. :func:`inject_worker_validation_warning`
instead wraps the MAIN-THREAD per-pipeline fold of worker warnings
(``_pool.merge_flowgroup_warnings``) to add one
:class:`~lhp.models.processing.ValidationWarningRecord`, exactly as if a worker
had recorded it. Everything downstream is real: the executor merge, the
facade's ``WarningEmitted(category="validation")`` emission, the renderer and
the ``--strict`` exit decision.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pytest

# The SQL view reads a STREAM, so the streaming-table append reads it with the
# matching kind and the read-mode lineage check (LHP-VAL-070) stays quiet.
CLEAN_FLOWGROUP_YAML = """pipeline: p_clean
flowgroup: fg_clean
actions:
  - name: load_a
    type: load
    source: {type: sql, sql: "SELECT * FROM STREAM(c.s.src)"}
    target: v_a
  - name: write_a
    type: write
    source: v_a
    write_target: {type: streaming_table, catalog: c, schema: s, table: t_clean}
"""


def write_clean_project(root: Path) -> Path:
    """Write a one-flowgroup project that validates and generates warning-free."""
    (root / "lhp.yaml").write_text('name: validation_warning_cli\nversion: "1.0"\n')
    (root / "substitutions").mkdir(parents=True, exist_ok=True)
    (root / "substitutions" / "dev.yaml").write_text("dev:\n  catalog: c\n")
    for sub in ("presets", "templates"):
        (root / sub).mkdir(exist_ok=True)
    fg_path = root / "pipelines" / "p_clean" / "fg.yaml"
    fg_path.parent.mkdir(parents=True, exist_ok=True)
    fg_path.write_text(CLEAN_FLOWGROUP_YAML)
    return fg_path


def inject_worker_validation_warning(
    monkeypatch: pytest.MonkeyPatch,
    *,
    code: str = "LHP-VAL-073",
    message: str = "stand-in worker validation warning",
) -> None:
    """Add one ``ValidationWarningRecord`` to every pipeline's worker warnings."""
    from lhp.core.coordination import _pool
    from lhp.models.processing import ValidationWarningRecord

    real_merge = _pool.merge_flowgroup_warnings

    def _merge_with_validation_warning(outcomes: Sequence[object]):
        record = ValidationWarningRecord(
            code=code,
            message=message,
            file=Path("pipelines/p_clean/fg.yaml"),
            flowgroup="fg_clean",
        )
        return (*real_merge(outcomes), record)

    monkeypatch.setattr(
        _pool, "merge_flowgroup_warnings", _merge_with_validation_warning
    )
