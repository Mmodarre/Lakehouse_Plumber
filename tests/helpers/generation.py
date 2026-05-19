"""Generation-output read helpers.

The worker pool writes formatted Python to disk and returns only filenames,
so tests that need to assert on generated content read it back from disk.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


def read_generated_pipeline(
    orchestrator: Any,
    *,
    pipeline_field: str,
    env: str,
    output_dir: Path,
    **kwargs: Any,
) -> Dict[str, str]:
    """Generate via ``generate_pipeline_by_field`` and return ``{filename: content}``.

    Files land at ``output_dir / pipeline_field / <filename>``; the helper
    reads each back with ``Path.read_text()`` so callers can assert on
    generated-Python content.
    """
    filenames = orchestrator.generate_pipeline_by_field(
        pipeline_field=pipeline_field,
        env=env,
        output_dir=output_dir,
        **kwargs,
    )
    out: Dict[str, str] = {}
    for filename in filenames:
        path = output_dir / pipeline_field / filename
        out[filename] = path.read_text()
    return out
