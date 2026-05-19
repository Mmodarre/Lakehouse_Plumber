"""Generation-output read helpers.

Since the Commit-3 payload diet stripped formatted code from the worker
return path, content-based assertions read from disk. This module provides
the small read-back helper that test files use to recover the previous
``{filename: content}`` shape — written to disk by the worker, read by
the helper, returned to the test.
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

    Used by tests that need to assert on generated-Python content. Files
    land at ``output_dir / pipeline_field / <filename>`` and are read back
    with ``Path.read_text()`` so the helper returns the same dict shape
    the orchestrator used to return in-memory before Commit 3.
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
