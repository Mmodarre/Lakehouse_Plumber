"""Generation-output read helpers.

The worker pool writes formatted Python to disk and returns only filenames,
so tests that need to assert on generated content read it back from disk.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


def read_generated_pipeline(
    runner: Any,
    *,
    pipeline_field: str,
    env: str,
    output_dir: Path,
    **kwargs: Any,
) -> Dict[str, str]:
    """Generate one pipeline and return ``{filename: content}``.

    Accepts either a :class:`LakehousePlumberApplicationFacade` (the public
    surface produced by ``for_project``) or a raw ``ActionOrchestrator`` —
    detected via the presence of ``.generation``. Files land at
    ``output_dir / pipeline_field / <filename>``; the helper reads each
    back with ``Path.read_text()`` so callers can assert on generated
    Python content.
    """
    if hasattr(runner, "generation"):
        # Facade path — collect the response and pull filenames from the
        # per-pipeline GenerationResponse.
        from lhp.api import collect_response

        response = collect_response(
            runner.generation.generate_pipelines(
                pipeline_filter=pipeline_field,
                env=env,
                output_dir=output_dir,
                **kwargs,
            )
        )
        pipeline_response = response.pipeline_responses.get(pipeline_field)
        filenames: tuple = (
            pipeline_response.generated_filenames if pipeline_response else ()
        )
    else:
        # Raw orchestrator path — uses the dict-returning batch method.
        outcomes = runner.generate_pipelines(
            pipeline_filter=pipeline_field,
            env=env,
            output_dir=output_dir,
            **kwargs,
        )
        filenames = outcomes.get(pipeline_field, ())

    out: Dict[str, str] = {}
    for filename in filenames:
        path = output_dir / pipeline_field / filename
        out[filename] = path.read_text()
    return out
