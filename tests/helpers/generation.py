"""The worker pool writes formatted Python to disk and returns only filenames, so tests read generated content back from disk."""

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
    """Accepts either a :class:`LakehousePlumberApplicationFacade` or a raw ``ActionOrchestrator`` — detected via the presence of ``.generation``."""
    if hasattr(runner, "generation"):
        # Facade path.
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
        # Raw orchestrator path.
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
