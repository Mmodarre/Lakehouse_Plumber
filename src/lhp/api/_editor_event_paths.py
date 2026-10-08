"""Translate temporary-mirror event paths back to selected project paths."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from lhp.api.events import (
    GenerationPlanCompleted,
    LHPEvent,
    ValidationCompleted,
    WarningEmitted,
)


def _visible_path(path: Path, mirror: Path, root: Path) -> Path:
    if path.is_absolute() and path.is_relative_to(mirror):
        return root / path.relative_to(mirror)
    return path


def visible_editor_event(event: LHPEvent, mirror: Path, root: Path) -> LHPEvent:
    """Keep stream DTOs free of short-lived mirror paths. :stability: provisional"""
    if isinstance(event, GenerationPlanCompleted):
        plan = event.response
        return replace(
            event,
            response=replace(
                plan,
                output_location=_visible_path(plan.output_location, mirror, root)
                if plan.output_location is not None
                else None,
                files=tuple(
                    replace(item, path=_visible_path(item.path, mirror, root))
                    for item in plan.files
                ),
            ),
        )
    if isinstance(event, ValidationCompleted):
        validation = event.response
        return replace(
            event,
            response=replace(
                validation,
                pipeline_responses={
                    name: replace(
                        pipeline,
                        issues=tuple(
                            replace(
                                issue,
                                file_path=_visible_path(issue.file_path, mirror, root)
                                if issue.file_path is not None
                                else None,
                            )
                            for issue in pipeline.issues
                        ),
                    )
                    for name, pipeline in validation.pipeline_responses.items()
                },
            ),
        )
    if isinstance(event, WarningEmitted) and event.file is not None:
        return replace(event, file=_visible_path(event.file, mirror, root))
    return event
