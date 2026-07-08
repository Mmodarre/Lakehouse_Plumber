"""Streaming validate / generate endpoints — the IDE's core long-running ops.

Two POST endpoints stream the public facade's ``Iterator[LHPEvent]`` runs to
the browser as ``application/x-ndjson`` (one JSON frame per line):

* ``POST /api/validate/stream`` → :meth:`facade.validate_pipelines`
* ``POST /api/generate/stream`` → :meth:`facade.generate_pipelines`

The sync→async bridge, frame protocol, §5.7 ordering, terminal-error framing,
and single-run serialization all live in
:mod:`lhp.webapp.services.stream_adapter`. This router only:

1. validates the JSON body (``{env, pipeline?}``),
2. binds the facade method into a zero-config ``run(progress) -> Iterator``
   via :func:`functools.partial` (every kwarg bound EXCEPT ``progress`` — the
   adapter injects the sink as ``progress=``), and
3. wraps :func:`stream_events` in
   :func:`~lhp.webapp.services.run_recorder.record_ndjson` (SQLite run
   history + ``run-updated`` bus events; frames pass through byte-identical)
   inside a ``StreamingResponse``.

``pipeline`` (optional) maps to the facade's ``pipeline_filter`` — the
single-pipeline selection filter; ``None`` runs the whole project.

Bundle sync is intentionally OFF for the IDE stream (``bundle_enabled=False``):
the DI facade is built via ``for_project`` with no ``pipeline_config_path``, so
the bundle preflight would have nothing to check; the IDE generate stream is a
code-generation preview, not a bundle deploy. (The bundle-detection helper also
lives in :mod:`lhp.bundle`, which this module may not import under the
``webapp-uses-public-api`` boundary contract — §5.3.)

Per that contract this module imports ONLY :mod:`lhp.api` from the ``lhp``
package, plus FastAPI / pydantic / the in-package adapter + DI helpers.

ROUTER CONVENTION: routes carry their full sub-path (``/validate/stream`` and
``/generate/stream``); the app mounts this router with ``prefix="/api"``.
"""

from __future__ import annotations

import functools
from collections.abc import Iterator
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from lhp.api import LakehousePlumberApplicationFacade, LHPEvent, ProgressSink
from lhp.webapp.dependencies import get_facade, get_project_root
from lhp.webapp.services.run_recorder import record_ndjson
from lhp.webapp.services.stream_adapter import stream_events

router = APIRouter(tags=["streaming"])

_NDJSON_MEDIA_TYPE = "application/x-ndjson"


class StreamRunRequest(BaseModel):
    """Request body for the validate / generate stream endpoints.

    ``env`` selects the substitution environment (e.g. ``"dev"``). ``pipeline``
    is the optional single-pipeline filter — ``None`` (the default) runs the
    whole project; a name restricts the run to that one pipeline (maps to the
    facade's ``pipeline_filter``).
    """

    env: str = Field(..., min_length=1, description="Substitution environment.")
    pipeline: str | None = Field(
        default=None,
        description="Optional single-pipeline filter; null runs the whole project.",
    )


@router.post("/validate/stream")
def validate_stream(
    body: StreamRunRequest,
    request: Request,
    facade: LakehousePlumberApplicationFacade = Depends(get_facade),
    project_root: Path = Depends(get_project_root),
) -> StreamingResponse:
    """Stream a validation run as NDJSON frames.

    Yields the §5.7 frame sequence (one ``OperationStarted`` first, phase /
    pipeline progress frames, then a terminal ``ValidationCompleted`` carrying
    the response — or a terminal ``error`` frame for a structural / config
    failure that aborts the run). Pipeline-level findings are REPORTED inside
    ``ValidationCompleted`` (``success=false`` + issues), not raised.
    The run is recorded into SQLite run history (see ``/api/runs``).
    """
    run = functools.partial(
        _validate_run,
        facade,
        env=body.env,
        pipeline_filter=body.pipeline,
    )
    frames = record_ndjson(
        stream_events(run),
        project_root=project_root,
        event_bus=request.app.state.event_bus,
        kind="validate",
        env=body.env,
        pipeline=body.pipeline,
    )
    return StreamingResponse(frames, media_type=_NDJSON_MEDIA_TYPE)


@router.post("/generate/stream")
def generate_stream(
    body: StreamRunRequest,
    request: Request,
    facade: LakehousePlumberApplicationFacade = Depends(get_facade),
    project_root: Path = Depends(get_project_root),
) -> StreamingResponse:
    """Stream a generation run as NDJSON frames.

    Yields the §5.7 frame sequence terminated by a ``GenerationCompleted``
    carrying the response (or a terminal ``error`` frame on a structural /
    config failure). Output is written to ``<project_root>/generated/<env>``,
    matching the ``lhp generate`` CLI default.
    The run is recorded into SQLite run history (see ``/api/runs``).
    """
    output_dir = project_root / "generated" / body.env
    run = functools.partial(
        _generate_run,
        facade,
        env=body.env,
        pipeline_filter=body.pipeline,
        output_dir=output_dir,
    )
    frames = record_ndjson(
        stream_events(run),
        project_root=project_root,
        event_bus=request.app.state.event_bus,
        kind="generate",
        env=body.env,
        pipeline=body.pipeline,
    )
    return StreamingResponse(frames, media_type=_NDJSON_MEDIA_TYPE)


def _validate_run(
    facade: LakehousePlumberApplicationFacade,
    progress: ProgressSink,
    *,
    env: str,
    pipeline_filter: str | None,
) -> Iterator[LHPEvent]:
    """Bind ``facade.validate_pipelines`` keywords; the adapter injects ``progress``.

    A thin shim (not a bare ``functools.partial`` on the method) so the adapter's
    POSITIONAL ``progress`` argument — it calls ``run(sink)`` — lands on a
    positional-or-keyword parameter here, then forwards to the facade method's
    keyword-only ``progress=``. ``facade`` is bound by the router's partial;
    ``progress`` is the single remaining positional slot the adapter fills.
    """
    return facade.validate_pipelines(
        env=env,
        pipeline_filter=pipeline_filter,
        bundle_enabled=False,
        progress=progress,
    )


def _generate_run(
    facade: LakehousePlumberApplicationFacade,
    progress: ProgressSink,
    *,
    env: str,
    pipeline_filter: str | None,
    output_dir: Path,
) -> Iterator[LHPEvent]:
    """Bind ``facade.generate_pipelines`` keywords; the adapter injects ``progress``."""
    return facade.generate_pipelines(
        env=env,
        pipeline_filter=pipeline_filter,
        output_dir=output_dir,
        bundle_enabled=False,
        progress=progress,
    )
