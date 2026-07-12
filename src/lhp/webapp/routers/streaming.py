"""Streaming validate / generate endpoints — the IDE's core long-running ops.

Two POST endpoints stream the public facade's ``Iterator[LHPEvent]`` runs to
the browser as ``application/x-ndjson`` (one JSON frame per line):

* ``POST /api/validate/stream`` → :meth:`facade.validate_pipelines`
* ``POST /api/generate/stream`` → :meth:`facade.generate_pipelines`

The sync→async bridge, frame protocol, §5.7 ordering, terminal-error framing,
and single-run serialization all live in
:mod:`lhp.webapp.services.stream_adapter`. This router only:

1. validates the JSON body (``{env, pipeline?, pipeline_config?, sandbox?}``),
2. binds the facade method into a zero-config ``run(progress) -> Iterator``
   via :func:`functools.partial` (every kwarg bound EXCEPT ``progress`` — the
   adapter injects the sink as ``progress=``), and
3. wraps :func:`stream_events` in
   :func:`~lhp.webapp.services.run_recorder.record_ndjson` (SQLite run
   history + ``run-updated`` bus events; frames pass through byte-identical)
   inside a ``StreamingResponse``.

``pipeline`` (optional) maps to the facade's ``pipeline_filter`` — the
single-pipeline selection filter; ``None`` runs the whole project.

``pipeline_config`` (optional) is the IDE counterpart of the CLI's
``--pipeline-config/-pc`` flag: a project-relative pipeline-config YAML path.
Without it the run uses the default (config-less) DI facade with
``bundle_enabled=False`` — a pure code-generation / validation preview, the
pre-existing behavior. With it the facade is obtained keyed by the resolved
path (:func:`~lhp.webapp.dependencies.get_facade_for`) and ``bundle_enabled``
mirrors the CLI decision via :func:`lhp.api.should_enable_bundle_support`
(``databricks.yml`` presence). The path is guarded like the files router:
escaping the project root → 403, missing file → 404. (The detection helper is
re-exported by :mod:`lhp.api`, so the ``webapp-uses-public-api`` boundary
contract — §5.3 — still holds: no :mod:`lhp.bundle` import here.)

Per that contract this module imports ONLY :mod:`lhp.api` from the ``lhp``
package, plus FastAPI / pydantic / the in-package adapter + DI helpers.

ROUTER CONVENTION: routes carry their full sub-path (``/validate/stream`` and
``/generate/stream``); the app mounts this router with ``prefix="/api"``.
"""

from __future__ import annotations

import functools
from collections.abc import Iterator
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from lhp.api import (
    LakehousePlumberApplicationFacade,
    LHPEvent,
    ProgressSink,
    should_enable_bundle_support,
)
from lhp.webapp.dependencies import get_facade_for, get_project_root
from lhp.webapp.services.run_recorder import record_ndjson
from lhp.webapp.services.stream_adapter import stream_events

router = APIRouter(tags=["streaming"])

_NDJSON_MEDIA_TYPE = "application/x-ndjson"


class StreamRunRequest(BaseModel):
    """Request body for the validate / generate stream endpoints.

    ``env`` selects the substitution environment (e.g. ``"dev"``). ``pipeline``
    is the optional single-pipeline filter — ``None`` (the default) runs the
    whole project; a name restricts the run to that one pipeline (maps to the
    facade's ``pipeline_filter``). ``pipeline_config`` is the optional
    project-relative pipeline-config YAML path (the CLI's
    ``--pipeline-config``); when set, bundle support mirrors the CLI's
    ``databricks.yml`` detection. ``sandbox`` (default ``false``) switches the
    run to developer-sandbox mode — scope and namespace come from
    ``.lhp/profile.yaml`` — and is mutually exclusive with ``pipeline``.
    """

    env: str = Field(..., min_length=1, description="Substitution environment.")
    pipeline: str | None = Field(
        default=None,
        description="Optional single-pipeline filter; null runs the whole project.",
    )
    pipeline_config: str | None = Field(
        default=None,
        min_length=1,
        description=(
            "Optional project-relative pipeline-config YAML path (e.g. "
            "'config/pipeline_config_dev.yaml'); null runs without a pipeline "
            "config, with bundle support off."
        ),
    )
    sandbox: bool = Field(
        default=False,
        description=(
            "Developer-sandbox mode: scope and namespace come from "
            ".lhp/profile.yaml. Mutually exclusive with 'pipeline'."
        ),
    )


def _resolve_pipeline_config(project_root: Path, pipeline_config: str) -> Path:
    """Resolve the request's config path inside the project root, or refuse.

    Mirrors the files router's guard mapping: a target resolving outside the
    project root (symlinks followed) → 403; an in-root target that is not an
    existing file → 404.
    """
    resolved = (project_root / pipeline_config).resolve()
    if not resolved.is_relative_to(project_root.resolve()):
        raise HTTPException(
            status_code=403,
            detail=(
                f"Path traversal not allowed: {pipeline_config!r} resolves "
                f"outside the project root"
            ),
        )
    if not resolved.is_file():
        raise HTTPException(
            status_code=404, detail=f"File not found: {pipeline_config}"
        )
    return resolved


def _facade_and_bundle(
    request: Request, project_root: Path, pipeline_config: str | None
) -> tuple[LakehousePlumberApplicationFacade, bool]:
    """Resolve the run's facade and bundle flag from the optional config path.

    No ``pipeline_config`` → the default (``None``-keyed) facade with
    ``bundle_enabled=False``, exactly the config-less behavior. With one: guard
    it (403 traversal / 404 missing), key the facade cache by the resolved
    absolute path, and take the CLI's bundle decision
    (:func:`lhp.api.should_enable_bundle_support` — ``databricks.yml``
    presence; the IDE has no ``--no-bundle`` override).
    """
    if pipeline_config is None:
        return get_facade_for(request, None), False
    resolved = _resolve_pipeline_config(project_root, pipeline_config)
    facade = get_facade_for(request, str(resolved))
    # Bundle detection keys off the server-configured root, never anything
    # request-derived — read it from settings rather than the view parameter.
    return facade, should_enable_bundle_support(get_project_root())


def _reject_sandbox_with_pipeline_filter(body: StreamRunRequest) -> None:
    """Refuse sandbox mode combined with the single-pipeline filter.

    Sandbox scope is profile-driven (``.lhp/profile.yaml``), so it cannot be
    narrowed with the request's ``pipeline`` filter. The CLI rejects
    ``--sandbox`` + ``-p/--pipeline`` with a native Click usage error (exit 2)
    before any facade work; the HTTP analogue is a 422 raised up front, before
    the run stream is opened (the facade would otherwise raise ``ValueError``
    only once the stream is iterated).
    """
    if body.sandbox and body.pipeline is not None:
        raise HTTPException(
            status_code=422,
            detail=(
                "sandbox mode cannot be combined with a single-pipeline "
                "filter: sandbox scope comes from .lhp/profile.yaml"
            ),
        )


@router.post("/validate/stream")
def validate_stream(
    body: StreamRunRequest,
    request: Request,
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
    _reject_sandbox_with_pipeline_filter(body)
    facade, bundle_enabled = _facade_and_bundle(
        request, project_root, body.pipeline_config
    )
    run = functools.partial(
        _validate_run,
        facade,
        env=body.env,
        pipeline_filter=body.pipeline,
        bundle_enabled=bundle_enabled,
        sandbox=body.sandbox,
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
    project_root: Path = Depends(get_project_root),
) -> StreamingResponse:
    """Stream a generation run as NDJSON frames.

    Yields the §5.7 frame sequence terminated by a ``GenerationCompleted``
    carrying the response (or a terminal ``error`` frame on a structural /
    config failure). Output is written to ``<project_root>/generated/<env>``,
    matching the ``lhp generate`` CLI default.
    The run is recorded into SQLite run history (see ``/api/runs``).
    """
    _reject_sandbox_with_pipeline_filter(body)
    facade, bundle_enabled = _facade_and_bundle(
        request, project_root, body.pipeline_config
    )
    output_dir = project_root / "generated" / body.env
    run = functools.partial(
        _generate_run,
        facade,
        env=body.env,
        pipeline_filter=body.pipeline,
        output_dir=output_dir,
        bundle_enabled=bundle_enabled,
        sandbox=body.sandbox,
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
    bundle_enabled: bool,
    sandbox: bool,
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
        bundle_enabled=bundle_enabled,
        progress=progress,
        sandbox=sandbox,
    )


def _generate_run(
    facade: LakehousePlumberApplicationFacade,
    progress: ProgressSink,
    *,
    env: str,
    pipeline_filter: str | None,
    output_dir: Path,
    bundle_enabled: bool,
    sandbox: bool,
) -> Iterator[LHPEvent]:
    """Bind ``facade.generate_pipelines`` keywords; the adapter injects ``progress``."""
    return facade.generate_pipelines(
        env=env,
        pipeline_filter=pipeline_filter,
        output_dir=output_dir,
        bundle_enabled=bundle_enabled,
        progress=progress,
        sandbox=sandbox,
    )
