"""Reject ``${name}`` tokens left unresolved in the project settings LHP writes out.

``lhp validate`` and ``lhp generate`` run this check in the shared project
preflight, over values already resolved for the environment:

* each pipeline's bundle resource settings, when bundle output is enabled:
  every ``pipeline_config.yaml`` value, plus the ``event_log`` block injected
  from ``lhp.yaml``;
* the monitoring union notebook's values: the event-log table references, the
  target table and ``monitoring.checkpoint_path``.

The monitoring job config is not checked. Only plain ``${name}`` references
count (see :func:`~lhp.core.processing.find_unresolved_tokens`), so Databricks
bundle references such as ``${var.x}`` or ``${bundle.target}`` pass. Mapping
keys are not checked, because substitution never applies to them.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from lhp.errors import LHPConfigError

from ...processing import find_unresolved_tokens, unresolved_tokens_error

_PROJECT_FILE = "lhp.yaml"
_EVENT_LOG = "event_log"
_MAX_LISTED_PIPELINES = 5

#: The ``lhp.yaml`` setting each union-notebook value is built from (see
#: ``MonitoringPipelineBuilder.build``). A key not listed is named ``monitoring``.
_NOTEBOOK_FIELDS = {
    "sources": _EVENT_LOG,
    "checkpoint_path": "monitoring.checkpoint_path",
    "target_fqn": (
        "monitoring.catalog, monitoring.schema or monitoring.streaming_table"
    ),
}

#: ``(file, field, token set)`` to the tokens in first-seen order and the
#: pipelines that share the location, in insertion order.
_Findings = Dict[
    Tuple[Union[str, Path], str, FrozenSet[str]], Tuple[List[str], List[str]]
]


@dataclass(frozen=True)
class PipelineSettings:
    """One pipeline's bundle resource settings, before and after resolution.

    ``resolved`` is what the resource file receives: the merged
    ``pipeline_config.yaml`` values plus any ``event_log`` injected from
    ``lhp.yaml``, with the environment's tokens substituted. ``raw`` is the
    merged ``pipeline_config.yaml`` alone, so an ``event_log`` mapping found in
    ``resolved`` but not in ``raw`` came from ``lhp.yaml``.
    """

    name: str
    resolved: Mapping[str, Any]
    raw: Mapping[str, Any]


def unresolved_setting_token_errors(
    *,
    env: str,
    project_root: Optional[Path] = None,
    pipeline_settings: Sequence[PipelineSettings] = (),
    pipeline_config_file: Optional[Union[str, Path]] = None,
    notebook_context: Optional[Mapping[str, Any]] = None,
) -> List[LHPConfigError]:
    """Return one ``LHP-CFG-010`` per setting that still holds a plain ``${name}``.

    ``pipeline_settings`` are the resolved bundle settings of each pipeline and
    ``pipeline_config_file`` the file their values come from.
    ``notebook_context`` is the resolved monitoring notebook context, or
    ``None`` when no notebook is generated. A ``pipeline_config.yaml`` value is
    named by its key path; a location several pipelines share (a
    ``project_defaults`` value, the ``lhp.yaml`` ``event_log``) is reported
    once, naming at most five of the pipelines. Each title names the tokens,
    file and key, because ``lhp validate`` prints only a preflight issue's code
    and title. The errors are returned, not raised.

    :raises ValueError: when ``pipeline_settings`` is given without
        ``pipeline_config_file``.
    """
    findings: _Findings = {}
    if pipeline_settings:
        if pipeline_config_file is None:
            raise ValueError("pipeline_settings requires pipeline_config_file")
        for settings in pipeline_settings:
            _record_pipeline(findings, settings, pipeline_config_file)
    if notebook_context is not None:
        _record_notebook(findings, notebook_context)
    return [
        unresolved_tokens_error(
            tokens,
            file=file,
            project_root=project_root,
            field=field,
            env=env,
            pipeline=_pipeline_list(pipelines),
            located_title=True,
            sql_note=False,
        )
        for (file, field, _), (tokens, pipelines) in findings.items()
    ]


def _pipeline_list(pipelines: List[str]) -> Optional[str]:
    if not pipelines:
        return None
    listed = ", ".join(pipelines[:_MAX_LISTED_PIPELINES])
    hidden = len(pipelines) - _MAX_LISTED_PIPELINES
    return f"{listed} (+{hidden} more)" if hidden > 0 else listed


def _record_pipeline(
    findings: _Findings,
    settings: PipelineSettings,
    pipeline_config_file: Union[str, Path],
) -> None:
    event_log_from_project = isinstance(
        settings.resolved.get(_EVENT_LOG), Mapping
    ) and not isinstance(settings.raw.get(_EVENT_LOG), Mapping)
    for key, value in settings.resolved.items():
        if key == _EVENT_LOG and event_log_from_project:
            _record(findings, _PROJECT_FILE, _EVENT_LOG, _tokens_in(value))
            continue
        for path, text in _string_leaves(value, str(key)):
            _record(
                findings,
                pipeline_config_file,
                path,
                find_unresolved_tokens(text),
                settings.name,
            )


def _record_notebook(findings: _Findings, context: Mapping[str, Any]) -> None:
    event_log_tokens = set(_tokens_in(context.get("sources")))
    for key, value in context.items():
        tokens = _tokens_in(value)
        if key == "target_fqn":
            # Without monitoring.catalog / monitoring.schema the target table
            # takes event_log's, whose tokens the event-log references report.
            tokens = [name for name in tokens if name not in event_log_tokens]
        _record(
            findings, _PROJECT_FILE, _NOTEBOOK_FIELDS.get(key, "monitoring"), tokens
        )


def _record(
    findings: _Findings,
    file: Union[str, Path],
    field: str,
    tokens: List[str],
    pipeline: Optional[str] = None,
) -> None:
    if not tokens:
        return
    _, pipelines = findings.setdefault((file, field, frozenset(tokens)), (tokens, []))
    if pipeline is not None and pipeline not in pipelines:
        pipelines.append(pipeline)


def _tokens_in(value: Any) -> List[str]:
    names: List[str] = []
    for _, text in _string_leaves(value, ""):
        names.extend(find_unresolved_tokens(text))
    return list(dict.fromkeys(names))


def _string_leaves(value: Any, path: str) -> Iterator[Tuple[str, str]]:
    """Yield ``(key path, text)`` for every string inside ``value``."""
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield from _string_leaves(item, f"{path}.{key}" if path else str(key))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            yield from _string_leaves(item, f"{path}[{index}]")
