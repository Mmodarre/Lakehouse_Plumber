"""Render a ``sql_path`` file as a Jinja template with an action's ``sql_parameters``.

An action that sets ``sql_parameters`` (directly, or because its template sets
``render_sql_files: true``) has its ``sql_path`` file rendered with those values
before ``${token}`` substitution, so the file can use the same ``{{ name }}``
expressions as the template that references it. Code generation (through
``load_sql_file``) and dependency extraction both render through this function,
so they read the same SQL.

Rendering uses strict undefined: a name the parameters do not define raises
``LHP-CFG-071`` instead of rendering as an empty string. It runs in Jinja's
immutable sandbox, like the editor's template preview, so a SQL file cannot
reach Python attributes or mutate its parameters. ``${token}`` and
``${secret:...}`` references are not Jinja syntax and pass through unchanged.
"""

from pathlib import Path
from typing import Any, Mapping, Optional, Union

from jinja2 import StrictUndefined, TemplateError, TemplateSyntaxError, UndefinedError
from jinja2.sandbox import ImmutableSandboxedEnvironment

from ...errors import ErrorFactory, LHPConfigError, codes
from .unresolved_tokens import _display_path

_ENVIRONMENT = ImmutableSandboxedEnvironment(  # nosec B701 — renders SQL text, not HTML
    undefined=StrictUndefined,
    keep_trailing_newline=True,
)


def render_sql_parameters(
    text: str,
    parameters: Mapping[str, Any],
    *,
    file: Union[str, Path],
    project_root: Optional[Path] = None,
    pipeline: Optional[str] = None,
    flowgroup: Optional[str] = None,
    action: Optional[str] = None,
) -> str:
    """Return ``text`` rendered as a Jinja template with ``parameters``.

    ``file`` names the ``sql_path`` file in the error, shown project-relative
    when it is an absolute path under ``project_root``; ``pipeline``,
    ``flowgroup`` and ``action`` name the referencing action when known.

    :raises LHPConfigError: ``LHP-CFG-071`` when ``text`` is not a valid Jinja
        template or references a name ``parameters`` does not define.
    """
    shown_file = _display_path(file, project_root)
    try:
        return _ENVIRONMENT.from_string(text).render(**parameters)
    except TemplateSyntaxError as exc:
        raise _render_error(
            f"is not a valid Jinja template (line {exc.lineno}): {exc.message}",
            shown_file,
            parameters,
            pipeline,
            flowgroup,
            action,
            line=exc.lineno,
        ) from exc
    except UndefinedError as exc:
        raise _render_error(
            f"references a name sql_parameters does not define: {exc.message}",
            shown_file,
            parameters,
            pipeline,
            flowgroup,
            action,
        ) from exc
    except TemplateError as exc:
        # SecurityError from the sandbox, or another render-time failure.
        raise _render_error(
            f"could not be rendered: {exc.message}",
            shown_file,
            parameters,
            pipeline,
            flowgroup,
            action,
        ) from exc


def _render_error(
    problem: str,
    shown_file: str,
    parameters: Mapping[str, Any],
    pipeline: Optional[str],
    flowgroup: Optional[str],
    action: Optional[str],
    line: Optional[int] = None,
) -> LHPConfigError:
    context = {"File": shown_file}
    if line is not None:
        context["Line"] = str(line)
    context["Parameters"] = ", ".join(sorted(parameters)) or "(none)"
    for label, value in (
        ("Pipeline", pipeline),
        ("FlowGroup", flowgroup),
        ("Action", action),
    ):
        if value:
            context[label] = value
    return ErrorFactory.config_error(
        codes.CFG_071,
        title=f"Cannot render SQL file {shown_file} with sql_parameters",
        details=(
            f"'{shown_file}' {problem}. The file is rendered as a Jinja template "
            "because its action sets sql_parameters (or its template sets "
            "render_sql_files: true)."
        ),
        suggestions=[
            "Check the {{ name }} expressions in the file against the parameters "
            "listed in the context (names are case-sensitive)",
            "Declare a missing name as a template parameter (with a default), or "
            "add it to the action's sql_parameters",
            "To keep literal '{{', '{%' or '{#' in the SQL, wrap that text in "
            "{% raw %}...{% endraw %}",
        ],
        context=context,
    )
