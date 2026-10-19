"""Render the ``sql_path`` files of template actions with the template parameters.

A SQL file referenced by a template action is rendered as part of template
expansion, so it behaves as if its text were written inline in the template:
``{{ name }}`` expressions, loops and conditions use the same parameters
(defaults applied), then ``${token}`` substitution and the unresolved-token
check run on the result like on any inline SQL. The rendered text becomes the
action's inline ``sql``; ``sql_path`` stays as the file's provenance, and every
consumer already prefers inline ``sql`` over ``sql_path``.

Only the three places a generator reads a ``sql_path`` from are rendered: an
SQL transform's ``sql_path``, an SQL load's ``source.sql_path`` and a
materialized view's ``write_target.sql_path``. Only a file containing a Jinja
delimiter (``{{``, ``{%``, ``{#``) is rendered; any other file, and a file that
does not exist at expansion time, is left to the existing ``sql_path`` path
unchanged.

Rendering uses strict undefined, so a name the parameters do not define raises
``LHP-CFG-071`` instead of rendering as an empty string. It runs in Jinja's
immutable sandbox, like the editor's template preview.
"""

from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from jinja2 import StrictUndefined, TemplateError, TemplateSyntaxError, UndefinedError
from jinja2.sandbox import ImmutableSandboxedEnvironment

from ...errors import ErrorFactory, LHPConfigError, codes

#: Reads one project-relative ``sql_path``: its text, or ``None`` when the file
#: does not exist. The composition root injects
#: ``functools.partial(lhp.core.loaders.read_sql_file, project_root)``
#: so this package does not import ``core/loaders``.
SqlFileReader = Callable[[str], Optional[str]]

_JINJA_DELIMITERS = ("{{", "{%", "{#")

_ENVIRONMENT = ImmutableSandboxedEnvironment(  # nosec B701 — renders SQL text, not HTML
    undefined=StrictUndefined,
)


def inline_template_sql_files(
    action_dict: Dict[str, Any],
    parameters: Mapping[str, Any],
    *,
    read_sql_file: SqlFileReader,
    template: str,
) -> Dict[str, Any]:
    """Return ``action_dict`` with its Jinja ``sql_path`` files rendered inline.

    Covers the three places a generator reads a ``sql_path`` from: the action
    of an SQL transform, the ``source`` of an SQL load and the ``write_target``
    of a materialized view. A place that already has inline ``sql`` is left
    alone, because the generator never reads its file.

    :raises LHPConfigError: ``LHP-CFG-071`` when a file is not a valid Jinja
        template, references a name ``parameters`` does not define, uses
        something the sandbox blocks, or renders to empty SQL.
    """

    def inlined(place: Dict[str, Any]) -> Dict[str, Any]:
        sql = _rendered_sql(place, parameters, read_sql_file, template, action_name)
        return place if sql is None else {**place, "sql": sql}

    action_name = action_dict.get("name")
    action_type = action_dict.get("type")
    result = action_dict
    if action_type == "transform" and action_dict.get("transform_type") == "sql":
        result = inlined(result)
    source = result.get("source")
    if (
        action_type == "load"
        and isinstance(source, dict)
        and source.get("type") == "sql"
    ):
        result = {**result, "source": inlined(source)}
    target = result.get("write_target")
    if (
        action_type == "write"
        and isinstance(target, dict)
        and target.get("type") == "materialized_view"
    ):
        result = {**result, "write_target": inlined(target)}
    return result


def render_template_sql_file(
    text: str,
    parameters: Mapping[str, Any],
    *,
    file: str,
    template: Optional[str] = None,
    action: Optional[str] = None,
) -> str:
    """Return ``text`` rendered as a Jinja template with ``parameters``.

    :raises LHPConfigError: ``LHP-CFG-071`` when ``text`` is not a valid Jinja
        template, references a name ``parameters`` does not define, or uses
        something the sandbox blocks.
    """
    try:
        return _ENVIRONMENT.from_string(text).render(**parameters)
    except TemplateSyntaxError as exc:
        raise _render_error(
            f"is not a valid Jinja template (line {exc.lineno}): {exc.message}",
            file,
            parameters,
            template,
            action,
            line=exc.lineno,
        ) from exc
    except UndefinedError as exc:
        raise _render_error(
            f"references a name the template does not define: {exc.message}",
            file,
            parameters,
            template,
            action,
        ) from exc
    except TemplateError as exc:
        # SecurityError from the sandbox, or another render-time failure.
        raise _render_error(
            f"could not be rendered: {exc.message}",
            file,
            parameters,
            template,
            action,
        ) from exc


def _rendered_sql(
    place: Mapping[str, Any],
    parameters: Mapping[str, Any],
    read_sql_file: SqlFileReader,
    template: str,
    action: Optional[str],
) -> Optional[str]:
    """The rendered text of ``place``'s ``sql_path`` file, or ``None`` to leave it."""
    sql_path = place.get("sql_path")
    if not isinstance(sql_path, str) or not sql_path or place.get("sql"):
        return None
    # Action.model_post_init normalises separators after expansion; read the
    # same file generation would.
    sql_path = sql_path.replace("\\", "/")
    text = read_sql_file(sql_path)
    if text is None or not any(d in text for d in _JINJA_DELIMITERS):
        return None
    rendered = render_template_sql_file(
        text, parameters, file=sql_path, template=template, action=action
    ).strip()
    if not rendered:
        # Empty inline sql would send the generator back to the raw file.
        raise _render_error(
            "renders to empty SQL", sql_path, parameters, template, action
        )
    return rendered


def _render_error(
    problem: str,
    file: str,
    parameters: Mapping[str, Any],
    template: Optional[str],
    action: Optional[str],
    line: Optional[int] = None,
) -> LHPConfigError:
    shown_file = Path(file).as_posix()
    context = {"File": shown_file}
    if line is not None:
        context["Line"] = str(line)
    context["Parameters"] = ", ".join(sorted(parameters)) or "(none)"
    for label, value in (("Template", template), ("Action", action)):
        if value:
            context[label] = value
    return ErrorFactory.config_error(
        codes.CFG_071,
        title=f"Cannot render SQL file {shown_file} with the template parameters",
        details=(
            f"'{shown_file}' {problem}. A SQL file referenced by a template action "
            "is rendered with the template parameters, like the template's own "
            "inline SQL."
        ),
        suggestions=[
            "Check the {{ name }} expressions in the file against the parameters "
            "listed in the context (names are case-sensitive)",
            "Declare a missing name as a template parameter, with a default if it "
            "is optional",
            "To keep literal '{{', '{%' or '{#' in the SQL, wrap that text in "
            "{% raw %}...{% endraw %}",
        ],
        context=context,
    )
