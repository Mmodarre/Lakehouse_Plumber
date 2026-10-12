"""Find ``${name}`` tokens left after substitution and build their LHP-CFG-010 error.

Substitution leaves a ``${name}`` it has no value for exactly as written. In a
file LHP copies into generated output (a ``sql_path`` query, a UC tags file)
that leftover is a silent bug: in SQL, Spark substitutes an empty string unless
a pipeline ``configuration:`` value or Spark conf defines the name at runtime,
and a tag is written to Unity Catalog as literal text. A pipeline parameter
meant for Spark is therefore referenced with a dotted key. Only plain
``${name}`` references count; dotted or
prefixed references (``${mypipeline.start}``, ``${hiveconf:x}``,
``${env:X}``, ``${var.x}``) are Spark / pipeline variables that LHP never
substitutes, and the deprecated bare ``{name}`` form is not checked.

Both functions are free functions over plain values, so any caller that reads
and substitutes a file can reuse them without holding a substitution manager.
"""

from pathlib import Path
from typing import List, Optional, Sequence, Union

from ...errors import ErrorFactory, LHPConfigError, codes
from .substitution import EnhancedSubstitutionManager


def find_unresolved_tokens(text: str) -> List[str]:
    """Return the names of the plain ``${name}`` tokens in ``text``.

    Each name is reported once, in order of first appearance.
    """
    names = EnhancedSubstitutionManager.DOLLAR_TOKEN_PATTERN.findall(text)
    return list(dict.fromkeys(names))


def unresolved_tokens_error(
    tokens: Sequence[str],
    *,
    file: Union[str, Path],
    project_root: Optional[Path] = None,
    field: Optional[str] = None,
    env: Optional[str] = None,
    pipeline: Optional[str] = None,
    flowgroup: Optional[str] = None,
    action: Optional[str] = None,
    located_title: bool = False,
    sql_note: bool = True,
) -> LHPConfigError:
    """Build the LHP-CFG-010 error for ``tokens`` left unresolved in ``file``.

    ``tokens`` are names as :func:`find_unresolved_tokens` returns them.
    ``file`` is shown project-relative when it is an absolute path under
    ``project_root``; ``field`` names the key inside the file, when the token
    sits in one configuration value rather than anywhere in the file. The
    pipeline, flowgroup and action are added to the error context when known.

    ``located_title`` puts the tokens, file and key in the title, for an error
    reported where only the code and title are printed. ``sql_note`` adds that
    Spark turns a leftover token in SQL into an empty string unless something
    defines it at runtime, and points a pipeline ``configuration:`` parameter
    at a dotted key; turn it off for values that are not SQL.
    """
    shown_file = _display_path(file, project_root)
    token_list = ", ".join(f"${{{name}}}" for name in tokens)
    where = f"'{shown_file}' ({field})" if field else f"'{shown_file}'"
    env_label = env or "<env>"
    context = {"File": shown_file}
    if field:
        context["Field"] = field
    context["Unresolved"] = token_list
    for label, value in (
        ("Environment", env),
        ("Pipeline", pipeline),
        ("FlowGroup", flowgroup),
        ("Action", action),
    ):
        if value:
            context[label] = value
    location = f"{shown_file} ({field})" if field else shown_file
    title = (
        f"Unresolved substitution token(s) {token_list} in {location}"
        if located_title
        else "Unresolved substitution tokens detected"
    )
    details = (
        f"{where} still contains {token_list} after substitution for "
        f"environment '{env_label}'. A token LHP has no value for is left in "
        "the generated output as written"
    )
    details += (
        "; in SQL, if nothing defines it at runtime (a pipeline 'configuration:' "
        "value or a Spark conf), Spark substitutes an empty string."
        if sql_note
        else "."
    )
    runtime_variable = (
        "If it is a pipeline 'configuration:' parameter meant for Spark at "
        "runtime, reference it with a dotted key (e.g. ${mypipeline.start_date}, "
        "renaming the configuration key to match), which LHP leaves untouched"
        if sql_note
        else "For a Spark or pipeline variable set at runtime, use a dotted or "
        "prefixed name such as ${mypipeline.start} or ${hiveconf:x}; LHP "
        "leaves those as written"
    )
    return ErrorFactory.config_error(
        codes.CFG_010,
        title=title,
        details=details,
        suggestions=[
            f"Add the missing key(s) to substitutions/{env_label}.yaml (under "
            f"'{env_label}:' or 'global:')",
            "Check the token name for typos (token names are case-sensitive)",
            runtime_variable,
        ],
        context=context,
    )


def _display_path(file: Union[str, Path], project_root: Optional[Path]) -> str:
    path = Path(file)
    if project_root is not None and path.is_relative_to(project_root):
        return path.relative_to(project_root).as_posix()
    return path.as_posix()
