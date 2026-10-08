"""Resolve one operational-metadata column expression for a flowgroup and environment.

The single resolution path for ``operational_metadata.columns.<name>.expression``
values from ``lhp.yaml``. The render path
(:class:`~lhp.core.codegen.operational_metadata.OperationalMetadataCatalog`,
reached from every generator's ``_get_operational_metadata``) and the validate
path (:class:`~lhp.core.validators.OperationalMetadataExpressionValidator`, run
during flowgroup resolution by both ``lhp validate`` and ``lhp generate``) both
call :func:`resolve_metadata_expression`, so the two commands cannot drift.

Expressions are PySpark code rather than YAML values, so only the ``${token}``
form is substituted. The deprecated bare ``{token}`` pass is never applied: it
would rewrite ``'\\d{8}'`` regex quantifiers and literal ``{word}`` text.
"""

import logging
import re
from typing import Dict, List, Mapping, Optional

from lhp.models import Action, ActionType

from ....errors import ErrorFactory, codes
from ...processing import EnhancedSubstitutionManager

logger = logging.getLogger(__name__)

# Substitution syntaxes that can survive into an expression: ``${...}``,
# ``%{...}`` (flowgroup local variables) and ``{{ ... }}`` (template
# parameters). The generic bare ``{word}`` pattern is deliberately excluded:
# regex quantifiers such as ``'\\d{8}'`` are legitimate in PySpark expressions.
_LEFTOVER_TOKEN_PATTERN = re.compile(r"\$\{[^}]*\}|%\{[^}]*\}|\{\{.*?\}\}")


def action_context_tokens(action: Optional[Action]) -> Dict[str, str]:
    """Return the context tokens an action supplies to its metadata expressions.

    Delta and JDBC loads supply ``${source_table}``: the table they read
    (``catalog.schema.table`` for a delta load whose namespace is set). Every
    other action supplies none.
    """
    if (
        action is None
        or action.type != ActionType.LOAD
        or not isinstance(action.source, dict)
    ):
        return {}
    source = action.source
    source_type = source.get("type")
    if source_type == "delta" and source.get("table"):
        catalog, schema, table = (
            source.get("catalog"),
            source.get("schema"),
            source["table"],
        )
        qualified = f"{catalog}.{schema}.{table}" if catalog and schema else table
        return {"source_table": str(qualified)}
    if source_type == "jdbc":
        table = source.get("table", "unknown_table")
        return {"source_table": str(table)} if table is not None else {}
    return {}


def resolve_metadata_expression(
    column_name: str,
    expression: str,
    *,
    context_tokens: Mapping[str, str],
    substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
) -> str:
    """Resolve one metadata column expression into the code to emit.

    Steps, in order:

    1. Context tokens (``${pipeline_name}``, ``${flowgroup_name}``, and any
       per-action token such as ``${source_table}``) are replaced first, so
       they win over a ``substitutions/<env>.yaml`` key of the same name.
    2. A ``${secret:...}`` reference is rejected.
    3. Environment ``${token}`` values are substituted. A secret reached
       through a token value is rejected as in step 2.
    4. Any remaining ``${...}``, ``%{...}`` or ``{{ ... }}`` is an error.

    Without ``substitution_mgr`` no environment is known, so only steps 1-2
    run (library callers rendering a generator directly keep their previous
    output). A manager built with ``skip_validation=True`` skips step 4, like
    the flowgroup-level unresolved-token check.

    Errors name the pipeline and flowgroup when ``context_tokens`` carries
    ``pipeline_name`` / ``flowgroup_name`` (the render path always sets them
    for a flowgroup), so the user can find which flowgroup selected the column.

    :raises LHPConfigError: ``LHP-CFG-070`` when the expression references a
        secret; ``LHP-CFG-010`` when a token is still unresolved after
        substitution.
    """
    resolved = expression
    for name, value in context_tokens.items():
        resolved = resolved.replace(f"${{{name}}}", value)

    env = substitution_mgr.env if substitution_mgr is not None else None
    error_context = _error_context(column_name, expression, env, context_tokens)
    _reject_secret_reference(column_name, resolved, env, error_context)
    if substitution_mgr is None:
        return resolved

    resolved = substitution_mgr.substitute_env_tokens(resolved)
    _reject_secret_reference(column_name, resolved, env, error_context)
    if not substitution_mgr.skip_validation:
        _reject_unresolved_tokens(column_name, resolved, env, error_context)
    if resolved != expression:
        logger.debug(f"Resolved metadata column '{column_name}' expression")
    return resolved


def _reject_secret_reference(
    column_name: str,
    resolved: str,
    env: Optional[str],
    error_context: Mapping[str, str],
) -> None:
    match = EnhancedSubstitutionManager.SECRET_PATTERN.search(resolved)
    if match is None:
        return
    env_file = f"substitutions/{env or '<env>'}.yaml"
    raise ErrorFactory.config_error(
        codes.CFG_070,
        title="Secret reference in an operational metadata expression",
        details=(
            f"Operational metadata column '{column_name}' references the secret "
            f"'{match.group(0)}'. Metadata expressions are evaluated for every row "
            "and their result is written into table data, so the secret value "
            "would be stored in the table."
        ),
        suggestions=[
            f"Remove the secret reference from the '{column_name}' expression in lhp.yaml",
            "For non-secret configuration that differs per environment, use a "
            f"${{token}} defined in {env_file}",
            "For per-table logic, add the column in a transform action instead of "
            "an operational metadata column",
        ],
        context=dict(error_context),
    )


def _reject_unresolved_tokens(
    column_name: str,
    resolved: str,
    env: Optional[str],
    error_context: Mapping[str, str],
) -> None:
    leftovers: List[str] = list(
        dict.fromkeys(_LEFTOVER_TOKEN_PATTERN.findall(resolved))
    )
    if not leftovers:
        return
    token_list = ", ".join(leftovers)
    raise ErrorFactory.config_error(
        codes.CFG_010,
        title="Unresolved substitution token in an operational metadata expression",
        details=(
            f"Operational metadata column '{column_name}' still contains "
            f"{token_list} after substitution for environment '{env}'. The "
            "generated code would write the literal token text into every row."
        ),
        suggestions=[
            f"Add the missing key to substitutions/{env}.yaml (under '{env}:' or "
            "'global:')",
            "Metadata expressions resolve ${pipeline_name}, ${flowgroup_name}, "
            "${source_table} (delta and jdbc loads) and ${token} values from "
            "substitutions/<env>.yaml; %{local_var} and {{ template_param }} are "
            "not resolved here, so put per-flowgroup values in a transform action",
            "Check the token name for typos (token names are case-sensitive)",
        ],
        context={**error_context, "Unresolved": token_list},
    )


def _error_context(
    column_name: str,
    expression: str,
    env: Optional[str],
    context_tokens: Mapping[str, str],
) -> Dict[str, str]:
    context: Dict[str, str] = {}
    for label, token in (
        ("Pipeline", "pipeline_name"),
        ("FlowGroup", "flowgroup_name"),
    ):
        if context_tokens.get(token):
            context[label] = context_tokens[token]
    context["Column"] = column_name
    context["Expression"] = expression
    if env is not None:
        context["Environment"] = env
    return context
