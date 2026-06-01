"""Snapshot-CDC source-function resolver.

Reads a user-supplied Python file, locates the named function via AST,
extracts its keyword-only signature, and validates this flowgroup's
parameters against that signature. Returns the function ``name`` and the
substituted parameter VALUES as a :class:`SourceFunctionResult`.

The function *body* is not extracted here: the copy-and-import path
(``core/codegen/python_file_copier.py``) copies the whole user module and
the generated code imports the function by name. This module only performs
generate-time validation (a friendly error ahead of a Databricks-runtime
``TypeError``) and parameter-value substitution.

Signature extraction is cached per pipeline. The cache lives in the
generation context under the key ``source_function_signature_cache`` and
is keyed by the RESOLVED ABSOLUTE PATH of the source file, so a file is
parsed at most once per pipeline regardless of how many flowgroups bind
to it. The cheap per-flowgroup work is the parameter check against the
cached :class:`FunctionSignature`.

This module is pure: it does not mutate any generator instance state.
The companion ``_build_source_expression`` (in ``streaming_table.py``)
consumes the result.
"""

import ast
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, NamedTuple, Optional

from lhp.core.codegen.imports import parse_user_module

from ...core.loaders.external_file_loader import resolve_external_file_path
from ...errors import ErrorCategory, LHPError
from ...utils.performance_timer import incr_event

logger = logging.getLogger(__name__)

# Allowed types for source_function parameter values
_ALLOWED_PARAM_TYPES = (str, int, float, bool, list, dict, type(None))


class SourceFunctionResult(NamedTuple):
    """Result of resolving a source_function configuration."""

    name: str
    parameters: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class FunctionSignature:
    """The cached, parse-once view of a source function's signature.

    Stored in the per-pipeline ``source_function_signature_cache`` keyed
    by resolved absolute path. Holds only what parameter validation
    needs: the set of keyword-only argument names and whether the
    function accepts ``**kwargs``.
    """

    kwonly_names: frozenset[str]
    has_kwargs: bool


def resolve_source_function(
    source_function_config: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
) -> SourceFunctionResult:
    """Resolve a source_function config to its name and validated parameters.

    Reads the source file (for AST only), extracts and caches its
    signature once per file per pipeline, validates this flowgroup's
    parameters against the cached signature, and returns the function
    name plus the substituted parameter values.

    Args:
        source_function_config: Dict with 'file', 'function', and optional
            'parameters' keys.
        context: Generation context. Used for ``project_root``,
            ``substitution_manager``, ``secret_references``, and the
            per-pipeline ``source_function_signature_cache``.

    Returns:
        SourceFunctionResult with name and optional parameters.

    Raises:
        LHPError: IO/001 (file not found, via the shared external-file
            loader), IO/003 (syntax error), IO/004 (function not found in
            file), CONFIG/005 (unsupported param type), CONFIG/006
            (unknown param name).
    """
    file_name = source_function_config.get("file")
    function_name = source_function_config.get("function")
    parameters = source_function_config.get("parameters")

    # NOTE: ``file``/``function`` presence is NOT re-validated here. The
    # snapshot-CDC validator (``SnapshotCdcConfigValidator``, reached via
    # ``ConfigValidator.validate_flowgroup`` → ``WriteActionValidator``)
    # already enforces both fields and runs UNCONDITIONALLY in the
    # per-flowgroup worker (``core/coordination/_flowgroup_pool.py``)
    # BEFORE the codegen branch in BOTH ``validate`` and ``generate``
    # modes — so a missing field is rejected (LHP-VAL-007) long before
    # this resolver is reached. The asserts below encode that upstream
    # contract for the type checker (narrowing ``Optional`` away) and act
    # as a cheap defensive guard; they are not a user-facing error path.
    assert file_name is not None, "source_function.file enforced upstream by validator"
    assert function_name is not None, (
        "source_function.function enforced upstream by validator"
    )

    # ``resolve_external_file_path`` raises a rich LHPFileError (IO-004,
    # via ``ErrorFormatter.file_not_found`` with structured search
    # locations) when the file is missing. Resolve first so we have a
    # stable ABSOLUTE-PATH cache key before reading.
    project_root = context.get("project_root", Path.cwd()) if context else Path.cwd()
    resolved_path = resolve_external_file_path(
        file_name, project_root, file_type="snapshot source function file"
    )
    cache_key = str(resolved_path.resolve())

    # Substitute parameter VALUES only; the function body is substituted by
    # the file copier.
    if context and "substitution_manager" in context:
        substitution_mgr = context["substitution_manager"]

        if parameters:
            parameters = substitution_mgr.substitute_yaml(parameters)

        # Single collection point after parameter substitution.
        secret_refs = substitution_mgr.secret_references
        if "secret_references" in context and context["secret_references"] is not None:
            context["secret_references"].update(secret_refs)

    # Per-pipeline signature cache: parse + extract the signature once per
    # unique source file. Subsequent flowgroups binding to the same file
    # hit the cache and skip re-parsing entirely (GATED INVARIANT — the
    # focused test fails if per-flowgroup parsing is reintroduced).
    signature_cache: Optional[Dict[str, Any]] = (
        context.get("source_function_signature_cache") if context else None
    )
    # Per-pipeline tree cache lives in the shared parse substrate (one parse
    # contract, DR-5: two caches, one parse). The substrate parses + caches
    # the ast.Module; the signature cache above stores the extracted
    # FunctionSignature on top.
    tree_cache: Optional[Dict[str, Any]] = (
        context.get("source_parse_cache") if context else None
    )

    signature: Optional[FunctionSignature] = None
    if signature_cache is not None:
        signature = signature_cache.get(cache_key)

    if signature is None:
        # MISS: a parse is about to run (fires even when there is no cache).
        incr_event("snapshot_sigcache_miss")
        signature = _extract_function_signature(
            resolved_path, file_name, function_name, tree_cache
        )
        if signature_cache is not None:
            signature_cache[cache_key] = signature
    elif signature_cache is not None:
        # HIT: a populated cache actually returned a signature. Guarded on
        # both conditions (not a blanket else) so a "no cache" path that
        # somehow produced a signature would not be miscounted as a hit.
        incr_event("snapshot_sigcache_hit")

    # Validate THIS flowgroup's parameters against the cached signature.
    if parameters:
        _validate_function_parameters(signature, function_name, parameters)

    return SourceFunctionResult(function_name, parameters or None)


def _extract_function_signature(
    resolved_path: Path,
    file_name: str,
    function_name: str,
    tree_cache: Optional[Dict[str, Any]],
) -> FunctionSignature:
    """Parse-once body invoked on a cache miss.

    ``file_name`` is the original config reference, used only in error messages.
    The parse + its IO/003 syntax-error contract live in the shared substrate
    (``parse_user_module``); ``tree_cache`` is the per-pipeline tree cache so
    a file is parsed at most once regardless of how many consumers request it.

    Raises:
        LHPError: IO/003 (syntax error, from the substrate) or IO/004
            (function not found).
    """
    tree = parse_user_module(resolved_path, cache=tree_cache)

    func_node = _find_function_node(tree, function_name)

    if func_node is None:
        raise LHPError(
            category=ErrorCategory.IO,
            code_number="004",
            title=f"Function '{function_name}' not found in file",
            details=f"The function '{function_name}' is not defined in the file '{file_name}'",
            suggestions=[
                f"Define a function named '{function_name}' in your file",
                "Check for typos in the function name",
                "Ensure the function is defined at the top level (not nested inside another function)",
                "Verify the function name matches exactly (case-sensitive)",
            ],
            example=f"""Add this function to {file_name}:

def {function_name}(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    \"\"\"
    Your snapshot processing logic here.

    Args:
        latest_version: Most recent version processed, or None for first run

    Returns:
        Tuple of (DataFrame, version_number) or None if no more data
    \"\"\"
    if latest_version is None:
        # First run logic
        df = spark.read.table("your_snapshot_table")
        return (df, 1)

    # Subsequent runs logic
    return None  # No more snapshots""",
            context={"File": file_name, "Expected Function": function_name},
        )

    kwonly_names = frozenset(arg.arg for arg in func_node.args.kwonlyargs)
    has_kwargs = func_node.args.kwarg is not None
    return FunctionSignature(kwonly_names=kwonly_names, has_kwargs=has_kwargs)


def _find_function_node(
    tree: ast.Module, function_name: str
) -> "ast.FunctionDef | None":
    """Find a top-level FunctionDef by name in the AST."""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return node
    return None


def _validate_function_parameters(
    signature: FunctionSignature,
    function_name: str,
    parameters: Dict[str, Any],
) -> None:
    """Validate that parameters match the function's keyword-only arguments.

    Skips name validation if the function accepts ``**kwargs``.

    Args:
        signature: Cached signature for the target function.
        function_name: Name of the target function (for error messages).
        parameters: Parameter dict from YAML config.

    Raises:
        LHPError: If parameter names don't match keyword-only args
                  (CONFIG/006), or if parameter values have unsupported
                  types (CONFIG/005).
    """
    # Type guard: validate parameter values
    for key, value in parameters.items():
        if not isinstance(value, _ALLOWED_PARAM_TYPES):
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="005",
                title="Unsupported parameter type in source_function",
                details=(
                    f"Parameter '{key}' has type '{type(value).__name__}', "
                    f"which is not supported."
                ),
                suggestions=[
                    "Supported types: str, int, float, bool, list, dict, None",
                    f"Convert '{key}' to one of the supported types",
                ],
            )

    # If function accepts **kwargs, skip name validation
    if signature.has_kwargs:
        logger.debug(
            f"Function '{function_name}' accepts **kwargs, "
            f"skipping parameter name validation"
        )
        return

    kw_only_names = signature.kwonly_names

    # Check for unknown parameter names
    unknown = set(parameters.keys()) - kw_only_names
    if unknown:
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="006",
            title="Unknown parameters for source_function",
            details=(
                f"Parameters {sorted(unknown)} are not keyword-only arguments "
                f"of function '{function_name}'."
            ),
            suggestions=[
                (
                    f"Available keyword-only arguments: {sorted(kw_only_names)}"
                    if kw_only_names
                    else f"Function '{function_name}' has no keyword-only arguments. "
                    f"Add a '*' separator before the parameters you want to bind."
                ),
                "Ensure parameter names in YAML match the function signature",
                f"Example function signature: def {function_name}("
                f"latest_version, *, {', '.join(sorted(parameters.keys()))})",
            ],
        )
