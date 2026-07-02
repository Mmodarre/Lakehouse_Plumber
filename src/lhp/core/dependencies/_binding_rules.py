"""Per-shape YAML parameter-binding rules mirroring codegen call conventions.

Each builder inspects one action shape and produces the
:class:`~lhp.core.dependencies._bindings.ParameterBindings` that mirrors —
EXACTLY — how the corresponding generator/template applies the YAML
``parameters`` to the user function at runtime. Any shape codegen would
reject (missing function name, a source shape the generator raises on)
binds nothing: ``None``, never a guess.

The custom-sink ``module_path`` and ForEachBatch ``batch_handler`` bodies
intentionally have NO builder here: their codegen
(``generators/write/sinks/custom_sink.py`` / ``foreachbatch_sink.py``) has
no parameters mechanism at all (class-based sink / inlined function body),
so those bodies always parse with ``bindings=None``.
"""

from typing import Any, Optional

from lhp.models import Action, TransformType

from ._bindings import DictValue, ParameterBindings, bound_from_yaml

__all__ = [
    "python_load_bindings",
    "snapshot_cdc_bindings",
    "transform_bindings",
]


def transform_bindings(action: Action) -> Optional[ParameterBindings]:
    """Bindings for a python-transform body.

    Mirrors ``templates/transform/python.py.j2``: the template calls
    ``function_name(...)`` with the YAML ``parameters`` dict as a single
    POSITIONAL argument — index 2 when the action has at least one source
    view (``fn(df_or_dataframes, spark, parameters)``, template lines
    17/20), index 1 when it has none (``fn(spark, parameters)``, line 25).
    The template renders and passes ``parameters`` regardless of whether
    YAML declared any, so an empty DictValue is the honest model when
    absent. Any shape codegen would reject (non-python transform, missing
    ``function_name``, a source shape
    ``_extract_source_views_from_action_source`` raises VAL_014 on) binds
    nothing — never speculate.
    """
    if getattr(action, "transform_type", None) != TransformType.PYTHON:
        return None
    function_name = getattr(action, "function_name", None)
    if not isinstance(function_name, str) or not function_name:
        return None
    source = getattr(action, "source", None)
    if isinstance(source, str):
        has_source_views = True
    elif isinstance(source, list):
        has_source_views = bool(source)
    else:
        # None / dict source: codegen raises VAL_014 before any call is
        # generated — there is no runtime call to mirror.
        return None
    dict_value = bound_from_yaml(getattr(action, "parameters", None) or {})
    if not isinstance(dict_value, DictValue):
        return None
    return ParameterBindings(
        function_name=function_name,
        dict_arg_index=2 if has_source_views else 1,
        dict_value=dict_value,
    )


def python_load_bindings(source: dict[str, Any]) -> Optional[ParameterBindings]:
    """Bindings for a python-load body.

    Mirrors ``generators/load/python.py`` (lines 46-47: ``function_name``
    defaults to ``"get_df"``) + ``templates/load/python.py.j2`` line 6:
    ``fn(spark, parameters)`` — the parameters dict is always passed as the
    single positional argument at index 1, even when YAML declared none.
    """
    function_name = source.get("function_name", "get_df")
    if not isinstance(function_name, str) or not function_name:
        return None
    dict_value = bound_from_yaml(source.get("parameters") or {})
    if not isinstance(dict_value, DictValue):
        return None
    return ParameterBindings(
        function_name=function_name, dict_arg_index=1, dict_value=dict_value
    )


def snapshot_cdc_bindings(
    source_function: dict[str, Any],
) -> Optional[ParameterBindings]:
    """Bindings for a snapshot_cdc ``source_function`` body.

    Mirrors ``generators/write/snapshot_cdc_source_function.py`` +
    ``streaming_table.py::_build_source_expression`` (~302-316): declared
    parameters are applied as KEYWORD arguments via ``functools.partial``.
    With no (or empty) parameters codegen emits the bare function reference
    — ``resolve_source_function`` maps empty params to ``None`` and
    ``_build_source_expression`` returns the plain qualified name, so NO
    kwargs are applied at runtime. An empty kwonly DictValue seeds nothing,
    which is the honest mirror of that.
    """
    function_name = source_function.get("function")
    if not isinstance(function_name, str) or not function_name:
        return None
    parameters = source_function.get("parameters")
    if not parameters:
        return ParameterBindings(function_name=function_name, kwonly=DictValue({}))
    if not isinstance(parameters, dict):
        return None  # Shape codegen would reject — never speculate.
    bound = bound_from_yaml(parameters)
    if not isinstance(bound, DictValue):
        return None
    return ParameterBindings(function_name=function_name, kwonly=bound)
