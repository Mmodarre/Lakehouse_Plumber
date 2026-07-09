"""Locate the SQL / Python bodies a single action carries.

Pure lookup helpers split out of :mod:`.source_parsing`: given one
``Action``, enumerate every place a parseable body can live (inline strings,
file references, snapshot-CDC source functions) together with the YAML
parameter bindings codegen would apply to that body. No file I/O, no
parsing — the :class:`~lhp.core.dependencies.source_parsing.SourceParser`
owns resolution and extraction.
"""

from collections.abc import Iterator
from typing import Any, Optional

from lhp.models import Action

from ._binding_rules import (
    python_load_bindings,
    snapshot_cdc_bindings,
    transform_bindings,
)
from ._bindings import ParameterBindings


def write_target_as_dict(action: Action) -> Optional[dict[str, Any]]:
    """Normalize ``action.write_target`` to a ``dict`` for uniform lookup.

    Returns ``None`` when the action has no write target. Handles both the
    Pydantic ``WriteTarget`` form and a raw dict (some code paths pre-dump
    it before reaching the analyzer).
    """
    wt = getattr(action, "write_target", None)
    if wt is None:
        return None
    if isinstance(wt, dict):
        return wt
    return wt.model_dump()


def iter_sql_bodies(action: Action) -> Iterator[tuple[Optional[str], Optional[str]]]:
    """Yield ``(inline_sql, sql_path)`` for every known SQL location.

    Covers:
      - ``action.sql`` / ``action.sql_path``
      - ``action.source["sql"]`` / ``action.source["sql_path"]`` (when
        ``source["type"] == "sql"``)
      - ``write_target["sql"]`` / ``write_target["sql_path"]``
        (materialized-view SQL)

    Either element of a yielded tuple may be ``None``; callers should
    treat each independently.
    """
    yield (
        getattr(action, "sql", None),
        getattr(action, "sql_path", None),
    )

    source = getattr(action, "source", None)
    if isinstance(source, dict) and source.get("type") == "sql":
        yield source.get("sql"), source.get("sql_path")

    wt = write_target_as_dict(action)
    if wt is not None:
        yield wt.get("sql"), wt.get("sql_path")


def iter_python_bodies(
    action: Action,
) -> Iterator[tuple[Optional[str], Optional[str], Optional[ParameterBindings]]]:
    """Yield ``(inline_python, file_path, bindings)`` for every Python location.

    Covers top-level ``action.module_path`` (Python transforms),
    python-load ``source["module_path"]``, ``write_target["module_path"]``
    (custom sinks), ``write_target["batch_handler"]`` (inline ForEachBatch
    code), and ``write_target["snapshot_cdc_config"]["source_function"]["file"]``.

    ``bindings`` carries the statically-known YAML parameter values for
    the body's entry function, mirroring exactly how codegen applies them
    (see the per-shape ``_*_bindings`` builders). Custom-sink and
    batch-handler bodies have NO parameters mechanism in codegen (the
    sink is class-based, the batch handler is an inlined function body —
    see ``generators/write/sinks/custom_sink.py`` /
    ``foreachbatch_sink.py``), so their bindings are always ``None``.
    """
    module_path = getattr(action, "module_path", None)
    if module_path:
        yield None, module_path, transform_bindings(action)

    source = getattr(action, "source", None)
    if isinstance(source, dict) and source.get("type") == "python":
        # Python-load actions reference their module via
        # ``source["module_path"]`` (generators/load/python.py). The file
        # resolves through the same YAML-dir-then-project-root logic as
        # every other shape (``_resolve_and_parse_file``).
        source_module_path = source.get("module_path")
        if source_module_path:
            yield None, source_module_path, python_load_bindings(source)

    wt = write_target_as_dict(action)
    if wt is None:
        return

    wt_module_path = wt.get("module_path")
    if wt_module_path:
        yield None, wt_module_path, None

    batch_handler = wt.get("batch_handler")
    if batch_handler:
        yield batch_handler, None, None

    cdc = wt.get("snapshot_cdc_config") or {}
    source_function = cdc.get("source_function") if isinstance(cdc, dict) else None
    if isinstance(source_function, dict):
        fn_file = source_function.get("file")
        if fn_file:
            yield None, fn_file, snapshot_cdc_bindings(source_function)
