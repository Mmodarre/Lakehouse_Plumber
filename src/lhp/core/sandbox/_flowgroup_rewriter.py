"""Structured-model sandbox rewrite pass over a single resolved flowgroup.

Runs on RESOLVED flowgroups (post-substitution) inside pool workers, for both
generate and validate, BEFORE codegen and dependency analysis — so grouping
keys, the cross-flowgroup barrier, and validation all see the rewritten names.

The pass rewrites only STRUCTURED table-reference sites; inline ``sql``
fields are owned uniformly by the separate TEXT pass. The sites are:

- WRITE ``write_target.table`` for ST/MV-style catalog/schema/table triples
  (Pydantic :class:`~lhp.models.WriteTarget` model or raw dict — both arrive
  as dicts after ``model_dump`` and re-validate back to their original form).
- WRITE delta-sink ``write_target.options.tableName`` (only ``sink_type:
  delta``; path-based and kafka/custom/foreachbatch sinks have no table
  identity).
- WRITE snapshot-CDC ``write_target.snapshot_cdc_config.source`` (a dotted
  table-ref string; ``source_function`` variants are files, not tables, and
  are untouched).
- LOAD delta ``source.catalog/schema/table`` triples.
- ``action.source`` entries that are strings (or strings inside a list) and
  canonically match the rename set. Bare view names (no dots) can never
  match: the producer index holds only 2-/3-part keys and 1-part refs are
  rejected by the matcher.
- TEST ``reference`` / ``lookup_table`` dotted-ref fields (``source`` is
  covered by the generic ``action.source`` rule above).
- PARAMETER-BINDING containers: table names passed as YAML ``parameters``
  into user Python code, which renders them into generated reads
  (``spark.read.table(param)``) that the per-site rewrites above never
  reach. There are exactly three — kept in lock-step with
  :mod:`lhp.core.dependencies._binding_rules` (the authoritative mirror of
  codegen's call conventions): the python-transform ``action.parameters``
  dict (``transform_type == python``), the python-load ``source.parameters``
  dict (``source.type == python``), and the snapshot-CDC
  ``write_target.snapshot_cdc_config.source_function.parameters`` kwargs.
  Each container is walked recursively; only string values that canonically
  match the rename set change, so arbitrary parameter strings are safe.

``depends_on`` is NEVER rewritten — it feeds the dependency DAG only and
never appears in generated code.

Every match decision goes through :func:`match_renamed_table` (the ONE
canonicalization) and every replacement leaf through :func:`rename_parts`
(the choke point). Rewrites keep each ref's part count and the original
spelling of catalog/schema; only the table LEAF changes, re-emitted with the
site's original backtick style.
"""

from typing import Any, Dict

from lhp.models import (
    ActionType,
    FlowGroup,
    LoadSourceType,
    TransformType,
    WriteTargetType,
)

from ._renames import (
    SandboxTableRenames,
    TableRenameStrategy,
    match_renamed_table,
    rename_parts,
)

#: Dotted-ref string fields on TEST actions (``source`` is handled by the
#: generic ``action.source`` pass shared with the other action types).
_TEST_REF_FIELDS = ("reference", "lookup_table")


def rewrite_flowgroup_tables(
    flowgroup: FlowGroup, renames: SandboxTableRenames
) -> FlowGroup:
    """Return a NEW flowgroup with in-scope table refs renamed.

    The input is never mutated: the pass walks ``flowgroup.model_dump()``
    (a deep copy) and re-validates via ``FlowGroup(**data)``, so the result
    is a fully valid model. Out-of-scope refs — anything
    :func:`match_renamed_table` resolves to ``None``, including ambiguous
    2-part refs and bare view names — pass through byte-identical.

    An empty rename set short-circuits to the input itself (identity).
    """
    if not renames.table_producers:
        return flowgroup
    data = flowgroup.model_dump()
    for action in data.get("actions") or []:
        _rewrite_action(action, renames)
    return FlowGroup(**data)


def _rewrite_action(action: Dict[str, Any], renames: SandboxTableRenames) -> None:
    """Rewrite one dumped action dict in place (``depends_on`` untouched)."""
    _rewrite_source_refs(action, renames)

    action_type = action.get("type")
    if action_type == ActionType.WRITE:
        write_target = action.get("write_target")
        if isinstance(write_target, dict):
            _rewrite_write_target(write_target, renames)
    elif action_type == ActionType.TRANSFORM:
        # Python transform: the template passes action.parameters positionally
        # to the user function (see _binding_rules.transform_bindings).
        if action.get("transform_type") == TransformType.PYTHON:
            params = action.get("parameters")
            if isinstance(params, dict):
                _rewrite_parameter_values(params, renames)
    elif action_type == ActionType.LOAD:
        source = action.get("source")
        if isinstance(source, dict):
            source_type = source.get("type")
            if source_type == LoadSourceType.DELTA:
                _rewrite_table_triple(source, renames)
            elif source_type == LoadSourceType.PYTHON:
                # Python load: source.parameters is passed positionally to the
                # user function (see _binding_rules.python_load_bindings).
                params = source.get("parameters")
                if isinstance(params, dict):
                    _rewrite_parameter_values(params, renames)
    elif action_type == ActionType.TEST:
        for field in _TEST_REF_FIELDS:
            value = action.get(field)
            if isinstance(value, str) and value:
                action[field] = _maybe_rewrite_ref(value, renames)


def _rewrite_source_refs(action: Dict[str, Any], renames: SandboxTableRenames) -> None:
    """Rewrite matching str / list[str] ``source`` entries in place.

    Non-string entries (dict configs) are owned by the per-type branches in
    :func:`_rewrite_action`; bare view names never match (1-part refs are
    rejected by the matcher), so view wiring is naturally preserved.
    """
    source = action.get("source")
    if isinstance(source, str):
        action["source"] = _maybe_rewrite_ref(source, renames)
    elif isinstance(source, list):
        action["source"] = [
            _maybe_rewrite_ref(entry, renames) if isinstance(entry, str) else entry
            for entry in source
        ]


def _rewrite_write_target(
    write_target: Dict[str, Any], renames: SandboxTableRenames
) -> None:
    """Rewrite a write_target dict in place: triple, delta sink, snapshot."""
    _rewrite_table_triple(write_target, renames)

    # Delta SINK destination: options.tableName is a dotted-ref string. Other
    # sink types have no table identity, even if an option spells "tableName".
    if (
        write_target.get("type") == WriteTargetType.SINK
        and write_target.get("sink_type") == "delta"
    ):
        options = write_target.get("options")
        if isinstance(options, dict):
            table_name = options.get("tableName")
            if isinstance(table_name, str) and table_name:
                options["tableName"] = _maybe_rewrite_ref(table_name, renames)

    # Snapshot CDC source table (plain-ref form only; source_function is a
    # file, not a table). Only dict-form write_targets can carry this key —
    # the WriteTarget model has no snapshot_cdc_config field.
    snapshot_config = write_target.get("snapshot_cdc_config")
    if isinstance(snapshot_config, dict):
        snapshot_source = snapshot_config.get("source")
        if isinstance(snapshot_source, str) and snapshot_source:
            snapshot_config["source"] = _maybe_rewrite_ref(snapshot_source, renames)

        # Snapshot-CDC source_function.parameters: applied as kwargs via
        # functools.partial into the user function (see
        # _binding_rules.snapshot_cdc_bindings). The source_function FILE/
        # function fields are untouched; only the parameter VALUES carry
        # table refs that render into reads.
        source_function = snapshot_config.get("source_function")
        if isinstance(source_function, dict):
            params = source_function.get("parameters")
            if isinstance(params, dict):
                _rewrite_parameter_values(params, renames)


def _rewrite_table_triple(config: Dict[str, Any], renames: SandboxTableRenames) -> None:
    """Rewrite a catalog/schema/table field triple in place (table leaf only).

    Mirrors the producer-side read (``catalog`` with deprecated ``database``
    fallback) so both sides of the match see the same ref. A catalog without
    a schema cannot form a valid 2-/3-part ref and is left untouched.
    """
    catalog = config.get("catalog") or config.get("database") or ""
    schema = config.get("schema") or ""
    table = config.get("table") or ""
    if not (isinstance(table, str) and table):
        return

    if catalog and schema:
        ref = f"{catalog}.{schema}.{table}"
    elif schema:
        ref = f"{schema}.{table}"
    elif catalog:
        return
    else:
        ref = table  # 1-part: can never match the 2-/3-part producer index

    if match_renamed_table(ref, renames) is not None:
        config["table"] = _rename_leaf(table, renames.strategy)


def _maybe_rewrite_ref(ref: str, renames: SandboxTableRenames) -> str:
    """Return ``ref`` with its table leaf renamed, or unchanged if exempt.

    Part count and the original spelling of every non-leaf part are
    preserved; only the leaf is re-emitted (with its backtick style kept).
    """
    if match_renamed_table(ref, renames) is None:
        return ref
    parts = ref.split(".")
    parts[-1] = _rename_leaf(parts[-1], renames.strategy)
    return ".".join(parts)


def _rewrite_parameter_values(
    params: Dict[str, Any], renames: SandboxTableRenames
) -> None:
    """Rewrite table-ref strings inside a YAML parameter container in place.

    Used for the three parameter-binding sites that feed user Python code
    (python transform, python load, snapshot-CDC source_function — see the
    module docstring and :mod:`lhp.core.dependencies._binding_rules`). The
    walk recurses through nested dicts and lists; each ``str`` value is run
    through :func:`_maybe_rewrite_ref`, so only values that canonically
    match an in-scope-produced 2-/3-part table ref change. All other scalars
    (int/bool/None/float) are left untouched.
    """
    for key, value in params.items():
        params[key] = _rewrite_parameter_value(value, renames)


def _rewrite_parameter_value(value: Any, renames: SandboxTableRenames) -> Any:
    """Rewrite one parameter value (recursing into lists/dicts)."""
    if isinstance(value, str):
        return _maybe_rewrite_ref(value, renames)
    if isinstance(value, list):
        return [_rewrite_parameter_value(item, renames) for item in value]
    if isinstance(value, dict):
        _rewrite_parameter_values(value, renames)
        return value
    return value


def _rename_leaf(leaf: str, strategy: TableRenameStrategy) -> str:
    """Rename one table leaf via the choke point, keeping its style.

    Backticks (and incidental padding) are stripped before formatting so the
    pattern receives the bare ORIGINAL spelling; a backticked site is
    re-emitted backticked.
    """
    stripped = leaf.replace("`", "").strip()
    renamed: str = rename_parts(strategy, None, None, stripped)[2]
    return f"`{renamed}`" if "`" in leaf else renamed
