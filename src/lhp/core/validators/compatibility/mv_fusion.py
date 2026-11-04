"""When a data_quality action's expectations move onto a materialized view.

Expectations on a view that only feeds a materialized view force the view to
recompute fully on every refresh, while the same expectations on the
materialized view itself keep it incremental. So code generation *fuses* a
data_quality action into the materialized view that reads it: the view gets
the action's expectations and reads the action's source view. The data_quality
view is still generated; nothing reads it.

A data_quality action fuses when all of these hold:

- it is in ``dqe`` mode (``quarantine`` recycles through a streaming flow);
- its effective read is a batch (:func:`effective_read_mode`);
- exactly one action of its flowgroup reads its view (a test action does not
  count: the data_quality view is still generated for it, so fusion does not
  depend on ``--include-tests``);
- that action is a materialized-view write whose ``source`` is the view alone
  (a string, or a one-element list; no ``sql``/``sql_path``), the only write to
  its table, and has no ``write_target.expectations_file`` of its own.

The last condition keeps the rules' meaning: unfused, the view's own rules see
only the rows the data_quality action's ``drop`` rules let through, and one
dataset cannot promise that order. A view with rules of its own keeps the
data_quality view as it is.

:func:`fusion_target` decides and names the view. Code generation
(``plan_dq_fusion``) and the expectation-name check
(``MvExpectationsValidator``) both ask it, so they cannot disagree.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Optional, Sequence

from lhp.models import Action, ActionType, TransformType

from ....errors import LHPError
from ...registry import determine_action_subtype

if TYPE_CHECKING:
    from ...processing import EnhancedSubstitutionManager
    from ...registry import ActionRegistry


def dq_source_view(dq_action: Action) -> str:
    """The view ``dq_action`` reads: its ``source``, or a mapping's ``view`` /
    ``source`` key; ``""`` when it names none.

    The one rule for which view a data_quality action reads: its generator,
    the read-mode inheritance and fusion all call it.
    """
    source = dq_action.source
    if isinstance(source, dict):
        source = source.get("view", source.get("source", ""))
    return source if isinstance(source, str) else ""


def fusion_target(
    dq_action: Action,
    flowgroup_actions: Sequence[Action],
    *,
    registry: ActionRegistry,
    project_root: Optional[Path] = None,
    substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
) -> Optional[Action]:
    """The materialized view ``dq_action``'s expectations move onto, else ``None``.

    ``flowgroup_actions`` are the actions of ``dq_action``'s flowgroup (see
    the module docstring for the conditions). ``registry``, ``project_root``
    and ``substitution_mgr`` are those of :func:`effective_read_mode`, which
    supplies the batch condition. View names compare case-insensitively, as in
    the read-mode lineage check.
    """
    if (
        dq_action.type != ActionType.TRANSFORM
        or dq_action.transform_type != TransformType.DATA_QUALITY
        or (dq_action.mode or "dqe") != "dqe"
        or not isinstance(dq_action.target, str)
        or not dq_action.target
        or not dq_source_view(dq_action)
    ):
        return None
    view = dq_action.target.casefold()
    if _count_producers(flowgroup_actions, view) != 1:
        return None
    readers = [
        a
        for a in flowgroup_actions
        if a is not dq_action
        and a.type != ActionType.TEST
        and view in _reads(a, project_root, substitution_mgr)
    ]
    if len(readers) != 1 or not _is_pass_through_view(
        readers[0], view, flowgroup_actions
    ):
        return None
    # Lazy: lhp.core.dependencies imports a service that imports validators.
    from ...dependencies import effective_read_mode

    read = effective_read_mode(
        dq_action,
        flowgroup_actions,
        registry=registry,
        project_root=project_root,
        substitution_mgr=substitution_mgr,
    )
    return readers[0] if read == "batch" else None


def _count_producers(actions: Sequence[Action], view: str) -> int:
    return sum(
        1
        for a in actions
        if a.type in (ActionType.LOAD, ActionType.TRANSFORM)
        and isinstance(a.target, str)
        and a.target.casefold() == view
    )


def _reads(
    action: Action,
    project_root: Optional[Path],
    substitution_mgr: Optional[EnhancedSubstitutionManager],
) -> set[str]:
    """The case-folded names ``action`` reads, SQL bodies included."""
    # Lazy: lhp.core.dependencies imports a service that imports validators.
    from ...dependencies import extract_action_sources, sql_source_reads

    names = {n.casefold() for n in extract_action_sources(action) if isinstance(n, str)}
    sql_reads = sql_source_reads(
        action, project_root=project_root, substitution_mgr=substitution_mgr
    )
    names.update(name.casefold() for name, _ in sql_reads or ())
    return names


def _sole_source(action: Action) -> Optional[str]:
    """The one view an action's ``source`` names: a string, or a one-element
    list (the materialized-view generator reads ``source[0]``)."""
    source = action.source
    if isinstance(source, list) and len(source) == 1:
        source = source[0]
    return source if isinstance(source, str) else None


def _is_pass_through_view(
    action: Action, view: str, flowgroup_actions: Sequence[Action]
) -> bool:
    """A materialized-view write that reads ``view`` alone, with no SQL and no
    expectations file of its own, and is the only write to its table."""
    # Lazy: lhp.core.dependencies imports a service that imports validators.
    from ...dependencies import write_table_key

    if action.type != ActionType.WRITE or not isinstance(action.write_target, dict):
        return False
    try:
        if determine_action_subtype(action) != "materialized_view":
            return False
    except LHPError:
        return False
    target = action.write_target
    if "sql" in target or "sql_path" in target or target.get("expectations_file"):
        return False
    source = _sole_source(action)
    if source is None or source.casefold() != view:
        return False
    key = write_table_key(action)
    return not any(
        a is not action and a.type == ActionType.WRITE and write_table_key(a) == key
        for a in flowgroup_actions
    )
