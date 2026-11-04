"""Plan which data_quality actions fuse into a materialized view.

Pure: reads the flowgroup's actions (and the SQL bodies the batch check
consults), writes nothing. Code generation computes the plan once per
flowgroup and hands each materialized-view generator its :class:`FusedDq`
through the generation context. The eligibility rules are
:func:`lhp.core.validators.is_fusable`, shared with the expectation-name
check.

:stability: internal
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Mapping, Optional, Sequence, Tuple

from lhp.models import Action, ActionType, TransformType

if TYPE_CHECKING:
    from ..processing import EnhancedSubstitutionManager
    from ..registry import ActionRegistry


@dataclass(frozen=True)
class FusedDq:
    """A data_quality action whose expectations move onto ``write_action``.

    ``source_view`` is the view the data_quality action reads, which the
    materialized view reads in its place. ``metadata_columns`` are the
    operational-metadata columns the data_quality action's own generator
    renders (``{}`` when none) and ``metadata_imports`` the imports it needs
    for them; the materialized view renders exactly these.
    """

    dq_action: Action
    write_action: Action
    source_view: str
    metadata_columns: Mapping[str, str] = field(default_factory=dict)
    metadata_imports: Tuple[str, ...] = ()


@dataclass(frozen=True)
class DqFusionPlan:
    """The fusions of one flowgroup, in the order its actions are listed."""

    fusions: Tuple[FusedDq, ...] = ()

    def for_write(self, write_action: Action) -> Optional[FusedDq]:
        """The fusion onto the write named like ``write_action``, if any.

        Matched by name: code generation hands the generator a copy of the
        write.
        """
        return next(
            (f for f in self.fusions if f.write_action.name == write_action.name),
            None,
        )


def plan_dq_fusion(
    flowgroup_actions: Sequence[Action],
    *,
    registry: Optional["ActionRegistry"] = None,
    project_root: Optional[Path] = None,
    substitution_mgr: Optional["EnhancedSubstitutionManager"] = None,
) -> DqFusionPlan:
    """The data_quality actions of ``flowgroup_actions`` that fuse, each with
    the materialized view it fuses into."""
    # Lazy: lhp.core.validators imports from codegen.
    from ..validators import dq_source_view, fusion_consumer, is_fusable

    fusions = []
    for action in flowgroup_actions:
        if (
            action.type != ActionType.TRANSFORM
            or action.transform_type != TransformType.DATA_QUALITY
        ):
            continue
        if not is_fusable(
            action,
            flowgroup_actions,
            registry=registry,
            project_root=project_root,
            substitution_mgr=substitution_mgr,
        ):
            continue
        consumer = fusion_consumer(
            action,
            flowgroup_actions,
            project_root=project_root,
            substitution_mgr=substitution_mgr,
        )
        if consumer is not None:
            fusions.append(FusedDq(action, consumer, dq_source_view(action)))
    return DqFusionPlan(tuple(fusions))
