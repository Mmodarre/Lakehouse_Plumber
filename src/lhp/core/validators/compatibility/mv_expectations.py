"""Expectation names of a fused data_quality action (``LHP-VAL-071``).

The ``@dp.expect_all*`` decorators map each expectation name to one
constraint, so two rules with one name cannot both be generated. When a
data_quality action is fused onto a materialized view
(:func:`~.mv_fusion.fusion_target`), its rules become the view's decorators.
This reports a name that more than one of the action's own rules gets. A
repeat inside a view's own ``expectations_file`` is reported while that file is
validated; a view with a file of its own is never a fusion target, so the two
sets of rules are never merged.
"""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Callable, List, Optional

from lhp.models import Action, ActionType, TransformType

from ....errors import ErrorFactory, LHPError, codes
from ...loaders import Expectations, load_action_expectations
from ...processing import expectation_names
from ..field import validate_expectation_rules
from .mv_fusion import fusion_target

if TYPE_CHECKING:
    from lhp.models import FlowGroup

    from ...processing import EnhancedSubstitutionManager
    from ...registry import ActionRegistry

logger = logging.getLogger(__name__)


class MvExpectationsValidator:
    """Check that a fused data_quality action's expectation names are unique."""

    def __init__(
        self, registry: ActionRegistry, project_root: Optional[Path] = None
    ) -> None:
        self.registry = registry
        self.project_root = project_root

    def validate(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: Optional[EnhancedSubstitutionManager] = None,
    ) -> List[LHPError]:
        """``substitution_mgr`` substitutes the SQL bodies the fusion decision
        reads, as code generation does."""
        actions = flowgroup.actions
        errors: List[LHPError] = []
        for dq in actions:
            if (
                dq.type != ActionType.TRANSFORM
                or dq.transform_type != TransformType.DATA_QUALITY
            ):
                continue
            view = fusion_target(
                dq,
                actions,
                registry=self.registry,
                project_root=self.project_root,
                substitution_mgr=substitution_mgr,
            )
            if view is not None:
                errors.extend(self._repeated_names(dq, view))
        return errors

    def _repeated_names(self, dq: Action, view: Action) -> List[LHPError]:
        names = self._fused_names(dq)
        if names is None:
            return []
        return [
            _error(dq, view, name, count)
            for name, count in Counter(names).items()
            if count > 1
        ]

    def _fused_names(self, dq: Action) -> Optional[List[str]]:
        """Names of the fused action's rules; ``None`` when they cannot be read
        (the action's own validation reports that)."""
        root = self.project_root or Path.cwd()
        return self._names(lambda: load_action_expectations(dq, root))

    def _names(self, load: Callable[[], Expectations]) -> Optional[List[str]]:
        try:
            rules = load()
            # Rules that are malformed are reported by their own validation.
            if validate_expectation_rules(rules, "the rules", "MV"):
                return None
            return expectation_names(rules)
        except (LHPError, OSError) as e:  # the action's own validation reports these
            logger.debug(f"Expectation names not checked: {e}")
            return None


def _error(dq: Action, view: Action, name: str, count: int) -> LHPError:
    return ErrorFactory.validation_error(
        codes.VAL_071,
        title="Duplicate expectation name",
        details=(
            f"{count} expectations of data_quality action '{dq.name}' are named "
            f"'{name}'. The action is fused onto materialized view '{view.name}' "
            "because the view only reads it, and an expectation name must be "
            "unique for a dataset."
        ),
        suggestions=[
            "Give each expectation its own name",
            f"Rename the repeated rule in '{dq.name}'",
        ],
        context={
            "Action": dq.name,
            "Materialized View": view.name,
            "Expectation": name,
            "Field": "name",
            "Provided": name,
        },
    )
