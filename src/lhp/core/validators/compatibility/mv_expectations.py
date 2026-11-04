"""Expectation names on a materialized view (``LHP-VAL-071``).

The ``@dp.expect_all*`` decorators map each expectation name to one
constraint, so two rules with one name cannot both be generated. A materialized
view takes its expectations from up to two places: its own
``write_target.expectations_file`` and the data_quality action fused onto it
(:func:`~.mv_fusion.is_fusable`). This reports a name that more than one of
those rules gets, including a repeat inside the fused action's own rules. A
repeat inside the view's own file is reported while that file is validated.
"""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from lhp.models import Action, ActionType, TransformType

from ....errors import ErrorFactory, LHPError, codes
from ...loaders import Expectations, load_action_expectations, load_expectations_file
from ...processing import expectation_names
from ..field import validate_expectation_rules
from .mv_fusion import fusion_consumer, is_fusable

if TYPE_CHECKING:
    from lhp.models import FlowGroup

    from ...registry import ActionRegistry

logger = logging.getLogger(__name__)


class MvExpectationsValidator:
    """Check that the expectations a materialized view receives have unique names."""

    def __init__(
        self,
        registry: Optional[ActionRegistry] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        self.registry = registry
        self.project_root = project_root

    def validate(self, flowgroup: FlowGroup) -> List[LHPError]:
        actions = flowgroup.actions
        errors: List[LHPError] = []
        for dq in actions:
            if (
                dq.type != ActionType.TRANSFORM
                or dq.transform_type != TransformType.DATA_QUALITY
            ):
                continue
            if not is_fusable(
                dq, actions, registry=self.registry, project_root=self.project_root
            ):
                continue
            view = fusion_consumer(dq, actions, project_root=self.project_root)
            if view is not None:
                errors.extend(self._shared_names(dq, view))
        return errors

    def _shared_names(self, dq: Action, view: Action) -> List[LHPError]:
        own = self._view_rules(view)
        fused = self._fused_rules(dq)
        if fused is None:
            return []
        counts = Counter(own) + Counter(fused)
        return [
            _error(dq, view, name, counts[name], name in own)
            for name in dict.fromkeys(fused)
            if counts[name] > 1
        ]

    def _view_rules(self, view: Action) -> List[str]:
        """Names in the view's own file; ``[]`` when there is none to read."""
        write_target = view.write_target if isinstance(view.write_target, dict) else {}
        path = write_target.get("expectations_file")
        if not path or self.project_root is None:
            return []
        return (
            self._names(lambda: load_expectations_file(path, self.project_root)) or []
        )

    def _fused_rules(self, dq: Action) -> Optional[List[str]]:
        """Names of the fused action's rules; ``None`` when they cannot be read
        (the action's own validation reports that)."""
        root = self.project_root or Path.cwd()
        return self._names(lambda: load_action_expectations(dq, root))

    def _names(self, load) -> Optional[List[str]]:
        try:
            rules: Expectations = load()
            # Rules that are malformed are reported by their own validation.
            if validate_expectation_rules(rules, "the rules", "MV"):
                return None
            return expectation_names(rules)
        except (LHPError, AssertionError, OSError) as e:
            logger.debug(f"Expectation names not checked: {e}")
            return None


def _error(dq: Action, view: Action, name: str, count: int, in_view: bool) -> LHPError:
    where = (
        "the view's expectations file and the rules of" if in_view else "the rules of"
    )
    return ErrorFactory.validation_error(
        codes.VAL_071,
        title="Duplicate expectation name",
        details=(
            f"{count} expectations that materialized view '{view.name}' receives "
            f"are named '{name}': {where} data_quality action '{dq.name}', which "
            "is fused onto the view because the view only reads it. An "
            "expectation name must be unique for a dataset."
        ),
        suggestions=[
            "Give each expectation its own name",
            f"Rename the rule in '{dq.name}' or in the view's expectations file",
        ],
        context={
            "Action": dq.name,
            "Materialized View": view.name,
            "Expectation": name,
            "Field": "name",
            "Provided": name,
        },
    )
