"""Unit tests for the ``flowgroup_has_uc_tags`` retention gate.

The pool uses this predicate to decide whether to keep a resolved flowgroup for
the commit-time tagging hook when ``include_tests`` is False. ``tags_file`` is
resolved only at commit time, so it must count as a tag source here — otherwise
a file-only-tag flowgroup is silently dropped.
"""

import pytest

from lhp.core.codegen.uc_tagging import flowgroup_has_uc_tags
from lhp.models import Action, ActionType, FlowGroup


def _fg(write_target):
    action = Action(
        name="w",
        type=ActionType.WRITE,
        source="v_src",
        write_target=write_target,
    )
    return FlowGroup(pipeline="p1", flowgroup="fg1", actions=[action])


def _st(**extra):
    base = {
        "type": "streaming_table",
        "catalog": "prod",
        "schema": "sales",
        "table": "orders",
        "create_table": True,
    }
    base.update(extra)
    return base


@pytest.mark.unit
class TestFlowgroupHasUCTags:
    def test_tags_only_true(self):
        assert flowgroup_has_uc_tags(_fg(_st(tags={"team": "x"}))) is True

    def test_tags_file_only_true(self):
        assert flowgroup_has_uc_tags(_fg(_st(tags_file="tags/orders.yaml"))) is True

    def test_neither_false(self):
        assert flowgroup_has_uc_tags(_fg(_st())) is False
