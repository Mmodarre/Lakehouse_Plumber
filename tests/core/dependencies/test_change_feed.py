"""``is_change_feed_read``: whether a delta load's options read the change feed."""

from __future__ import annotations

import pytest

from lhp.core.dependencies import is_change_feed_read

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("value", [True, "true", "True", "TRUE", "tRuE"])
def test_true_values_read_the_change_feed(value):
    assert is_change_feed_read({"readChangeFeed": value}) is True


@pytest.mark.parametrize("value", [False, "false", "False", "", None, " true", "yes"])
def test_other_values_do_not(value):
    assert is_change_feed_read({"readChangeFeed": value}) is False


def test_absent_option_does_not():
    assert is_change_feed_read({"skipChangeCommits": "true"}) is False
    assert is_change_feed_read({}) is False
