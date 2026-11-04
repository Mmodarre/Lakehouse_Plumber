"""``load_action_expectations``: a data_quality action's rules, inline or in a file.

The data_quality generator and the materialized-view generator a data_quality
action fuses onto both read the rules through it. Real files on disk.
"""

from pathlib import Path

import pytest
import yaml

from lhp.core.loaders import load_action_expectations
from lhp.errors import LHPError
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit

_FILE_RULES = [{"name": "from_file", "constraint": "a > 0", "failureAction": "warn"}]
_INLINE_RULES = [{"name": "inline", "constraint": "b > 0", "failureAction": "drop"}]


def _action(**fields) -> Action:
    return Action(
        name="dq",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.DATA_QUALITY,
        source=fields.pop("source", "v_in"),
        target="v_out",
        **fields,
    )


@pytest.fixture
def root(tmp_path: Path) -> Path:
    (tmp_path / "rules.yaml").write_text(yaml.safe_dump(_FILE_RULES))
    return tmp_path


def test_it_reads_the_actions_file(root):
    action = _action(expectations_file="rules.yaml")

    assert load_action_expectations(action, root) == _FILE_RULES


def test_inline_rules_win_over_the_file(root):
    action = _action(expectations=_INLINE_RULES, expectations_file="rules.yaml")

    assert load_action_expectations(action, root) == _INLINE_RULES


def test_a_source_mappings_rules_win_over_the_file(root):
    source = {"view": "v_in", "expectations": _INLINE_RULES}
    action = _action(source=source, expectations_file="rules.yaml")

    assert load_action_expectations(action, root) == _INLINE_RULES


def test_an_action_with_no_rules_has_none(root):
    assert load_action_expectations(_action(), root) == {}


def test_a_missing_file_raises_the_loaders_error(root):
    with pytest.raises(LHPError):
        load_action_expectations(_action(expectations_file="nope.yaml"), root)
