"""How the data_quality generator finds and reads its expectations.

The file is read through ``load_expectations_file``. ``lhp validate`` rejects a
malformed file first; if one reaches the generator anyway, the loader's
LHP-VAL-008 propagates instead of generating a view with no rules.
Real files on disk, no mocks.
"""

import pytest
import yaml

from lhp.errors import LHPValidationError
from lhp.generators.transform.data_quality import DataQualityTransformGenerator
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit

_RULES = {"id IS NOT NULL": {"action": "fail", "name": "id_check"}}


def _dq_action(**overrides) -> Action:
    fields = {
        "name": "validate",
        "type": ActionType.TRANSFORM,
        "transform_type": TransformType.DATA_QUALITY,
        "source": "v_src",
        "target": "v_out",
        "readMode": "stream",
        "expectations_file": "rules.yaml",
    }
    fields.update(overrides)
    return Action(**fields)


def test_relative_file_resolves_against_spec_dir(tmp_path):
    (tmp_path / "rules.yaml").write_text(yaml.dump(_RULES))

    code = DataQualityTransformGenerator().generate(
        _dq_action(), {"spec_dir": tmp_path}
    )

    assert '@dp.expect_all_or_fail({"id_check": "id IS NOT NULL"})' in code


def test_relative_file_resolves_against_cwd_without_spec_dir(tmp_path, monkeypatch):
    (tmp_path / "rules.yaml").write_text(yaml.dump(_RULES))
    monkeypatch.chdir(tmp_path)

    code = DataQualityTransformGenerator().generate(_dq_action(), {})

    assert '@dp.expect_all_or_fail({"id_check": "id IS NOT NULL"})' in code


@pytest.mark.parametrize(
    ("text", "actual_type"),
    [("just a string\n", "str"), ("expectations: null\n", "NoneType")],
    ids=["scalar-document", "wrapped-null"],
)
def test_malformed_file_raises_instead_of_generating_no_rules(
    tmp_path, text, actual_type
):
    (tmp_path / "rules.yaml").write_text(text)

    with pytest.raises(LHPValidationError) as exc_info:
        DataQualityTransformGenerator().generate(_dq_action(), {"spec_dir": tmp_path})

    assert exc_info.value.code == "LHP-VAL-008"
    assert exc_info.value.context["Actual Type"] == actual_type


def test_inline_expectations_take_precedence_over_the_file(tmp_path):
    (tmp_path / "rules.yaml").write_text(yaml.dump(_RULES))
    action = _dq_action(
        expectations=[
            {"name": "inline", "constraint": "x > 0", "failureAction": "drop"}
        ]
    )

    code = DataQualityTransformGenerator().generate(action, {"spec_dir": tmp_path})

    assert '@dp.expect_all_or_drop({"inline": "x > 0"})' in code
    assert "id_check" not in code
