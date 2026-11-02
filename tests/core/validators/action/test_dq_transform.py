"""``validate_data_quality_transform`` checks the expectations file in dqe mode too.

Only quarantine mode used to read the file at validate time, so a dqe-mode
typo (``action: fial``) surfaced as a silent warn expectation. Real files on
disk, no mocks.
"""

import pytest
import yaml

from lhp.core.validators import ConfigValidator
from lhp.core.validators.action._dq_transform import validate_data_quality_transform
from lhp.errors import LHPError
from lhp.models import Action, FlowGroup

pytestmark = pytest.mark.unit

_PREFIX = "Action[1] 'dq'"


def _dq(**fields) -> Action:
    return Action(
        name="dq",
        type="transform",
        transform_type="data_quality",
        source="v_candidates",
        target="v_candidates_dq",
        expectations_file="expectations/typo.yaml",
        **fields,
    )


@pytest.fixture
def project(tmp_path):
    (tmp_path / "expectations").mkdir()
    (tmp_path / "expectations" / "typo.yaml").write_text(
        yaml.dump({"reconciled = true": {"action": "fial", "name": "reconciles"}})
    )
    return tmp_path


@pytest.mark.parametrize("mode", [None, "dqe"], ids=["default", "dqe"])
def test_dqe_mode_reports_a_bad_rule(project, mode):
    [error] = validate_data_quality_transform(_dq(mode=mode), _PREFIX, project)

    assert isinstance(error, LHPError)
    assert error.code == "LHP-VAL-006"
    assert error.context["Expectation"] == "'reconciles'"


def test_dqe_mode_reports_a_missing_file(tmp_path):
    [error] = validate_data_quality_transform(_dq(), _PREFIX, tmp_path)

    assert error.startswith(f"{_PREFIX}: Failed to load expectations file: ")
    assert "LHP-IO-001" in error


def test_unknown_project_root_skips_the_file():
    assert validate_data_quality_transform(_dq(), _PREFIX, None) == []


def test_flowgroup_validation_surfaces_the_rule_error(project):
    flowgroup = FlowGroup(
        pipeline="p_f",
        flowgroup="f_dq_action_typo",
        actions=[
            {
                "name": "load_src",
                "type": "load",
                "readMode": "stream",
                "source": {
                    "type": "delta",
                    "catalog": "c",
                    "schema": "s",
                    "table": "t",
                },
                "target": "v_candidates",
            },
            _dq().model_dump(exclude_none=True),
            {
                "name": "write_st",
                "type": "write",
                "source": "v_candidates_dq",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "c",
                    "schema": "s",
                    "table": "gated",
                },
            },
        ],
    )

    errors = ConfigValidator(project_root=project).validate_flowgroup(flowgroup)

    assert [e.code for e in errors if isinstance(e, LHPError)] == ["LHP-VAL-006"]
    assert [e for e in errors if not isinstance(e, LHPError)] == []
