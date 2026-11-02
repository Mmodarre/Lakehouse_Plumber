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
        **{
            "name": "dq",
            "type": "transform",
            "transform_type": "data_quality",
            "source": "v_candidates",
            "target": "v_candidates_dq",
            "expectations_file": "expectations/typo.yaml",
            **fields,
        }
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
    assert error.context["Expectation"] == "reconciles"


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


class TestInlineRules:
    """Inline rules win over the file in the generator, so they are checked too."""

    @pytest.fixture
    def valid_file(self, tmp_path):
        (tmp_path / "expectations").mkdir()
        (tmp_path / "expectations" / "typo.yaml").write_text(
            yaml.dump({"id > 0": {"action": "drop", "name": "valid_id"}})
        )
        return tmp_path

    def test_inline_expectations_typo_is_reported(self, valid_file):
        action = _dq(
            expectations=[
                {"name": "bad", "constraint": "y > 0", "failureAction": "fial"}
            ]
        )

        [error] = validate_data_quality_transform(action, _PREFIX, valid_file)

        assert isinstance(error, LHPError)
        assert error.code == "LHP-VAL-006"
        assert error.details == (
            f"{_PREFIX}: expectation 'bad' in the action's 'expectations' has "
            "invalid value 'fial' for 'failureAction'. Valid values: drop, fail, warn"
        )

    def test_flowgroup_validation_reports_the_inline_typo(self, valid_file):
        dq = _dq(
            expectations=[
                {"name": "bad", "constraint": "y > 0", "failureAction": "fial"}
            ]
        )
        flowgroup = FlowGroup(
            pipeline="p",
            flowgroup="inline",
            actions=[
                {
                    "name": "load_src",
                    "type": "load",
                    "source": {
                        "type": "delta",
                        "catalog": "c",
                        "schema": "s",
                        "table": "t",
                    },
                    "target": "v_candidates",
                },
                dq.model_dump(exclude_none=True),
            ],
        )

        errors = ConfigValidator(project_root=valid_file).validate_flowgroup(flowgroup)

        assert [e.code for e in errors if isinstance(e, LHPError)] == ["LHP-VAL-006"]

    def test_source_expectations_are_checked(self, valid_file):
        action = _dq(source={"view": "v_candidates", "expectations": {"": {}}})

        [error] = validate_data_quality_transform(action, _PREFIX, valid_file)

        assert error.code == "LHP-VAL-001"
        assert "in the action's 'source.expectations'" in error.details

    def test_source_expectations_with_unexpected_format(self, valid_file):
        action = _dq(source={"view": "v_candidates", "expectations": "id > 0"})

        assert validate_data_quality_transform(action, _PREFIX, valid_file) == [
            f"{_PREFIX}: The expectations in the action's 'source.expectations' "
            "have unexpected format"
        ]

    def test_empty_inline_rules_in_quarantine_mode(self, valid_file):
        action = _dq(
            mode="quarantine",
            quarantine={"dlq_table": "c.s.dlq", "source_table": "c.s.src"},
            source={"view": "v_candidates", "expectations": []},
        )

        assert validate_data_quality_transform(action, _PREFIX, valid_file) == [
            f"{_PREFIX}: Quarantine mode requires at least one expectation, but "
            "the action's 'source.expectations' contains none. An empty "
            "expectations set would produce an invalid inverse filter."
        ]

    def test_valid_inline_rules_pass(self, valid_file):
        action = _dq(
            expectations=[
                {"name": "ok", "constraint": "y > 0", "failureAction": "warn"}
            ]
        )

        assert validate_data_quality_transform(action, _PREFIX, valid_file) == []
