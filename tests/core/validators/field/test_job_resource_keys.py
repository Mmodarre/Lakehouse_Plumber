"""Unit tests for job_config keys LHP also writes into job resources (#351).

LHP writes ``name`` and ``tasks`` into every orchestration and monitoring job
resource. ``tasks`` are appended after the generated tasks, so their shape is
checked; ``name`` is rejected. Every failure is ``LHP-CFG-072``.
"""

from __future__ import annotations

import pytest

from lhp.core.validators import validate_job_resource_keys
from lhp.errors import LHPConfigError

pytestmark = pytest.mark.unit


def _job_error(config, generated=("bronze_pipeline",)) -> LHPConfigError:
    with pytest.raises(LHPConfigError) as exc_info:
        validate_job_resource_keys(
            config, job_name="my_job", generated_task_keys=generated
        )
    assert exc_info.value.code == "LHP-CFG-072"
    return exc_info.value


class TestJobResourceKeys:
    def test_config_without_generated_keys_passes(self):
        validate_job_resource_keys(
            {"max_concurrent_runs": 1},
            job_name="my_job",
            generated_task_keys=["bronze_pipeline"],
        )

    def test_user_tasks_pass(self):
        validate_job_resource_keys(
            {
                "tasks": [
                    {
                        "task_key": "notify",
                        "depends_on": [{"task_key": "bronze_pipeline"}],
                        "notebook_task": {"notebook_path": "/Workspace/n"},
                    }
                ]
            },
            job_name="my_job",
            generated_task_keys=["bronze_pipeline"],
        )

    def test_name_rejected(self):
        error = _job_error({"name": "Pretty name"})
        assert "'name'" in str(error)

    def test_tasks_not_a_list_rejected(self):
        error = _job_error({"tasks": {"task_key": "notify"}})
        assert "list" in str(error)

    @pytest.mark.parametrize(
        "task", ["notify", {}, {"task_key": ""}, {"task_key": 7}, {"notebook_task": {}}]
    )
    def test_task_without_usable_task_key_rejected(self, task):
        error = _job_error({"tasks": [task]})
        assert "task_key" in str(error)

    def test_task_key_colliding_with_generated_task_rejected(self):
        error = _job_error(
            {"tasks": [{"task_key": "bronze_pipeline"}]},
            generated=["bronze_pipeline", "silver_pipeline"],
        )
        assert "bronze_pipeline" in str(error)

    def test_duplicate_user_task_keys_rejected(self):
        error = _job_error({"tasks": [{"task_key": "notify"}, {"task_key": "notify"}]})
        assert "notify" in str(error)
