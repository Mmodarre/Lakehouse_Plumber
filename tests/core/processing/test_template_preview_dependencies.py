"""Template preview guards and fingerprints a materialized view's expectations file.

``WriteActionValidator`` reads ``write_target.expectations_file``, so preview
must containment-check it before validation runs and record it as a saved
dependency.
"""

import pytest
import yaml

from lhp.core.processing.template_preview_dependencies import (
    DependencySnapshot,
    PreviewConfigValidator,
)
from lhp.generators.registration import register_all
from lhp.models import FlowGroup

pytestmark = pytest.mark.unit


def _flowgroup(expectations_file: str) -> FlowGroup:
    return FlowGroup(
        pipeline="p_gold",
        flowgroup="f_gate",
        actions=[
            {
                "name": "write_gate",
                "type": "write",
                "write_target": {
                    "type": "materialized_view",
                    "catalog": "main",
                    "schema": "gold",
                    "table": "gate",
                    "sql": "SELECT 1 AS id",
                    "expectations_file": expectations_file,
                },
            }
        ],
    )


@pytest.fixture
def root(tmp_path):
    register_all()
    project = (tmp_path / "project").resolve()
    (project / "expectations").mkdir(parents=True)
    return project


def test_expectations_file_is_a_watched_dependency(root):
    (root / "expectations" / "gate.yaml").write_text(
        yaml.safe_dump([{"name": "valid_id", "constraint": "id IS NOT NULL"}])
    )
    snapshot = DependencySnapshot(root)

    errors = PreviewConfigValidator(snapshot, None).validate_flowgroup(
        _flowgroup("expectations/gate.yaml")
    )

    assert errors == []
    assert "expectations/gate.yaml" in [view["path"] for view in snapshot.views()]


def test_file_outside_the_project_is_rejected_before_it_is_read(root, tmp_path):
    outside = tmp_path / "outside.yaml"
    outside.write_text("SECRET_CONTENT")
    (root / "expectations" / "gate.yaml").symlink_to(outside)
    snapshot = DependencySnapshot(root)

    with pytest.raises(PermissionError, match="inside the project"):
        PreviewConfigValidator(snapshot, None).validate_flowgroup(
            _flowgroup("expectations/gate.yaml")
        )

    assert snapshot.views() == []
