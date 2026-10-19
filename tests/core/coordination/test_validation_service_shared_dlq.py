"""``ValidationService.validate_cross_flowgroup`` runs the shared-DLQ check.

The service is the single composition site of the cross-flowgroup validators
(§9.24). These tests drive it with real models to pin that the
``LHP-VAL-069`` validator is wired in and honours ``pipeline_filter``.
"""

from pathlib import Path
from typing import List

import pytest

from lhp.core.coordination.validation_service import ValidationService
from lhp.models import Action, ActionType, FlowGroup, QuarantineConfig, TransformType

pytestmark = pytest.mark.unit

DLQ = "cat.sch.orders_dlq"
SOURCE = "cat.sch.orders"


def _quarantine_flowgroup(pipeline: str, flowgroup: str) -> FlowGroup:
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=flowgroup,
        actions=[
            Action(
                name=f"dq_{flowgroup}",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.DATA_QUALITY,
                source=f"v_{flowgroup}_raw",
                target=f"v_{flowgroup}_validated",
                expectations_file="expectations/rules.yaml",
                mode="quarantine",
                quarantine=QuarantineConfig(dlq_table=DLQ, source_table=SOURCE),
            )
        ],
    )


def _flowgroups_sharing_a_dlq_slice() -> List[FlowGroup]:
    return [
        _quarantine_flowgroup("A", "orders_us"),
        _quarantine_flowgroup("A", "orders_eu"),
    ]


def test_shared_dlq_and_source_table_in_one_pipeline_is_reported(tmp_path: Path):
    service = ValidationService(tmp_path)

    result = service.validate_cross_flowgroup(
        _flowgroups_sharing_a_dlq_slice(), pipeline_filter="A"
    )

    assert len(result.shared_dlq_errors) == 1
    assert "orders_us.dq_orders_us" in result.shared_dlq_errors[0]
    assert "orders_eu.dq_orders_eu" in result.shared_dlq_errors[0]
    assert result.table_creation_errors == []
    assert result.cdc_fanin_errors == []


def test_pipeline_filter_excludes_flowgroups_of_other_pipelines(tmp_path: Path):
    service = ValidationService(tmp_path)

    result = service.validate_cross_flowgroup(
        _flowgroups_sharing_a_dlq_slice(), pipeline_filter="B"
    )

    assert result.shared_dlq_errors == []
