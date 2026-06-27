"""Project-level configuration (lhp.yaml top-level model)."""

from typing import List, Optional

from pydantic import BaseModel, Field

from ._monitoring import EventLogConfig, MonitoringConfig
from ._operational_metadata import ProjectOperationalMetadataConfig
from ._test_reporting import TestReportingConfig
from ._uc_tagging import UCTaggingConfig


class WheelConfig(BaseModel):
    """Per-project wheel packaging configuration."""

    artifact_volume: Optional[str] = None


class ProjectConfig(BaseModel):
    """Project-level configuration loaded from lhp.yaml."""

    name: str
    version: str = "1.0"
    description: Optional[str] = None
    author: Optional[str] = None
    created_date: Optional[str] = None
    include: Optional[List[str]] = None
    blueprint_include: Optional[List[str]] = None
    instance_include: Optional[List[str]] = None
    operational_metadata: Optional[ProjectOperationalMetadataConfig] = None
    event_log: Optional[EventLogConfig] = None
    monitoring: Optional[MonitoringConfig] = None
    required_lhp_version: Optional[str] = None
    test_reporting: Optional[TestReportingConfig] = None
    uc_tagging: Optional[UCTaggingConfig] = None
    wheel: Optional[WheelConfig] = None
    apply_formatting: bool = Field(
        True,
        description=(
            "Whether to run the terminal code-formatting pass over generated "
            "Python files. Set to false to skip formatting for faster builds; "
            "the AST-parse validity guard (LHP-CFG-031) always runs regardless. "
            "The CLI --no-format flag overrides this key."
        ),
    )
