"""Project configuration loader for LakehousePlumber.

Loads project-level configuration from lhp.yaml including operational metadata definitions.
Sub-config parsing/validation lives in `_*_config_parser.py` modules in this package;
this class is a thin coordinator that wires them together.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from lhp.errors import ErrorFactory, LHPError, codes
from lhp.models import (
    EventLogConfig,
    MonitoringConfig,
    ProjectConfig,
)

from ._event_log_config_parser import parse_event_log_config
from ._include_patterns_parser import parse_include_patterns
from ._monitoring_config_parser import (
    _validate_monitoring_config as _validate_monitoring_config_impl,
)
from ._monitoring_config_parser import (
    parse_monitoring_config,
)
from ._operational_metadata_config_parser import parse_operational_metadata_config
from ._test_reporting_config_parser import parse_test_reporting_config
from ._uc_tagging_config_parser import parse_uc_tagging_config
from ._wheel_config_parser import parse_wheel_config


class ProjectConfigLoader:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        self.config_file = project_root / "lhp.yaml"

    def load_project_config(self) -> Optional[ProjectConfig]:
        """Load project configuration from lhp.yaml. Returns ``None`` if absent or empty."""
        if not self.config_file.exists():
            self.logger.info(
                f"No project configuration file found at {self.config_file}"
            )
            return None

        try:
            from ...parsers.yaml_loader import load_yaml_file

            config_data = load_yaml_file(
                self.config_file,
                allow_empty=False,
                error_context="project configuration file",
            )

            if not config_data:
                self.logger.warning(
                    f"Empty project configuration file: {self.config_file}"
                )
                return None

            project_config = self._parse_project_config(config_data)

            self.logger.info(f"Loaded project configuration from {self.config_file}")
            return project_config

        except LHPError:
            # Well-formed LHPError from a sub-parser — propagate the specific
            # code unchanged; never swallow/re-wrap.
            raise
        except ValueError as e:
            # yaml_loader converts YAML and file errors to ValueError with clear context
            error_msg = str(e)
            self.logger.exception(f"Project configuration loading failed: {error_msg}")

            if "Invalid YAML" in error_msg:
                raise ErrorFactory.config_error(
                    codes.CFG_001,
                    title="Invalid project configuration YAML",
                    details=error_msg,
                    suggestions=[
                        "Check YAML syntax in lhp.yaml",
                        "Ensure proper indentation and structure",
                        "Validate YAML online or with a linter",
                    ],
                ) from e

        except Exception as e:
            error_msg = (
                f"Error loading project configuration from {self.config_file}: {e}"
            )
            self.logger.exception(error_msg)
            raise ErrorFactory.config_error(
                codes.CFG_002,
                title="Project configuration loading failed",
                details=error_msg,
                suggestions=[
                    "Check file permissions and accessibility",
                    "Verify file is not corrupted",
                    "Check project configuration structure",
                ],
            ) from e

    def _parse_project_config(self, config_data: Dict[str, Any]) -> ProjectConfig:
        operational_metadata_config = None
        if "operational_metadata" in config_data:
            operational_metadata_config = parse_operational_metadata_config(
                config_data["operational_metadata"]
            )

        # blueprint_include / instance_include reuse the same shape/pattern
        # validation as `include`; defaults are applied at discovery time
        # inside BlueprintDiscoverer (one source of truth there).
        (
            include_patterns,
            blueprint_include_patterns,
            instance_include_patterns,
        ) = parse_include_patterns(config_data)

        event_log_config = None
        if "event_log" in config_data:
            event_log_config = parse_event_log_config(config_data["event_log"])

        # monitoring is cross-validated against event_log
        monitoring_config = None
        if "monitoring" in config_data:
            monitoring_config = self._parse_monitoring_config(
                config_data["monitoring"], event_log_config
            )

        test_reporting_config = None
        if "test_reporting" in config_data:
            test_reporting_config = parse_test_reporting_config(
                config_data["test_reporting"]
            )

        uc_tagging_config = None
        if "uc_tagging" in config_data:
            uc_tagging_config = parse_uc_tagging_config(config_data["uc_tagging"])

        wheel_config = None
        if "wheel" in config_data:
            wheel_config = parse_wheel_config(config_data["wheel"])

        return ProjectConfig(
            name=config_data.get("name", "unnamed_project"),
            version=config_data.get("version", "1.0"),
            description=config_data.get("description"),
            author=config_data.get("author"),
            created_date=config_data.get("created_date"),
            include=include_patterns,
            blueprint_include=blueprint_include_patterns,
            instance_include=instance_include_patterns,
            operational_metadata=operational_metadata_config,
            event_log=event_log_config,
            monitoring=monitoring_config,
            required_lhp_version=config_data.get("required_lhp_version"),
            test_reporting=test_reporting_config,
            uc_tagging=uc_tagging_config,
            wheel=wheel_config,
            apply_formatting=config_data.get("apply_formatting", True),
        )

    # Instance-method wrappers so unit tests can patch these on the class.
    def _parse_monitoring_config(
        self,
        monitoring_data: Any,
        event_log_config: Optional[EventLogConfig],
    ) -> MonitoringConfig:
        return parse_monitoring_config(
            monitoring_data, event_log_config, self.project_root
        )

    def _validate_monitoring_config(
        self,
        config: MonitoringConfig,
        event_log_config: Optional[EventLogConfig],
    ) -> None:
        _validate_monitoring_config_impl(config, event_log_config, self.project_root)
