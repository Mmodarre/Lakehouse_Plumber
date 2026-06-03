"""Data quality transformation generator."""

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Union

from lhp.models import Action

from ...core.loaders.external_file_loader import resolve_external_file_path
from ...core.processing.dqe import DQEParser
from ...core.registry import BaseActionGenerator
from ...errors import ErrorFactory, codes

logger = logging.getLogger(__name__)


class DataQualityTransformGenerator(BaseActionGenerator):
    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.dqe_parser = DQEParser()

    def generate(self, action: Action, flowgroup_config: Dict[str, Any]) -> str:
        logger.debug(
            f"Generating data quality transform for target '{action.target}', action '{action.name}'"
        )
        readMode = action.readMode or "stream"
        if readMode != "stream":
            raise ErrorFactory.invalid_read_mode(
                action_name=action.name,
                action_type="data_quality",
                provided=readMode,
                valid_modes=["stream"],
            )

        expectations_file = action.expectations_file
        if not expectations_file:
            raise ErrorFactory.missing_required_field(
                field_name="expectations_file",
                component_type="Data quality transform action",
                component_name=action.name,
                field_description="The expectations file containing data quality rules is required.",
                example_config="""actions:
  - name: check_quality
    type: transform
    sub_type: data_quality
    source: v_raw_data
    target: v_validated_data
    expectations_file: "expectations/quality_rules.yaml" """,
            )

        expectations = self._load_expectations(action, flowgroup_config.get("spec_dir"))

        total_rules = len(expectations) if isinstance(expectations, (list, dict)) else 0
        logger.debug(
            f"Data quality '{action.name}': {total_rules} expectation rules loaded, source='{self._extract_source_view(action.source)}'"
        )

        dq_mode = getattr(action, "mode", None) or "dqe"

        if dq_mode == "quarantine":
            return self._generate_quarantine_mode(
                action, expectations, flowgroup_config
            )
        return self._generate_dqe_mode(action, expectations, flowgroup_config)

    def _generate_dqe_mode(
        self, action: Action, expectations, flowgroup_config: Dict[str, Any]
    ) -> str:
        readMode = action.readMode or "stream"

        if expectations and isinstance(expectations, list):
            # Old format: list of dicts with constraint/type fields
            expect_all, expect_all_or_drop, expect_all_or_fail = (
                self.dqe_parser.parse_expectations(expectations)
            )
            fail_expectations = expect_all_or_fail
            drop_expectations = expect_all_or_drop
            warn_expectations = expect_all
        else:
            # New format: dict where key is constraint, value has action/name
            fail_expectations = {}
            drop_expectations = {}
            warn_expectations = {}

            for constraint, exp_config in (expectations or {}).items():
                action_type = exp_config.get("action", "warn").lower()
                name = exp_config.get("name", constraint)

                if action_type == "fail":
                    fail_expectations[name] = constraint
                elif action_type == "drop":
                    drop_expectations[name] = constraint
                else:  # warn or default
                    warn_expectations[name] = constraint

        source_view = self._extract_source_view(action.source)

        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, flowgroup_config
        )

        template_context = {
            "target_view": action.target,
            "source_view": source_view,
            "readMode": readMode,
            "fail_expectations": fail_expectations,
            "drop_expectations": drop_expectations,
            "warn_expectations": warn_expectations,
            "description": action.description
            or f"Data quality checks for {action.source}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
        }

        return self.render_template("transform/data_quality.py.j2", template_context)

    def _generate_quarantine_mode(
        self,
        action,
        expectations: Union[List[Dict], Dict[str, Any]],
        flowgroup_config: Dict[str, Any],
    ) -> str:
        """Generate quarantine mode code with DLQ recycling."""
        logger.debug(f"Generating quarantine mode for action '{action.name}'")

        self.add_import("from delta.tables import DeltaTable")
        self.add_import("from pyspark.sql import functions as F")
        self.add_import("from pyspark.sql.types import MapType, StringType")
        self.add_import("from pyspark.sql.window import Window")

        quarantine_config = action.quarantine
        if isinstance(quarantine_config, dict):
            dlq_table = quarantine_config.get("dlq_table", "")
            source_table = quarantine_config.get("source_table", "")
        else:
            dlq_table = quarantine_config.dlq_table
            source_table = quarantine_config.source_table

        dlq_outbox_table = dlq_table + "_outbox"

        # Parse ALL expectations as drop — DQEParser is the single owner
        all_expectations = self.dqe_parser.get_all_expectations_as_drop(expectations)

        # Filter out _rescued_data expectations for recycled path
        recycled_expectations = {
            k: v for k, v in all_expectations.items() if "_rescued_data" not in v
        }

        # Defensive check (validator should catch this first)
        if not all_expectations:
            raise ErrorFactory.validation_error(
                codes.VAL_014,
                title="Quarantine mode requires at least one expectation",
                details=(
                    f"Action '{action.name}' has mode='quarantine' but "
                    f"the expectations file contains no rules."
                ),
                suggestions=[
                    "Add at least one expectation to the expectations file.",
                    "Or remove mode='quarantine' to use standard DQE mode.",
                ],
            )

        # Sanitize source view for use as a Python identifier
        source_view = self._extract_source_view(action.source)
        safe_source_view = re.sub(r"[^a-zA-Z0-9_]", "_", source_view)

        inverse_filter = "NOT ({})".format(
            " AND ".join(f"({rule})" for rule in all_expectations.values())
        )

        failed_rule_data = [
            {"name": name, "rule": rule} for name, rule in all_expectations.items()
        ]

        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, flowgroup_config
        )

        from ...core.codegen.operational_metadata import OperationalMetadataService

        service = OperationalMetadataService()
        project_config = flowgroup_config.get("project_config")
        hash_exclude_columns = sorted(
            service.get_all_metadata_column_names(project_config)
        )

        template_context = {
            "target_view": action.target,
            "source_view": source_view,
            "safe_source_view": safe_source_view,
            "description": (
                action.description or f"Data quality checks for {action.source}"
            ),
            "expectations": all_expectations,
            "inverse_filter": inverse_filter,
            "failed_rule_data": failed_rule_data,
            "dlq_table": dlq_table,
            "source_table": source_table,
            "dlq_outbox_table": dlq_outbox_table,
            "recycled_expectations": recycled_expectations,
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "hash_exclude_columns": hash_exclude_columns,
        }

        return self.render_template(
            "transform/data_quality_quarantine.py.j2", template_context
        )

    def _extract_source_view(self, source) -> str:
        if isinstance(source, str):
            return source
        if isinstance(source, dict):
            return source.get("view", source.get("source", ""))
        return ""

    def _load_expectations(self, action: Action, spec_dir: Path | None = None) -> List:
        # Check if action has expectations as an attribute (from test)
        if hasattr(action, "expectations") and action.expectations:
            return action.expectations

        if isinstance(action.source, dict) and "expectations" in action.source:
            return action.source["expectations"]

        expectations_file = None
        if hasattr(action, "expectations_file"):
            expectations_file = action.expectations_file
        elif isinstance(action.source, dict) and "expectations_file" in action.source:
            expectations_file = action.source["expectations_file"]

        if expectations_file:
            from ...parsers.yaml_loader import load_yaml_file

            project_root = spec_dir or Path.cwd()
            resolved_path = resolve_external_file_path(
                expectations_file, project_root, file_type="expectations file"
            )

            # Load the YAML file (resolve_external_file_path raises if not found)
            data = load_yaml_file(
                resolved_path, error_context="data quality expectations file"
            )

            if isinstance(data, dict):
                # Old format: dict with 'expectations' key
                if "expectations" in data:
                    return data["expectations"]
                # New format: direct dict of constraints
                return data
            if isinstance(data, list):
                return data
            logger.warning(
                f"Expectations file '{expectations_file}' has unexpected format "
                f"(expected dict or list, got {type(data).__name__}), "
                "proceeding with empty expectations"
            )

        return {}
