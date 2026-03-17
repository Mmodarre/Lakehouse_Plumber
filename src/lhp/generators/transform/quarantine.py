"""Quarantine mode code generator for data quality transforms.

Generates DLQ (Dead Letter Queue) recycling pipeline components:
- Clean view with @dp.expect_all_or_drop
- DLQ foreach_batch_sink with MERGE
- Append flow with inverse filter
- Output view (UNION of clean + recycled)

Delegates expectation parsing to DQEParser (single owner of that logic).
"""

import logging
import re
from typing import Any, Dict, List, Union

from ...utils.dqe import DQEParser
from ...utils.error_formatter import ErrorCategory, LHPError

logger = logging.getLogger(__name__)


class QuarantineCodeGenerator:
    """Generates quarantine-mode data quality code.

    Companion to DataQualityTransformGenerator — called when mode=quarantine.
    Uses the parent generator's infrastructure for imports, template rendering,
    and source view extraction.

    Args:
        parent: The DataQualityTransformGenerator instance (provides
                add_import, render_template, _extract_source_view,
                _get_operational_metadata)
    """

    def __init__(self, parent) -> None:
        self._parent = parent
        self._dqe_parser = DQEParser()

    def generate(
        self,
        action,
        expectations: Union[List[Dict], Dict[str, Any]],
        flowgroup_config: Dict[str, Any],
    ) -> str:
        """Generate quarantine mode code with DLQ recycling.

        Args:
            action: The data quality Action with mode=quarantine
            expectations: Loaded expectations (list or dict format)
            flowgroup_config: Flowgroup context dict

        Returns:
            Generated Python code string
        """
        logger.debug(f"Generating quarantine mode for action '{action.name}'")

        # Add quarantine-specific imports
        self._parent.add_import("from delta.tables import DeltaTable")
        self._parent.add_import("from pyspark.sql import functions as F")
        self._parent.add_import("from pyspark.sql.types import MapType, StringType")
        self._parent.add_import("from pyspark.sql.window import Window")

        # Get quarantine config
        quarantine_config = action.quarantine
        if isinstance(quarantine_config, dict):
            dlq_table = quarantine_config.get("dlq_table", "")
            source_table = quarantine_config.get("source_table", "")
        else:
            dlq_table = quarantine_config.dlq_table
            source_table = quarantine_config.source_table

        # Derive outbox table name from DLQ table
        dlq_outbox_table = dlq_table + "_outbox"

        # Parse ALL expectations as drop — DQEParser is the single owner
        all_expectations = self._dqe_parser.get_all_expectations_as_drop(expectations)

        # Filter out _rescued_data expectations for recycled path
        recycled_expectations = {
            k: v for k, v in all_expectations.items() if "_rescued_data" not in v
        }

        # Defensive check (validator should catch this first)
        if not all_expectations:
            raise LHPError(
                category=ErrorCategory.VALIDATION,
                code_number="014",
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

        # Extract source view and sanitize for Python identifiers
        source_view = self._parent._extract_source_view(action.source)
        safe_source_view = re.sub(r"[^a-zA-Z0-9_]", "_", source_view)

        # Build inverse filter expression
        inverse_filter = "NOT ({})".format(
            " AND ".join(f"({rule})" for rule in all_expectations.values())
        )

        # Build failed rule expressions data for template
        failed_rule_data = [
            {"name": name, "rule": rule} for name, rule in all_expectations.items()
        ]

        # Handle operational metadata
        add_operational_metadata, metadata_columns = (
            self._parent._get_operational_metadata(action, flowgroup_config)
        )

        # Collect ALL metadata column names for hash exclusion
        from ...core.services.operational_metadata_service import (
            OperationalMetadataService,
        )

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

        return self._parent.render_template(
            "transform/data_quality_quarantine.py.j2", template_context
        )
