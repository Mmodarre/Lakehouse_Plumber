"""Operational metadata core: the ``OperationalMetadata`` class.

Owns the catalog of built-in metadata columns, the merge of preset / flowgroup
/ action selections, and the substitution context. SQL adaptation and column
resolution live in sibling modules and are reached via thin delegator methods
to keep this class focused.
"""

import logging
from typing import Any, Dict, Optional, Set

from ....models.config import (
    Action,
    FlowGroup,
    MetadataColumnConfig,
    ProjectOperationalMetadataConfig,
)

from . import _column_resolution, _sql_adaptation
from .detector import ImportDetector


class OperationalMetadata:
    """Enhanced operational metadata handler with project-level configuration and ImportManager integration."""

    def __init__(
        self, project_config: Optional[ProjectOperationalMetadataConfig] = None
    ):
        self.logger = logging.getLogger(__name__)
        self.import_detector = ImportDetector(strategy="ast")
        self.project_config = project_config

        # Default metadata columns with adaptive expressions
        # These will be dynamically adjusted based on available imports
        self.default_columns = {
            "_ingestion_timestamp": MetadataColumnConfig(
                expression="F.current_timestamp()",  # Default format
                description="When the record was ingested",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
            "_source_file": MetadataColumnConfig(
                expression="F.input_file_name()",  # Default format
                description="Source file path",
                applies_to=["view"],  # Only views (load actions)
            ),
            "_pipeline_run_id": MetadataColumnConfig(
                expression='F.lit(spark.conf.get("pipelines.id", "unknown"))',  # Default format
                description="Pipeline run identifier",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
            "_pipeline_name": MetadataColumnConfig(
                expression='F.lit("${pipeline_name}")',  # Default format
                description="Pipeline name",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
            "_flowgroup_name": MetadataColumnConfig(
                expression='F.lit("${flowgroup_name}")',  # Default format
                description="FlowGroup name",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
        }

        # Alternative expressions for when wildcard imports are available
        self.wildcard_expressions = {
            "_ingestion_timestamp": "current_timestamp()",
            "_source_file": "input_file_name()",
            "_pipeline_run_id": 'lit(spark.conf.get("pipelines.id", "unknown"))',
            "_pipeline_name": 'lit("${pipeline_name}")',
            "_flowgroup_name": 'lit("${flowgroup_name}")',
        }

        # Context for substitutions
        self.pipeline_name = None
        self.flowgroup_name = None

    def update_context(self, pipeline_name: str, flowgroup_name: str):
        """Update context for substitutions."""
        self.pipeline_name = pipeline_name
        self.flowgroup_name = flowgroup_name

    def adapt_expressions_for_imports(self, import_manager=None) -> None:
        """Adapt expressions based on available imports from ImportManager.

        Delegates to :mod:`_sql_adaptation` to keep this class focused.
        """
        _sql_adaptation.adapt_expressions_for_imports(self, import_manager)

    def get_all_column_names(self) -> set:
        """Return union of built-in default AND project-defined column names.

        Used for defensive filtering (e.g. excluding metadata from hash calculations)
        where over-excluding is harmless.
        """
        names = set(self.default_columns.keys())
        if self.project_config and self.project_config.columns:
            names.update(self.project_config.columns.keys())
        return names

    def resolve_metadata_selection(
        self,
        flowgroup: Optional[FlowGroup],
        action: Optional[Action],
        preset_config: dict,
    ) -> Optional[Dict[str, Any]]:
        """Resolve metadata selection across preset, flowgroup, and action levels.

        Args:
            flowgroup: FlowGroup configuration
            action: Action configuration
            preset_config: Preset configuration dictionary

        Returns:
            Resolved metadata selection or None if disabled
        """
        # Check for explicit disable at action level first
        if (
            action
            and hasattr(action, "operational_metadata")
            and action.operational_metadata is False
        ):
            # Explicitly disabled at action level - no metadata at all
            return None

        # Always collect from all levels for additive behavior
        result = {}

        # Add preset level selection
        if "operational_metadata" in preset_config:
            result["preset"] = preset_config["operational_metadata"]

        # Add flowgroup level selection
        if (
            flowgroup
            and hasattr(flowgroup, "operational_metadata")
            and flowgroup.operational_metadata is not None
        ):
            result["flowgroup"] = flowgroup.operational_metadata

        # Add action level selection (unless it's False, which we already handled)
        if (
            action
            and hasattr(action, "operational_metadata")
            and action.operational_metadata is not None
        ):
            result["action"] = action.operational_metadata

        # Return combined result or None if no selections found
        return result if result else None

    def get_selected_columns(
        self, selection: Dict[str, Any], target_type: str
    ) -> Dict[str, str]:
        """Get selected columns with expressions for the target type.

        Delegates to :mod:`_column_resolution`.
        """
        return _column_resolution.get_selected_columns(self, selection, target_type)

    def _validate_target_type(self, target_type: str) -> None:
        """Validate target type. Thin delegator to :mod:`_column_resolution`."""
        _column_resolution._validate_target_type(self, target_type)

    def _apply_substitutions(self, expression: str) -> str:
        """Apply context substitutions to expression.

        Args:
            expression: Expression with possible substitutions

        Returns:
            Expression with substitutions applied
        """
        if self.pipeline_name:
            expression = expression.replace("${pipeline_name}", self.pipeline_name)
        if self.flowgroup_name:
            expression = expression.replace("${flowgroup_name}", self.flowgroup_name)

        return expression

    def get_required_imports(self, columns: Dict[str, str]) -> Set[str]:
        """Get required imports for selected columns.

        Args:
            columns: Dictionary of column_name -> expression

        Returns:
            Set of import statements required
        """
        if not columns:
            return set()

        imports = set()
        available_columns = _column_resolution._get_available_columns(self)

        for column_name, expression in columns.items():
            # Get imports from expression
            imports.update(self.import_detector.detect_imports(expression))

            # Add additional imports from configuration
            if column_name in available_columns:
                column_config = available_columns[column_name]
                if column_config.additional_imports:
                    imports.update(column_config.additional_imports)

        return imports
