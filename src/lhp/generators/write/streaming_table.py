"""Streaming table write generator."""

import logging
from pathlib import Path
from typing import Any, Dict, List

from ...core.loaders.external_file_loader import (
    is_file_path,
    load_external_file_text,
    resolve_external_file_path,
)
from ...core.processing.dqe import DQEParser
from ...core.registry import BaseActionGenerator
from ...errors import ErrorFormatter
from lhp.models import Action
from ...parsers.schema_parser import SchemaParser
from .source_function_loader import SourceFunctionResult, load_source_function

logger = logging.getLogger(__name__)


class StreamingTableWriteGenerator(BaseActionGenerator):
    """Generate streaming table write actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.schema_parser = SchemaParser()

    def generate(self, action: Action, context: dict) -> str:
        """Generate streaming table code."""
        target_config = action.write_target
        if not target_config:
            raise ErrorFormatter.missing_required_field(
                field_name="write_target",
                component_type="Streaming table write action",
                component_name=action.name,
                field_description="The write_target configuration is required for streaming table actions.",
                example_config="""actions:
  - name: write_data
    type: write
    sub_type: streaming_table
    source: v_transformed
    write_target:
      table: my_table
      catalog: my_catalog
      schema: my_schema""",
            )
        logger.debug(f"Generating streaming table write for action '{action.name}'")

        # Extract source views as a list
        source_views = self._extract_source_views(action.source)

        # Get readMode from action or default to stream
        readMode = action.readMode or "stream"

        # Extract configuration
        mode = target_config.get(
            "mode", "standard"
        )  # Valid modes: "standard" (default), "cdc", "snapshot_cdc"
        catalog = target_config.get("catalog")
        schema = target_config.get("schema")
        table = target_config.get("table")

        # Snapshot CDC still requires a dedicated table; other modes honor the flag.
        if mode == "snapshot_cdc":
            create_table = True
        else:
            create_table = target_config.get("create_table", True)

        # Build full table name (normalizer guarantees catalog/schema are present)
        full_table_name = f"{catalog}.{schema}.{table}" if catalog and schema else table
        logger.debug(
            f"Streaming table '{action.name}': target='{full_table_name}', mode='{mode}', readMode='{readMode}', sources={source_views}"
        )

        # Table properties
        properties = {}
        if target_config.get("table_properties"):
            properties.update(target_config["table_properties"])

        # Spark configuration
        spark_conf = target_config.get("spark_conf", {})

        # Schema definition (SQL DDL string or StructType)
        schema_value = target_config.get("table_schema")
        schema = None

        if schema_value:
            # Check if it's a file path
            if is_file_path(schema_value):
                # Load from external file
                project_root = context.get("project_root", Path.cwd())
                file_ext = Path(schema_value).suffix.lower()

                if file_ext in [".yaml", ".yml", ".json"]:
                    # YAML/JSON schema - parse and convert to DDL
                    resolved_path = resolve_external_file_path(
                        schema_value, project_root, file_type="table schema file"
                    )
                    schema_data = self.schema_parser.parse_schema_file(resolved_path)
                    schema = self.schema_parser.to_schema_hints(schema_data)
                else:
                    # DDL/SQL file - load as plain text
                    schema = load_external_file_text(
                        schema_value, project_root, file_type="table schema file"
                    ).strip()
            else:
                # Inline DDL
                schema = schema_value

        # Row filter clause
        row_filter = target_config.get("row_filter")

        # Temporary table flag
        temporary = target_config.get("temporary", False)

        # Handle CDC configuration for auto_cdc mode
        cdc_config = target_config.get("cdc_config", {}) if mode == "cdc" else {}

        # Check if we need struct import for sequence_by
        if (
            mode == "cdc"
            and cdc_config.get("sequence_by")
            and isinstance(cdc_config["sequence_by"], list)
        ):
            self.add_import("from pyspark.sql.functions import struct")

        # Handle snapshot CDC configuration for snapshot_cdc mode
        snapshot_cdc_config = (
            target_config.get("snapshot_cdc_config", {})
            if mode == "snapshot_cdc"
            else {}
        )

        # Process source function code for snapshot_cdc mode
        source_function_code = None
        source_expression = None
        if mode == "snapshot_cdc" and snapshot_cdc_config.get("source_function"):
            result = load_source_function(
                snapshot_cdc_config["source_function"], context
            )
            source_function_code = result.code
            source_expression = self._build_source_expression(result)

        # Process data quality expectations
        expectations = context.get("expectations", [])
        expect_all = {}
        expect_all_or_drop = {}
        expect_all_or_fail = {}

        if expectations:
            dqe_parser = DQEParser()
            expect_all, expect_all_or_drop, expect_all_or_fail = (
                dqe_parser.parse_expectations(expectations)
            )

        # Metadata is added at load level and flows through naturally — write
        # actions intentionally have no operational-metadata columns of their own.
        metadata_columns = {}
        flowgroup = context.get("flowgroup")

        # Per-flow CDC config for the single-action path (empty for non-CDC).
        flow_cdc_config = self._build_flow_cdc_config(mode, cdc_config)

        # Check if this is a combined action with individual metadata
        if hasattr(action, "_action_metadata") and action._action_metadata:
            # Use new action metadata structure for individual append flows
            action_metadata = action._action_metadata
            flow_name = action_metadata[0][
                "flow_name"
            ]  # Use first flow name for template compatibility
            flow_names = [meta["flow_name"] for meta in action_metadata]
        elif hasattr(action, "_flow_names") and action._flow_names:
            # Legacy combined actions - convert to new structure
            flow_names = action._flow_names
            flow_name = flow_names[0]
            action_metadata = []
            for i, (source_view, flow_name_item) in enumerate(
                zip(source_views, flow_names)
            ):
                action_metadata.append(
                    {
                        "action_name": f"{action.name}_{i+1}",
                        "source_view": source_view,
                        "once": action.once or False,  # Legacy: same once flag for all
                        "flow_name": flow_name_item,
                        "description": action.description
                        or f"Append flow to {full_table_name}",
                        "flow_cdc_config": flow_cdc_config,
                    }
                )
        else:
            # Single action - create metadata structure for each source view
            base_flow_name = action.name.replace("-", "_").replace(" ", "_")
            if base_flow_name.startswith("write_"):
                base_flow_name = base_flow_name[6:]  # Remove "write_" prefix
            base_flow_name = (
                f"f_{base_flow_name}"
                if not base_flow_name.startswith("f_")
                else base_flow_name
            )

            action_metadata = []
            flow_names = []

            if len(source_views) > 1:
                # Multiple sources: create separate append flow for each
                for i, source_view in enumerate(source_views):
                    flow_name = f"{base_flow_name}_{i+1}"
                    action_metadata.append(
                        {
                            "action_name": f"{action.name}_{i+1}",
                            "source_view": source_view,
                            "once": action.once or False,
                            "readMode": action.readMode,  # Preserve readMode
                            "flow_name": flow_name,
                            "description": action.description
                            or f"Append flow to {full_table_name} from {source_view}",
                            "flow_cdc_config": flow_cdc_config,
                        }
                    )
                    flow_names.append(flow_name)
            else:
                # Single source: create one append flow
                flow_name = base_flow_name
                action_metadata.append(
                    {
                        "action_name": action.name,
                        "source_view": source_views[0] if source_views else "",
                        "once": action.once or False,
                        "readMode": action.readMode,  # Preserve readMode
                        "flow_name": flow_name,
                        "description": action.description
                        or f"Append flow to {full_table_name}",
                        "flow_cdc_config": flow_cdc_config,
                    }
                )
                flow_names.append(flow_name)

            # Set flow_name for backward compatibility (use first flow name)
            flow_name = flow_names[0] if flow_names else base_flow_name

        template_context = {
            "action_name": action.name,
            "table_name": table.replace(".", "_"),  # Function name safe
            "full_table_name": full_table_name,
            "source_views": source_views,  # Keep for backward compatibility
            "source_view": (
                source_views[0] if source_views and mode == "cdc" else None
            ),  # CDC only supports single source
            "flow_name": flow_name,  # Keep for backward compatibility
            "mode": mode,
            "create_table": create_table,  # Pass create_table flag to template
            "properties": properties,
            "spark_conf": spark_conf,
            "schema": schema,
            "row_filter": row_filter,
            "temporary": temporary,
            "partitions": target_config.get("partition_columns"),
            "cluster_by": target_config.get("cluster_columns"),
            "comment": target_config.get("comment", f"Streaming table: {table}"),
            "table_path": target_config.get("path"),
            "cdc_config": cdc_config,
            "snapshot_cdc_config": snapshot_cdc_config,
            "source_function_code": source_function_code,
            "source_expression": source_expression,
            "expect_all": expect_all,
            "expect_all_or_drop": expect_all_or_drop,
            "expect_all_or_fail": expect_all_or_fail,
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
            "description": action.description or f"Append flow to {full_table_name}",
            "once": action.once or False,  # Keep for backward compatibility
            "action_metadata": action_metadata,  # New: individual action metadata
            "readMode": readMode,
        }

        return self.render_template("write/streaming_table.py.j2", template_context)

    def _build_flow_cdc_config(
        self, mode: str, cdc_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build the per-flow CDC config dict for a single CDC action.

        Returns an empty dict for non-CDC modes so the template can safely
        index into it without branching on mode.
        """
        if mode != "cdc":
            return {}
        return {
            "ignore_null_updates": cdc_config.get("ignore_null_updates"),
            "apply_as_deletes": cdc_config.get("apply_as_deletes"),
            "apply_as_truncates": cdc_config.get("apply_as_truncates"),
            "column_list": cdc_config.get("column_list"),
            "except_column_list": cdc_config.get("except_column_list"),
        }

    def _extract_source_views(self, source) -> List[str]:
        """Extract source views as a list from action source."""
        if isinstance(source, str):
            return [source]
        elif isinstance(source, list):
            result = []
            for item in source:
                if isinstance(item, str):
                    result.append(item)
                else:
                    logger.warning(
                        f"Unexpected source item type {type(item).__name__}, skipping"
                    )
            return result
        else:
            logger.warning(
                f"Unexpected source type {type(source).__name__}, returning empty list"
            )
            return []

    def _build_source_expression(self, result: SourceFunctionResult) -> str:
        """Build the source= expression for snapshot CDC.

        Returns a bare function name when no parameters, or a partial()
        expression with keyword arguments when parameters are present.
        """
        if not result.parameters:
            return result.name

        param_parts = [f"{k}={repr(v)}" for k, v in result.parameters.items()]
        params_str = ",\n        ".join(param_parts)
        self.add_import("from functools import partial")
        return f"partial(\n        {result.name},\n        {params_str}\n    )"
