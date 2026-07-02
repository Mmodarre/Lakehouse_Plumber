"""Streaming table write generator."""

import logging
from pathlib import Path
from typing import Any, Dict, List

from lhp.models import Action

from ...core.codegen import copy_user_module_for_pipeline
from ...core.loaders import resolve_table_schema
from ...core.processing.dqe import DQEParser
from ...core.registry import BaseActionGenerator
from ...errors import ErrorFactory
from ...parsers.schema_parser import SchemaParser
from .snapshot_cdc_source_function import (
    SourceFunctionResult,
    resolve_source_function,
)

logger = logging.getLogger(__name__)


class StreamingTableWriteGenerator(BaseActionGenerator):
    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.schema_parser = SchemaParser()

    def generate(self, action: Action, context: dict) -> str:
        target_config = action.write_target
        if not target_config:
            raise ErrorFactory.missing_required_field(
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

        source_views = self._extract_source_views(action.source)
        readMode = action.readMode or "stream"

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

        properties = {}
        if target_config.get("table_properties"):
            properties.update(target_config["table_properties"])
        spark_conf = target_config.get("spark_conf", {})
        schema_value = target_config.get("table_schema")
        schema = None

        if schema_value:
            project_root = context.get("project_root", Path.cwd())
            schema = resolve_table_schema(
                schema_value, project_root, self.schema_parser
            )

        row_filter = target_config.get("row_filter")
        temporary = target_config.get("temporary", False)
        cdc_config = target_config.get("cdc_config", {}) if mode == "cdc" else {}

        if (
            mode == "cdc"
            and cdc_config.get("sequence_by")
            and isinstance(cdc_config["sequence_by"], list)
        ):
            self.add_import("from pyspark.sql.functions import struct")

        snapshot_cdc_config = (
            target_config.get("snapshot_cdc_config", {})
            if mode == "snapshot_cdc"
            else {}
        )

        # Process source function for snapshot_cdc mode. The function body is
        # never inlined: the user's module is copied alongside the generated
        # pipeline file (same path as custom_py/datasource/sink) and imported
        # under a ``_snap_<mod>`` alias. ``spark`` and ``dbutils`` are injected
        # into the copied module's globals via pre-pipeline statements so the
        # source function resolves bare names against the pipeline's ambient
        # session at Databricks runtime.
        source_expression = None
        if mode == "snapshot_cdc" and snapshot_cdc_config.get("source_function"):
            sf_config = snapshot_cdc_config["source_function"]
            result = resolve_source_function(sf_config, context)
            module_name = copy_user_module_for_pipeline(
                sf_config["file"],
                context,
                component_label="snapshot source function",
            )
            alias = f"_snap_{module_name}"
            self.add_import(f"import custom_python_functions.{module_name} as {alias}")
            self.add_pre_pipeline_statement(f"{alias}.spark = spark")
            self.add_pre_pipeline_statement(f"{alias}.dbutils = dbutils")
            source_expression = self._build_source_expression(result, alias)

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

        flow_cdc_config = self._build_flow_cdc_config(mode, cdc_config)

        if hasattr(action, "_action_metadata") and action._action_metadata:
            action_metadata = action._action_metadata
            flow_name = action_metadata[0][
                "flow_name"
            ]  # Use first flow name for template compatibility
            flow_names = [meta["flow_name"] for meta in action_metadata]
        elif hasattr(action, "_flow_names") and action._flow_names:
            flow_names = action._flow_names
            flow_name = flow_names[0]
            action_metadata = []
            for i, (source_view, flow_name_item) in enumerate(
                zip(source_views, flow_names, strict=False)
            ):
                action_metadata.append(
                    {
                        "action_name": f"{action.name}_{i + 1}",
                        "source_view": source_view,
                        "once": action.once or False,
                        "flow_name": flow_name_item,
                        "description": action.description
                        or f"Append flow to {full_table_name}",
                        "flow_cdc_config": flow_cdc_config,
                    }
                )
        else:
            base_flow_name = action.name.replace("-", "_").replace(" ", "_")
            if base_flow_name.startswith("write_"):
                base_flow_name = base_flow_name[6:]
            base_flow_name = (
                f"f_{base_flow_name}"
                if not base_flow_name.startswith("f_")
                else base_flow_name
            )

            action_metadata = []
            flow_names = []

            if len(source_views) > 1:
                for i, source_view in enumerate(source_views):
                    flow_name = f"{base_flow_name}_{i + 1}"
                    action_metadata.append(
                        {
                            "action_name": f"{action.name}_{i + 1}",
                            "source_view": source_view,
                            "once": action.once or False,
                            "readMode": action.readMode,
                            "flow_name": flow_name,
                            "description": action.description
                            or f"Append flow to {full_table_name} from {source_view}",
                            "flow_cdc_config": flow_cdc_config,
                        }
                    )
                    flow_names.append(flow_name)
            else:
                flow_name = base_flow_name
                action_metadata.append(
                    {
                        "action_name": action.name,
                        "source_view": source_views[0] if source_views else "",
                        "once": action.once or False,
                        "readMode": action.readMode,
                        "flow_name": flow_name,
                        "description": action.description
                        or f"Append flow to {full_table_name}",
                        "flow_cdc_config": flow_cdc_config,
                    }
                )
                flow_names.append(flow_name)

            flow_name = flow_names[0] if flow_names else base_flow_name

        template_context = {
            "action_name": action.name,
            "table_name": table.replace(".", "_"),
            "full_table_name": full_table_name,
            "source_views": source_views,
            "source_view": (
                source_views[0] if source_views and mode == "cdc" else None
            ),  # CDC only supports single source
            "flow_name": flow_name,
            "mode": mode,
            "create_table": create_table,
            "properties": properties,
            "spark_conf": spark_conf,
            "schema": schema,
            "row_filter": row_filter,
            "temporary": temporary,
            "partitions": target_config.get("partition_columns"),
            "cluster_by": target_config.get("cluster_columns"),
            "cluster_by_auto": target_config.get("cluster_by_auto"),
            "comment": target_config.get("comment", f"Streaming table: {table}"),
            "table_path": target_config.get("path"),
            "cdc_config": cdc_config,
            "snapshot_cdc_config": snapshot_cdc_config,
            "source_expression": source_expression,
            "expect_all": expect_all,
            "expect_all_or_drop": expect_all_or_drop,
            "expect_all_or_fail": expect_all_or_fail,
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
            "description": action.description or f"Append flow to {full_table_name}",
            "once": action.once or False,
            "action_metadata": action_metadata,
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
        if isinstance(source, str):
            return [source]
        if isinstance(source, list):
            result = []
            for item in source:
                if isinstance(item, str):
                    result.append(item)
                else:
                    logger.warning(
                        f"Unexpected source item type {type(item).__name__}, skipping"
                    )
            return result
        logger.warning(
            f"Unexpected source type {type(source).__name__}, returning empty list"
        )
        return []

    def _build_source_expression(self, result: SourceFunctionResult, alias: str) -> str:
        """Build the source= expression for snapshot CDC.

        The function is referenced through the copied module's import alias
        (``{alias}.{name}``); with parameters it becomes the first positional
        arg of a ``functools.partial(...)``.
        """
        qualified_name = f"{alias}.{result.name}"
        if not result.parameters:
            return qualified_name

        param_parts = [f"{k}={v!r}" for k, v in result.parameters.items()]
        params_str = ",\n        ".join(param_parts)
        self.add_import("from functools import partial")
        return f"partial(\n        {qualified_name},\n        {params_str}\n    )"
