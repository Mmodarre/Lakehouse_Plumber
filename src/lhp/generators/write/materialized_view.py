"""Materialized view write generator for LakehousePlumber."""

import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    cast,
)

from lhp.models import Action

from ...core.loaders import (
    Expectations,
    load_action_expectations,
    load_expectations_file,
    load_sql_file,
    resolve_table_schema,
)
from ...core.processing import DQEParser
from ...core.registry import BaseActionGenerator
from ...errors import ErrorFactory, codes
from ._schema_tags_warning import warn_if_schema_tags_dropped

if TYPE_CHECKING:
    from ...core.codegen import FusedDq

logger = logging.getLogger(__name__)


def assemble_expectations(
    sources: Sequence[Expectations],
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    """Merge expectation sources into the view's three decorator mappings.

    Returns ``(expect_all, expect_all_or_drop, expect_all_or_fail)``, each
    mapping an expectation name to its constraint, in source order. Every
    source must already be validated, names included (the validators reject a
    name two rules share). A name met again in another category or a later
    source is an internal error here.
    """
    merged: Tuple[Dict[str, str], Dict[str, str], Dict[str, str]] = ({}, {}, {})
    seen: Set[str] = set()
    parser = DQEParser()
    for source in sources:
        for bucket, parsed in zip(
            merged, parser.parse_expectations(source), strict=True
        ):
            for name, constraint in parsed.items():
                if name in seen:
                    raise AssertionError(
                        f"Unvalidated duplicate expectation name {name!r}"
                    )
                seen.add(name)
                bucket[name] = constraint
    return merged


class MaterializedViewWriteGenerator(BaseActionGenerator):
    """Uses @dp.materialized_view decorator."""

    default_read_mode = "batch"
    fixed_read_mode = True

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.add_import("from pyspark.sql import DataFrame")

    def generate(self, action: Action, context: dict) -> str:
        target_config = action.write_target
        if not target_config:
            raise ErrorFactory.missing_required_field(
                field_name="write_target",
                component_type="Materialized view write action",
                component_name=action.name,
                field_description="The write_target configuration is required for materialized view actions.",
                example_config="""actions:
  - name: write_mv
    type: write
    sub_type: materialized_view
    source: v_transformed
    write_target:
      table: my_materialized_view
      catalog: my_catalog
      schema: my_schema""",
            )

        catalog = target_config.get("catalog")
        schema = target_config.get("schema")
        table = target_config.get("table")

        # Build full table name (normalizer guarantees catalog/schema are present)
        full_table_name = f"{catalog}.{schema}.{table}" if catalog and schema else table
        logger.debug(
            f"Generating materialized view write for target '{full_table_name}', action '{action.name}'"
        )

        properties = target_config.get("table_properties", {})
        spark_conf = target_config.get("spark_conf", {})
        schema_value = target_config.get("table_schema")
        schema = None

        if schema_value:
            project_root = context.get("project_root", Path.cwd())
            resolved = resolve_table_schema(schema_value, project_root)
            schema = resolved.text
            if resolved.schema_data is not None:
                warn_if_schema_tags_dropped(
                    resolved.schema_data,
                    target_config,
                    resolved.resolved_path,
                    project_root,
                )

        row_filter = target_config.get("row_filter")
        temporary = target_config.get("temporary", False)
        refresh_schedule = target_config.get("refresh_schedule")
        has_sql = "sql" in target_config or "sql_path" in target_config
        logger.debug(
            f"Materialized view '{action.name}': has_sql={has_sql}, has_schema={schema is not None}, refresh_schedule={refresh_schedule}"
        )

        substitution_mgr = context.get("substitution_manager")
        sql_query = None
        if "sql" in target_config:
            sql_query = target_config["sql"].strip()
            if sql_query and substitution_mgr is not None:
                sql_query = substitution_mgr._process_string(sql_query)
        elif "sql_path" in target_config:
            flowgroup = context.get("flowgroup")
            sql_query = load_sql_file(
                target_config["sql_path"],
                context.get("project_root", Path.cwd()),
                substitution_mgr,
                pipeline=getattr(flowgroup, "pipeline", None),
                flowgroup=getattr(flowgroup, "flowgroup", None),
                action=action.name,
            )

        if sql_query and substitution_mgr is not None:
            secret_refs = substitution_mgr.secret_references
            if (
                "secret_references" in context
                and context["secret_references"] is not None
            ):
                context["secret_references"].update(secret_refs)

        # A data_quality action fused onto this view (code generation, see
        # plan_dq_fusion): the view reads the action's source view, carries its
        # expectations and its operational-metadata columns. The action's own
        # view is generated unchanged and nothing reads it.
        fused: Optional["FusedDq"] = context.get("fused_dq")

        # No SQL: must have a source view.
        source_view = None
        if fused is not None:
            source_view = fused.source_view
        elif not sql_query:
            source_view = self._extract_source_view(action.source)

        # Metadata is added at load level and flows through naturally — write
        # actions intentionally have no operational-metadata columns of their own,
        # except those of a fused data_quality action.
        metadata_columns: Dict[str, str] = {}
        if fused is not None:
            # Planned by code generation from the data_quality generator; this
            # view only renders what it is given (renders_operational_metadata
            # stays False).
            metadata_columns = dict(fused.metadata_columns)
            for import_stmt in fused.metadata_imports:
                self.add_import(import_stmt)
        flowgroup = context.get("flowgroup")

        # Action keeps write_target as the plain mapping it was given.
        expectation_sources = _expectation_sources(
            cast(Dict[str, Any], target_config),
            context.get("project_root", Path.cwd()),
            fused,
        )
        expect_all, expect_all_or_drop, expect_all_or_fail = assemble_expectations(
            expectation_sources
        )

        template_context = {
            "action_name": action.name,
            "table_name": table.replace(".", "_"),  # Function name safe
            "full_table_name": full_table_name,
            "source_view": source_view,
            "sql_query": sql_query,
            "properties": properties,
            "spark_conf": spark_conf,
            "schema": schema,
            "row_filter": row_filter,
            "temporary": temporary,
            "private": target_config.get("private", False),
            "partitions": target_config.get("partition_columns"),
            "cluster_by": target_config.get("cluster_columns"),
            "cluster_by_auto": target_config.get("cluster_by_auto"),
            "table_path": target_config.get("path"),
            "comment": target_config.get("comment", f"Materialized view: {table}"),
            "refresh_schedule": refresh_schedule,
            "refresh_policy": target_config.get("refresh_policy"),
            "description": action.description
            or f"Write to {full_table_name} from multiple sources",
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
            "expect_all": expect_all,
            "expect_all_or_drop": expect_all_or_drop,
            "expect_all_or_fail": expect_all_or_fail,
        }

        return self.render_template("write/materialized_view.py.j2", template_context)

    def _extract_source_view(self, source) -> str:
        if isinstance(source, str):
            return source
        if isinstance(source, list) and source:
            first_item = source[0]
            if isinstance(first_item, str):
                return first_item
            logger.warning(
                f"Unexpected source list item type {type(first_item).__name__}, "
                f"converting to string"
            )
            return str(first_item)
        raise ErrorFactory.validation_error(
            codes.VAL_017,
            title="Invalid source configuration for materialized view write",
            details=(
                "Materialized view write requires a valid source configuration. "
                "Source must be a string (view name) or non-empty list of strings."
            ),
            suggestions=[
                "Provide a source view name: source: v_transformed",
                "Or provide a list of source views",
            ],
            context={"Source Type": type(source).__name__},
        )


def _expectation_sources(
    target_config: Dict[str, Any],
    project_root: Path,
    fused: Optional["FusedDq"] = None,
) -> List[Expectations]:
    """The view's own file, then the fused data_quality action's rules."""
    sources: List[Expectations] = []
    expectations_file = target_config.get("expectations_file")
    if expectations_file:
        # WriteActionValidator has already checked the file and its rules.
        sources.append(load_expectations_file(expectations_file, project_root))
    if fused is not None:
        # The data_quality action's validator has checked its rules, and
        # MvExpectationsValidator their names against the view's.
        sources.append(load_action_expectations(fused.dq_action, project_root))
    return sources
