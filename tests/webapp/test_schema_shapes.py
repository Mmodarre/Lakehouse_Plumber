"""Shape contract tests for the web IDE response schemas.

Each schema is constructed and its ``model_dump()`` is asserted to emit
exactly the JSON field names the React frontend consumes. These names are a
public HTTP contract — renaming a field is a breaking change.
"""

from __future__ import annotations

import pytest

from lhp.api.views import ActionView, FlowgroupView, ProcessedFlowgroupView
from lhp.webapp.schemas.common import (
    ErrorDetail,
    ErrorResponse,
    PaginatedResponse,
    PaginationParams,
    SuccessResponse,
)
from lhp.webapp.schemas.dependency import (
    CircularDependencyResponse,
    CrossPipelineConnection,
    CrossPipelineSummary,
    DependencyResponse,
    ExecutionOrderResponse,
    ExportResponse,
    ExternalSourcesResponse,
    GraphEdge,
    GraphMetadata,
    GraphNode,
    GraphResponse,
)
from lhp.webapp.schemas.flowgroup import (
    FlowgroupActionSummary,
    FlowgroupConfig,
    FlowgroupDetailResponse,
    FlowgroupListResponse,
    FlowgroupRelatedFilesResponse,
    FlowgroupSummary,
    PipelineFlowgroupsResponse,
    RelatedFileInfo,
    ResolvedFlowgroupResponse,
)
from lhp.webapp.schemas.health import HealthResponse, VersionResponse
from lhp.webapp.schemas.pipeline import (
    PipelineConfigResponse,
    PipelineDetailResponse,
    PipelineListResponse,
    PipelineSummary,
)
from lhp.webapp.schemas.preset import (
    PresetDetailResponse,
    PresetListDetailResponse,
    PresetListResponse,
    PresetSummary,
)
from lhp.webapp.schemas.project import (
    ProjectConfigResponse,
    ProjectInfoResponse,
    ProjectStatsResponse,
    ResourceCounts,
)
from lhp.webapp.schemas.table import (
    TableListResponse,
    TableSummary,
    build_table_summary,
)
from lhp.webapp.schemas.template import (
    TemplateDetailResponse,
    TemplateInfoResponse,
    TemplateListDetailResponse,
    TemplateListResponse,
    TemplateSummary,
)

pytestmark = pytest.mark.webapp


# --- health -----------------------------------------------------------------


def test_health_response_fields() -> None:
    dumped = HealthResponse(status="healthy", version="0.9.1").model_dump()
    assert set(dumped) == {"status", "version", "project_state", "root"}
    # project_state/root default to the "working project" reading; the app
    # lifespan overrides them per request context.
    assert dumped["project_state"] == "ok"
    assert dumped["root"] == ""
    # dropped feature: dev_mode must not be present
    assert "dev_mode" not in dumped
    assert "python_version" not in dumped


def test_version_response_fields() -> None:
    dumped = VersionResponse(
        lhp_version="0.9.1",
        python_version="3.12.0",
        dependencies={"pydantic": "2.0.0"},
    ).model_dump()
    assert set(dumped) == {"lhp_version", "python_version", "dependencies"}


# --- project ----------------------------------------------------------------


def test_resource_counts_fields() -> None:
    dumped = ResourceCounts(
        pipelines=1, flowgroups=2, presets=3, templates=4, environments=5
    ).model_dump()
    assert set(dumped) == {
        "pipelines",
        "flowgroups",
        "presets",
        "templates",
        "environments",
    }


def test_project_info_response_fields() -> None:
    dumped = ProjectInfoResponse(
        name="demo",
        version="1.0",
        resource_counts=ResourceCounts(
            pipelines=0, flowgroups=0, presets=0, templates=0, environments=0
        ),
    ).model_dump()
    assert set(dumped) == {
        "name",
        "version",
        "description",
        "author",
        "resource_counts",
    }


def test_project_config_response_fields() -> None:
    dumped = ProjectConfigResponse(config={"name": "demo"}).model_dump()
    assert set(dumped) == {"config"}


def test_project_stats_response_fields() -> None:
    dumped = ProjectStatsResponse(
        total_pipelines=1,
        total_flowgroups=2,
        total_actions=3,
        actions_by_type={"load": 1},
        pipelines=[{"name": "p"}],
    ).model_dump()
    assert set(dumped) == {
        "total_pipelines",
        "total_flowgroups",
        "total_actions",
        "actions_by_type",
        "pipelines",
    }


# --- pipeline ---------------------------------------------------------------


def test_pipeline_summary_fields() -> None:
    dumped = PipelineSummary(name="p", flowgroup_count=1, action_count=2).model_dump()
    assert set(dumped) == {"name", "flowgroup_count", "action_count"}


def test_pipeline_list_response_fields() -> None:
    dumped = PipelineListResponse(
        pipelines=[PipelineSummary(name="p", flowgroup_count=1, action_count=2)],
        total=1,
    ).model_dump()
    assert set(dumped) == {"pipelines", "total"}


def test_pipeline_detail_response_fields() -> None:
    dumped = PipelineDetailResponse(
        name="p", flowgroup_count=1, flowgroups=["fg"], config={"k": "v"}
    ).model_dump()
    assert set(dumped) == {"name", "flowgroup_count", "flowgroups", "config"}


def test_pipeline_config_response_fields() -> None:
    dumped = PipelineConfigResponse(pipeline="p", config={"k": "v"}).model_dump()
    assert set(dumped) == {"pipeline", "config"}


# --- preset -----------------------------------------------------------------


def test_preset_summary_fields() -> None:
    dumped = PresetSummary(name="bronze").model_dump()
    assert set(dumped) == {"name", "description", "extends"}


def test_preset_list_response_fields() -> None:
    dumped = PresetListResponse(presets=["bronze"], total=1).model_dump()
    assert set(dumped) == {"presets", "total"}


def test_preset_list_detail_response_fields() -> None:
    dumped = PresetListDetailResponse(
        presets=[PresetSummary(name="bronze")], total=1
    ).model_dump()
    assert set(dumped) == {"presets", "total"}


def test_preset_detail_response_fields() -> None:
    dumped = PresetDetailResponse(
        name="bronze",
        raw={"name": "bronze"},
        resolved={"defaults": {}},
        chain=["bronze"],
    ).model_dump()
    assert set(dumped) == {"name", "raw", "resolved", "chain"}


# --- template ---------------------------------------------------------------


def test_template_info_response_fields() -> None:
    dumped = TemplateInfoResponse(
        name="t", version="1.0", description="d", parameters=[], action_count=0
    ).model_dump()
    assert set(dumped) == {
        "name",
        "version",
        "description",
        "parameters",
        "action_count",
    }


def test_template_summary_fields() -> None:
    dumped = TemplateSummary(
        name="t", parameter_count=1, action_count=2, action_types=["load"]
    ).model_dump()
    assert set(dumped) == {
        "name",
        "description",
        "parameter_count",
        "action_count",
        "action_types",
    }


def test_template_list_response_fields() -> None:
    dumped = TemplateListResponse(templates=["t"], total=1).model_dump()
    assert set(dumped) == {"templates", "total"}


def test_template_list_detail_response_fields() -> None:
    dumped = TemplateListDetailResponse(
        templates=[
            TemplateSummary(
                name="t", parameter_count=0, action_count=0, action_types=[]
            )
        ],
        total=1,
    ).model_dump()
    assert set(dumped) == {"templates", "total"}


def test_template_detail_response_fields() -> None:
    dumped = TemplateDetailResponse(
        name="t",
        template=TemplateInfoResponse(
            name="t", version="1.0", description="d", parameters=[], action_count=0
        ),
    ).model_dump()
    assert set(dumped) == {"name", "template"}


# --- dependency -------------------------------------------------------------


def test_graph_node_fields() -> None:
    dumped = GraphNode(
        id="a", label="A", type="load", pipeline="p", flowgroup="fg"
    ).model_dump()
    assert set(dumped) == {
        "id",
        "label",
        "type",
        "pipeline",
        "flowgroup",
        "stage",
        "metadata",
    }


def test_graph_edge_fields() -> None:
    dumped = GraphEdge(source="a", target="b", type="internal").model_dump()
    assert set(dumped) == {"source", "target", "type"}


def test_graph_metadata_fields() -> None:
    dumped = GraphMetadata(
        level="action",
        total_nodes=1,
        total_edges=0,
        stages=1,
        has_circular=False,
        circular_dependencies=[],
        external_sources=[],
    ).model_dump()
    assert set(dumped) == {
        "level",
        "total_nodes",
        "total_edges",
        "stages",
        "has_circular",
        "circular_dependencies",
        "external_sources",
    }


def test_graph_response_fields_nodes_edges_metadata() -> None:
    dumped = GraphResponse(
        nodes=[GraphNode(id="a", label="A", type="load", pipeline="p", flowgroup="fg")],
        edges=[GraphEdge(source="a", target="b", type="internal")],
        metadata=GraphMetadata(
            level="action",
            total_nodes=1,
            total_edges=1,
            stages=1,
            has_circular=False,
            circular_dependencies=[],
            external_sources=[],
        ),
    ).model_dump()
    assert set(dumped) == {"nodes", "edges", "metadata"}


def test_dependency_response_fields() -> None:
    dumped = DependencyResponse(
        total_pipelines=1,
        total_external_sources=0,
        execution_stages=[],
        circular_dependencies=[],
        external_sources=[],
        pipeline_dependencies={},
    ).model_dump()
    assert set(dumped) == {
        "total_pipelines",
        "total_external_sources",
        "execution_stages",
        "circular_dependencies",
        "external_sources",
        "pipeline_dependencies",
    }


def test_execution_order_response_fields() -> None:
    dumped = ExecutionOrderResponse(
        stages=[], total_stages=0, flat_order=[]
    ).model_dump()
    assert set(dumped) == {"stages", "total_stages", "flat_order"}


def test_circular_dependency_response_fields() -> None:
    dumped = CircularDependencyResponse(
        has_circular=False, cycles=[], total_cycles=0
    ).model_dump()
    assert set(dumped) == {"has_circular", "cycles", "total_cycles"}


def test_external_sources_response_fields() -> None:
    dumped = ExternalSourcesResponse(sources=[], total=0).model_dump()
    assert set(dumped) == {"sources", "total"}


def test_export_response_fields() -> None:
    dumped = ExportResponse(format="dot", content="digraph {}").model_dump()
    assert set(dumped) == {"format", "content"}


def test_cross_pipeline_connection_fields() -> None:
    dumped = CrossPipelineConnection(
        direction="downstream", target="fg_c", target_pipeline="silver"
    ).model_dump()
    assert set(dumped) == {"direction", "target", "target_pipeline"}


def test_cross_pipeline_summary_fields() -> None:
    dumped = CrossPipelineSummary(
        pipeline="bronze",
        connections={
            "fg_b": [
                CrossPipelineConnection(
                    direction="upstream", target="fg_d", target_pipeline="gold"
                )
            ]
        },
    ).model_dump()
    assert set(dumped) == {"pipeline", "connections"}
    assert set(dumped["connections"]["fg_b"][0]) == {
        "direction",
        "target",
        "target_pipeline",
    }


# --- table ------------------------------------------------------------------


def test_table_summary_fields() -> None:
    dumped = TableSummary(
        full_name="cat.sch.tbl",
        target_type="streaming_table",
        pipeline="p",
        flowgroup="fg",
        source_file="pipelines/p/fg.yaml",
    ).model_dump()
    assert set(dumped) == {
        "full_name",
        "target_type",
        "pipeline",
        "flowgroup",
        "write_mode",
        "scd_type",
        "source_file",
    }
    # write_mode / scd_type are optional and default to None
    assert dumped["write_mode"] is None
    assert dumped["scd_type"] is None


def test_build_table_summary_round_trip() -> None:
    summary = build_table_summary(
        full_name="cat.sch.tbl",
        target_type="streaming_table",
        pipeline="p",
        flowgroup="fg",
        write_mode="cdc",
        scd_type=2,
        source_file="pipelines/p/fg.yaml",
    )
    assert isinstance(summary, TableSummary)
    assert summary.write_mode == "cdc"
    assert summary.scd_type == 2


def test_table_list_response_fields() -> None:
    dumped = TableListResponse(
        tables=[
            TableSummary(
                full_name="cat.sch.tbl",
                target_type="streaming_table",
                pipeline="p",
                flowgroup="fg",
                source_file="pipelines/p/fg.yaml",
            )
        ],
        total=1,
    ).model_dump()
    assert set(dumped) == {"tables", "total", "warnings"}


# --- common -----------------------------------------------------------------


def test_error_detail_fields() -> None:
    dumped = ErrorDetail(
        code="LHP-VAL-003",
        category="VALIDATION",
        message="bad",
        details="explain",
        http_status=422,
    ).model_dump()
    assert set(dumped) == {
        "code",
        "category",
        "message",
        "details",
        "suggestions",
        "context",
        "http_status",
    }


def test_error_response_fields() -> None:
    dumped = ErrorResponse(
        error=ErrorDetail(
            code="LHP-VAL-003",
            category="VALIDATION",
            message="bad",
            details="explain",
            http_status=422,
        )
    ).model_dump()
    assert set(dumped) == {"error"}


def test_success_response_fields() -> None:
    dumped = SuccessResponse(message="ok").model_dump()
    assert set(dumped) == {"message", "details"}


def test_pagination_params_fields() -> None:
    dumped = PaginationParams().model_dump()
    assert set(dumped) == {"offset", "limit"}
    assert dumped == {"offset": 0, "limit": 50}


def test_paginated_response_fields() -> None:
    dumped = PaginatedResponse[str](
        items=["a"], total=1, offset=0, limit=50
    ).model_dump()
    assert set(dumped) == {"items", "total", "offset", "limit"}


# --- flowgroup --------------------------------------------------------------


def _flowgroup_view() -> FlowgroupView:
    return FlowgroupView(
        name="customers",
        pipeline="bronze",
        file_path=None,
        presets=("bronze_layer",),
        template="ingest_template",
        load_action_count=1,
        transform_action_count=2,
        write_action_count=1,
        test_action_count=0,
        job_name="bronze_job",
    )


def _processed_flowgroup_view() -> ProcessedFlowgroupView:
    return ProcessedFlowgroupView(
        flowgroup=_flowgroup_view(),
        actions=(
            ActionView(
                name="load_customers",
                action_type="load",
                target="v_customers",
                description="Load raw customers",
            ),
            ActionView(
                name="write_customers",
                action_type="write",
                target="customers",
                write_mode="cdc",
                scd_type=2,
                target_full_name="cat.sch.customers",
            ),
        ),
        job_name="bronze_job",
        variables={"region": "us"},
    )


def test_flowgroup_summary_fields() -> None:
    dumped = FlowgroupSummary(
        name="customers",
        pipeline="bronze",
        action_count=4,
        action_types=["load", "transform", "write"],
        source_file="pipelines/bronze/customers.yaml",
        presets=["bronze_layer"],
    ).model_dump()
    assert set(dumped) == {
        "name",
        "pipeline",
        "action_count",
        "action_types",
        "source_file",
        "presets",
        "template",
    }
    assert dumped["template"] is None


def test_flowgroup_summary_from_view() -> None:
    summary = FlowgroupSummary.from_view(
        _flowgroup_view(), source_file="pipelines/bronze/customers.yaml"
    )
    assert isinstance(summary, FlowgroupSummary)
    assert summary.name == "customers"
    assert summary.pipeline == "bronze"
    # 1 load + 2 transform + 1 write + 0 test = 4
    assert summary.action_count == 4
    # only non-zero types, sorted
    assert summary.action_types == ["load", "transform", "write"]
    assert summary.presets == ["bronze_layer"]
    assert summary.template == "ingest_template"
    assert summary.source_file == "pipelines/bronze/customers.yaml"


def test_flowgroup_list_response_fields() -> None:
    dumped = FlowgroupListResponse(
        flowgroups=[
            FlowgroupSummary(
                name="customers",
                pipeline="bronze",
                action_count=1,
                action_types=["load"],
                source_file="pipelines/bronze/customers.yaml",
                presets=[],
            )
        ],
        total=1,
    ).model_dump()
    assert set(dumped) == {"flowgroups", "total"}


def test_flowgroup_list_response_from_views() -> None:
    resp = FlowgroupListResponse.from_views(
        [(_flowgroup_view(), "pipelines/bronze/customers.yaml")]
    )
    assert resp.total == 1
    assert resp.flowgroups[0].name == "customers"
    assert resp.flowgroups[0].action_count == 4


def test_pipeline_flowgroups_response_from_views() -> None:
    resp = PipelineFlowgroupsResponse.from_views(
        [(_flowgroup_view(), "pipelines/bronze/customers.yaml")]
    )
    dumped = resp.model_dump()
    assert set(dumped) == {"flowgroups", "total"}
    assert resp.total == 1


def test_flowgroup_action_summary_fields() -> None:
    dumped = FlowgroupActionSummary(name="a", type="write").model_dump()
    assert set(dumped) == {
        "name",
        "type",
        "target",
        "description",
        "transform_type",
        "test_type",
        "write_mode",
        "scd_type",
        "target_full_name",
    }


def test_flowgroup_action_summary_from_view_maps_action_type_to_type() -> None:
    action = ActionView(
        name="write_customers",
        action_type="write",
        target="customers",
        write_mode="cdc",
        scd_type=2,
        target_full_name="cat.sch.customers",
    )
    summary = FlowgroupActionSummary.from_view(action)
    # the public field is "type", mapped from the view's "action_type"
    assert summary.type == "write"
    assert summary.write_mode == "cdc"
    assert summary.scd_type == 2
    assert summary.target_full_name == "cat.sch.customers"


def test_flowgroup_config_from_view() -> None:
    config = FlowgroupConfig.from_view(_processed_flowgroup_view())
    assert isinstance(config, FlowgroupConfig)
    assert config.name == "customers"
    assert config.pipeline == "bronze"
    assert config.job_name == "bronze_job"
    assert config.variables == {"region": "us"}
    assert [a.type for a in config.actions] == ["load", "write"]
    assert config.actions[1].write_mode == "cdc"


def test_flowgroup_detail_response_from_view() -> None:
    resp = FlowgroupDetailResponse.from_view(
        _processed_flowgroup_view(), source_file="pipelines/bronze/customers.yaml"
    )
    dumped = resp.model_dump()
    assert set(dumped) == {"flowgroup", "source_file"}
    # frontend reads flowgroup.actions[].type / name / target opaquely
    assert set(dumped["flowgroup"]) == {
        "name",
        "pipeline",
        "presets",
        "template",
        "job_name",
        "actions",
        "variables",
    }
    assert dumped["flowgroup"]["actions"][0]["type"] == "load"
    assert dumped["source_file"] == "pipelines/bronze/customers.yaml"


def test_resolved_flowgroup_response_from_view() -> None:
    resp = ResolvedFlowgroupResponse.from_view(
        _processed_flowgroup_view(), environment="dev"
    )
    dumped = resp.model_dump()
    assert set(dumped) == {
        "flowgroup",
        "environment",
        "applied_presets",
        "applied_template",
    }
    assert dumped["environment"] == "dev"
    assert dumped["applied_presets"] == ["bronze_layer"]
    assert dumped["applied_template"] == "ingest_template"


def test_related_file_info_fields_and_from_related_file() -> None:
    dumped = RelatedFileInfo(
        path="sql/customers.sql",
        category="sql",
        action_name="load_customers",
        field="sql_path",
        exists=True,
    ).model_dump()
    assert set(dumped) == {"path", "category", "action_name", "field", "exists"}

    from lhp.webapp.services.related_files_extractor import RelatedFile

    rf = RelatedFile(
        path="sql/customers.sql",
        category="sql",
        action_name="load_customers",
        field="sql_path",
        exists=False,
    )
    info = RelatedFileInfo.from_related_file(rf)
    assert info.path == "sql/customers.sql"
    assert info.category == "sql"
    assert info.exists is False


def test_flowgroup_related_files_response_fields() -> None:
    src = RelatedFileInfo(
        path="pipelines/bronze/customers.yaml",
        category="sql",
        action_name="",
        field="source_file",
        exists=True,
    )
    dumped = FlowgroupRelatedFilesResponse(
        flowgroup="customers",
        source_file=src,
        related_files=[],
        environment="dev",
    ).model_dump()
    assert set(dumped) == {
        "flowgroup",
        "source_file",
        "related_files",
        "environment",
    }
