"""Real-project contracts for the public editor integration surface."""

from __future__ import annotations

import json
import pickle
import shutil
from dataclasses import FrozenInstanceError, fields, is_dataclass
from pathlib import Path
from typing import get_type_hints

import pytest

from lhp.api import (
    EditorDocumentOverlay,
    ErrorEmitted,
    GenerationPlanCompleted,
    OperationStarted,
    ValidationCompleted,
    editor_catalog,
    inspect_editor_document,
    inspect_editor_project,
    preview_editor_project,
    scaffold_editor_bronze,
    scaffold_editor_instance,
    to_dict,
    validate_editor_project,
)
from lhp.errors import LHPError
from lhp.parsers import source_spans


@pytest.fixture
def editor_project(tmp_path: Path) -> Path:
    source = Path("docs/_fixtures/first_pipeline")
    target = tmp_path / "project"
    shutil.copytree(source, target, ignore=shutil.ignore_patterns("generated", ".lhp"))
    return target


def test_snapshot_is_public_frozen_json_and_graph_backed(editor_project: Path) -> None:
    view = inspect_editor_project(editor_project)
    assert len(view.flowgroups) == 1
    assert view.diagnostics == ()
    assert view.dependencies is not None
    assert view.dependencies.action_graph is not None
    assert len(view.dependencies.action_graph.nodes) == len(view.flowgroups[0].actions)
    assert view.flowgroups[0].source.path == "pipelines/bronze_ingest.yaml"
    assert view.flowgroups[0].source.yaml_path == ()
    assert view.flowgroups[0].instance == view.flowgroups[0].source
    assert all(action.source.line is not None for action in view.flowgroups[0].actions)
    assert all(action.editable for action in view.flowgroups[0].actions)
    assert is_dataclass(view) and view.__dataclass_params__.frozen
    with pytest.raises(FrozenInstanceError):
        view.environment = "prod"  # type: ignore[misc]
    assert pickle.loads(pickle.dumps(view)) == view
    payload = json.loads(json.dumps(to_dict(view)))
    assert payload["flowgroups"][0]["pipeline"] == "bronze_ingest"
    assert payload["dependencies"]["action_graph"]["nodes"]


def test_catalog_exposes_model_and_installed_action_help(editor_project: Path) -> None:
    catalog = editor_catalog(editor_project)
    assert "test_type" in catalog.action_schema["properties"]
    assert catalog.action_help["entries"]
    assert any("test" in entry["id"] for entry in catalog.action_help["entries"])
    assert all(is_dataclass(item) for item in catalog.templates)
    assert "flowgroup" in catalog.schemas


def test_overlay_uses_mirror_and_keeps_saved_project(editor_project: Path) -> None:
    path = editor_project / "pipelines" / "bronze_ingest.yaml"
    saved = path.read_text(encoding="utf-8")
    draft = saved.replace("orders_ingest", "orders_draft")
    assert draft != saved
    overlay = EditorDocumentOverlay("pipelines/bronze_ingest.yaml", draft, 7)
    view = inspect_editor_project(editor_project, overlays=(overlay,))
    assert view.flowgroups[0].name == "orders_draft"
    assert view.flowgroups[0].source.path == overlay.path
    assert str(editor_project) not in json.dumps(to_dict(view.flowgroups[0].source))
    assert path.read_text(encoding="utf-8") == saved
    assert not (editor_project / ".lhp").exists()


def test_invalid_second_overlay_is_diagnosed_at_its_own_source(
    editor_project: Path,
) -> None:
    valid = EditorDocumentOverlay("substitutions/dev.yaml", "variables: {}\n", 1)
    bad = EditorDocumentOverlay("pipelines/bronze_ingest.yaml", "name: 😀\nbad: [\n", 2)
    view = inspect_editor_project(editor_project, overlays=(valid, bad))
    assert view.stale
    diagnostic = next(item for item in view.diagnostics if item.layer == "syntax")
    assert diagnostic.source is not None
    assert diagnostic.source.path == bad.path
    assert diagnostic.source.line == 2
    assert diagnostic.source.column is not None
    document = inspect_editor_document(editor_project, path=bad.path, text=bad.text)
    assert document.diagnostics[0].source == diagnostic.source


def test_streams_start_and_finish_with_canonical_responses(
    editor_project: Path,
) -> None:
    validation = list(validate_editor_project(editor_project, env="dev"))
    assert isinstance(validation[0], OperationStarted)
    assert sum(isinstance(event, OperationStarted) for event in validation) == 1
    assert isinstance(validation[-1], ValidationCompleted)
    preview = list(preview_editor_project(editor_project, env="dev"))
    assert isinstance(preview[0], OperationStarted)
    assert sum(isinstance(event, OperationStarted) for event in preview) == 1
    assert isinstance(preview[-1], GenerationPlanCompleted)
    assert preview[-1].response.files
    assert preview[-1].response.output_location == editor_project / "generated" / "dev"
    assert not (editor_project / "generated").exists()


def test_unsaved_sql_overlay_changes_text_preview_without_write() -> None:
    project = Path("docs/_fixtures/sample_project")
    sql_path = project / "sql" / "sales_by_nation.sql"
    saved = sql_path.read_text(encoding="utf-8")
    draft = saved.replace("AS order_count,", "AS order_count_draft,")
    assert draft != saved
    terminal = list(
        preview_editor_project(
            project,
            env="dev",
            overlays=(EditorDocumentOverlay("sql/sales_by_nation.sql", draft, 5),),
        )
    )[-1]
    assert isinstance(terminal, GenerationPlanCompleted)
    rendered = "\n".join(item.content for item in terminal.response.files)
    assert "AS order_count_draft," in rendered
    assert sql_path.read_text(encoding="utf-8") == saved


def test_generated_monitoring_node_remains_visible_and_noneditable() -> None:
    view = inspect_editor_project(Path("docs/_fixtures/sample_project"))
    generated = tuple(item for item in view.flowgroups if item.origin == "generated")
    assert len(generated) == 1
    monitoring = generated[0]
    assert monitoring.source.path == "lhp.yaml"
    assert monitoring.source.yaml_path == ("monitoring",)
    assert monitoring.source.line is not None
    assert monitoring.instance is None
    assert not monitoring.editable
    assert all(
        action.origin == "generated" and not action.editable
        for action in monitoring.actions
    )
    assert any(item.severity == "information" for item in view.diagnostics)


def test_source_spans_utf16_stream_and_cycle_limit() -> None:
    spans = source_spans("label: 😀foo\n---\n- name: bar\n")
    assert spans[(0, ("label",))].column == 7
    assert spans[(0, ("label",))].end_column == 12
    assert spans[(1, (0, "name"))].document_index == 1
    with pytest.raises(ValueError, match="aliases"):
        source_spans("a: &a {b: *a}\n")


def test_bronze_scaffold_is_native_yaml_and_does_not_write(
    editor_project: Path,
) -> None:
    result = scaffold_editor_bronze(
        name="customers",
        pipeline="bronze",
        source_path="/Volumes/raw/customers",
        format="csv",
        target="main.bronze.customers",
    )
    assert result.kind == "bronze"
    assert "cloudfiles" in result.content
    assert "streaming_table" in result.content
    assert not (editor_project / "pipelines" / "customers.yaml").exists()
    assert "catalog: main" in result.content
    assert "schema: bronze" in result.content
    (editor_project / "pipelines" / "customers.yaml").write_text(
        result.content, encoding="utf-8"
    )
    terminal = list(validate_editor_project(editor_project, env="dev"))[-1]
    assert isinstance(terminal, ValidationCompleted)
    assert terminal.response.success


def test_nested_template_and_blueprint_scaffolds_use_project_catalog() -> None:
    project = Path("tests/e2e/fixtures/testing_project")
    template = scaffold_editor_instance(
        project,
        kind="template",
        reference="ingestion/csv_ingestion_template",
        parameters={
            "landing_folder": "/Volumes/raw/customer",
            "schema_file": "schemas/customer.yaml",
            "table_name": "customer",
        },
        pipeline="raw",
        flowgroup="customer",
    )
    assert "use_template: ingestion/csv_ingestion_template" in template.content
    blueprint = scaffold_editor_instance(
        project,
        kind="blueprint",
        reference="medallion_demo",
        parameters={"site_name": "alpha", "domain_id": "ALPHA001"},
    )
    assert "use_blueprint: medallion_demo" in blueprint.content


def test_editor_dto_fields_are_frozen_contract_types(editor_project: Path) -> None:
    from lhp.api import editor_views

    for name, candidate in vars(editor_views).items():
        if not name.startswith("Editor"):
            continue
        if isinstance(candidate, type) and is_dataclass(candidate):
            assert candidate.__dataclass_params__.frozen
            assert fields(candidate)
            for hint in get_type_hints(candidate).values():
                assert "typing.Any" not in str(hint)
                assert "typing.Dict" not in str(hint)
                assert "typing.List" not in str(hint)
                assert "Exception" not in str(hint)
    project = inspect_editor_project(editor_project)
    invalid = inspect_editor_document(
        editor_project, path="pipelines/draft.yaml", text="broken: [\n"
    )
    related_project = inspect_editor_project(Path("tests/e2e/fixtures/testing_project"))
    related = next(
        file
        for flowgroup in related_project.flowgroups
        for action in flowgroup.actions
        for file in action.related_files
    )
    sample = (
        EditorDocumentOverlay("pipelines/draft.yaml", "draft: true", 1),
        project.flowgroups[0].source,
        invalid.diagnostics[0],
        related,
        project.flowgroups[0].actions[0],
        project.flowgroups[0],
        project.catalog,
        project,
        invalid,
        scaffold_editor_bronze(
            name="draft",
            pipeline="bronze",
            source_path="/Volumes/raw/draft",
            format="csv",
            target="main.bronze.draft",
        ),
    )
    assert {type(item).__name__ for item in sample} == {
        name
        for name, candidate in vars(editor_views).items()
        if name.startswith("Editor")
        and isinstance(candidate, type)
        and is_dataclass(candidate)
    }
    for item in sample:
        assert pickle.loads(pickle.dumps(item)) == item
        assert json.loads(json.dumps(to_dict(item))) == to_dict(item)


def test_multidocument_nested_flowgroup_points_to_exact_node(
    editor_project: Path,
) -> None:
    path = editor_project / "pipelines" / "bronze_ingest.yaml"
    original = path.read_text(encoding="utf-8")
    path.write_text(
        original + "\n---\npipeline: bronze_ingest\nflowgroup: document_second\n"
        "actions: []\n",
        encoding="utf-8",
    )
    view = inspect_editor_project(editor_project)
    first = next(item for item in view.flowgroups if item.name == "orders_ingest")
    second = next(item for item in view.flowgroups if item.name == "document_second")
    assert first.source.document_index == 0 and first.source.yaml_path == ()
    assert second.source.document_index == 1
    assert second.source.yaml_path == ()
    assert second.instance == second.source
    assert second.raw["flowgroup"] == "document_second"
    path.write_text(
        "pipeline: bronze_ingest\nflowgroups:\n"
        "  - flowgroup: nested_first\n    actions: []\n"
        "  - flowgroup: nested_second\n    actions: []\n",
        encoding="utf-8",
    )
    view = inspect_editor_project(editor_project)
    nested = next(item for item in view.flowgroups if item.name == "nested_second")
    assert nested.source.document_index == 0
    assert nested.source.yaml_path == ("flowgroups", 1)
    assert nested.instance == nested.source
    assert nested.raw["flowgroup"] == "nested_second"


def test_blueprint_invocation_and_nested_template_are_distinct() -> None:
    view = inspect_editor_project(Path("tests/e2e/fixtures/testing_project"))
    blueprint = next(item for item in view.flowgroups if item.origin == "blueprint")
    assert blueprint.raw["use_blueprint"] == "medallion_demo"
    assert blueprint.definition_raw is not None
    assert "flowgroup" in blueprint.definition_raw
    assert blueprint.source == blueprint.instance
    assert blueprint.source.path.startswith("pipelines/10_blueprint_demo/sites/")
    assert blueprint.definition is not None
    assert blueprint.definition.path == "blueprints/medallion_demo.yaml"
    nested_template = next(
        item
        for item in view.flowgroups
        if item.name == "customer_ingestion_incremental"
    )
    assert nested_template.definition is not None
    assert (
        nested_template.definition.path
        == "templates/ingestion/csv_ingestion_template.yaml"
    )
    assert all(action.origin == "template" for action in nested_template.actions)
    assert all(action.source.line is not None for action in nested_template.actions)


def test_snapshot_keeps_sql_extracted_cross_flowgroup_edge() -> None:
    view = inspect_editor_project(Path("tests/e2e/fixtures/testing_project"))
    assert view.dependencies is not None and view.dependencies.action_graph is not None
    edges = view.dependencies.action_graph.edges
    assert any(
        edge.source == "acmi_edw_silver.customer_silver_dim.write_customer_silver"
        and edge.target
        == "gold_load.customer_lifetime_value.customer_lifetime_value_sql"
        for edge in edges
    )


def test_overlay_path_size_and_symlink_are_rejected(
    editor_project: Path, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="relative"):
        inspect_editor_project(
            editor_project,
            overlays=(EditorDocumentOverlay("../outside.yaml", "a: b", 1),),
        )
    with pytest.raises(ValueError, match="2 MiB"):
        inspect_editor_project(
            editor_project,
            overlays=(
                EditorDocumentOverlay(
                    "pipelines/draft.yaml", "x" * (2 * 1024 * 1024 + 1), 1
                ),
            ),
        )
    outside = tmp_path / "outside.yaml"
    outside.write_text("secret: no\n", encoding="utf-8")
    (editor_project / "pipelines" / "link.yaml").symlink_to(outside)
    with pytest.raises(ValueError, match="symlinked"):
        list(preview_editor_project(editor_project, env="dev"))


def test_mirror_rejects_skipped_input_and_total_copy_limit(
    editor_project: Path,
) -> None:
    config = editor_project / "lhp.yaml"
    original = config.read_text(encoding="utf-8")
    config.write_text(
        original + "\ninclude:\n  - generated/**/*.yaml\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="configured include"):
        list(preview_editor_project(editor_project, env="dev"))
    config.write_text(original, encoding="utf-8")
    oversized = editor_project / "large_unused.bin"
    with oversized.open("wb") as stream:
        stream.truncate(256 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="copy limit"):
        list(preview_editor_project(editor_project, env="dev"))


def test_preview_lhp_error_emits_before_raise(editor_project: Path) -> None:
    source = (editor_project / "pipelines" / "bronze_ingest.yaml").read_text(
        encoding="utf-8"
    )
    draft = "use_template: nonexistent_template\n" + source
    stream = preview_editor_project(
        editor_project,
        env="dev",
        overlays=(EditorDocumentOverlay("pipelines/bronze_ingest.yaml", draft, 1),),
    )
    observed = []
    with pytest.raises(LHPError):
        for event in stream:
            observed.append(event)
    assert isinstance(observed[0], OperationStarted)
    assert isinstance(observed[-1], ErrorEmitted)
    assert sum(isinstance(event, OperationStarted) for event in observed) == 1


@pytest.mark.parametrize("operation", [validate_editor_project, preview_editor_project])
def test_invalid_project_stream_emits_error_before_raise(
    editor_project: Path, operation: object
) -> None:
    events = []
    stream = operation(
        editor_project,
        env="dev",
        overlays=(EditorDocumentOverlay("lhp.yaml", "name: [\n", 1),),
    )
    with pytest.raises(LHPError):
        for event in stream:
            events.append(event)
    assert [type(event) for event in events] == [OperationStarted, ErrorEmitted]


def test_wheel_preview_rejects_before_binary_plan() -> None:
    events = []
    stream = preview_editor_project(
        Path("tests/e2e/fixtures/testing_project"),
        env="dev",
        pipeline_config_path="config/pipeline_config_wheel.yaml",
    )
    with pytest.raises(ValueError, match="Wheel projects"):
        for event in stream:
            events.append(event)
    assert len(events) == 1 and isinstance(events[0], OperationStarted)
