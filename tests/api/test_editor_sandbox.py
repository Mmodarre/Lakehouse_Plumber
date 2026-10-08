"""Real sandbox editor parity and draft isolation, based on the E2E project."""

from __future__ import annotations

import hashlib
import json
import pickle
import shutil
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml

from lhp.api import (
    EditorDocumentOverlay,
    ErrorEmitted,
    GenerationCompleted,
    GenerationPlanCompleted,
    LakehousePlumberApplicationFacade,
    OperationStarted,
    ValidationCompleted,
    WarningEmitted,
    inspect_editor_project,
    preview_editor_project,
    to_dict,
    validate_editor_project,
)
from lhp.errors import LHPError


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Copy existing integration inputs; never edit tracked E2E fixtures."""
    root = tmp_path / "project"
    shutil.copytree(
        Path(__file__).parents[1] / "e2e/fixtures/testing_project",
        root,
        ignore=shutil.ignore_patterns("*baseline*", "generated", ".lhp", ".git"),
    )
    config = yaml.safe_load((root / "lhp.yaml").read_text())
    config["include"] = [
        "18_helper_imports/**",
        "19_dependency_bindings/**",
        "editor/**",
    ]
    (root / "lhp.yaml").write_text(yaml.safe_dump(config))
    return root


def fingerprint(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }


def profile(
    namespace: str = "alice", pipelines: tuple[str, ...] = ("dep_bindings",)
) -> str:
    return yaml.safe_dump(
        {"sandbox": {"namespace": namespace, "pipelines": list(pipelines)}}
    )


def drafts(root: Path) -> tuple[EditorDocumentOverlay, ...]:
    config = yaml.safe_load((root / "lhp.yaml").read_text())
    config["sandbox"] = {
        "strategy": "table",
        "table_pattern": "{namespace}__{table}",
        "allowed_envs": ["dev"],
    }
    environment = (
        (root / "substitutions/dev.yaml")
        .read_text()
        .replace("acme_edw_dev", "draft_catalog")
    )
    module_path = "py_functions/dep_bindings_opaque_transform.py"
    module = (
        (root / module_path)
        .read_text()
        .replace(
            'lookup = spark.read.table(os.environ["DEP_BINDINGS_LOOKUP_TABLE"])',
            'lookup = spark.read.table(os.environ["DEP_BINDINGS_LOOKUP_TABLE"])\n'
            '    lookup = lookup.unionByName(spark.read.table("draft_catalog.edw_silver.configured_union"))',
        )
    )
    flow = {
        "pipeline": "draft_sql",
        "flowgroup": "editor_sql",
        "actions": [
            {
                "name": "load_sql",
                "type": "load",
                "source": {"type": "sql", "sql_path": "sql/editor.sql"},
                "target": "v_editor",
            },
            {
                "name": "write_sql",
                "type": "write",
                "source": "v_editor",
                "write_target": {
                    "type": "materialized_view",
                    "database": "${catalog}.${silver_schema}",
                    "table": "editor_out",
                },
            },
            {
                "name": "check_sql",
                "type": "test",
                "test_type": "uniqueness",
                "source": "v_editor",
                "target": "editor_check",
                "columns": ["id"],
                "on_violation": "warn",
                "test_id": "EDITOR-1",
            },
        ],
    }
    return tuple(
        EditorDocumentOverlay(path, text, 1)
        for path, text in (
            (
                ".lhp/profile.yaml",
                profile("alice", ("dep_bindings", "helper_imports", "draft_sql")),
            ),
            ("lhp.yaml", yaml.safe_dump(config)),
            ("substitutions/dev.yaml", environment),
            (module_path, module),
            ("pipelines/editor/sql.yaml", yaml.safe_dump(flow)),
            (
                "sql/editor.sql",
                "SELECT * FROM ${catalog}.${silver_schema}.configured_union UNION ALL SELECT * FROM ${catalog}.${bronze_schema}.shared_input",
            ),
        )
    )


def test_draft_scope_and_source_preview_match_real_sandbox_generation(
    project: Path, tmp_path: Path
) -> None:
    overlays = drafts(project)
    before = fingerprint(project)
    snapshot = inspect_editor_project(project, overlays=overlays, sandbox=True)
    assert snapshot.sandbox_enabled and not snapshot.stale
    scope = snapshot.sandbox
    assert scope and scope.error is None and scope.profile_exists
    assert scope.namespace == "alice"
    assert scope.resolved_pipelines == ("dep_bindings", "draft_sql", "helper_imports")
    assert (
        scope.allowed_envs == ("dev",) and scope.table_pattern == "{namespace}__{table}"
    )
    assert {fg.pipeline for fg in snapshot.flowgroups} > set(scope.resolved_pipelines)
    assert pickle.loads(pickle.dumps(snapshot)) == snapshot
    assert json.loads(json.dumps(to_dict(snapshot)))["sandbox"]["namespace"] == "alice"
    with pytest.raises(FrozenInstanceError):
        snapshot.sandbox_enabled = False  # type: ignore[misc]

    validation = list(
        validate_editor_project(project, env="dev", overlays=overlays, sandbox=True)
    )
    assert isinstance(validation[-1], ValidationCompleted)
    assert validation[-1].response.success
    preview = list(
        preview_editor_project(
            project, env="dev", overlays=overlays, sandbox=True, include_tests=True
        )
    )
    assert isinstance(preview[-1], GenerationPlanCompleted)
    plan = preview[-1].response
    planned = {str(item.path): item.content for item in plan.files}
    assert plan.output_location == project / "generated/dev"

    actual = tmp_path / "actual"
    shutil.copytree(project, actual)
    for overlay in overlays:
        target = actual / overlay.path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(overlay.text)
    facade = LakehousePlumberApplicationFacade.for_project(
        actual, no_cache=True, enforce_version=False, max_workers=1
    )
    generated = list(
        facade.generate_pipelines(
            env="dev",
            output_dir=actual / "generated/dev",
            sandbox=True,
            include_tests=True,
            bundle_enabled=False,
        )
    )
    assert (
        isinstance(generated[-1], GenerationCompleted)
        and generated[-1].response.success
    )
    written = {
        path.relative_to(actual / "generated/dev").as_posix(): path.read_text()
        for path in (actual / "generated/dev").rglob("*")
        if path.is_file()
    }
    assert planned == written
    rendered = "\n".join(planned.values())
    assert "alice__configured_union" in rendered
    assert "alice__helper_snapshot_dim" in rendered  # bound Python parameter
    assert "draft_catalog.edw_bronze.shared_input" in rendered  # shared input unchanged
    assert any("dep_bindings_opaque_transform.py" in path for path in planned)
    assert "def __lhp_sandbox_table(" in rendered
    assert "__lhp_sandbox_table(os.environ" in rendered
    assert any("_test_reporting_hook.py" in path for path in planned)
    assert [
        (event.code, event.category)
        for event in preview
        if isinstance(event, WarningEmitted)
    ] == [
        (event.code, event.category)
        for event in generated
        if isinstance(event, WarningEmitted)
    ]
    assert fingerprint(project) == before
    assert not (project / ".lhp").exists()


def test_scope_retains_out_of_scope_graph_and_off_mode_metadata(project: Path) -> None:
    overlay = EditorDocumentOverlay(".lhp/profile.yaml", profile(), 1)
    view = inspect_editor_project(project, overlays=(overlay,), sandbox=True)
    assert view.sandbox and view.sandbox.resolved_pipelines == ("dep_bindings",)
    assert {fg.pipeline for fg in view.flowgroups} >= {"dep_bindings", "helper_imports"}
    off = inspect_editor_project(project, overlays=(overlay,))
    assert not off.sandbox_enabled and off.sandbox == view.sandbox


@pytest.mark.parametrize(
    "text", ["sandbox: [", "sandbox: {namespace: 'BAD!', pipelines: [dep_bindings]}"]
)
def test_invalid_profile_draft_is_never_presented_as_valid_scope(
    project: Path, text: str
) -> None:
    private = project / ".lhp"
    private.mkdir()
    (private / "profile.yaml").write_text(profile("saved"))
    before = fingerprint(project)
    overlay = EditorDocumentOverlay(".lhp/profile.yaml", text, 1)
    view = inspect_editor_project(project, overlays=(overlay,), sandbox=True)
    assert view.sandbox and view.sandbox.error and not view.sandbox.resolved_pipelines
    assert any(item.severity == "error" for item in view.diagnostics)
    assert fingerprint(project) == before
    # Off ignores sandbox validity for canonical validation and preview.
    assert isinstance(
        list(validate_editor_project(project, env="dev", overlays=(overlay,)))[-1],
        ValidationCompleted,
    )
    assert isinstance(
        list(preview_editor_project(project, env="dev", overlays=(overlay,)))[-1],
        GenerationPlanCompleted,
    )


@pytest.mark.parametrize("operation", [preview_editor_project, validate_editor_project])
@pytest.mark.parametrize(
    "condition,code",
    [
        ("missing", "LHP-IO-025"),
        ("env", "LHP-CFG-065"),
        ("scope", "LHP-VAL-064"),
        ("malformed", "LHP-CFG-064"),
    ],
)
def test_sandbox_errors_emit_once_then_raise(
    project: Path, operation, condition: str, code: str
) -> None:
    overlays: tuple[EditorDocumentOverlay, ...] = ()
    if condition != "missing":
        text = (
            profile(pipelines=("missing_pipeline",))
            if condition == "scope"
            else profile()
        )
        if condition == "malformed":
            text = "sandbox: ["
        overlays = (EditorDocumentOverlay(".lhp/profile.yaml", text, 1),)
    if condition == "env":
        config = yaml.safe_load((project / "lhp.yaml").read_text())
        config["sandbox"] = {"allowed_envs": ["prod"]}
        overlays += (EditorDocumentOverlay("lhp.yaml", yaml.safe_dump(config), 1),)
    events = []
    with pytest.raises(LHPError) as raised:
        for event in operation(project, env="dev", overlays=overlays, sandbox=True):
            events.append(event)
    assert raised.value.code == code
    assert isinstance(events[0], OperationStarted)
    assert sum(isinstance(event, OperationStarted) for event in events) == 1
    assert sum(isinstance(event, ErrorEmitted) for event in events) == 1
    assert isinstance(events[-1], ErrorEmitted)
    assert not (project / "generated").exists()


@pytest.mark.parametrize(
    "path",
    [
        ".lhp/state.json",
        ".lhp/cache/profile.yaml",
        "./.lhp/profile.yaml",
        ".lhp//profile.yaml",
        ".lhp/../profile.yaml",
    ],
)
def test_only_exact_private_profile_overlay_is_allowed(
    project: Path, path: str
) -> None:
    with pytest.raises(ValueError):
        inspect_editor_project(
            project, overlays=(EditorDocumentOverlay(path, profile(), 1),)
        )


@pytest.mark.parametrize("parent", [True, False])
def test_profile_symlinks_are_rejected_even_inside_project(
    project: Path, parent: bool
) -> None:
    target = project / "private_target"
    target.mkdir()
    (target / "profile.yaml").write_text(profile())
    if parent:
        (project / ".lhp").symlink_to(target, target_is_directory=True)
    else:
        (project / ".lhp").mkdir()
        (project / ".lhp/profile.yaml").symlink_to(target / "profile.yaml")
    with pytest.raises(ValueError, match="symlink"):
        list(preview_editor_project(project, env="dev", sandbox=True))


def test_template_references_include_unused_definitions(project: Path) -> None:
    template = project / "templates/editor_unused.yaml"
    template.write_text(
        "name: unused_editor\nactions:\n  - name: unused\n    type: transform\n    transform_type: python\n    module_path: py_functions/dep_bindings_opaque_transform.py\n    function_name: opaque_helper_read\n    source: src\n    target: dest\n"
    )
    view = inspect_editor_project(project)
    refs = view.catalog.template_related_files["templates/editor_unused.yaml"]
    assert len(refs) == 1 and refs[0].exists
    assert refs[0].path == "py_functions/dep_bindings_opaque_transform.py"
    assert refs[0].source.path == "templates/editor_unused.yaml"
    assert refs[0].source.line == 5
    assert not any(
        fg.definition and fg.definition.path == "templates/editor_unused.yaml"
        for fg in view.flowgroups
    )


def test_out_of_scope_wheel_does_not_block_source_sandbox_preview(
    project: Path,
) -> None:
    config_path = "config/editor_packaging.yaml"
    (project / config_path).write_text("pipeline: helper_imports\npackaging: wheel\n")
    overlays = (EditorDocumentOverlay(".lhp/profile.yaml", profile(), 1),)
    events = list(
        preview_editor_project(
            project,
            env="dev",
            overlays=overlays,
            sandbox=True,
            pipeline_config_path=config_path,
        )
    )
    assert isinstance(events[-1], GenerationPlanCompleted)
    assert {item.pipeline for item in events[-1].response.files} == {"dep_bindings"}
    with pytest.raises(ValueError, match="Wheel"):
        list(
            preview_editor_project(project, env="dev", pipeline_config_path=config_path)
        )


def test_template_parameter_resources_have_each_concrete_consumer(
    project: Path,
) -> None:
    template = project / "templates/editor_resources.yaml"
    template.write_text("""name: editor_resources
parameters:
  - name: suffix
    type: string
    required: true
  - name: module
    type: string
    required: true
  - name: query
    type: string
    required: true
actions:
  - name: load_{{ suffix }}
    type: load
    source:
      type: sql
      sql_path: "{{ query }}"
    target: v_{{ suffix }}
  - name: transform_{{ suffix }}
    type: transform
    transform_type: python
    module_path: "{{ module }}"
    function_name: opaque_helper_read
    source: v_{{ suffix }}
    target: out_{{ suffix }}
  - name: write_{{ suffix }}
    type: write
    source: out_{{ suffix }}
    write_target:
      type: materialized_view
      database: "${catalog}.${silver_schema}"
      table: "resource_{{ suffix }}"
""")
    (project / "sql/editor_shared.sql").write_text("SELECT 1 AS id")
    directory = project / "pipelines/editor"
    directory.mkdir()
    for suffix in ("one", "two"):
        spec = {
            "pipeline": "editor_resources",
            "flowgroup": suffix,
            "use_template": "editor_resources",
            "template_parameters": {
                "suffix": suffix,
                "module": "py_functions/dep_bindings_opaque_transform.py",
                "query": "sql/editor_shared.sql",
            },
        }
        (directory / f"{suffix}.yaml").write_text(yaml.safe_dump(spec))
    view = inspect_editor_project(project)
    consumers = [fg for fg in view.flowgroups if fg.pipeline == "editor_resources"]
    assert len(consumers) == 2
    for fg in consumers:
        refs = [ref for action in fg.actions for ref in action.related_files]
        assert {ref.path for ref in refs} == {
            "sql/editor_shared.sql",
            "py_functions/dep_bindings_opaque_transform.py",
        }
        assert all(
            ref.exists and ref.source.path == "templates/editor_resources.yaml"
            for ref in refs
        )
    declared = view.catalog.template_related_files["templates/editor_resources.yaml"]
    assert len(declared) == 2
    assert all(not ref.exists and "{{" in ref.path for ref in declared)
