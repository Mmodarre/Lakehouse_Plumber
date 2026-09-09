"""Template authoring parity, source identity, and read-only preview guarantees."""

from __future__ import annotations

import hashlib
import multiprocessing
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import yaml

from lhp.api import preview_template, template_catalog, template_source
from lhp.core.processing.template_authoring import inspect_source
from lhp.core.processing.template_engine import TemplateEngine
from lhp.core.processing.template_preview import run_preview
from lhp.core.processing.template_preview_dependencies import PreviewConfigValidator
from lhp.generators.registration import register_all
from lhp.models import FlowGroup, FlowGroupContext
from lhp.parsers.yaml_parser import YAMLParser

FIXTURE = Path(__file__).resolve().parents[2] / "docs/_fixtures/guide_reuse_templates"
SIMPLE = """name: display_name
parameters:
  - name: entity
    required: true
actions:
  - name: load_{{ entity }}
    type: load
    source:
      type: sql
      sql: SELECT 1 AS id
    target: v_{{ entity }}
  - name: write_{{ entity }}
    type: write
    source: v_{{ entity }}
    write_target:
      type: streaming_table
      catalog: example
      schema: bronze
      table: "{{ entity }}"
"""


@pytest.fixture
def project(tmp_path):
    destination = tmp_path / "project"
    shutil.copytree(FIXTURE, destination)
    register_all()
    return destination


def request(source=SIMPLE, stage="expanded", **updates):
    value = {
        "source_path": "templates/draft.yaml",
        "source_yaml": source,
        "request_revision": "revision-1",
        "stage": stage,
        "sample_parameters": {"entity": "orders"},
    }
    value.update(updates)
    return value


def files(root):
    return {
        p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


def test_catalog_path_identity_nested_invalid_and_extensions(project):
    nested = project / "templates/ingestion"
    nested.mkdir()
    (nested / "callable.yaml").write_text(SIMPLE)
    (nested / "callable.yml").write_text(SIMPLE)
    (nested / "broken.yaml").write_text("name: [broken")
    entries = {e["source_path"]: e for e in template_catalog(project)["templates"]}
    entry = entries["templates/ingestion/callable.yaml"]
    assert entry["declared_name"] == "display_name"
    assert entry["reference"] == "ingestion/callable"
    assert entry["state"] == "ready"
    assert entries["templates/ingestion/callable.yml"]["reference"] is None
    assert (
        entries["templates/ingestion/callable.yml"]["state"] == "unsupported_extension"
    )
    assert entries["templates/ingestion/broken.yaml"]["state"] == "invalid"
    assert entries["templates/ingestion/broken.yaml"]["diagnostics"][0]["line"] == 2


def test_catalog_keeps_default_presence_and_native_values(project):
    params = [{"name": "absent"}] + [
        {"name": f"value_{index}", "default": value}
        for index, value in enumerate([None, False, 0, "", [], {}])
    ]
    (project / "templates/defaults.yaml").write_text(
        yaml.safe_dump({"name": "defaults", "parameters": params})
    )
    parameters = template_source(project, path="templates/defaults.yaml")["template"][
        "parameters"
    ]
    assert parameters[0]["has_default"] is False
    for actual, original in zip(parameters[1:], params[1:], strict=False):
        assert actual["has_default"] is True
        assert actual["default"] == original["default"]
        assert type(actual["default"]) is type(original["default"])


def test_invalid_metadata_and_duplicate_parameters_are_actionable(project):
    source = "name: t\nparameters:\n - name: item\n - name: item\n   required: nope\n"
    model, _, diagnostics = inspect_source(source, "templates/t.yaml")
    assert model is not None
    assert len([d for d in diagnostics if d["severity"] == "error"]) == 2
    assert diagnostics[0]["field_path"] == ["parameters", 1, "name"]
    assert diagnostics[0]["line"] == 4


@pytest.mark.parametrize(
    "path",
    [
        "../outside.yaml",
        "/tmp/source.yaml",
        "pipelines/source.yaml",
        "templates/../../outside.yaml",
    ],
)
def test_source_identity_containment(project, path):
    with pytest.raises((PermissionError, ValueError)):
        template_source(project, path=path)
    with pytest.raises((PermissionError, ValueError)):
        preview_template(project, request=request(source_path=path))


def test_catalog_does_not_read_external_symlink(project, tmp_path):
    outside = tmp_path / "outside.yaml"
    outside.write_text("name: DO_NOT_DISCLOSE")
    (project / "templates/outside.yaml").symlink_to(outside)
    entry = next(
        e
        for e in template_catalog(project)["templates"]
        if e["source_path"] == "templates/outside.yaml"
    )
    assert entry["state"] == "invalid"
    assert "DO_NOT_DISCLOSE" not in str(entry)
    with pytest.raises(PermissionError):
        template_source(project, path="templates/outside.yaml")


def test_inspect_compiles_jinja_without_sample_values(project):
    invalid = SIMPLE.replace("load_{{ entity }}", "load_{{ entity + }}")
    result = run_preview(project, request(invalid, "inspect", sample_parameters={}))
    assert result["status"] == "invalid"
    assert result["diagnostics"][0]["field_path"] == ["actions", 0, "name"]
    assert result["diagnostics"][0]["line"] == 6


def test_inspect_and_expansion_separate_missing_required_even_with_default(project):
    source = SIMPLE.replace("required: true", "required: true\n    default: fallback")
    assert (
        run_preview(project, request(source, "inspect", sample_parameters={}))["status"]
        == "ready"
    )
    missing = run_preview(project, request(source, sample_parameters={}))
    assert missing["status"] == "needs_parameters"
    assert missing["missing_parameters"] == ["entity"]
    assert "expanded_actions" not in missing
    explicit = run_preview(
        project, request(source, sample_parameters={"entity": "given"})
    )
    assert explicit["expanded_actions"][0]["name"] == "load_given"


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        (False, False),
        (0, 0),
        ([], []),
        ({}, {}),
        (1.25, "1.25"),
        ("001", 1),
    ],
)
def test_expansion_matches_engine_coercion(project, value, expected):
    raw = yaml.safe_load(SIMPLE)
    raw["parameters"].append({"name": "value", "default": value})
    raw["actions"][0]["source"]["options"] = {"value": "{{ value }}"}
    source = yaml.safe_dump(raw)
    result = run_preview(project, request(source))
    assert result["status"] == "ready", result
    actual = result["expanded_actions"][0]["source"]["options"]["value"]
    assert actual == expected and type(actual) is type(expected)
    model = YAMLParser.parse_template_data(raw)
    expected_actions = TemplateEngine().render_model(model, {"entity": "orders"})
    assert result["expanded_actions"] == [
        a.model_dump(mode="json", exclude_none=True) for a in expected_actions
    ]


def test_supported_jinja_and_current_nonrendered_syntax_match_engine(project):
    raw = yaml.safe_load(SIMPLE)
    raw["actions"][0]["source"]["options"] = {
        "control": "{% if enabled %}yes{% else %}no{% endif %}",
        "loop": "{% for v in values %}{{ v|upper }}{% endfor %}",
        "{{ key }}": "literal mapping key",
        "optional": "{{ absent }}",
        "environment": "${catalog}/{{ entity }}",
    }
    params = {"entity": "orders", "enabled": True, "values": ["a", "b"]}
    result = run_preview(
        project, request(yaml.safe_dump(raw), sample_parameters=params)
    )
    expected = TemplateEngine().render_model(
        YAMLParser.parse_template_data(raw), params
    )
    assert result["expanded_actions"] == [
        a.model_dump(mode="json", exclude_none=True) for a in expected
    ]
    assert result["status"] == "ready"
    assert result["diagnostics"]  # advisory unknown inputs/control/key behavior


def test_resolved_requires_context_before_render_and_keeps_draft(project):
    before = files(project)
    missing = run_preview(project, request(stage="resolved"))
    assert missing["status"] == "needs_context"
    assert files(project) == before


def test_documentation_fixture_resolves_with_real_presets_and_substitutions(project):
    from lhp.core.loaders.project_config_loader import ProjectConfigLoader
    from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
    from lhp.core.processing.substitution import EnhancedSubstitutionManager
    from lhp.core.validators import ConfigValidator, SecretValidator
    from lhp.presets.preset_manager import PresetManager

    path = "templates/bronze_ingest.yaml"
    source = (project / path).read_text()
    before = files(project)
    context = {
        "pipeline": "bronze",
        "flowgroup": "orders_ingest",
        "environment": "dev",
        "presets": ["high_throughput"],
    }
    params = {"entity": "orders", "cluster_columns": ["order_id", "order_date"]}
    result = run_preview(
        project,
        request(
            source,
            "resolved",
            source_path=path,
            context=context,
            sample_parameters=params,
        ),
    )
    assert result["status"] == "ready", result
    flowgroup = FlowGroup(
        pipeline="bronze",
        flowgroup="orders_ingest",
        presets=["high_throughput"],
        use_template="bronze_ingest",
        template_parameters=params,
    )
    resolver = FlowgroupResolutionService(
        TemplateEngine(project / "templates"),
        PresetManager(project / "presets"),
        ConfigValidator(project, ProjectConfigLoader(project).load_project_config()),
        SecretValidator(),
    )
    expected = resolver.resolve(
        FlowGroupContext(flowgroup, project / path),
        EnhancedSubstitutionManager(project / "substitutions/dev.yaml", "dev"),
    ).flowgroup
    assert result["resolved_flowgroup"] == expected.model_dump(
        mode="json", exclude_none=True
    )
    assert {d["path"] for d in result["saved_dependencies"]} >= {
        "lhp.yaml",
        "presets/bronze_defaults.yaml",
        "presets/high_throughput.yaml",
        "substitutions/dev.yaml",
    }
    assert files(project) == before


def test_local_variables_resolve_before_template_parameters(project):
    result = run_preview(
        project,
        request(
            stage="resolved",
            sample_parameters={"entity": "%{table}"},
            context={
                "pipeline": "p",
                "flowgroup": "f",
                "environment": "dev",
                "variables": {"table": "orders"},
            },
        ),
    )
    assert result["status"] == "ready", result
    assert result["resolved_flowgroup"]["actions"][0]["name"] == "load_orders"
    assert result["effective_parameters"]["entity"] == "orders"


def test_dependencies_changed_during_resolution_are_stale(project, monkeypatch):
    original = PreviewConfigValidator.validate_flowgroup

    def changing(self, flowgroup):
        path = project / "substitutions/dev.yaml"
        path.write_text(path.read_text() + "\n# external edit\n")
        return original(self, flowgroup)

    monkeypatch.setattr(PreviewConfigValidator, "validate_flowgroup", changing)
    result = run_preview(
        project,
        request(
            stage="resolved",
            context={"pipeline": "p", "flowgroup": "f", "environment": "dev"},
        ),
    )
    assert result["status"] == "stale"
    assert "resolved_flowgroup" not in result


def test_indirect_external_schema_is_rejected_before_read(project, tmp_path):
    external = tmp_path / "external.yaml"
    external.write_text("SECRET_CONTENT")
    (project / "schemas/external.yaml").parent.mkdir(exist_ok=True)
    (project / "schemas/external.yaml").symlink_to(external)
    raw = yaml.safe_load(SIMPLE)
    raw["actions"].append(
        {
            "name": "schema",
            "type": "transform",
            "transform_type": "schema",
            "source": "v_orders",
            "target": "v_schema",
            "schema_file": "schemas/external.yaml",
        }
    )
    result = run_preview(
        project,
        request(
            yaml.safe_dump(raw),
            "resolved",
            context={"pipeline": "p", "flowgroup": "f", "environment": "dev"},
        ),
    )
    assert result["status"] == "invalid"
    assert "inside the project" in str(result["diagnostics"])
    assert "SECRET_CONTENT" not in str(result)


def test_preview_restricts_unsafe_jinja_without_writing(project):
    raw = yaml.safe_load(SIMPLE)
    target = project / "must-not-exist"
    raw["actions"][0]["source"]["sql"] = (
        "{{ cycler.__init__.__globals__.os.system('touch " + str(target) + "') }}"
    )
    before = files(project)
    result = preview_template(project, request=request(yaml.safe_dump(raw)))
    assert result["status"] == "invalid"
    assert result["diagnostics"][-1]["code"] == "LHP-TEMPLATE-PREVIEW-RESTRICTED"
    assert "preview only" in result["diagnostics"][-1]["message"]
    assert files(project) == before and not target.exists()


def test_process_preview_resolves_cold_and_isolates_concurrent_samples(project):
    def preview(entity):
        return preview_template(
            project,
            request=request(
                stage="resolved",
                sample_parameters={"entity": entity},
                context={"pipeline": "p", "flowgroup": "f", "environment": "dev"},
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(preview, ["one", "two"]))
    assert [r["status"] for r in results] == ["ready", "ready"]
    assert [r["resolved_flowgroup"]["actions"][0]["name"] for r in results] == [
        "load_one",
        "load_two",
    ]


def test_timeout_reaps_worker_and_releases_slot(project, monkeypatch):
    import lhp.api._template_preview_worker as workers

    before = {p.pid for p in multiprocessing.active_children()}
    monkeypatch.setattr(workers, "PREVIEW_TIMEOUT_SECONDS", 0.001)
    result = preview_template(project, request=request())
    assert result["diagnostics"][0]["code"] == "LHP-TEMPLATE-TIMEOUT"
    assert {p.pid for p in multiprocessing.active_children()} == before
    assert workers._slots.acquire(blocking=False)
    workers._slots.release()


def test_size_and_recursive_yaml_are_bounded(project):
    with pytest.raises(ValueError, match="limit"):
        preview_template(project, request=request("x" * 524289))
    cyclic = "name: t\nactions: &cycle\n - *cycle\n"
    result = run_preview(project, request(cyclic, "inspect"))
    assert result["status"] == "invalid"


def test_transitive_preset_dependencies_use_the_real_inheritance_chain(project):
    (project / "presets/base.yaml").write_text(
        "name: base\ndefaults:\n  write_actions:\n    streaming_table:\n      table_properties:\n        inherited: base\n"
    )
    (project / "presets/child.yaml").write_text(
        "name: child\nextends: base\ndefaults:\n  write_actions:\n    streaming_table:\n      table_properties:\n        child: value\n"
    )
    result = run_preview(
        project,
        request(
            stage="resolved",
            context={
                "pipeline": "p",
                "flowgroup": "f",
                "environment": "dev",
                "presets": ["child"],
            },
        ),
    )
    assert result["status"] == "ready", result
    properties = result["resolved_flowgroup"]["actions"][1]["write_target"][
        "table_properties"
    ]
    assert properties == {"inherited": "base", "child": "value"}
    assert {d["path"] for d in result["saved_dependencies"]} >= {
        "presets/base.yaml",
        "presets/child.yaml",
    }


def test_preview_output_limit_returns_a_small_diagnostic(project):
    raw = yaml.safe_load(SIMPLE)
    raw["actions"][0]["source"]["sql"] = "{{ 'a' * 2097152 }}"
    result = preview_template(project, request=request(yaml.safe_dump(raw)))
    assert result["status"] == "invalid"
    assert result["diagnostics"][0]["code"] == "LHP-TEMPLATE-LIMIT"
    assert "expanded_actions" not in result


def test_raw_schema_allows_test_actions_and_templated_enum_values():
    import json

    from jsonschema import Draft7Validator

    schema = json.loads(
        (
            Path(__file__).resolve().parents[2] / "src/lhp/schemas/template.schema.json"
        ).read_text()
    )
    raw = {
        "name": "patterns",
        "parameters": [{"name": "mode", "type": "custom_hint"}],
        "actions": [
            {"name": "test", "type": "test", "test_type": "custom_sql"},
            {
                "name": "dynamic",
                "type": "{{ action_type }}",
                "readMode": "{{ mode }}",
                "write_target": "{{ config }}",
            },
        ],
    }
    assert list(Draft7Validator(schema).iter_errors(raw)) == []
