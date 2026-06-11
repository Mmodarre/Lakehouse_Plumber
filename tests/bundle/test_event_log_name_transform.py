"""Tests for BundleManager's ``event_log_name_transform`` constructor param.

The transform is an opaque ``str -> str`` callable applied to the COMPOSED
project-level event-log table name (``{name_prefix}{pipeline}{name_suffix}``)
inside ``_inject_project_event_log``. BundleManager stays unaware of WHY the
name changes — the caller (wired in a later task) passes the callable.
Per-pipeline explicit ``event_log:`` dicts are never transformed (v1 locked).
"""

import shutil
import tempfile
from pathlib import Path

import pytest
import yaml

from lhp.bundle.manager import BundleManager
from lhp.models import EventLogConfig, ProjectConfig

PIPELINE = "bronze_load"


@pytest.fixture
def temp_project():
    """Temporary project with substitutions + default pipeline_config.yaml."""
    temp_dir = tempfile.mkdtemp()
    project_root = Path(temp_dir)

    (project_root / "substitutions").mkdir(parents=True)
    (project_root / "config").mkdir(parents=True)
    (project_root / "generated").mkdir(parents=True)

    sub_content = {"dev": {"catalog": "dev_catalog", "schema": "dev_schema"}}
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(sub_content, f)

    (project_root / "config" / "pipeline_config.yaml").write_text(
        "project_defaults:\n"
        "  catalog: test_catalog\n"
        "  schema: test_schema\n"
        "  serverless: true\n"
    )

    yield project_root

    shutil.rmtree(temp_dir)


def _project_config():
    return ProjectConfig(
        name="test",
        event_log=EventLogConfig(
            catalog="meta_catalog",
            schema="_meta",
            name_suffix="_event_log",
        ),
    )


def _render(project_root, project_config, **manager_kwargs):
    """Render the real resource YAML for PIPELINE via a fresh BundleManager."""
    manager = BundleManager(
        project_root=project_root,
        pipeline_config_path=str(project_root / "config" / "pipeline_config.yaml"),
        project_config=project_config,
        **manager_kwargs,
    )
    return manager.generate_resource_file_content(
        PIPELINE, project_root / "generated", "dev"
    )


def _diff_lines(baseline: str, other: str):
    """Pairs of differing lines between two same-shape renders."""
    base_lines = baseline.splitlines()
    other_lines = other.splitlines()
    assert len(base_lines) == len(other_lines)
    return [(b, o) for b, o in zip(base_lines, other_lines, strict=False) if b != o]


class TestEventLogNameTransform:
    def test_none_transform_output_byte_identical(self, temp_project):
        """Explicit None must render byte-identical output to omitting the param."""
        baseline = _render(temp_project, _project_config())
        with_none = _render(
            temp_project, _project_config(), event_log_name_transform=None
        )

        assert with_none == baseline
        # Same composed-name expectation the existing event-log tests assert.
        assert f"{PIPELINE}_event_log" in baseline

    def test_prefix_style_transform_applied_to_composed_name(self, temp_project):
        baseline = _render(temp_project, _project_config())
        transformed = _render(
            temp_project,
            _project_config(),
            event_log_name_transform=lambda n: f"alice_{n}",
        )

        assert f"alice_{PIPELINE}_event_log" in transformed
        diffs = _diff_lines(baseline, transformed)
        assert len(diffs) == 1
        before, after = diffs[0]
        assert f"{PIPELINE}_event_log" in before
        assert f"alice_{PIPELINE}_event_log" in after

    def test_suffix_style_transform_applied_to_composed_name(self, temp_project):
        """Callable (not a prefix value): suffix-style patterns must work too."""
        baseline = _render(temp_project, _project_config())
        transformed = _render(
            temp_project,
            _project_config(),
            event_log_name_transform=lambda n: f"{n}_alice",
        )

        assert f"{PIPELINE}_event_log_alice" in transformed
        diffs = _diff_lines(baseline, transformed)
        assert len(diffs) == 1

    def test_transform_never_called_without_project_event_log(self, temp_project):
        """No project event_log config: no injection, transform never invoked."""
        calls = []

        def recording(name):
            calls.append(name)
            return f"alice_{name}"

        content = _render(
            temp_project,
            ProjectConfig(name="test"),  # no event_log block
            event_log_name_transform=recording,
        )

        # Parse rather than substring-match: the template contains a
        # commented-out event_log example block.
        parsed = yaml.safe_load(content)
        pipeline_def = parsed["resources"]["pipelines"][f"{PIPELINE}_pipeline"]
        assert "event_log" not in pipeline_def
        assert calls == []

    def test_explicit_pipeline_event_log_dict_not_transformed(self, temp_project):
        """Per-pipeline explicit event_log dicts are NOT rewritten (v1 locked)."""
        (temp_project / "config" / "pipeline_config.yaml").write_text(
            "---\n"
            "project_defaults:\n"
            "  catalog: test_catalog\n"
            "  schema: test_schema\n"
            "  serverless: true\n"
            "\n"
            "---\n"
            f"pipeline: {PIPELINE}\n"
            "event_log:\n"
            "  name: custom_event_log\n"
            "  catalog: explicit_catalog\n"
            "  schema: explicit_schema\n"
        )

        calls = []

        def recording(name):
            calls.append(name)
            return f"alice_{name}"

        content = _render(
            temp_project,
            _project_config(),
            event_log_name_transform=recording,
        )

        assert "custom_event_log" in content
        assert "alice_" not in content
        assert calls == []
