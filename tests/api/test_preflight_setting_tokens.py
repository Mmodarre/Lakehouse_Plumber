"""Project preflight: unresolved ``${tokens}`` in bundle and monitoring settings (#298).

Drives the real :func:`lhp.api._preflight._run_project_preflight` over an
isolated copy of the e2e fixture project, with ``event_log`` / ``monitoring``
blocks appended to its ``lhp.yaml`` and values edited in its pipeline config.
Nothing LHP owns is mocked.

Contract: a plain ``${name}`` left after substitution in a value LHP writes
out yields ``LHP-CFG-010`` naming the file and the key: the bundle resource
settings (``pipeline_config.yaml`` values and the injected project
``event_log``) when bundle output is enabled, and the monitoring notebook's
event-log references and ``checkpoint_path`` whether or not it is. Defined
tokens and dotted Databricks references pass; the monitoring job config is not
checked; the helper never raises.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Iterator, List, Tuple

import pytest

from lhp.api import LakehousePlumberApplicationFacade
from lhp.api._preflight import _run_project_preflight
from lhp.api.views import ValidationIssueView

pytestmark = pytest.mark.unit

_FIXTURE_PATH = Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"
_PIPELINE_CONFIG_REL = "config/pipeline_config.yaml"
_MONITORING_PIPELINE = "acme_edw_event_log_monitoring"


@pytest.fixture
def project_root(tmp_path: Path) -> Iterator[Path]:
    root = tmp_path / "test_project"
    shutil.copytree(_FIXTURE_PATH, root)
    original_cwd = os.getcwd()
    os.chdir(root)
    try:
        yield root
    finally:
        os.chdir(original_cwd)


def _append_lhp_yaml(root: Path, block: str) -> None:
    with (root / "lhp.yaml").open("a", encoding="utf-8") as handle:
        handle.write("\n" + block)


def _event_log(schema: str) -> str:
    return (
        "event_log:\n"
        '  catalog: "${catalog}"\n'
        f'  schema: "{schema}"\n'
        '  name_suffix: "_event_log"\n'
    )


def _monitoring(checkpoint_path: str, extra: str = "") -> str:
    return (
        "monitoring:\n"
        f'  checkpoint_path: "{checkpoint_path}"\n'
        '  job_config_path: "config/monitoring_job_config.yaml"\n' + extra
    )


def _set_project_defaults(root: Path, extra: str) -> None:
    path = root / _PIPELINE_CONFIG_REL
    text = path.read_text(encoding="utf-8")
    anchor = "  serverless: true\n"
    assert anchor in text
    path.write_text(text.replace(anchor, anchor + extra, 1), encoding="utf-8")


def _preflight(root: Path, *, bundle_enabled: bool) -> Tuple[ValidationIssueView, ...]:
    facade = LakehousePlumberApplicationFacade.for_project(
        root, pipeline_config_path=_PIPELINE_CONFIG_REL, enforce_version=False
    )
    return _run_project_preflight(
        facade._orchestrator,
        env="dev",
        bundle_enabled=bundle_enabled,
        include_tests=False,
    )


def _cfg_010(issues: Tuple[ValidationIssueView, ...]) -> List[ValidationIssueView]:
    return [issue for issue in issues if issue.code == "LHP-CFG-010"]


def _only_cfg_010(issues: Tuple[ValidationIssueView, ...]) -> ValidationIssueView:
    found = _cfg_010(issues)
    assert len(found) == 1, [(i.code, i.context) for i in issues]
    return found[0]


class TestBundleEventLog:
    def test_undefined_event_log_token_without_monitoring_is_cfg_010(
        self, project_root
    ):
        _append_lhp_yaml(project_root, _event_log("${undefined_evt}"))

        issue = _only_cfg_010(_preflight(project_root, bundle_enabled=True))

        assert issue.title == (
            "Unresolved substitution token(s) ${undefined_evt} in lhp.yaml (event_log)"
        )
        assert issue.context["File"] == "lhp.yaml"
        assert issue.context["Field"] == "event_log"
        assert issue.context["Unresolved"] == "${undefined_evt}"
        assert issue.context["Environment"] == "dev"
        assert "${undefined_evt}" in (issue.details or "")

    def test_no_bundle_output_means_no_event_log_check(self, project_root):
        _append_lhp_yaml(project_root, _event_log("${undefined_evt}"))

        assert _cfg_010(_preflight(project_root, bundle_enabled=False)) == []

    def test_defined_event_log_token_passes(self, project_root):
        _append_lhp_yaml(project_root, _event_log("${raw_schema}"))

        assert _cfg_010(_preflight(project_root, bundle_enabled=True)) == []

    def test_monitoring_own_catalog_schema_still_reports_event_log(self, project_root):
        _append_lhp_yaml(
            project_root,
            _event_log("${undefined_evt}")
            + _monitoring(
                "/Volumes/${catalog}/_meta/checkpoints",
                '  catalog: "${catalog}"\n  schema: "_analytics"\n',
            ),
        )

        issue = _only_cfg_010(_preflight(project_root, bundle_enabled=True))

        assert (issue.context["File"], issue.context["Field"]) == (
            "lhp.yaml",
            "event_log",
        )
        assert issue.context["Unresolved"] == "${undefined_evt}"


class TestMonitoringNotebook:
    @pytest.mark.parametrize("bundle_enabled", [False, True])
    def test_undefined_checkpoint_path_token_is_cfg_010(
        self, project_root, bundle_enabled
    ):
        _append_lhp_yaml(
            project_root,
            _event_log("_meta")
            + _monitoring("/Volumes/${catalog}/${undefined_ckpt}/checkpoints"),
        )

        issue = _only_cfg_010(_preflight(project_root, bundle_enabled=bundle_enabled))

        assert issue.context["File"] == "lhp.yaml"
        assert issue.context["Field"] == "monitoring.checkpoint_path"
        assert issue.context["Unresolved"] == "${undefined_ckpt}"

    def test_undefined_event_log_reference_without_bundle_is_cfg_010(
        self, project_root
    ):
        _append_lhp_yaml(
            project_root,
            _event_log("${undefined_evt}")
            + _monitoring(
                "/Volumes/${catalog}/_meta/checkpoints",
                '  catalog: "${catalog}"\n  schema: "_analytics"\n',
            ),
        )

        issue = _only_cfg_010(_preflight(project_root, bundle_enabled=False))

        assert issue.context["Field"] == "event_log"

    def test_job_config_bundle_target_token_is_not_checked(self, project_root):
        # config/monitoring_job_config.yaml holds ``environment: ${bundle_target}``.
        _append_lhp_yaml(
            project_root,
            _event_log("_meta") + _monitoring("/Volumes/${catalog}/_meta/ckpt"),
        )

        assert _cfg_010(_preflight(project_root, bundle_enabled=True)) == []


class TestPipelineConfig:
    def test_undefined_token_in_a_pipeline_config_key_is_cfg_010(self, project_root):
        _set_project_defaults(
            project_root, '  configuration:\n    source_root: "${undefined_cfg}"\n'
        )

        issue = _only_cfg_010(_preflight(project_root, bundle_enabled=True))

        assert issue.context["File"] == _PIPELINE_CONFIG_REL
        assert issue.context["Field"] == "configuration.source_root"
        assert issue.context["Unresolved"] == "${undefined_cfg}"
        # Every pipeline shares project_defaults; the list is capped at five.
        assert issue.context["Pipeline"].startswith(
            "12_test_actions, 13_sinks, 14_jdbc_load, 15_python_load, 16_temp_table (+"
        )
        assert issue.context["Pipeline"].endswith(" more)")

    def test_dotted_bundle_references_pass(self, project_root):
        _set_project_defaults(
            project_root,
            "  configuration:\n"
            '    target: "${bundle.target}"\n'
            '    owner: "${var.owner}"\n'
            '    root: "${workspace.file_path}/data"\n',
        )

        assert _cfg_010(_preflight(project_root, bundle_enabled=True)) == []

    def test_monitoring_pipeline_settings_are_checked(self, project_root):
        _append_lhp_yaml(
            project_root,
            _event_log("_meta") + _monitoring("/Volumes/${catalog}/_meta/ckpt"),
        )
        with (project_root / _PIPELINE_CONFIG_REL).open("a", encoding="utf-8") as fh:
            fh.write(
                f"\n---\npipeline: {_MONITORING_PIPELINE}\n"
                'catalog: "${catalog}"\nschema: "${undefined_mon}"\n'
            )

        issue = _only_cfg_010(_preflight(project_root, bundle_enabled=True))

        assert issue.context["Field"] == "schema"
        assert issue.context["Pipeline"] == _MONITORING_PIPELINE

    def test_unreadable_pipeline_config_is_left_to_the_catalog_schema_check(
        self, project_root
    ):
        (project_root / _PIPELINE_CONFIG_REL).unlink()

        issues = _preflight(project_root, bundle_enabled=True)

        assert _cfg_010(issues) == []
        reported = [
            issue
            for issue in issues
            if "Pipeline config file not found" in (issue.details or "")
        ]
        assert len(reported) == 1
        # The loader's own error, not re-labelled as a catalog/schema failure.
        assert reported[0].code == "LHP-IO-001"
        assert reported[0].category == "IO"
        assert reported[0].title == "Pipeline config file not found"

    def test_one_pipeline_failing_to_resolve_does_not_skip_the_others(
        self, project_root
    ):
        # 13_sinks sorts first and fails substitution (malformed secret,
        # LHP-CFG-059); 14_jdbc_load must still be checked.
        with (project_root / _PIPELINE_CONFIG_REL).open("a", encoding="utf-8") as fh:
            fh.write(
                "\n---\npipeline: 13_sinks\n"
                'configuration:\n  key: "${secret:/x}"\n'
                "\n---\npipeline: 14_jdbc_load\n"
                'configuration:\n  root: "${undefined_cfg}"\n'
            )

        issue = _only_cfg_010(_preflight(project_root, bundle_enabled=True))

        assert issue.context["Pipeline"] == "14_jdbc_load"
        assert issue.context["Field"] == "configuration.root"


class TestIsolation:
    def test_unexpected_bundle_failure_keeps_the_monitoring_check(
        self, project_root, monkeypatch
    ):
        # An unexpected (non-LHP) failure cannot be produced from project files,
        # so it is injected at the bundle boundary the preflight calls.
        from lhp.bundle.manager import BundleManager

        def _boom(self, pipeline_name, env):
            raise RuntimeError("unexpected")

        monkeypatch.setattr(BundleManager, "resolve_pipeline_settings", _boom)
        _append_lhp_yaml(
            project_root,
            _event_log("_meta")
            + _monitoring("/Volumes/${catalog}/${undefined_ckpt}/checkpoints"),
        )

        issue = _only_cfg_010(_preflight(project_root, bundle_enabled=True))

        assert issue.context["Field"] == "monitoring.checkpoint_path"
