"""Unit tests for :meth:`MonitoringFinalizerService.resolve_notebook_context` (#298).

The project preflight scans the monitoring union notebook's values for
unresolved ``${tokens}`` before anything is written. It reads them through
this method, which substitutes the build result's template context exactly
as :meth:`finalize_artifacts` does before rendering the notebook.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.core._interfaces import BaseMonitoringFinalizerService
from lhp.core.coordination.monitoring_service import MonitoringFinalizerService
from lhp.core.registry.factories import OrchestrationDependencies
from lhp.models import EventLogConfig, FlowGroup, MonitoringConfig, ProjectConfig

pytestmark = pytest.mark.unit


def _service(root: Path, *, monitoring: bool = True) -> MonitoringFinalizerService:
    (root / "substitutions").mkdir(parents=True, exist_ok=True)
    (root / "substitutions" / "dev.yaml").write_text("dev:\n  catalog: main\n")
    (root / "config").mkdir(exist_ok=True)
    (root / "config" / "job.yaml").write_text("max_concurrent_runs: 1\n")
    project_config = ProjectConfig(
        name="acme",
        event_log=EventLogConfig(
            catalog="${catalog}", schema="_meta", name_suffix="_event_log"
        ),
        monitoring=(
            MonitoringConfig(
                checkpoint_path="/Volumes/${catalog}/${undefined_ckpt}/checkpoints",
                job_config_path="config/job.yaml",
            )
            if monitoring
            else None
        ),
    )
    service = MonitoringFinalizerService(
        project_config=project_config,
        project_root=root,
        dependencies=OrchestrationDependencies(),
    )
    service.build_flowgroup([FlowGroup(pipeline="bronze", flowgroup="fg")])
    return service


def test_context_is_substituted_for_the_environment(tmp_path):
    context = _service(tmp_path).resolve_notebook_context("dev")

    assert context is not None
    assert context["sources"] == [["bronze", "main._meta.bronze_event_log"]]
    assert context["target_fqn"] == "main._meta.all_pipelines_event_log"
    assert context["max_concurrent_streams"] == 10


def test_undefined_token_is_left_as_written(tmp_path):
    context = _service(tmp_path).resolve_notebook_context("dev")

    assert context is not None
    assert context["checkpoint_path"] == "/Volumes/main/${undefined_ckpt}/checkpoints"


def test_no_monitoring_means_no_notebook_context(tmp_path):
    assert _service(tmp_path, monitoring=False).resolve_notebook_context("dev") is None


def test_finalize_renders_the_same_values(tmp_path):
    service = _service(tmp_path)
    context = service.resolve_notebook_context("dev")
    assert context is not None

    service.finalize_artifacts("dev", tmp_path / "generated" / "dev")

    notebook = (tmp_path / "monitoring" / "dev" / "union_event_logs.py").read_text()
    assert f'CHECKPOINT_BASE = "{context["checkpoint_path"]}"' in notebook
    assert f'TARGET_TABLE = "{context["target_fqn"]}"' in notebook
    assert f'"{context["sources"][0][1]}"' in notebook


def test_base_service_default_has_no_notebook_context():
    """A monitoring service that builds no notebook needs no override."""

    class _NoNotebook(BaseMonitoringFinalizerService):
        def build_flowgroup(self, discovered_flowgroups):
            return None

        def finalize_artifacts(self, env, output_dir):
            return None

        def cleanup_artifacts(self, env, output_dir):
            return None

    assert _NoNotebook().resolve_notebook_context("dev") is None
