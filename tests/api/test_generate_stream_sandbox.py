"""Sandbox wiring of the §5.7 generate stream (``sandbox=True``).

Drives the REAL generation facade on temp projects (no mocks — discovery, the
sandbox pre-pass, the engine, gate and commit are the production drivers) and
pins the five sandbox-mode behaviors wired into
:func:`lhp.api._generate_stream._stream_pipeline_generation`:

1. **§1.4 rendezvous** — a sandbox preflight failure (missing
   ``.lhp/profile.yaml`` → ``LHP-IO-025``) yields exactly one
   :class:`ErrorEmitted` and THEN raises; the raise closes the stream (no
   terminal :class:`GenerationCompleted`).
2. **Scope** — the profile scope becomes the worklist: only in-scope
   pipelines generate (on disk under ``generated/<env>/``), and the rewritten
   per-developer table names appear in the generated code.
3. **Pre-pass warnings** — a mixed-producer sink surfaces as ONE
   :class:`WarningEmitted` with ``category="sandbox"`` / ``LHP-VAL-065``
   through the same deduped emission path as the worker warnings.
4. **Monitoring guard** — a sandbox run emits NO ``monitoring`` phase events
   and leaves pre-existing ``monitoring/<env>/`` artifacts untouched on disk
   (the finalizer would otherwise wipe-and-rewrite shared committed artifacts
   referencing pipelines a sandbox run never generates).
5. **Non-sandbox parity** — ``sandbox=False`` on the same monitoring-enabled
   project keeps the ``monitoring`` phase (and demonstrates the clobber the
   guard prevents: the finalizer removes the pre-seeded artifact).

Tests import strictly from :mod:`lhp.api` and :mod:`lhp.errors`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lhp.api import (
    ErrorEmitted,
    GenerationCompleted,
    LakehousePlumberApplicationFacade,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineStarted,
    WarningEmitted,
)
from lhp.errors import LHPError

_SANDBOX_POLICY = (
    'sandbox:\n  strategy: table\n  table_pattern: "{namespace}__{table}"\n'
)

_MONITORING_POLICY = (
    "event_log:\n"
    '  catalog: "dev_catalog"\n'
    "  schema: _meta\n"
    '  name_suffix: "_event_log"\n'
    "monitoring:\n"
    '  checkpoint_path: "/Volumes/dev_catalog/_meta/checkpoints/event_logs"\n'
    '  job_config_path: "config/monitoring_job_config.yaml"\n'
)


def _write_flowgroup(project_root: Path, pipeline: str, tables: list[str]) -> None:
    """One flowgroup for ``pipeline`` with a load → write chain per table."""
    pdir = project_root / "pipelines" / pipeline
    pdir.mkdir(parents=True, exist_ok=True)
    actions = []
    for table in tables:
        actions.append(
            {
                "name": f"load_{table}",
                "type": "load",
                "target": f"v_{table}_raw",
                "source": {
                    "type": "cloudfiles",
                    "path": "${landing_path}/" + table,
                    "format": "json",
                },
            }
        )
        actions.append(
            {
                "name": f"write_{table}",
                "type": "write",
                "source": f"v_{table}_raw",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": table,
                    "create_table": True,
                },
            }
        )
    spec = {"pipeline": pipeline, "flowgroup": f"{pipeline}_fg", "actions": actions}
    with open(pdir / f"{pipeline}_fg.yaml", "w") as f:
        yaml.dump(spec, f)


def _build_project(
    tmp_path: Path,
    *,
    pipelines: dict[str, list[str]],
    lhp_yaml_extra: str = "",
    profile: str | None = None,
) -> Path:
    """Write a minimal project; ``pipelines`` maps pipeline name → table list.

    ``lhp_yaml_extra`` is appended verbatim to ``lhp.yaml`` (sandbox policy /
    monitoring config blocks). ``profile`` is the full ``.lhp/profile.yaml``
    content; ``None`` writes no profile (the missing-profile case).
    """
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text(
        "name: sandbox_stream_proj\nversion: '1.0'\n" + lhp_yaml_extra
    )
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(
            {
                "dev": {
                    "catalog": "dev_catalog",
                    "bronze_schema": "bronze",
                    "landing_path": "/mnt/dev/landing",
                }
            },
            f,
        )
    for name, tables in pipelines.items():
        _write_flowgroup(project_root, name, tables)
    if profile is not None:
        (project_root / ".lhp").mkdir(parents=True, exist_ok=True)
        (project_root / ".lhp" / "profile.yaml").write_text(profile)
    if "monitoring:" in lhp_yaml_extra:
        (project_root / "config").mkdir(exist_ok=True)
        (project_root / "config" / "monitoring_job_config.yaml").write_text(
            "max_concurrent_runs: 1\n"
            "performance_target: STANDARD\n"
            "tags:\n"
            "  managed_by: lakehouse_plumber\n"
        )
    return project_root


def _facade(project_root: Path, monkeypatch) -> LakehousePlumberApplicationFacade:
    monkeypatch.chdir(project_root)
    return LakehousePlumberApplicationFacade.for_project(
        project_root, enforce_version=False
    )


def _drain(gen):
    """Collect events until exhaustion or an LHPError raise."""
    events: list = []
    raised: LHPError | None = None
    try:
        for event in gen:
            events.append(event)
    except LHPError as exc:
        raised = exc
    return events, raised


def _phase_starts(events: list) -> list[str]:
    return [e.phase for e in events if isinstance(e, PhaseStarted)]


@pytest.mark.integration
def test_missing_profile_emits_error_then_raises(tmp_path, monkeypatch):
    """§1.4 rendezvous: LHP-IO-025 raises AFTER exactly one ErrorEmitted; the
    raise closes the stream (no terminal GenerationCompleted)."""
    project_root = _build_project(
        tmp_path,
        pipelines={"in_pipe": ["orders"], "out_pipe": ["customers"]},
        lhp_yaml_extra=_SANDBOX_POLICY,
        profile=None,
    )
    facade = _facade(project_root, monkeypatch)

    events, raised = _drain(
        facade.generation.generate_pipelines(
            env="dev",
            output_dir=project_root / "generated" / "dev",
            sandbox=True,
            max_workers=1,
        )
    )

    assert raised is not None
    assert raised.code == "LHP-IO-025"
    # The ErrorEmitted PRECEDED the raise and carries the SAME structured
    # error; it is the stream's last event (the raise closed the stream).
    assert isinstance(events[0], OperationStarted)
    error_events = [e for e in events if isinstance(e, ErrorEmitted)]
    assert len(error_events) == 1
    assert isinstance(events[-1], ErrorEmitted)
    assert events[-1].lhp_error is raised
    # The pre-pass runs AFTER the discover phase pair; no terminal.
    assert _phase_starts(events) == ["discover"]
    assert not any(isinstance(e, GenerationCompleted) for e in events)


@pytest.mark.integration
def test_scope_expansion_and_table_renames_on_disk(tmp_path, monkeypatch):
    """D6: only the profile-scoped pipeline generates (the glob expands against
    the discovered names); generated code carries the renamed table leaf."""
    project_root = _build_project(
        tmp_path,
        pipelines={"in_pipe": ["orders"], "out_pipe": ["customers"]},
        lhp_yaml_extra=_SANDBOX_POLICY,
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - in_*\n",
    )
    facade = _facade(project_root, monkeypatch)
    output_dir = project_root / "generated" / "dev"

    events, raised = _drain(
        facade.generation.generate_pipelines(
            env="dev", output_dir=output_dir, sandbox=True, max_workers=1
        )
    )

    assert raised is None
    assert isinstance(events[-1], GenerationCompleted)
    assert events[-1].response.success is True

    # Per-pipeline events: ONLY the in-scope pipeline ran.
    assert [e.pipeline for e in events if isinstance(e, PipelineStarted)] == ["in_pipe"]
    assert [e.pipeline for e in events if isinstance(e, PipelineCompleted)] == [
        "in_pipe"
    ]

    # On disk: only the in-scope pipeline dir exists under generated/<env>/.
    pipeline_dirs = sorted(p.name for p in output_dir.iterdir() if p.is_dir())
    assert pipeline_dirs == ["in_pipe"]

    # The produced table was renamed through the {namespace}__{table} pattern.
    generated_text = "".join(p.read_text() for p in sorted(output_dir.rglob("*.py")))
    assert "alice__orders" in generated_text


@pytest.mark.integration
def test_mixed_producer_warning_emitted_with_sandbox_category(tmp_path, monkeypatch):
    """D8: a sink produced by BOTH partitions surfaces as one WarningEmitted
    with category='sandbox' / LHP-VAL-065 (carried off the pre-pass plan)."""
    project_root = _build_project(
        tmp_path,
        pipelines={"in_pipe": ["orders", "shared"], "out_pipe": ["shared"]},
        lhp_yaml_extra=_SANDBOX_POLICY,
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - in_pipe\n",
    )
    facade = _facade(project_root, monkeypatch)

    events, raised = _drain(
        facade.generation.generate_pipelines(
            env="dev",
            output_dir=project_root / "generated" / "dev",
            sandbox=True,
            max_workers=1,
        )
    )

    assert raised is None
    assert isinstance(events[-1], GenerationCompleted)
    sandbox_warnings = [
        e for e in events if isinstance(e, WarningEmitted) and e.category == "sandbox"
    ]
    assert len(sandbox_warnings) == 1
    assert sandbox_warnings[0].code == "LHP-VAL-065"
    # The folded record names the conflicted sink and its out-of-scope producer.
    assert "shared" in sandbox_warnings[0].message
    assert "out_pipe" in sandbox_warnings[0].message


@pytest.mark.integration
def test_sandbox_run_skips_monitoring_phase_and_preserves_artifacts(
    tmp_path, monkeypatch
):
    """A monitoring-ENABLED project: the sandbox run emits NO monitoring phase
    events and leaves a pre-seeded monitoring/<env>/ artifact untouched."""
    project_root = _build_project(
        tmp_path,
        pipelines={"in_pipe": ["orders"], "out_pipe": ["customers"]},
        lhp_yaml_extra=_SANDBOX_POLICY + _MONITORING_POLICY,
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - in_pipe\n",
    )
    sentinel = project_root / "monitoring" / "dev" / "sentinel.py"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("# shared committed artifact\n")
    facade = _facade(project_root, monkeypatch)

    events, raised = _drain(
        facade.generation.generate_pipelines(
            env="dev",
            output_dir=project_root / "generated" / "dev",
            sandbox=True,
            max_workers=1,
        )
    )

    assert raised is None
    assert isinstance(events[-1], GenerationCompleted)
    assert events[-1].response.success is True

    # NO monitoring phase events of either kind.
    assert "monitoring" not in _phase_starts(events)
    assert not any(
        isinstance(e, PhaseCompleted) and e.phase == "monitoring" for e in events
    )

    # monitoring/<env>/ untouched on disk: the sentinel survives with its
    # content, and the finalizer wrote nothing.
    assert sentinel.exists()
    assert sentinel.read_text() == "# shared committed artifact\n"
    assert not (project_root / "monitoring" / "dev" / "union_event_logs.py").exists()


@pytest.mark.integration
def test_non_sandbox_run_keeps_monitoring_phase(tmp_path, monkeypatch):
    """Control: ``sandbox=False`` on the SAME monitoring-enabled project keeps
    the monitoring phase — and the finalizer's wipe-and-rewrite removes the
    pre-seeded artifact (exactly the clobber the sandbox guard prevents)."""
    project_root = _build_project(
        tmp_path,
        pipelines={"in_pipe": ["orders"], "out_pipe": ["customers"]},
        lhp_yaml_extra=_SANDBOX_POLICY + _MONITORING_POLICY,
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - in_pipe\n",
    )
    sentinel = project_root / "monitoring" / "dev" / "sentinel.py"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("# shared committed artifact\n")
    facade = _facade(project_root, monkeypatch)

    events, raised = _drain(
        facade.generation.generate_pipelines(
            pipeline_fields=["in_pipe"],
            env="dev",
            output_dir=project_root / "generated" / "dev",
            max_workers=1,
        )
    )

    assert raised is None
    assert isinstance(events[-1], GenerationCompleted)
    assert events[-1].response.success is True

    # The monitoring phase ran (paired) and the finalizer reconciled the
    # monitoring/<env>/ tree: stale sentinel removed, real notebook written.
    starts = _phase_starts(events)
    assert "monitoring" in starts
    assert any(
        isinstance(e, PhaseCompleted) and e.phase == "monitoring" and e.success
        for e in events
    )
    assert not sentinel.exists()
    assert (project_root / "monitoring" / "dev" / "union_event_logs.py").exists()
