"""§5.7 generate-stream coverage for deterministic per-pipeline WHEEL packaging.

Mixed-mode project: one ``packaging: source`` pipeline (``p_src``) and one
``packaging: wheel`` pipeline (``p_whl``), the latter opted in through a
per-pipeline ``packaging`` override in the ``config/pipeline_config.yaml`` the
facade is built with. The project's ``lhp.yaml`` declares the required
``wheel.artifact_volume`` (a literal ``/Volumes/...`` path).

The harness is copied from ``tests/api/test_generate_stream_protocol.py`` (the
canonical end-to-end api-stream driver): write a minimal project on disk, ``chdir``
into it, build the facade via
:meth:`LakehousePlumberApplicationFacade.for_project`, then drive
``facade.generation.generate_pipelines(...)`` and collect the emitted
:class:`~lhp.api.LHPEvent` stream. Two deltas from that test:

  * the facade is built WITH ``pipeline_config_path`` (the per-pipeline
    ``packaging`` lives there), and
  * ``generate_pipelines`` is driven with ``bundle_enabled=True`` so the
    ``bundle_sync`` phase runs and ``emit_wheels_bundle_file`` fires (a no-op
    only when there are zero wheel pipelines — here there is one). Bundle sync
    never reads ``databricks.yml`` (``BundleManager`` docstring), so none is
    written; ``bundle_enabled`` is a direct facade param, not auto-detected here.

What is asserted (WHEEL_PACKAGING_SPEC, mirrored against the §5.7 contract in
``lhp.api._generate_stream`` and ``tests/e2e/test_wheel_packaging_e2e.py``):

1. **Event ordering is unchanged.** Wheel packaging introduces NO new event type
   and NO new phase. The phase order is exactly today's:
   ``OperationStarted`` → ``discover`` → ``preflight`` → ``generate`` →
   ``format`` → ``monitoring`` → ``bundle_sync`` → ``GenerationCompleted`` (the
   canonical order documented in ``_stream_pipeline_generation``'s docstring).
2. ``resources/lhp/_wheels.bundle.yml`` is emitted (≥1 wheel pipeline).
3. The wheeled pipeline dir holds ONLY its ``<import_pkg>_runner.py`` runner (no
   loose flowgroup ``.py``); the source pipeline dir holds its normal loose ``.py``.
4. **Wheel bytes are formatter-independent (R-invariant).** The on-disk ruff
   formatter never touches in-wheel bytes — the packager normalizes member bytes
   itself — so the built ``.whl`` filename (which embeds the 12-char content
   hash) is IDENTICAL with formatting ENABLED (``apply_formatting=None`` → the
   project default) and DISABLED (``apply_formatting=False``, the ``--no-format``
   equivalent at the facade level, per the CLI's
   ``apply_formatting=(False if no_format else None)``).

Tests import strictly from :mod:`lhp.api` — no internal modules. ``chdir`` +
``for_project`` mirror the source test's fixtures exactly.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lhp.api import (
    GenerationCompleted,
    LakehousePlumberApplicationFacade,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
)

# Mixed-mode pipeline names. ``p_whl`` opts into wheel packaging; ``p_src`` is
# the default source-mode pipeline that must keep its loose flowgroup .py.
SOURCE_PIPELINE = "p_src"
WHEEL_PIPELINE = "p_whl"
# The pipeline-config file the facade is built with; its final document flips
# p_whl into wheel mode while p_src inherits the source default.
PIPELINE_CONFIG_REL = "config/pipeline_config.yaml"
# A literal /Volumes/... artifact volume (no substitution coupling).
ARTIFACT_VOLUME = "/Volumes/dev_catalog/artifacts/wheels"


def _flowgroup_doc(pipeline: str, table: str) -> dict:
    """A minimal load+write streaming-table flowgroup that resolves, codegens
    and formats cleanly (mirrors ``test_generate_stream_protocol``'s ``p_ok``).
    """
    return {
        "pipeline": pipeline,
        "flowgroup": f"{pipeline}_fg",
        "actions": [
            {
                "name": f"load_{table}",
                "type": "load",
                "target": f"v_{table}_raw",
                "source": {
                    "type": "cloudfiles",
                    "path": "${landing_path}/" + table,
                    "format": "json",
                },
            },
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
            },
        ],
    }


def _write_mixed_project(project_root: Path) -> None:
    """Write a minimal MIXED-MODE project: one source pipeline + one wheel pipeline.

    ``lhp.yaml`` carries the ``wheel.artifact_volume`` the wheel path requires.
    The per-pipeline ``packaging: wheel`` override lives in the pipeline-config
    file (``config/pipeline_config.yaml``) the facade is built with, so ``p_whl``
    resolves to wheel mode while ``p_src`` inherits the hard ``"source"`` default.
    """
    for sub in ("presets", "templates", "substitutions", "config"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        "name: stream_wheel_project\n"
        "version: '1.0'\n"
        "wheel:\n"
        f"  artifact_volume: {ARTIFACT_VOLUME}\n"
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

    # Per-pipeline packaging override: only p_whl is wheel; p_src stays source.
    # ``project_defaults`` carries catalog/schema so the bundle preflight
    # catalog/schema check (LHP-CFG-026, run because bundle_enabled=True) passes.
    with open(project_root / PIPELINE_CONFIG_REL, "w") as f:
        yaml.dump_all(
            [
                {
                    "project_defaults": {
                        "catalog": "${catalog}",
                        "schema": "${bronze_schema}",
                    }
                },
                {"pipeline": SOURCE_PIPELINE},
                {"pipeline": WHEEL_PIPELINE, "packaging": "wheel"},
            ],
            f,
        )

    for pipeline, table in (
        (SOURCE_PIPELINE, "src_table"),
        (WHEEL_PIPELINE, "whl_table"),
    ):
        fg_dir = project_root / "pipelines" / pipeline
        fg_dir.mkdir(parents=True, exist_ok=True)
        with open(fg_dir / f"{pipeline}_fg.yaml", "w") as f:
            yaml.dump(_flowgroup_doc(pipeline, table), f)


@pytest.fixture
def mixed_mode_project(tmp_path: Path):
    """Facade (built WITH the wheel pipeline-config) + project root + output dir
    over a mixed-mode project; ``chdir``'d in, restored on teardown.

    Mirrors ``test_generate_stream_protocol``'s fixtures, plus
    ``pipeline_config_path`` so the per-pipeline ``packaging`` override is loaded.
    """
    project_root = tmp_path / "proj"
    project_root.mkdir()
    _write_mixed_project(project_root)
    output_dir = project_root / "generated" / "dev"
    original_cwd = os.getcwd()
    os.chdir(project_root)
    try:
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root,
            pipeline_config_path=PIPELINE_CONFIG_REL,
            enforce_version=False,
        )
        yield facade, project_root, output_dir
    finally:
        os.chdir(original_cwd)


def _collect(facade, output_dir, *, apply_formatting):
    """Drive the generate stream to completion (bundle ENABLED) and return the
    collected event list. Raises if the stream raises (no failure is expected)."""
    collected: list = []
    gen = facade.generation.generate_pipelines(
        pipeline_fields=[SOURCE_PIPELINE, WHEEL_PIPELINE],
        env="dev",
        output_dir=output_dir,
        bundle_enabled=True,
        apply_formatting=apply_formatting,
    )
    for event in gen:
        collected.append(event)
    return collected


def _wheel_filename(output_dir: Path) -> str:
    """Return the single built ``.whl`` filename under
    ``generated/<env>/_wheels/<pipeline>/dist/`` (it embeds the 12-char content
    hash — the stable R-invariant signal)."""
    dist_dir = output_dir / "_wheels" / WHEEL_PIPELINE / "dist"
    wheels = sorted(dist_dir.glob("*.whl"))
    assert len(wheels) == 1, (
        f"Exactly one wheel should be built under {dist_dir}; found: "
        f"{[w.name for w in wheels]}"
    )
    return wheels[0].name


@pytest.mark.integration
class TestGenerateStreamWheelMixedMode:
    """The §5.7 generate stream over a mixed source/wheel project."""

    def test_event_order_unchanged_for_mixed_project(self, mixed_mode_project):
        """Wheel packaging adds NO new event type and NO new phase: the phase
        order is exactly today's canonical sequence (the order documented in
        ``_stream_pipeline_generation``'s docstring), terminating in a single
        ``GenerationCompleted``.
        """
        facade, _project_root, output_dir = mixed_mode_project

        collected = _collect(facade, output_dir, apply_formatting=None)

        # First event is OperationStarted; last is exactly one GenerationCompleted.
        assert isinstance(collected[0], OperationStarted)
        completed = [e for e in collected if isinstance(e, GenerationCompleted)]
        assert len(completed) == 1, (
            f"Expected exactly one terminal GenerationCompleted; got "
            f"{[type(e).__name__ for e in collected]}"
        )
        assert isinstance(collected[-1], GenerationCompleted)
        assert completed[0].response.success is True

        # The phase pairs appear in the canonical order. With bundle_enabled and
        # a clean (non-dry-run, formatting-on) generate, ALL six phases run.
        expected_phases = [
            "discover",
            "preflight",
            "generate",
            "format",
            "monitoring",
            "bundle_sync",
        ]
        started_order = [e.phase for e in collected if isinstance(e, PhaseStarted)]
        completed_order = [e.phase for e in collected if isinstance(e, PhaseCompleted)]
        assert started_order == expected_phases, (
            f"PhaseStarted order changed: {started_order} != {expected_phases}"
        )
        assert completed_order == expected_phases, (
            f"PhaseCompleted order changed: {completed_order} != {expected_phases}"
        )

        # Each phase's Started strictly precedes its own Completed, which precedes
        # the next phase's Started — i.e. the pairs are properly nested in order.
        for phase in expected_phases:
            i_started = next(
                i
                for i, e in enumerate(collected)
                if isinstance(e, PhaseStarted) and e.phase == phase
            )
            i_completed = next(
                i
                for i, e in enumerate(collected)
                if isinstance(e, PhaseCompleted) and e.phase == phase
            )
            assert i_started < i_completed, f"{phase}: Started must precede Completed"

        # Every phase completed successfully.
        assert all(e.success for e in collected if isinstance(e, PhaseCompleted)), (
            "All phases must complete with success=True on a clean mixed-mode run"
        )

        # Sanity: there is exactly ONE bundle_sync pair (the wheel path runs
        # inside the existing bundle_sync phase — it does not add a new phase).
        assert started_order.count("bundle_sync") == 1
        assert completed_order.count("bundle_sync") == 1

    def test_wheels_bundle_file_emitted(self, mixed_mode_project):
        """With ≥1 wheel pipeline, ``resources/lhp/_wheels.bundle.yml`` is emitted
        by the bundle_sync phase's ``emit_wheels_bundle_file`` call.
        """
        facade, project_root, output_dir = mixed_mode_project

        _collect(facade, output_dir, apply_formatting=None)

        wheels_bundle = project_root / "resources" / "lhp" / "_wheels.bundle.yml"
        assert wheels_bundle.is_file(), (
            "_wheels.bundle.yml must be emitted when the project has a wheel pipeline"
        )
        content = wheels_bundle.read_text()
        # Load-bearing wiring: the resolved artifact volume, and NO 'artifacts:'
        # block. The header comment documents why there is no artifacts block (the
        # word "artifacts" appears in prose), so the no-artifacts check parses the
        # YAML and asserts no top-level 'artifacts' KEY — a naive substring check
        # would false-positive on the comment.
        doc = yaml.safe_load(content)
        assert "artifacts" not in doc, (
            "No top-level 'artifacts:' block: each wheel is a prebuilt local "
            "library reference that DAB uploads + rewrites itself"
        )
        assert ARTIFACT_VOLUME in content

    def test_wheeled_dir_runner_only_source_dir_loose_py(self, mixed_mode_project):
        """The wheeled pipeline dir contains ONLY its runner ``.py`` (no loose
        flowgroup .py); the source pipeline dir contains its normal loose .py.
        """
        facade, _project_root, output_dir = mixed_mode_project

        _collect(facade, output_dir, apply_formatting=None)

        # Wheeled pipeline dir: exactly one .py, the runner (no loose flowgroup .py).
        wheel_dir = output_dir / WHEEL_PIPELINE
        assert wheel_dir.is_dir(), "Wheeled pipeline dir should be generated"
        wheel_pys = sorted(p.name for p in wheel_dir.glob("*.py"))
        assert len(wheel_pys) == 1, (
            f"Wheeled pipeline dir must hold exactly one .py (the runner); found: "
            f"{wheel_pys}"
        )
        assert wheel_pys[0].endswith("_runner.py"), (
            f"The lone .py in the wheel dir must be the runner; found: {wheel_pys[0]}"
        )
        # The loose flowgroup module must NOT be on disk (it ships in the wheel).
        assert not (wheel_dir / f"{WHEEL_PIPELINE}_fg.py").exists()
        # No .whl lands in the synced pipeline dir (it stages under _wheels/).
        assert not list(wheel_dir.rglob("*.whl"))

        # Source pipeline dir: normal loose flowgroup .py, and NO runner.
        source_dir = output_dir / SOURCE_PIPELINE
        assert source_dir.is_dir(), "Source pipeline dir should be generated"
        source_pys = sorted(p.name for p in source_dir.glob("*.py"))
        assert source_pys == [f"{SOURCE_PIPELINE}_fg.py"], (
            f"Source pipeline dir must hold its loose flowgroup .py; found: "
            f"{source_pys}"
        )
        assert not any(name.endswith("_runner.py") for name in source_pys), (
            "A source-mode pipeline must NOT emit a runner"
        )

    def test_wheel_filename_is_formatter_independent(self, mixed_mode_project):
        """R-invariant: the built ``.whl`` filename (hence the 12-char content
        hash it embeds) is IDENTICAL with formatting ON and OFF.

        In wheel mode the packager normalizes member bytes itself; the on-disk
        ruff formatter never touches in-wheel bytes, so toggling the
        ``apply_formatting`` facade param (``None`` = project default / ON vs
        ``False`` = the ``--no-format`` equivalent) cannot change the wheel's
        content identity.
        """
        facade, _project_root, output_dir = mixed_mode_project

        # Run 1: formatting ENABLED (apply_formatting=None -> project default).
        format_on = _collect(facade, output_dir, apply_formatting=None)
        assert format_on[-1].response.success is True
        whl_format_on = _wheel_filename(output_dir)
        # The format phase actually ran on this pass.
        assert any(
            isinstance(e, PhaseStarted) and e.phase == "format" for e in format_on
        ), "format phase should run with apply_formatting=None"

        # Run 2: formatting DISABLED (apply_formatting=False, --no-format equiv).
        # Every run is a full regenerate (the env root is wiped up front), so the
        # wheel is rebuilt from scratch here.
        format_off = _collect(facade, output_dir, apply_formatting=False)
        assert format_off[-1].response.success is True
        whl_format_off = _wheel_filename(output_dir)
        # The format phase was SKIPPED on this pass (no PhaseStarted('format')).
        assert not any(
            isinstance(e, PhaseStarted) and e.phase == "format" for e in format_off
        ), "format phase must be skipped with apply_formatting=False"

        # The content-addressed wheel filename is identical across both runs:
        # the on-disk formatter never reaches in-wheel bytes.
        assert whl_format_on == whl_format_off, (
            "Wheel filename (and thus its embedded content hash) must be "
            f"formatter-independent: ON={whl_format_on!r} OFF={whl_format_off!r}"
        )
        # And it carries the deterministic lhp_<pipeline>_<env>_<hash> identity
        # (the ``lhp_`` brand prefix marks LHP-generated wheels).
        assert whl_format_on.startswith(f"lhp_{WHEEL_PIPELINE}_dev_")
        assert whl_format_on.endswith("-py3-none-any.whl")
