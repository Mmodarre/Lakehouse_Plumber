"""Unit tests for :class:`PipelineStateManager` — the worker-side manager.

Focus areas (Plan 1 deliverables):

- Construction from fresh project (no shard) and from existing shard.
- ``track_generated_file`` / ``track_pipeline_artifact`` mutate state and
  flip the dirty flag; ``save`` is a no-op when not dirty.
- **Multi-env load-modify-write preservation**: a worker mutating env A's
  entries must NOT touch env B's entries on save. This is the load-modify-
  write invariant called out explicitly in Plan 1.
- Two pipelines are isolated (separate shards on disk).
- ``_state_from_payload`` / ``_state_to_payload`` round-trip is lossless.

Workers MUST NOT see :class:`ProjectStateManager` — that contract is enforced
by Plan 2's worker entry signature and exercised by
``tests/test_pipeline_executor_no_state_in_workers.py``. This module only
covers the manager class itself.
"""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from lhp.core.state.pipeline_state_manager import (
    PipelineState,
    PipelineStateManager,
    _state_from_payload,
    _state_to_payload,
)
from lhp.core.state.state_persistence import StatePersistence
from lhp.core.state_models import (
    DependencyInfo,
    FileState,
    PipelineStatePayload,
)


# ----------------------------------------------------------------------
# Fixtures / helpers
# ----------------------------------------------------------------------


def _seeded_project(tmp: Path) -> Path:
    """Initialize a temp project root with the directory layout LHP expects."""
    project_root = tmp / "project"
    project_root.mkdir()
    (project_root / "pipelines" / "raw").mkdir(parents=True)
    (project_root / "generated" / "raw").mkdir(parents=True)
    # Seed a tiny YAML and generated file so checksum calc has something to hash.
    (project_root / "pipelines" / "raw" / "customers.yaml").write_text(
        "pipeline: raw_pipeline\nflowgroup: customers\nactions: []\n"
    )
    (project_root / "generated" / "raw" / "customers.py").write_text(
        "# generated\n"
    )
    return project_root


def _make_file_state(
    *,
    generated_path: str = "generated/raw/customers.py",
    environment: str = "dev",
    pipeline: str = "raw_pipeline",
    flowgroup: str = "customers",
) -> FileState:
    return FileState(
        source_yaml="pipelines/raw/customers.yaml",
        generated_path=generated_path,
        checksum="aaaa",
        source_yaml_checksum="bbbb",
        timestamp=datetime.now().isoformat(),
        environment=environment,
        pipeline=pipeline,
        flowgroup=flowgroup,
    )


# ----------------------------------------------------------------------
# _state_from_payload / _state_to_payload
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestPayloadRoundTrip:
    """``_state_from_payload`` and ``_state_to_payload`` are mutual inverses."""

    def test_none_payload_yields_empty_state(self):
        state = _state_from_payload(None, "pipeline_x")
        assert isinstance(state, PipelineState)
        assert state.pipeline == "pipeline_x"
        assert state.environments == {}

    def test_round_trip_preserves_pipeline_and_envs(self):
        fs = _make_file_state()
        payload = PipelineStatePayload(
            pipeline="raw_pipeline",
            environments={"dev": {fs.generated_path: fs}},
        )
        state = _state_from_payload(payload, "raw_pipeline")
        round_trip = _state_to_payload(state)

        assert round_trip.pipeline == "raw_pipeline"
        assert "dev" in round_trip.environments
        assert round_trip.environments["dev"][fs.generated_path] is fs

    def test_payload_with_missing_pipeline_uses_fallback(self):
        """Older / partially-written shards may omit ``pipeline``."""
        payload = PipelineStatePayload(pipeline="", environments={})
        state = _state_from_payload(payload, "fallback_name")
        assert state.pipeline == "fallback_name"


# ----------------------------------------------------------------------
# Construction
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestPipelineStateManagerConstruction:
    def test_fresh_project_constructs_empty_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"

            pm = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)

            assert pm.pipeline_name == "raw_pipeline"
            assert pm.environment == "dev"
            assert pm.state_dir == state_dir
            assert pm.project_root == project_root
            assert pm.state.environments == {}
            assert pm.dirty is False

    def test_loads_existing_shard(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"
            fs = _make_file_state()
            StatePersistence.save_pipeline_shard(
                state_dir,
                "raw_pipeline",
                PipelineStatePayload(
                    pipeline="raw_pipeline",
                    environments={"dev": {fs.generated_path: fs}},
                ),
            )

            pm = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)

            assert "dev" in pm.state.environments
            assert fs.generated_path in pm.state.environments["dev"]
            assert pm.dirty is False


# ----------------------------------------------------------------------
# track_generated_file
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestTrackGeneratedFile:
    def test_mutates_state_and_sets_dirty(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"

            pm = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)

            pm.track_generated_file(
                generated_path=project_root / "generated" / "raw" / "customers.py",
                source_yaml=project_root / "pipelines" / "raw" / "customers.yaml",
                flowgroup="customers",
            )

            assert pm.dirty is True
            assert "dev" in pm.state.environments
            recorded = pm.state.environments["dev"]
            assert any(
                fs.flowgroup == "customers" for fs in recorded.values()
            ), "Expected the customers flowgroup entry to be present"

    def test_save_persists_shard_and_clears_dirty(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"

            pm = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)
            pm.track_generated_file(
                generated_path=project_root / "generated" / "raw" / "customers.py",
                source_yaml=project_root / "pipelines" / "raw" / "customers.yaml",
                flowgroup="customers",
            )
            pm.save()

            assert pm.dirty is False
            assert (state_dir / "raw_pipeline.json").exists()

            # Reload via a fresh manager and verify the entry survives.
            pm2 = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)
            assert "dev" in pm2.state.environments
            assert any(
                fs.flowgroup == "customers"
                for fs in pm2.state.environments["dev"].values()
            )

    def test_save_is_noop_when_not_dirty(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"

            pm = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)
            pm.save()
            assert not (state_dir / "raw_pipeline.json").exists()


# ----------------------------------------------------------------------
# track_pipeline_artifact
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestTrackPipelineArtifact:
    def test_records_artifact_with_sentinel_flowgroup(self):
        """Artifacts live as FileState entries with flowgroup='__test_reporting__'.

        Today's representation predates the refactor — Plan 1 keeps it
        intact. The sentinel flowgroup is what cleanup logic uses to detect
        pipeline-level artifacts.
        """
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"

            # The artifact tracker reads ``lhp.yaml`` for a source checksum;
            # seed it so the checksum calc has something to hash.
            (project_root / "lhp.yaml").write_text("name: demo\n")
            artifact = project_root / "generated" / "raw" / "_reporting.py"
            artifact.write_text("# hook\n")

            pm = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)
            pm.track_pipeline_artifact(artifact, "test_reporting_hook")

            assert pm.dirty is True
            recorded = list(pm.state.environments["dev"].values())
            assert any(fs.flowgroup == "__test_reporting__" for fs in recorded)
            assert any(fs.artifact_type == "test_reporting_hook" for fs in recorded)


# ----------------------------------------------------------------------
# Multi-env load-modify-write invariant
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestMultiEnvPreservation:
    """A worker mutating env A's entries must NOT touch env B's entries on save."""

    def test_env_b_entries_survive_env_a_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"

            # Pre-seed a shard with two envs.
            fs_prod = _make_file_state(
                generated_path="generated/raw/customers_prod.py",
                environment="prod",
            )
            StatePersistence.save_pipeline_shard(
                state_dir,
                "raw_pipeline",
                PipelineStatePayload(
                    pipeline="raw_pipeline",
                    environments={"prod": {fs_prod.generated_path: fs_prod}},
                ),
            )

            # Construct a worker scoped to env=dev. Mutate dev's slice only.
            pm = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)
            pm.track_generated_file(
                generated_path=project_root / "generated" / "raw" / "customers.py",
                source_yaml=project_root / "pipelines" / "raw" / "customers.yaml",
                flowgroup="customers",
            )
            pm.save()

            # Reload the shard from disk and verify both envs intact.
            reloaded = StatePersistence.load_pipeline_shard(state_dir, "raw_pipeline")
            assert reloaded is not None
            assert "dev" in reloaded.environments
            assert "prod" in reloaded.environments, (
                "Worker on env=dev must not erase env=prod entries"
            )
            assert (
                fs_prod.generated_path in reloaded.environments["prod"]
            )

    def test_worker_does_not_mutate_other_env(self):
        """Defensive: even after a worker save, the in-memory state for env B
        equals what was on disk before construction."""
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"

            fs_prod = _make_file_state(
                generated_path="generated/raw/customers_prod.py",
                environment="prod",
            )
            StatePersistence.save_pipeline_shard(
                state_dir,
                "raw_pipeline",
                PipelineStatePayload(
                    pipeline="raw_pipeline",
                    environments={"prod": {fs_prod.generated_path: fs_prod}},
                ),
            )

            pm = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)
            # The in-memory state still shows prod (loaded from shard).
            assert "prod" in pm.state.environments
            prod_before = dict(pm.state.environments["prod"])

            pm.track_generated_file(
                generated_path=project_root / "generated" / "raw" / "customers.py",
                source_yaml=project_root / "pipelines" / "raw" / "customers.yaml",
                flowgroup="customers",
            )
            assert pm.state.environments["prod"] == prod_before


# ----------------------------------------------------------------------
# Pipeline isolation
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestPipelineIsolation:
    """Two pipelines write to disjoint shards; mutations don't cross over."""

    def test_separate_pipelines_separate_shards(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"
            (project_root / "generated" / "silver").mkdir(parents=True)
            (project_root / "generated" / "silver" / "orders.py").write_text("# generated\n")

            pm_raw = PipelineStateManager(state_dir, "raw_pipeline", "dev", project_root)
            pm_raw.track_generated_file(
                generated_path=project_root / "generated" / "raw" / "customers.py",
                source_yaml=project_root / "pipelines" / "raw" / "customers.yaml",
                flowgroup="customers",
            )
            pm_raw.save()

            pm_silver = PipelineStateManager(state_dir, "silver_pipeline", "dev", project_root)
            pm_silver.track_generated_file(
                generated_path=project_root / "generated" / "silver" / "orders.py",
                source_yaml=project_root / "pipelines" / "raw" / "customers.yaml",
                flowgroup="orders",
            )
            pm_silver.save()

            assert (state_dir / "raw_pipeline.json").exists()
            assert (state_dir / "silver_pipeline.json").exists()

            raw_payload = StatePersistence.load_pipeline_shard(state_dir, "raw_pipeline")
            silver_payload = StatePersistence.load_pipeline_shard(state_dir, "silver_pipeline")

            assert raw_payload is not None and silver_payload is not None
            raw_paths = set(raw_payload.environments["dev"].keys())
            silver_paths = set(silver_payload.environments["dev"].keys())
            assert raw_paths.isdisjoint(silver_paths)
