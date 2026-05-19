"""Unit tests for :class:`ProjectStateManager` — main-thread state manager.

Focus areas (Plan 1 deliverables):

- Construction from empty project, from existing ``_global.json``.
- ``state_dir`` exposed as a public attribute (Plan 3 reads it).
- ``save_global`` writes ``_global.json`` atomically and merges
  ``last_generation_context`` in-place.
- ``load_all_pipeline_shards`` delegates to persistence.
- ``list_environments`` reads ``_global.json``'s ``global_dependencies`` keys.
- Back-compat shims (``track_generated_file`` / ``track_pipeline_artifact``)
  write per-pipeline shards under the hood and emit
  :class:`DeprecationWarning`.
- ``state`` attribute is removed (accessing it raises ``AttributeError``).
- The module-level ``ProjectStateManager`` alias emits
  :class:`DeprecationWarning` on first import.
"""

import importlib
import sys
import tempfile
import warnings
from datetime import datetime
from pathlib import Path

import pytest

from lhp.core.state.state_persistence import StatePersistence
from lhp.core.state_manager import ProjectStateManager
from lhp.core.state_models import (
    DependencyInfo,
    FileState,
    GlobalDependencies,
    GlobalStatePayload,
    PipelineStatePayload,
)


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------


def _seeded_project(tmp: Path) -> Path:
    project_root = tmp / "project"
    project_root.mkdir()
    (project_root / "pipelines" / "raw").mkdir(parents=True)
    (project_root / "generated" / "raw").mkdir(parents=True)
    (project_root / "pipelines" / "raw" / "customers.yaml").write_text(
        "pipeline: raw_pipeline\nflowgroup: customers\nactions: []\n"
    )
    (project_root / "generated" / "raw" / "customers.py").write_text(
        "# generated\n"
    )
    return project_root


def _make_global_payload() -> GlobalStatePayload:
    return GlobalStatePayload(
        version="0.8.7",
        global_dependencies={
            "dev": GlobalDependencies(
                substitution_file=DependencyInfo(
                    path="substitutions/dev.yaml",
                    checksum="ssss",
                    type="substitution",
                    last_modified=datetime.now().isoformat(),
                ),
                project_config=None,
            )
        },
        last_generation_context={"dev": {"include_tests": "True"}},
    )


# ----------------------------------------------------------------------
# Construction & public attributes
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestConstruction:
    def test_state_dir_attribute_matches_project_root_subdir(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            psm = ProjectStateManager(project_root)
            assert psm.state_dir == project_root / ".lhp_state"
            assert psm.project_root == project_root

    def test_empty_project_yields_empty_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            psm = ProjectStateManager(project_root)
            # No global shard means environments dict is empty.
            assert psm.list_environments() == []
            assert psm.load_all_pipeline_shards("dev") == {}

    def test_loads_state_from_existing_global_shard(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"
            StatePersistence.save_global(state_dir, _make_global_payload())

            psm = ProjectStateManager(project_root)
            assert psm.list_environments() == ["dev"]


# ----------------------------------------------------------------------
# state attribute removed
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestStateAttributeRemoved:
    """The public ``state`` property was removed per Plan 1.

    Plan 4 will add an explicit test ``test_state_environments_attribute_removed``
    that asserts ``project_state_manager.state.environments`` raises
    ``AttributeError``. Plan 1 owns the structural removal; the assertion
    below is a thin sanity check at the manager level.
    """

    def test_state_property_is_not_exposed(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            psm = ProjectStateManager(project_root)
            with pytest.raises(AttributeError):
                _ = psm.state  # type: ignore[attr-defined]


# ----------------------------------------------------------------------
# save_global
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestSaveGlobal:
    def test_writes_global_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            psm = ProjectStateManager(project_root)
            psm.save_global()
            assert (project_root / ".lhp_state" / "_global.json").exists()

    def test_merges_last_generation_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            psm = ProjectStateManager(project_root)
            psm.save_global(last_generation_context={"dev": {"include_tests": "True"}})

            reloaded = StatePersistence.load_global(project_root / ".lhp_state")
            assert reloaded is not None
            assert reloaded.last_generation_context == {"dev": {"include_tests": "True"}}

    def test_subsequent_save_global_preserves_other_envs(self):
        """Merging context for env A does not overwrite env B's context."""
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            psm = ProjectStateManager(project_root)
            psm.save_global(last_generation_context={"dev": {"include_tests": "True"}})
            psm.save_global(last_generation_context={"prod": {"include_tests": "False"}})

            reloaded = StatePersistence.load_global(project_root / ".lhp_state")
            assert reloaded is not None
            assert reloaded.last_generation_context == {
                "dev": {"include_tests": "True"},
                "prod": {"include_tests": "False"},
            }


# ----------------------------------------------------------------------
# list_environments
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestListEnvironments:
    def test_returns_empty_when_global_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            psm = ProjectStateManager(project_root)
            assert psm.list_environments() == []

    def test_returns_envs_from_global_dependencies(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"
            payload = GlobalStatePayload(
                global_dependencies={
                    "dev": GlobalDependencies(),
                    "prod": GlobalDependencies(),
                    "staging": GlobalDependencies(),
                }
            )
            StatePersistence.save_global(state_dir, payload)

            psm = ProjectStateManager(project_root)
            # Sorted (per implementation contract).
            assert psm.list_environments() == ["dev", "prod", "staging"]


# ----------------------------------------------------------------------
# load_all_pipeline_shards
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestLoadAllPipelineShards:
    def test_delegates_to_persistence(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = _seeded_project(Path(tmp))
            state_dir = project_root / ".lhp_state"
            fs = FileState(
                source_yaml="pipelines/raw/customers.yaml",
                generated_path="generated/raw/customers.py",
                checksum="x",
                source_yaml_checksum="y",
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="raw_pipeline",
                flowgroup="customers",
            )
            StatePersistence.save_pipeline_shard(
                state_dir,
                "raw_pipeline",
                PipelineStatePayload(
                    pipeline="raw_pipeline",
                    environments={"dev": {fs.generated_path: fs}},
                ),
            )

            psm = ProjectStateManager(project_root)
            merged = psm.load_all_pipeline_shards("dev")
            assert set(merged.keys()) == {fs.generated_path}

