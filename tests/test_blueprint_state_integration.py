"""Spec-driven unit tests for blueprint state-tracking integrations.

Covers Phase 8 (instance dependency cache round-trip), Phase 8b (synthetic
flag round-trip through state save/load), Phase 8c / B3 (slow-path cleanup
skip for synthetic FileStates), and code 059 (instance file outside project
root).
"""

from datetime import datetime
from pathlib import Path

import pytest

from lhp.core.services.blueprint_expander import BlueprintProvenance
from lhp.core.state.dependency_tracker import DependencyTracker
from lhp.core.state.state_cleanup_service import StateCleanupService
from lhp.core.state_dependency_resolver import StateDependencyResolver
from lhp.core.state_manager import ProjectStateManager
from lhp.core.state_models import FileState, ProjectState
from lhp.models.config import FlowGroup
from lhp.utils.error_formatter import LHPError

pytestmark = pytest.mark.unit


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _make_synthetic_flowgroup(pipeline="apac_sg_raw", flowgroup="apac_sg_orders"):
    fg = FlowGroup(pipeline=pipeline, flowgroup=flowgroup, actions=[])
    fg._synthetic = True
    return fg


# ---------------------------------------------------------------------------
# Phase 8 — cache round-trip for the instance dependency
# ---------------------------------------------------------------------------


def test_instance_dependency_present_after_first_call_and_persists_on_cache_hit(
    tmp_path,
):
    """Phase 8: instance dep is in the dict on miss and after cache-hit lookup."""
    blueprint_path = _write(
        tmp_path / "blueprints" / "erp.yaml",
        """
name: erp_ingestion
parameters:
  - name: site_name
    required: true
flowgroups:
  - pipeline: "%{site_name}_raw"
    flowgroup: "%{site_name}_orders"
    actions: []
""",
    )
    instance_path = _write(
        tmp_path / "pipelines" / "erp" / "bronze" / "sg.yaml",
        """
blueprint: erp_ingestion
site_name: apac_sg
""",
    )
    fg = _make_synthetic_flowgroup()
    provenance = {
        ("apac_sg_raw", "apac_sg_orders"): BlueprintProvenance(
            blueprint_name="erp_ingestion",
            blueprint_path=blueprint_path,
            instance_path=instance_path,
            flowgroup=fg,
            spec_index=0,
        )
    }
    resolver = StateDependencyResolver(tmp_path)
    resolver.set_blueprint_provenance(provenance)

    # First call — cache miss path.
    deps_first = resolver.resolve_file_dependencies(
        blueprint_path,
        environment="dev",
        pipeline="apac_sg_raw",
        flowgroup_name="apac_sg_orders",
    )
    instance_keys = [k for k, v in deps_first.items() if v.type == "instance"]
    assert len(instance_keys) == 1
    assert "pipelines/erp/bronze/sg.yaml" in instance_keys[0].replace("\\", "/")

    # Second call — cache hit path. Instance dep must still be present.
    deps_second = resolver.resolve_file_dependencies(
        blueprint_path,
        environment="dev",
        pipeline="apac_sg_raw",
        flowgroup_name="apac_sg_orders",
    )
    instance_types = [v.type for v in deps_second.values()]
    assert instance_types.count("instance") == 1


# ---------------------------------------------------------------------------
# B3 — slow-path orphan check skips synthetic FileStates
# ---------------------------------------------------------------------------


def test_cleanup_slow_path_skips_synthetic_files(tmp_path):
    """A synthetic FileState whose source YAML no longer exists must NOT be
    reported as orphaned by the slow path (Phase 8c / B3)."""
    cleaner = StateCleanupService(tmp_path)

    state = ProjectState()
    state.environments["dev"] = {
        # Synthetic file: blueprint path does not exist; slow path must skip it.
        "generated/dev/site_a/orders.py": FileState(
            source_yaml="blueprints/erp.yaml",
            generated_path="generated/dev/site_a/orders.py",
            checksum="abc",
            source_yaml_checksum="src_checksum",
            timestamp=datetime.now().isoformat(),
            environment="dev",
            pipeline="site_a_raw",
            flowgroup="site_a_orders",
            synthetic=True,
        ),
        # Regular file with a missing source — slow path should report this orphan.
        "generated/dev/regular/x.py": FileState(
            source_yaml="pipelines/missing.yaml",
            generated_path="generated/dev/regular/x.py",
            checksum="def",
            source_yaml_checksum="src_checksum",
            timestamp=datetime.now().isoformat(),
            environment="dev",
            pipeline="regular",
            flowgroup="x",
            synthetic=False,
        ),
    }
    # Slow path: invoked when active_flowgroups is None.
    orphaned = cleaner.find_orphaned_files(
        state, environment="dev", active_flowgroups=None
    )
    orphan_paths = [o.generated_path for o in orphaned]
    assert "generated/dev/regular/x.py" in orphan_paths
    assert "generated/dev/site_a/orders.py" not in orphan_paths


# ---------------------------------------------------------------------------
# Phase 8b — synthetic flag round-trips through state save/load
# ---------------------------------------------------------------------------


def test_synthetic_flag_roundtrips_through_state_persistence(tmp_path):
    """``FileState.synthetic`` must persist through worker shard save/load.

    Worker writes the per-pipeline shard via
    :class:`PipelineStateManager`; the aggregate manager surfaces it via
    :meth:`load_all_pipeline_shards`. The ``synthetic`` flag is part of
    the on-disk schema (introduced for blueprint expansion) so it round-
    trips identically to any other ``FileState`` field.
    """
    from lhp.core.state.pipeline_state_manager import PipelineStateManager

    pm = PipelineStateManager(
        state_dir=tmp_path / ".lhp_state",
        pipeline_name="site_a_raw",
        environment="dev",
        project_root=tmp_path,
    )
    pm._state.environments["dev"] = {
        "generated/dev/site_a.py": FileState(
            source_yaml="blueprints/erp.yaml",
            generated_path="generated/dev/site_a.py",
            checksum="c",
            source_yaml_checksum="sc",
            timestamp=datetime.now().isoformat(),
            environment="dev",
            pipeline="site_a_raw",
            flowgroup="site_a_orders",
            synthetic=True,
        ),
    }
    pm._dirty = True
    pm.save()

    sm2 = ProjectStateManager(tmp_path)
    env_files = sm2.load_all_pipeline_shards("dev")
    fs2 = env_files["generated/dev/site_a.py"]
    assert fs2.synthetic is True


def test_synthetic_default_is_false_for_legacy_state(tmp_path):
    """An older state file lacking ``synthetic`` must hydrate with the default."""
    fs = FileState(
        source_yaml="pipelines/raw.yaml",
        generated_path="generated/dev/raw.py",
        checksum="c",
        source_yaml_checksum="sc",
        timestamp="2023-01-01T00:00:00",
        environment="dev",
        pipeline="raw",
        flowgroup="raw",
    )
    assert fs.synthetic is False


# ---------------------------------------------------------------------------
# Phase 8b — DependencyTracker sets synthetic=True from provenance map
# ---------------------------------------------------------------------------


def test_dependency_tracker_sets_synthetic_from_provenance(tmp_path):
    """When provenance has an entry for (pipeline, flowgroup), the tracked
    FileState must carry synthetic=True (Phase 8b)."""
    blueprint_path = _write(
        tmp_path / "blueprints" / "erp.yaml",
        "name: erp\nparameters: []\nflowgroups: []\n",
    )
    instance_path = _write(
        tmp_path / "pipelines" / "erp" / "bronze" / "sg.yaml",
        "blueprint: erp\n",
    )
    generated = _write(
        tmp_path / "generated" / "dev" / "site_a.py",
        "# synthetic content\n",
    )
    fg = _make_synthetic_flowgroup("site_a_raw", "site_a_orders")
    provenance = {
        ("site_a_raw", "site_a_orders"): BlueprintProvenance(
            blueprint_name="erp",
            blueprint_path=blueprint_path,
            instance_path=instance_path,
            flowgroup=fg,
            spec_index=0,
        )
    }
    tracker = DependencyTracker(tmp_path)
    tracker.set_blueprint_provenance(provenance)
    state = ProjectState()
    tracker.track_generated_file(
        state,
        generated_path=generated,
        source_yaml=blueprint_path,
        environment="dev",
        pipeline="site_a_raw",
        flowgroup="site_a_orders",
    )
    file_state = list(state.environments["dev"].values())[0]
    assert file_state.synthetic is True


# ---------------------------------------------------------------------------
# Code 059 — instance path resolves outside project root
# ---------------------------------------------------------------------------


def test_instance_outside_project_root_raises_059(tmp_path):
    """An instance path that resolves outside project_root must raise 059
    (cache-stability bug — Phase B.2)."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    blueprint_path = _write(
        project_root / "blueprints" / "erp.yaml",
        "name: erp\nparameters: []\nflowgroups: []\n",
    )
    # Place the instance file OUTSIDE the project root.
    outside_dir = tmp_path / "elsewhere"
    outside_dir.mkdir()
    instance_path = _write(
        outside_dir / "sg.yaml",
        "blueprint: erp\n",
    )
    fg = _make_synthetic_flowgroup("site_a_raw", "site_a_orders")
    provenance = {
        ("site_a_raw", "site_a_orders"): BlueprintProvenance(
            blueprint_name="erp",
            blueprint_path=blueprint_path,
            instance_path=instance_path,
            flowgroup=fg,
            spec_index=0,
        )
    }
    resolver = StateDependencyResolver(project_root)
    resolver.set_blueprint_provenance(provenance)
    # `resolve_file_dependencies` defensively logs LHPErrors to a warning, so
    # invoke the injection helper directly — it is the documented site for the
    # 059 raise per the spec's Phase 13 error code table.
    with pytest.raises(LHPError) as exc:
        resolver._inject_instance_dependency({}, "site_a_raw", "site_a_orders")
    # Spec says code 059 lives in state_dependency_resolver.py; the actual
    # category is IO (it concerns a file path constraint).
    assert exc.value.code.endswith("-059")
