"""Unit tests for the per-pipeline shard methods on :class:`StatePersistence`.

Covers the six new methods added in Plan 1:

- :meth:`save_pipeline_shard` / :meth:`load_pipeline_shard`
- :meth:`save_global` / :meth:`load_global`
- :meth:`load_all_pipeline_shards`
- :meth:`maybe_remove_legacy_state`

Plus the shared :func:`_atomic_write_json` helper and the reserved-stem guard.
The legacy ``save_state`` / ``load_state`` paths are exercised by
``tests/test_state_persistence_atomic.py`` — this module focuses on the new
shard format only.
"""

import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from lhp.core.state.state_persistence import (
    StatePersistence,
    _atomic_write_json,
)
from lhp.core.state_models import (
    DependencyInfo,
    FileState,
    GlobalDependencies,
    GlobalStatePayload,
    PipelineStatePayload,
)
from lhp.utils.error_formatter import LHPFileError


# ----------------------------------------------------------------------
# Fixtures / helpers
# ----------------------------------------------------------------------


def _make_file_state(
    *,
    source_yaml: str = "pipelines/raw/customers.yaml",
    generated_path: str = "generated/raw/customers.py",
    pipeline: str = "raw_pipeline",
    flowgroup: str = "customers",
    environment: str = "dev",
    with_deps: bool = False,
) -> FileState:
    deps = None
    if with_deps:
        deps = {
            "presets/bronze.yaml": DependencyInfo(
                path="presets/bronze.yaml",
                checksum="aaaa",
                type="preset",
                last_modified=datetime.now().isoformat(),
                mtime=1.0,
            )
        }
    return FileState(
        source_yaml=source_yaml,
        generated_path=generated_path,
        checksum="deadbeef",
        source_yaml_checksum="cafebabe",
        timestamp=datetime.now().isoformat(),
        environment=environment,
        pipeline=pipeline,
        flowgroup=flowgroup,
        file_dependencies=deps,
    )


# ----------------------------------------------------------------------
# Atomic write helper
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestAtomicWriteJson:
    """Covers the durability invariant: tempfile + os.replace + fsync."""

    def test_writes_payload_to_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "out.json"
            _atomic_write_json(
                target, {"hello": "world"}, logging.getLogger("test")
            )
            assert target.exists()
            with open(target) as f:
                assert json.load(f) == {"hello": "world"}

    def test_creates_parent_directory_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "nested" / "deep" / "out.json"
            _atomic_write_json(target, {"k": 1}, logging.getLogger("test"))
            assert target.exists()

    def test_failure_does_not_leave_temp_file_behind(self, monkeypatch):
        """When the write fails, the temp file is cleaned up."""
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "out.json"

            def _bad_replace(*_args, **_kwargs):
                raise OSError("simulated replace failure")

            monkeypatch.setattr("lhp.core.state.state_persistence.os.replace", _bad_replace)

            with pytest.raises(LHPFileError):
                _atomic_write_json(target, {"k": 1}, logging.getLogger("test"))

            leftover = list(Path(tmp).glob("*.tmp"))
            assert leftover == []
            assert not target.exists()


# ----------------------------------------------------------------------
# Pipeline shard save/load
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestPipelineShardRoundTrip:
    """Bytes-on-disk symmetry for the per-pipeline shard format."""

    def test_save_then_load_recovers_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            fs = _make_file_state()
            payload = PipelineStatePayload(
                pipeline="raw_pipeline",
                environments={"dev": {fs.generated_path: fs}},
            )

            StatePersistence.save_pipeline_shard(state_dir, "raw_pipeline", payload)
            loaded = StatePersistence.load_pipeline_shard(state_dir, "raw_pipeline")

            assert loaded is not None
            assert loaded.pipeline == "raw_pipeline"
            assert loaded.schema_version == "2"
            assert "dev" in loaded.environments
            recovered = loaded.environments["dev"][fs.generated_path]
            assert recovered.source_yaml == fs.source_yaml
            assert recovered.checksum == fs.checksum
            assert recovered.flowgroup == "customers"

    def test_round_trip_preserves_file_dependencies(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            fs = _make_file_state(with_deps=True)
            payload = PipelineStatePayload(
                pipeline="raw_pipeline",
                environments={"dev": {fs.generated_path: fs}},
            )
            StatePersistence.save_pipeline_shard(state_dir, "raw_pipeline", payload)
            loaded = StatePersistence.load_pipeline_shard(state_dir, "raw_pipeline")

            assert loaded is not None
            recovered = loaded.environments["dev"][fs.generated_path]
            assert recovered.file_dependencies is not None
            assert "presets/bronze.yaml" in recovered.file_dependencies
            dep = recovered.file_dependencies["presets/bronze.yaml"]
            assert isinstance(dep, DependencyInfo)
            assert dep.type == "preset"

    def test_load_missing_shard_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            assert StatePersistence.load_pipeline_shard(state_dir, "absent") is None

    def test_load_malformed_shard_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            state_dir.mkdir()
            (state_dir / "broken.json").write_text("{not valid json")

            with pytest.raises(LHPFileError) as exc_info:
                StatePersistence.load_pipeline_shard(state_dir, "broken")
            assert "Malformed" in exc_info.value.title

    def test_load_shard_with_wrong_schema_version_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            state_dir.mkdir()
            (state_dir / "ancient.json").write_text(
                json.dumps({"schema_version": "1", "pipeline": "ancient", "environments": {}})
            )
            with pytest.raises(LHPFileError) as exc_info:
                StatePersistence.load_pipeline_shard(state_dir, "ancient")
            assert "Incompatible" in exc_info.value.title

    def test_reserved_global_name_raises_on_save(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            payload = PipelineStatePayload(pipeline="_global", environments={})
            with pytest.raises(ValueError, match="reserved"):
                StatePersistence.save_pipeline_shard(state_dir, "_global", payload)


# ----------------------------------------------------------------------
# Global shard save/load
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestGlobalShardRoundTrip:
    """``_global.json`` round-trip and timestamp behavior."""

    def test_save_then_load_recovers_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            payload = GlobalStatePayload(
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
            StatePersistence.save_global(state_dir, payload)
            loaded = StatePersistence.load_global(state_dir)

            assert loaded is not None
            assert loaded.version == "0.8.7"
            assert loaded.schema_version == "2"
            assert loaded.last_updated  # populated by save_global
            assert "dev" in loaded.global_dependencies
            assert loaded.global_dependencies["dev"].substitution_file.type == "substitution"
            assert loaded.last_generation_context == {"dev": {"include_tests": "True"}}

    def test_save_global_stamps_last_updated_each_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            payload = GlobalStatePayload(version="1.0")
            payload.last_updated = ""  # pre-populate as empty
            StatePersistence.save_global(state_dir, payload)
            assert payload.last_updated  # mutated in-place
            first = payload.last_updated

            # Mutate and save again — last_updated should change.
            payload.last_updated = ""
            StatePersistence.save_global(state_dir, payload)
            assert payload.last_updated != ""
            # First and second timestamps can be equal if test runs in <1ms;
            # what we care about is that save_global stamps the value at all.
            assert first  # not blank

    def test_load_missing_global_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            assert StatePersistence.load_global(state_dir) is None

    def test_load_global_with_wrong_schema_version_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            state_dir.mkdir()
            (state_dir / "_global.json").write_text(
                json.dumps({"schema_version": "1"})
            )
            with pytest.raises(LHPFileError) as exc_info:
                StatePersistence.load_global(state_dir)
            assert "Incompatible" in exc_info.value.title


# ----------------------------------------------------------------------
# load_all_pipeline_shards
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestLoadAllPipelineShards:
    """Aggregate consumer API: merge one env's slice across all shards."""

    def test_missing_state_dir_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "does-not-exist"
            assert StatePersistence.load_all_pipeline_shards(state_dir, "dev") == {}

    def test_merges_multiple_shards_for_one_env(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            fs_a = _make_file_state(
                generated_path="generated/raw/a.py", pipeline="pipeline_a"
            )
            fs_b = _make_file_state(
                generated_path="generated/raw/b.py", pipeline="pipeline_b"
            )
            StatePersistence.save_pipeline_shard(
                state_dir,
                "pipeline_a",
                PipelineStatePayload(
                    pipeline="pipeline_a",
                    environments={"dev": {fs_a.generated_path: fs_a}},
                ),
            )
            StatePersistence.save_pipeline_shard(
                state_dir,
                "pipeline_b",
                PipelineStatePayload(
                    pipeline="pipeline_b",
                    environments={"dev": {fs_b.generated_path: fs_b}},
                ),
            )

            merged = StatePersistence.load_all_pipeline_shards(state_dir, "dev")
            assert set(merged.keys()) == {fs_a.generated_path, fs_b.generated_path}
            assert merged[fs_a.generated_path].pipeline == "pipeline_a"
            assert merged[fs_b.generated_path].pipeline == "pipeline_b"

    def test_skips_global_shard_and_other_dotfiles(self):
        """``_global.json`` (reserved) must NOT contribute file entries.

        The aggregate API merges per-pipeline shards only. Mixing
        ``_global.json``'s content in would surface fake "FileState" entries
        that don't exist on disk.
        """
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            state_dir.mkdir()
            (state_dir / "_global.json").write_text(
                json.dumps(
                    {
                        "schema_version": "2",
                        "version": "1.0",
                        "last_updated": "",
                        "global_dependencies": {},
                        "last_generation_context": {},
                    }
                )
            )

            fs = _make_file_state()
            StatePersistence.save_pipeline_shard(
                state_dir,
                "regular",
                PipelineStatePayload(
                    pipeline="regular",
                    environments={"dev": {fs.generated_path: fs}},
                ),
            )

            merged = StatePersistence.load_all_pipeline_shards(state_dir, "dev")
            assert list(merged.keys()) == [fs.generated_path]

    def test_returns_empty_when_env_not_in_any_shard(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / ".lhp_state"
            fs = _make_file_state(environment="dev")
            StatePersistence.save_pipeline_shard(
                state_dir,
                "p1",
                PipelineStatePayload(
                    pipeline="p1",
                    environments={"dev": {fs.generated_path: fs}},
                ),
            )
            assert StatePersistence.load_all_pipeline_shards(state_dir, "prod") == {}


# ----------------------------------------------------------------------
# maybe_remove_legacy_state
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestMaybeRemoveLegacyState:
    """Auto-delete is gated on ``batch_succeeded=True`` — safety net pattern."""

    def test_no_op_when_legacy_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            assert (
                StatePersistence.maybe_remove_legacy_state(Path(tmp), True) is False
            )

    def test_removes_legacy_on_full_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_root = Path(tmp)
            legacy = project_root / ".lhp_state.json"
            legacy.write_text("{}")
            removed = StatePersistence.maybe_remove_legacy_state(project_root, True)
            assert removed is True
            assert not legacy.exists()

    def test_retains_legacy_on_partial_failure(self, caplog):
        """On batch_succeeded=False the legacy file is kept and a warning is logged."""
        with tempfile.TemporaryDirectory() as tmp:
            project_root = Path(tmp)
            legacy = project_root / ".lhp_state.json"
            legacy.write_text("{}")
            with caplog.at_level(logging.WARNING):
                removed = StatePersistence.maybe_remove_legacy_state(
                    project_root, batch_succeeded=False
                )
            assert removed is False
            assert legacy.exists()
            assert any(
                "retained" in record.message for record in caplog.records
            )
