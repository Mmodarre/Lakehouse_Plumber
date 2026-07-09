"""Cold-discovery parallel parse pool (Phase D2) tests.

Covers the ``core/discovery/_parse_pool`` worker seam and its integration
into ``FlowgroupDiscoveryService.discover_all_flowgroups_with_paths``:

- serial == parallel equivalence (same ``(FlowGroup, Path)`` list, same order);
- fatal-error fidelity: with the pool enabled, a failing file raises the SAME
  LHPError (type/code/message) as a fully serial run — the pool never
  transports or re-raises worker errors, the serial loop does;
- threshold / degeneracy gates: the pool is never invoked below
  ``MIN_FILES_FOR_PARALLEL_PARSE`` or when ``max_workers == 1``;
- DTO pickle round-trip and the worker never-raise contract (§5.6 / §9.6);
- blueprint-instance files: empty flowgroups but seeded documents serve the
  instance pass without a physical re-read.

Patch-target note (mirrors ``test_single_parse_pass.py``): the parent-process
physical-read spy patches ``lhp.parsers.yaml_parser.load_yaml_documents_all``
— the binding the cache shell's miss-loader resolves — NOT the definition
site in ``yaml_loader``. Spawn workers re-import modules in their own
process, so parent patches never leak into workers.

:stability: internal
"""

import pickle
from pathlib import Path

import pytest
import yaml

import lhp.core.discovery._parse_pool as parse_pool
import lhp.parsers.yaml_parser as yaml_parser_module
from lhp.core.discovery.flowgroup_discoverer import FlowgroupDiscoveryService
from lhp.errors import LHPError
from lhp.models import FlowGroup
from lhp.parsers.yaml_parser import CachingYAMLParser


def _flowgroup_doc(pipeline: str, table: str) -> dict:
    return {
        "pipeline": pipeline,
        "flowgroup": f"{table}_fg",
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


def _build_project(tmpdir: Path, n: int = 10) -> Path:
    """Lay down ``n`` regular flowgroup files under ``pipelines/ingest``."""
    project_root = Path(tmpdir)
    (project_root / "pipelines" / "ingest").mkdir(parents=True)
    for i in range(n):
        table = f"t{i:02d}"
        fg_path = project_root / "pipelines" / "ingest" / f"{table}_fg.yaml"
        with open(fg_path, "w") as f:
            yaml.dump(_flowgroup_doc("ingest", table), f)
    return project_root


def _discover(project_root: Path, max_workers: int):
    """Fresh discoverer + fresh caching parser (cold in-memory caches)."""
    discoverer = FlowgroupDiscoveryService(
        project_root,
        None,
        yaml_parser=CachingYAMLParser(),
        max_workers=max_workers,
    )
    return discoverer.discover_all_flowgroups_with_paths()


def _enable_pool(monkeypatch) -> list:
    """Force the pool on for tiny projects and spy on its invocations.

    Returns the (initially empty) call-record list; each engaged pool run
    appends ``(paths, max_workers)``. The spy wraps the REAL
    ``run_parse_pool`` so parsing behavior is unchanged (§8.8).
    """
    monkeypatch.setattr(parse_pool, "MIN_FILES_FOR_PARALLEL_PARSE", 0)
    monkeypatch.setattr(parse_pool, "FILES_PER_WORKER", 1)
    calls: list = []
    real_run_parse_pool = parse_pool.run_parse_pool

    def spying_run_parse_pool(paths, max_workers):
        calls.append((list(paths), max_workers))
        return real_run_parse_pool(paths, max_workers)

    monkeypatch.setattr(parse_pool, "run_parse_pool", spying_run_parse_pool)
    return calls


@pytest.mark.integration
def test_parallel_pool_matches_serial_discovery(tmp_path, monkeypatch):
    """workers=2 pool run yields the identical ordered pair list as serial,
    and the serial loop is a pure cache read (zero parent physical reads)."""
    project_root = _build_project(tmp_path / "equiv_project", n=10)

    serial_pairs = _discover(project_root, max_workers=1)
    assert len(serial_pairs) == 10

    calls = _enable_pool(monkeypatch)

    parent_reads: list = []
    real_load = yaml_parser_module.load_yaml_documents_all

    def counting_load(file_path, *args, **kwargs):
        parent_reads.append(Path(file_path))
        return real_load(file_path, *args, **kwargs)

    monkeypatch.setattr(yaml_parser_module, "load_yaml_documents_all", counting_load)
    parallel_pairs = _discover(project_root, max_workers=2)

    assert calls, "run_parse_pool was never invoked (vacuous equivalence)"
    assert calls[0][1] == 2
    assert sorted(calls[0][0]) == sorted(p for _, p in serial_pairs)
    assert parallel_pairs == serial_pairs
    assert not parent_reads, (
        "Seeded files must be served from the in-memory cache; the parent "
        f"process physically re-read: {parent_reads}"
    )


def _assert_same_lhp_error(project_root: Path, monkeypatch, expected_code=None):
    """Serial and pool-enabled discovery must raise the identical LHPError."""
    with pytest.raises(LHPError) as serial_exc:
        _discover(project_root, max_workers=1)

    _enable_pool(monkeypatch)
    with pytest.raises(LHPError) as pool_exc:
        _discover(project_root, max_workers=2)

    assert type(pool_exc.value) is type(serial_exc.value)
    assert pool_exc.value.code == serial_exc.value.code
    assert str(pool_exc.value) == str(serial_exc.value)
    if expected_code is not None:
        assert pool_exc.value.code == expected_code


@pytest.mark.integration
def test_fatal_fidelity_invalid_action_type(tmp_path, monkeypatch):
    project_root = _build_project(tmp_path / "invalid_action_project", n=9)
    bad = _flowgroup_doc("ingest", "zz_bad")
    bad["actions"][0]["type"] = "looad"
    with open(project_root / "pipelines" / "ingest" / "zz_bad_fg.yaml", "w") as f:
        yaml.dump(bad, f)

    _assert_same_lhp_error(project_root, monkeypatch)


@pytest.mark.integration
def test_fatal_fidelity_blueprint_definition_in_pipelines(tmp_path, monkeypatch):
    project_root = _build_project(tmp_path / "blueprint_in_pipelines", n=9)
    blueprint_doc = {
        "parameters": {"table": {"type": "string"}},
        "flowgroups": [{"flowgroup": "bp_fg", "actions": []}],
    }
    with open(project_root / "pipelines" / "ingest" / "zz_bp.yaml", "w") as f:
        yaml.dump(blueprint_doc, f)

    _assert_same_lhp_error(project_root, monkeypatch, expected_code="LHP-CFG-040")


@pytest.mark.integration
def test_pool_not_invoked_below_threshold(tmp_path, monkeypatch):
    """10 misses < the production threshold of 500 -> no pool, serial parse."""
    assert parse_pool.MIN_FILES_FOR_PARALLEL_PARSE == 500
    project_root = _build_project(tmp_path / "below_threshold", n=10)

    def forbidden_pool(paths, max_workers):
        raise AssertionError("run_parse_pool must not be invoked below threshold")

    monkeypatch.setattr(parse_pool, "run_parse_pool", forbidden_pool)
    pairs = _discover(project_root, max_workers=4)
    assert len(pairs) == 10


@pytest.mark.integration
def test_pool_not_invoked_when_workers_is_one(tmp_path, monkeypatch):
    """max_workers=1 stays fully serial even with the threshold forced to 0."""
    project_root = _build_project(tmp_path / "workers_one", n=10)
    monkeypatch.setattr(parse_pool, "MIN_FILES_FOR_PARALLEL_PARSE", 0)

    def forbidden_pool(paths, max_workers):
        raise AssertionError("run_parse_pool must not be invoked for workers=1")

    monkeypatch.setattr(parse_pool, "run_parse_pool", forbidden_pool)
    pairs = _discover(project_root, max_workers=1)
    assert len(pairs) == 10


@pytest.mark.unit
def test_parsed_file_outcome_pickle_round_trip():
    success = parse_pool.ParsedFileOutcome(
        path=Path("/tmp/x.yaml"),
        documents=({"pipeline": "p", "flowgroup": "f", "actions": []},),
        flowgroups=(FlowGroup(pipeline="p", flowgroup="f"),),
    )
    failure = parse_pool.ParsedFileOutcome(
        path=Path("/tmp/y.yaml"),
        documents=(),
        flowgroups=(),
        failed=True,
        failure_summary="ValueError: boom",
    )
    for outcome in (success, failure):
        restored = pickle.loads(pickle.dumps(outcome))
        assert restored == outcome


@pytest.mark.unit
def test_worker_batch_never_raises(tmp_path):
    """Unreadable/nonexistent and malformed paths yield failed DTOs in-order."""
    good = tmp_path / "good.yaml"
    with open(good, "w") as f:
        yaml.dump(_flowgroup_doc("ingest", "ok"), f)
    malformed = tmp_path / "malformed.yaml"
    malformed.write_text("pipeline: [unclosed\n  actions: {::")
    missing = tmp_path / "does_not_exist.yaml"

    outcomes = parse_pool._parse_file_batch([missing, malformed, good])

    assert len(outcomes) == 3
    assert [o.path for o in outcomes] == [missing, malformed, good]
    assert outcomes[0].failed and outcomes[0].failure_summary
    assert outcomes[1].failed and outcomes[1].failure_summary
    assert not outcomes[2].failed
    assert outcomes[2].flowgroups[0].flowgroup == "ok_fg"


@pytest.mark.integration
def test_blueprint_instance_file_through_pool(tmp_path, monkeypatch):
    """An instance file parses to zero flowgroups but its documents are
    seeded, so the instance pass is served without a physical re-read."""
    project_root = _build_project(tmp_path / "instance_project", n=3)
    instance_doc = {
        "use_blueprint": "some_blueprint",
        "instances": [{"table": "orders"}],
    }
    instance_path = project_root / "pipelines" / "ingest" / "zz_instance.yaml"
    with open(instance_path, "w") as f:
        yaml.dump(instance_doc, f)

    serial_pairs = _discover(project_root, max_workers=1)
    assert len(serial_pairs) == 3  # the instance file contributes none

    calls = _enable_pool(monkeypatch)
    parser = CachingYAMLParser()
    discoverer = FlowgroupDiscoveryService(
        project_root, None, yaml_parser=parser, max_workers=2
    )
    parallel_pairs = discoverer.discover_all_flowgroups_with_paths()

    assert calls, "run_parse_pool was never invoked (vacuous instance test)"
    assert parallel_pairs == serial_pairs

    # The seeded documents sub-cache must now serve the instance file with
    # no physical read: replace the miss-loader's binding with a raiser.
    def no_read_allowed(file_path, *args, **kwargs):
        raise AssertionError(f"physical read of {file_path} after seeding")

    monkeypatch.setattr(yaml_parser_module, "load_yaml_documents_all", no_read_allowed)
    documents = parser.load_documents_all(instance_path)
    assert documents == [instance_doc]


@pytest.mark.unit
def test_windows_clamps_workers_to_61(monkeypatch, tmp_path):
    """ProcessPoolExecutor rejects >61 workers on Windows; the sizing clamps."""
    import sys as _sys

    from lhp.core.discovery import _parse_pool as parse_pool

    captured: dict = {}

    class _FakeExecutor:
        def __init__(self, max_workers, **kwargs):
            captured["max_workers"] = max_workers

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def submit(self, fn, batch):
            raise RuntimeError("no real work in this test")

    monkeypatch.setattr(parse_pool, "ProcessPoolExecutor", _FakeExecutor)
    monkeypatch.setattr(parse_pool, "as_completed", lambda mapping: iter(()))
    monkeypatch.setattr(parse_pool, "FILES_PER_WORKER", 1)
    monkeypatch.setattr(_sys, "platform", "win32")

    paths = [tmp_path / f"f{i}.yaml" for i in range(200)]
    parse_pool.run_parse_pool(paths, max_workers=100)
    assert captured["max_workers"] == 61


@pytest.mark.unit
def test_non_windows_keeps_requested_workers(monkeypatch, tmp_path):
    from lhp.core.discovery import _parse_pool as parse_pool

    captured: dict = {}

    class _FakeExecutor:
        def __init__(self, max_workers, **kwargs):
            captured["max_workers"] = max_workers

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def submit(self, fn, batch):
            raise RuntimeError("no real work in this test")

    monkeypatch.setattr(parse_pool, "ProcessPoolExecutor", _FakeExecutor)
    monkeypatch.setattr(parse_pool, "as_completed", lambda mapping: iter(()))
    monkeypatch.setattr(parse_pool, "FILES_PER_WORKER", 1)

    paths = [tmp_path / f"f{i}.yaml" for i in range(200)]
    parse_pool.run_parse_pool(paths, max_workers=100)
    assert captured["max_workers"] == 100
