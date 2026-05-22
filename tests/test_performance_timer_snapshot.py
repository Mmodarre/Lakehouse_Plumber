"""Self-tests for the perf-summary snapshot accessor.

Pins the contract used by ``tests/performance/benchmark.py``:
``get_perf_summary()`` returns a well-formed dict whether perf timing is
enabled or not, populates correctly after ``perf_timer`` / ``record_count``,
and yields a caller-owned object that cannot mutate internal state.
"""

import pytest

import lhp.utils.performance_timer as pt


@pytest.fixture(autouse=True)
def _reset_perf_state():
    pt._enabled = False
    pt._start_wall_clock = None
    pt.reset_perf_summary()
    yield
    pt._enabled = False
    pt._start_wall_clock = None
    pt.reset_perf_summary()


def test_snapshot_when_disabled_returns_empty_shape():
    snap = pt.get_perf_summary()
    assert snap["enabled"] is False
    assert snap["started_at"] is None
    assert snap["phases"] == {}
    assert snap["sub_phases"] == {}
    assert snap["categories"] == {}
    assert snap["counts"] == {}


def test_snapshot_populates_after_perf_timer_and_record_count():
    pt.enable_perf_timing(None)

    with pt.perf_timer("phase_a", phase=True):
        pass
    with pt.perf_timer("phase_b", phase=True):
        pass
    with pt.perf_timer("nested_under_a", phase=True, parent_phase="phase_a"):
        pass
    with pt.perf_timer("worker", category="things"):
        pass
    with pt.perf_timer("worker", category="things"):
        pass
    pt.record_count("widgets", 42)
    pt.record_count("gizmos", 7)

    snap = pt.get_perf_summary()

    assert snap["enabled"] is True
    assert snap["started_at"] is not None

    assert set(snap["phases"]) == {"phase_a", "phase_b"}
    for elapsed in snap["phases"].values():
        assert elapsed >= 0.0

    assert list(snap["sub_phases"]) == ["phase_a"]
    sub_entries = snap["sub_phases"]["phase_a"]
    assert len(sub_entries) == 1
    sub_name, sub_dur = sub_entries[0]
    assert sub_name == "nested_under_a"
    assert sub_dur >= 0.0

    things = snap["categories"]["things"]
    assert things["cnt"] == 2
    assert things["min"] >= 0.0
    assert things["max"] >= things["min"]
    assert things["total"] >= things["max"]
    assert things["avg"] == pytest.approx(things["total"] / things["cnt"])

    assert snap["counts"] == {"widgets": 42, "gizmos": 7}


def test_snapshot_is_caller_owned_immutable():
    pt.enable_perf_timing(None)

    with pt.perf_timer("phase_a", phase=True):
        pass
    with pt.perf_timer("nested", phase=True, parent_phase="phase_a"):
        pass
    with pt.perf_timer("worker", category="things"):
        pass
    pt.record_count("widgets", 42)

    snap1 = pt.get_perf_summary()
    snap1["phases"]["evil_phase"] = 999.0
    snap1["sub_phases"]["evil_parent"] = [("child", 1.0)]
    snap1["sub_phases"]["phase_a"].append(("evil_child", 99.0))
    snap1["categories"]["things"]["cnt"] = 0
    snap1["categories"]["evil_cat"] = {
        "cnt": 1, "total": 1.0, "min": 1.0, "max": 1.0, "avg": 1.0,
    }
    snap1["counts"]["widgets"] = 0
    snap1["counts"]["evil_count"] = 1

    snap2 = pt.get_perf_summary()
    assert "evil_phase" not in snap2["phases"]
    assert "evil_parent" not in snap2["sub_phases"]
    assert len(snap2["sub_phases"]["phase_a"]) == 1
    assert snap2["categories"]["things"]["cnt"] == 1
    assert "evil_cat" not in snap2["categories"]
    assert snap2["counts"]["widgets"] == 42
    assert "evil_count" not in snap2["counts"]
