"""Architectural invariant: Phase A workers never touch the state manager.

Under the new ``ProcessPoolExecutor`` design, this invariant is enforced
structurally — the worker function ``_process_flowgroup_for_generate``
does not accept a ``state_manager`` parameter at all, and the dispatch
in :func:`run_generate_pool` cannot supply one through the picklable
partial. The state manager lives on the main thread; workers can only
emit :class:`CopiedModuleRecord` entries through ``phase_a_records``,
which Phase B replays via ``PythonFileCopier.apply_copy_record``.

These tests verify that:
  1. The worker function's signature exposes no path to state_manager
     or python_copier (closed by construction; verified by signature).
  2. ``PythonFileCopier.apply_copy_record`` — the Phase B replay path —
     correctly calls ``StateManager.track_generated_file`` exactly once
     per record, with the path arguments derived from the record.

Together they pin both halves of the invariant: workers can't reach
state_manager, and the Phase B replay is the single canonical site that
does.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Set
from unittest.mock import MagicMock

import pytest

from lhp.core.pipeline_executor import _process_flowgroup_for_generate
from lhp.core.state_manager import StateManager
from lhp.generators.python_file_copier import CopiedModuleRecord, PythonFileCopier


@pytest.mark.unit
class TestWorkerSignatureExcludesStateManager:
    """The worker MUST NOT accept any state-manager-shaped parameter.

    Closing the door by signature rather than by absence-of-call removes
    a whole class of regression: there is no way for a future caller (or
    test) to accidentally pass a state_manager into a worker process.
    """

    def test_worker_signature_excludes_state_manager(self) -> None:
        params: Set[str] = set(
            inspect.signature(_process_flowgroup_for_generate).parameters.keys()
        )
        # Generic "state" parameter shapes.
        for forbidden in ("state_manager", "state_mgr", "state"):
            assert forbidden not in params, (
                f"_process_flowgroup_for_generate must not expose {forbidden!r} "
                f"to worker processes; current signature: {sorted(params)}"
            )

    def test_worker_signature_excludes_python_file_copier(self) -> None:
        params: Set[str] = set(
            inspect.signature(_process_flowgroup_for_generate).parameters.keys()
        )
        # PythonFileCopier holds a threading.Lock and is main-thread-only.
        for forbidden in ("python_copier", "python_file_copier"):
            assert forbidden not in params, (
                f"_process_flowgroup_for_generate must not expose {forbidden!r} "
                f"to worker processes; current signature: {sorted(params)}"
            )


@pytest.mark.unit
class TestPhaseBReplayCallsStateManager:
    """The Phase B replay path is the ONLY caller of state_manager for copied modules.

    Given a :class:`CopiedModuleRecord` produced by a worker, the main
    thread's ``apply_copy_record`` must (a) write the file to disk and
    (b) call ``state_manager.track_generated_file`` exactly once with
    the destination path. The test exercises this directly — no pool,
    no spawn cost — because the replay is plain main-thread Python.
    """

    def test_apply_copy_record_invokes_track_generated_file(
        self, tmp_path: Path
    ) -> None:
        copier = PythonFileCopier()
        custom_dir = tmp_path / "custom_python_functions"

        record = CopiedModuleRecord(
            source_path="user_module.py",
            dest_path=custom_dir / "user_module.py",
            content="# LHP source: user_module.py\nX = 1\n",
            module_path="user_module.py",
            custom_functions_dir=custom_dir,
        )

        state_mgr = MagicMock(spec=StateManager)
        flowgroup = MagicMock()
        flowgroup.flowgroup = "fg_a"
        flowgroup.pipeline = "p1"
        source_yaml = tmp_path / "p1.yaml"
        source_yaml.write_text("# source yaml\n")

        copier.apply_copy_record(
            record,
            state_manager=state_mgr,
            source_yaml=source_yaml,
            flowgroup=flowgroup,
            env="dev",
        )

        # File got written.
        assert (custom_dir / "user_module.py").read_text() == record.content

        # State manager was called for the copied module exactly once.
        # The replay also tracks __init__.py if it was newly created, so
        # we filter to calls whose generated_path matches the record.
        track_calls = [
            call
            for call in state_mgr.track_generated_file.call_args_list
            if call.kwargs.get("generated_path") == record.dest_path
        ]
        assert len(track_calls) == 1, (
            "Phase B replay must call state_manager.track_generated_file "
            f"exactly once for the copied module; got {track_calls}"
        )
        assert track_calls[0].kwargs["flowgroup"] == "fg_a"
        assert track_calls[0].kwargs["pipeline"] == "p1"
        assert track_calls[0].kwargs["environment"] == "dev"
