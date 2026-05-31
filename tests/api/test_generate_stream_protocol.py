"""§3bis.3 stream-protocol guard for the all-or-nothing generate gate.

This is the dedicated test the parallel-execution consolidation spec
(`LOCAL/parallel_execution_consolidation_spec.md` §3bis.3) calls out as the
single highest-risk item: the global generate GATE's aggregated failure must
surface as **exactly one** :class:`~lhp.api.ErrorEmitted` followed by a
``raise`` of the aggregate :class:`~lhp.errors.LHPError`, emitted in the OUTER
``generate_pipelines`` stream wrapper — *outside* ``_do_generate_pipelines``'s
graceful-DTO ``except Exception -> return failure`` body. If the gate raise were
swallowed into a DTO there, no ``ErrorEmitted`` would fire and the §1.4 / §9.19
event protocol would silently break.

Two failure classes are exercised, because they take DIFFERENT surfacing paths
and the spec's §3bis.3 risk is specifically about the GATE path:

* **Gate-aggregate failure (the §3bis.3 path).** A flowgroup that PARSES
  cleanly (valid action types) but fails per-flowgroup *validation* in the
  worker — here ``LHP-CFG-004`` "Multiple table creators detected" — reaches the
  all-or-nothing gate, which raises through ``_do_generate_pipelines``'s
  ``except LHPError`` and into the ``generate_pipelines`` rendezvous. THIS is
  what must yield exactly one ``ErrorEmitted`` then raise.
* **Non-LHP commit failure.** An ``OSError`` from a
  commit-time disk write AFTER the gate has passed degrades to a batch-failure
  DTO (``BatchGenerationResponse.success is False``) — NOT an ``ErrorEmitted`` /
  raise, and NOT a silent success. This is the documented, intentional
  trade-off (a partial tree may remain); it is the counter-case that proves the
  ``ErrorEmitted``+raise rendezvous is reserved for ``LHPError`` only.

Tests import strictly from :mod:`lhp.api` and :mod:`lhp.errors` — no internal
modules — except the single ``_commit.write_normalized`` patch target for the
OSError injection (the lowest-level commit-time disk write, reached only on the
gate-passed path), which is necessarily an implementation seam.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lhp.api import (
    BatchGenerationResponse,
    ErrorEmitted,
    GenerationCompleted,
    LakehousePlumberApplicationFacade,
    OperationStarted,
    collect_response,
)
from lhp.errors import LHPError


def _write_project(project_root: Path, *, include_gate_failure: bool) -> None:
    """Write a minimal project: one valid pipeline (+ one gate-failing one).

    The valid ``p_ok`` flowgroup is a load+write streaming-table that resolves,
    codegens and formats cleanly. When ``include_gate_failure`` is set, a second
    pipeline ``p_bad`` declares two ``create_table: true`` writes to the SAME
    table — valid action types (so it passes YAML parsing and discovery) but a
    per-flowgroup validation failure (``LHP-CFG-004``) inside the engine, which
    the all-or-nothing gate aggregates and raises.
    """
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text(
        "name: stream_proto_project\nversion: '1.0'\n"
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

    ok_dir = project_root / "pipelines" / "p_ok"
    ok_dir.mkdir(parents=True, exist_ok=True)
    with open(ok_dir / "p_ok_fg.yaml", "w") as f:
        yaml.dump(
            {
                "pipeline": "p_ok",
                "flowgroup": "p_ok_fg",
                "actions": [
                    {
                        "name": "load_ok",
                        "type": "load",
                        "target": "v_ok_raw",
                        "source": {
                            "type": "cloudfiles",
                            "path": "${landing_path}/ok",
                            "format": "json",
                        },
                    },
                    {
                        "name": "write_ok",
                        "type": "write",
                        "source": "v_ok_raw",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "${catalog}",
                            "schema": "${bronze_schema}",
                            "table": "ok_table",
                            "create_table": True,
                        },
                    },
                ],
            },
            f,
        )

    if not include_gate_failure:
        return

    bad_dir = project_root / "pipelines" / "p_bad"
    bad_dir.mkdir(parents=True, exist_ok=True)
    with open(bad_dir / "p_bad_fg.yaml", "w") as f:
        yaml.dump(
            {
                "pipeline": "p_bad",
                "flowgroup": "p_bad_fg",
                "actions": [
                    {
                        "name": "load_bad",
                        "type": "load",
                        "target": "v_bad_raw",
                        "source": {
                            "type": "cloudfiles",
                            "path": "${landing_path}/bad",
                            "format": "json",
                        },
                    },
                    {
                        "name": "write_bad_a",
                        "type": "write",
                        "source": "v_bad_raw",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "${catalog}",
                            "schema": "${bronze_schema}",
                            "table": "dup_table",
                            "create_table": True,
                        },
                    },
                    {
                        "name": "write_bad_b",
                        "type": "write",
                        "source": "v_bad_raw",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "${catalog}",
                            "schema": "${bronze_schema}",
                            "table": "dup_table",
                            "create_table": True,
                        },
                    },
                ],
            },
            f,
        )


@pytest.fixture
def gate_failure_project(tmp_path: Path):
    """A facade + output dir over a project whose gate FAILS (``LHP-CFG-004``)."""
    project_root = tmp_path / "proj"
    project_root.mkdir()
    _write_project(project_root, include_gate_failure=True)
    output_dir = project_root / "generated" / "dev"
    original_cwd = os.getcwd()
    os.chdir(project_root)
    try:
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        yield facade, project_root, output_dir
    finally:
        os.chdir(original_cwd)


@pytest.fixture
def all_valid_project(tmp_path: Path):
    """A facade + output dir over an all-valid project (gate PASSES)."""
    project_root = tmp_path / "proj"
    project_root.mkdir()
    _write_project(project_root, include_gate_failure=False)
    output_dir = project_root / "generated" / "dev"
    original_cwd = os.getcwd()
    os.chdir(project_root)
    try:
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        yield facade, project_root, output_dir
    finally:
        os.chdir(original_cwd)


def _py_files(output_dir: Path):
    return sorted(output_dir.rglob("*.py")) if output_dir.exists() else []


@pytest.mark.integration
class TestGenerateGateStreamProtocol:
    """§3bis.3: the gate aggregate yields exactly one ``ErrorEmitted`` then raises."""

    def test_gate_failure_emits_exactly_one_error_then_raises(
        self, gate_failure_project
    ):
        """The §3bis.3 guard: drive ``generate_pipelines`` on a gate-failing
        config; assert the stream yields EXACTLY ONE ``ErrorEmitted`` and that
        iterating to completion RAISES the same ``LHPError`` — and that no
        terminal ``GenerationCompleted`` and no output files appear.
        """
        facade, _project_root, output_dir = gate_failure_project

        collected: list = []
        gen = facade.generation.generate_pipelines(
            pipeline_fields=["p_ok", "p_bad"],
            env="dev",
            output_dir=output_dir,
        )
        with pytest.raises(LHPError) as exc_info:
            for event in gen:
                collected.append(event)

        # Exactly one ErrorEmitted, and it is the final event before the raise.
        error_events = [e for e in collected if isinstance(e, ErrorEmitted)]
        assert len(error_events) == 1, (
            f"Expected exactly one ErrorEmitted; got {len(error_events)} in "
            f"{[type(e).__name__ for e in collected]}"
        )
        assert isinstance(collected[-1], ErrorEmitted)

        # The emitted event carries the SAME live error instance that is raised
        # (the failure-rendezvous carve-out, §9.21).
        assert error_events[0].lhp_error is exc_info.value

        # This is the gate aggregate for the multiple-table-creators failure.
        assert exc_info.value.code == "LHP-CFG-004"

        # No terminal Completed on the failure path; first event is the start.
        assert isinstance(collected[0], OperationStarted)
        assert not any(isinstance(e, GenerationCompleted) for e in collected)

        # All-or-nothing: zero files written (gate precedes any commit/wipe).
        assert _py_files(output_dir) == [], (
            f"Gate failure must write zero files; found "
            f"{[str(p) for p in _py_files(output_dir)]}"
        )

    def test_collect_response_reraises_gate_failure(self, gate_failure_project):
        """``collect_response`` walking the same failing stream RE-RAISES the
        gate aggregate (it does not swallow it into a returned DTO).
        """
        facade, _project_root, output_dir = gate_failure_project
        with pytest.raises(LHPError) as exc_info:
            collect_response(
                facade.generation.generate_pipelines(
                    pipeline_fields=["p_ok", "p_bad"],
                    env="dev",
                    output_dir=output_dir,
                )
            )
        assert exc_info.value.code == "LHP-CFG-004"
        assert _py_files(output_dir) == []


@pytest.mark.integration
class TestGenerateCommitFailureOptionB:
    """A non-LHP commit failure is a DTO, never a raise."""

    def test_commit_oserror_surfaces_as_batch_failure_dto_not_raise(
        self, all_valid_project, monkeypatch
    ):
        """An ``OSError`` from the commit-time disk write (AFTER the gate has
        passed) must surface as a batch-failure DTO — NOT via ``ErrorEmitted`` /
        raise, and NOT as a silent success.

        The patch targets the lowest-level commit write
        (``_commit.write_normalized``, reached only on the gate-passed commit
        path). Per the pinned contract this is routed through
        ``_do_generate_pipelines``'s ``except Exception -> return failure DTO``
        arm (reserved for non-``LHPError`` infra failures), so the stream still
        terminates with a ``GenerationCompleted`` whose
        ``BatchGenerationResponse.success`` is ``False`` and whose error info is
        populated — never an ``ErrorEmitted`` and never an ``LHPError`` raise.
        """
        import lhp.core.coordination._commit as commit_mod

        facade, _project_root, output_dir = all_valid_project

        def _boom_write(*_args, **_kwargs):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(commit_mod, "write_normalized", _boom_write)

        collected: list = []
        gen = facade.generation.generate_pipelines(
            pipeline_fields=["p_ok"],
            env="dev",
            output_dir=output_dir,
        )
        # Iterating to completion must NOT raise an LHPError for a non-LHP
        # commit failure. Any LHPError here would be a contract
        # regression, so let it propagate (fail loudly) rather than catch it.
        for event in gen:
            collected.append(event)

        # No ErrorEmitted was yielded for the OSError (the rendezvous is
        # reserved for LHPError).
        assert not any(isinstance(e, ErrorEmitted) for e in collected), (
            "A non-LHP commit failure must NOT emit ErrorEmitted; got "
            f"{[type(e).__name__ for e in collected]}"
        )

        # The stream still terminates with a GenerationCompleted carrying the
        # failed batch DTO.
        completed = [e for e in collected if isinstance(e, GenerationCompleted)]
        assert len(completed) == 1, (
            f"Expected one terminal GenerationCompleted; got "
            f"{[type(e).__name__ for e in collected]}"
        )
        response = completed[-1].response
        assert isinstance(response, BatchGenerationResponse)

        # Not a silent success: success is explicitly False with error info.
        assert response.success is False
        assert not response.is_successful()
        assert "No space left on device" in (response.error_message or ""), (
            f"Expected the OSError text in error_message; got "
            f"{response.error_message!r}"
        )
        # error_code is None: a plain OSError has no LHP ``.code`` — confirming
        # this did NOT travel the LHP-coded failure channel.
        assert response.error_code is None

    def test_collect_response_returns_failed_dto_on_commit_oserror(
        self, all_valid_project, monkeypatch
    ):
        """``collect_response`` returns the failed batch DTO (does NOT raise)
        for the non-LHP commit OSError — the Option-B counterpart of the gate
        re-raise above.
        """
        import lhp.core.coordination._commit as commit_mod

        facade, _project_root, output_dir = all_valid_project

        def _boom_write(*_args, **_kwargs):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(commit_mod, "write_normalized", _boom_write)

        response = collect_response(
            facade.generation.generate_pipelines(
                pipeline_fields=["p_ok"],
                env="dev",
                output_dir=output_dir,
            )
        )
        assert isinstance(response, BatchGenerationResponse)
        assert response.success is False
        assert "No space left on device" in (response.error_message or "")
