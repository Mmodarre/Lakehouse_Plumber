"""End-to-end tests for the parallel ``lhp generate`` flow.

Covers the path from the Click CLI down to the orchestrator and verifies:

  * ``--max-workers N`` is accepted and propagated.
  * Per-pipeline ``✅`` lines fire via the ``on_pipeline_complete`` callback
    on the main thread (``capsys`` sees them in click.echo output).
  * Generated file content is byte-identical between ``--max-workers 1``
    (sequential) and ``--max-workers 4`` (parallel).
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Optional

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli
from lhp.core.codegen.formatter import CodeFormatter
from lhp.errors import ErrorCategory, LHPConfigError

# Marker embedded in one flowgroup's generated source (via its table name)
# so the N4 fallback formatter can single it out across the spawn boundary.
_CFG031_MARKER = "cfg031_unparseable_target"


class _Cfg031Formatter(CodeFormatter):
    """Picklable ``CodeFormatter`` that injects an ``LHP-CFG-031`` for ONE flowgroup.

    Used by N4 as the documented fallback (see the test docstring). The
    worker pool runs under ``ProcessPoolExecutor(mp_context="spawn")`` and
    re-imports this module fresh in each child, so the injected formatter
    must be a TOP-LEVEL (picklable) class shipped on the
    ``_FlowgroupWorkerState`` — a parent-process ``monkeypatch`` would not
    cross the spawn boundary. ``format_code`` raises the real
    :class:`LHPConfigError` ``031`` (identical category/code/title to
    :meth:`CodeFormatter.format_code`'s ``InvalidInput`` arm) only when the
    generated source contains :data:`_CFG031_MARKER`; every other
    flowgroup formats normally, so this isolates a single Black-unparseable
    flowgroup while exercising the REAL gate + facade + CLI path.
    """

    def format_code(self, code: str, line_length: Optional[int] = None) -> str:
        if _CFG031_MARKER in code:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="031",
                title="Generated source failed Black parsing",
                details=(
                    "Injected Black-unparseable generated source for one "
                    "flowgroup (N4 documented fallback)."
                ),
            )
        return super().format_code(code, line_length)


def _patch_worker_state_with_cfg031_formatter(monkeypatch) -> None:
    """Make the generate worker pool use :class:`_Cfg031Formatter`.

    Patches ``ActionOrchestrator._build_generate_worker_state`` to swap the
    real formatter on the (frozen) ``_FlowgroupWorkerState`` for the
    ``031``-injecting one. The state — formatter included — is what the
    engine pickles to each spawned worker via ``initializer=``.
    """
    from lhp.core.coordination.orchestrator import ActionOrchestrator

    original = ActionOrchestrator._build_generate_worker_state

    def _patched(self, env, include_tests):
        state = original(self, env, include_tests)
        return dataclasses.replace(state, formatter=_Cfg031Formatter())

    monkeypatch.setattr(ActionOrchestrator, "_build_generate_worker_state", _patched)


def _build_multipipeline_project(project_root: Path, pipeline_names) -> None:
    """Project with one small flowgroup per pipeline covering load + transform + write."""
    (project_root / "presets").mkdir(parents=True, exist_ok=True)
    (project_root / "templates").mkdir(parents=True, exist_ok=True)
    (project_root / "substitutions").mkdir(parents=True, exist_ok=True)
    for name in pipeline_names:
        (project_root / "pipelines" / name).mkdir(parents=True, exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        "name: test_parallel_project\nversion: '1.0'\n"
    )

    subs = {
        "dev": {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
        }
    }
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(subs, f)

    for name in pipeline_names:
        flowgroup = {
            "pipeline": name,
            "flowgroup": f"{name}_fg",
            "actions": [
                {
                    "name": f"load_{name}",
                    "type": "load",
                    "target": f"v_{name}_raw",
                    "source": {
                        "type": "cloudfiles",
                        "path": "${landing_path}/" + name,
                        "format": "json",
                    },
                },
                {
                    "name": f"clean_{name}",
                    "type": "transform",
                    "transform_type": "sql",
                    "source": f"v_{name}_raw",
                    "target": f"v_{name}_clean",
                    "sql": f"SELECT * FROM v_{name}_raw",
                },
                {
                    "name": f"write_{name}",
                    "type": "write",
                    "source": f"v_{name}_clean",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "${catalog}",
                        "schema": "${bronze_schema}",
                        "table": name,
                        "create_table": True,
                    },
                },
            ],
        }
        with open(project_root / "pipelines" / name / f"{name}_fg.yaml", "w") as f:
            yaml.dump(flowgroup, f)


def _add_invalid_flowgroup(project_root: Path, pipeline_name: str) -> None:
    """Add ONE flowgroup whose action uses an unknown ``type``.

    An unknown action type (``not_a_real_action_type``) is rejected by the
    per-flowgroup validation that runs inside the generate engine, so the
    all-or-nothing gate aggregates it and aborts the
    whole run before any write. Used to prove that one bad flowgroup among
    several valid ones yields zero output.
    """
    bad_dir = project_root / "pipelines" / pipeline_name
    bad_dir.mkdir(parents=True, exist_ok=True)
    flowgroup = {
        "pipeline": pipeline_name,
        "flowgroup": f"{pipeline_name}_fg",
        "actions": [
            {
                "name": "broken_action",
                "type": "not_a_real_action_type",
                "source": "v_nope",
                "target": "v_broken",
            },
        ],
    }
    with open(bad_dir / f"{pipeline_name}_fg.yaml", "w") as f:
        yaml.dump(flowgroup, f)


def _py_files_under(output_root: Path):
    """All generated ``.py`` files under an env output dir (empty if absent)."""
    if not output_root.exists():
        return []
    return sorted(output_root.rglob("*.py"))


class TestGenerateCommandParallel:
    PIPELINES = ["p_alpha", "p_beta", "p_gamma"]

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_max_workers_flag_accepted_with_4_workers(
        self, runner, tmp_path, monkeypatch
    ):
        project_root = tmp_path
        _build_multipipeline_project(project_root, self.PIPELINES)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "4",
                "--no-bundle",
            ],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        for name in self.PIPELINES:
            assert (
                project_root / "generated" / "dev" / name / f"{name}_fg.py"
            ).exists(), f"Expected file missing for {name}"

    def test_per_pipeline_completion_line_per_pipeline(
        self, runner, tmp_path, monkeypatch
    ):
        project_root = tmp_path
        _build_multipipeline_project(project_root, self.PIPELINES)

        monkeypatch.chdir(project_root)
        # ``--show-all`` opts into the full per-pipeline summary table;
        # the failures-only default would suppress the table on a
        # successful run, but this test asserts on per-pipeline row
        # visibility.
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "4",
                "--no-bundle",
                "--show-all",
            ],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        for name in self.PIPELINES:
            assert (
                name in result.output
            ), f"Pipeline {name} missing from CLI output:\n{result.output}"

    def test_parallel_output_byte_identical_to_sequential(
        self, runner, tmp_path, monkeypatch
    ):
        """``--max-workers 1`` and ``--max-workers 4`` produce identical files.

        Relies on generator output being fully deterministic. If a future
        generator change introduces non-determinism, fix the generator —
        do not silence this test.
        """
        par_root = tmp_path / "par"
        seq_root = tmp_path / "seq"
        par_root.mkdir()
        seq_root.mkdir()
        _build_multipipeline_project(par_root, self.PIPELINES)
        _build_multipipeline_project(seq_root, self.PIPELINES)

        monkeypatch.chdir(par_root)
        par_result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "4",
                "--no-bundle",
            ],
        )
        assert par_result.exit_code == 0

        monkeypatch.chdir(seq_root)
        seq_result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "1",
                "--no-bundle",
            ],
        )
        assert seq_result.exit_code == 0

        for name in self.PIPELINES:
            par_file = par_root / "generated" / "dev" / name / f"{name}_fg.py"
            seq_file = seq_root / "generated" / "dev" / name / f"{name}_fg.py"
            assert (
                par_file.read_bytes() == seq_file.read_bytes()
            ), f"Bytewise diff for {name} between parallel and sequential runs"

    def test_N1_one_failing_flowgroup_writes_zero_files_and_lists_failure(
        self, runner, tmp_path, monkeypatch
    ):
        """N1: all-or-nothing on a single bad flowgroup.

        A project with several VALID flowgroups plus ONE invalid one (unknown
        action type) must:

        * exit non-zero (the gate raises the aggregated ``LHPError``),
        * write ZERO ``.py`` files under ``generated/<env>/`` — proving no
          partial output (the valid pipelines are NOT committed), and
        * leave any pre-existing output tree UNTOUCHED — the whole-env wipe
          now runs only AFTER the gate passes (``_commit.
          _wipe_env_output_dir``), so a sentinel seeded before the run
          survives a gate failure,
        * name the failing flowgroup / its error in the CLI output.
        """
        project_root = tmp_path
        _build_multipipeline_project(project_root, self.PIPELINES)
        _add_invalid_flowgroup(project_root, "p_broken")

        # Seed a sentinel into generated/dev to prove the failed run does NOT
        # wipe a pre-existing output tree (all-or-nothing leaves it untouched).
        output_root = project_root / "generated" / "dev"
        output_root.mkdir(parents=True, exist_ok=True)
        sentinel = output_root / "PRIOR_OUTPUT.sentinel"
        sentinel.write_text("prior output that must survive a failed generate")

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "4",
                "--no-bundle",
            ],
        )

        # Non-zero exit: the gate raised; the CLI fail-fast boundary mapped
        # it to a non-zero code.
        assert result.exit_code != 0, (
            f"Expected non-zero exit on a failing flowgroup; got "
            f"{result.exit_code}:\n{result.output}"
        )

        # Zero .py written: no partial output for the valid pipelines.
        py_files = _py_files_under(output_root)
        assert py_files == [], (
            f"Expected ZERO generated .py files on all-or-nothing failure; "
            f"found: {[str(p) for p in py_files]}"
        )

        # The pre-existing tree was left untouched (no post-gate wipe ran).
        assert sentinel.exists(), (
            "A gate failure must NOT wipe the pre-existing output tree; the "
            "sentinel file was deleted."
        )

        # The failure is surfaced: the bad pipeline and its unknown action
        # type both appear in the rendered error output.
        assert (
            "p_broken" in result.output
        ), f"Failing pipeline 'p_broken' missing from output:\n{result.output}"
        assert (
            "not_a_real_action_type" in result.output
        ), f"Offending action type missing from error output:\n{result.output}"

    def test_N2_max_workers_1_vs_8_byte_identical_tree(
        self, runner, tmp_path, monkeypatch
    ):
        """N2: ``--max-workers 1`` vs ``8`` parity.

        Two temp copies of the same all-valid project generated with
        ``--max-workers 1`` (sequential) and ``--max-workers 8`` (parallel)
        must produce BYTE-FOR-BYTE identical output trees: identical RELATIVE
        file sets AND identical file contents. Relies on the generator being
        fully deterministic and on the flat-per-flowgroup engine producing
        order-independent output. If a future change introduces
        non-determinism, fix the generator — do not weaken this test.
        """
        seq_root = tmp_path / "w1"
        par_root = tmp_path / "w8"
        seq_root.mkdir()
        par_root.mkdir()
        _build_multipipeline_project(seq_root, self.PIPELINES)
        _build_multipipeline_project(par_root, self.PIPELINES)

        monkeypatch.chdir(seq_root)
        seq_result = runner.invoke(
            cli,
            ["generate", "--env", "dev", "--max-workers", "1", "--no-bundle"],
        )
        assert (
            seq_result.exit_code == 0
        ), f"max-workers=1 run failed {seq_result.exit_code}: {seq_result.output}"

        monkeypatch.chdir(par_root)
        par_result = runner.invoke(
            cli,
            ["generate", "--env", "dev", "--max-workers", "8", "--no-bundle"],
        )
        assert (
            par_result.exit_code == 0
        ), f"max-workers=8 run failed {par_result.exit_code}: {par_result.output}"

        seq_out = seq_root / "generated" / "dev"
        par_out = par_root / "generated" / "dev"

        # 1) Identical RELATIVE file sets (compare paths relative to each
        #    env output root so the tmp prefixes don't matter).
        seq_rel = {p.relative_to(seq_out) for p in _py_files_under(seq_out)}
        par_rel = {p.relative_to(par_out) for p in _py_files_under(par_out)}
        assert seq_rel, "Expected generated .py files; found none for max-workers=1"
        assert seq_rel == par_rel, (
            "File set differs between --max-workers 1 and 8:\n"
            f"  only in w1: {sorted(str(p) for p in seq_rel - par_rel)}\n"
            f"  only in w8: {sorted(str(p) for p in par_rel - seq_rel)}"
        )

        # 2) Byte-identical contents for every file in the set.
        for rel in sorted(seq_rel, key=str):
            seq_bytes = (seq_out / rel).read_bytes()
            par_bytes = (par_out / rel).read_bytes()
            assert (
                seq_bytes == par_bytes
            ), f"Bytewise diff for {rel} between --max-workers 1 and 8"

    def test_N4_black_unparseable_cfg031_aborts_with_zero_files(
        self, runner, tmp_path, monkeypatch
    ):
        """N4: a Black-unparseable flowgroup aborts the run.

        When ONE flowgroup's generated source fails Black parsing, the worker
        surfaces ``LHP-CFG-031`` as a ``FlowgroupOutcome`` failure (workers
        never raise), the all-or-nothing gate aggregates it, and the
        run aborts with ZERO files written. The other (valid)
        pipeline must NOT be committed.

        FALLBACK (documented): this injects the ``LHP-CFG-031`` rather than
        provoking it through config. A genuine Black-unparseable GENERATION
        could not be constructed via config on this branch — the seams that
        embed user text into GENERATED (Black-formatted) source all sanitize
        it: ``sql`` transform bodies are escaped (verified: a ``\"\"\"``-/
        backslash-/unbalanced-paren payload still produces valid, formatted
        Python), and custom-Python (``module_path``) / snapshot-CDC
        (``source_function``) modules are COPIED verbatim and AST-validated,
        never run through ``CodeFormatter.format_code``. ``LHP-CFG-031`` by
        design only fires on an actual LHP generator/template bug. So the
        fallback injects a real :class:`LHPConfigError` ``031`` from a
        picklable formatter shipped on the worker state (see
        :func:`_patch_worker_state_with_cfg031_formatter`) — the gate, the
        facade ``ErrorEmitted``+raise rendezvous, and the no-write contract
        are all exercised for real; only the Black failure itself is injected.
        """
        project_root = tmp_path
        # Two valid pipelines; the second's table name carries the marker so
        # ONLY its generated source trips the injected 031.
        _build_multipipeline_project(project_root, ["p_ok"])
        bad_dir = project_root / "pipelines" / "p_cfg031"
        bad_dir.mkdir(parents=True)
        bad_fg = {
            "pipeline": "p_cfg031",
            "flowgroup": "p_cfg031_fg",
            "actions": [
                {
                    "name": "load_p_cfg031",
                    "type": "load",
                    "target": "v_p_cfg031_raw",
                    "source": {
                        "type": "cloudfiles",
                        "path": "${landing_path}/p_cfg031",
                        "format": "json",
                    },
                },
                {
                    "name": "write_p_cfg031",
                    "type": "write",
                    "source": "v_p_cfg031_raw",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "${catalog}",
                        "schema": "${bronze_schema}",
                        # Marker leaks into the generated source -> the
                        # injected formatter raises LHP-CFG-031 for this fg.
                        "table": _CFG031_MARKER,
                        "create_table": True,
                    },
                },
            ],
        }
        with open(bad_dir / "p_cfg031_fg.yaml", "w") as f:
            yaml.dump(bad_fg, f)

        _patch_worker_state_with_cfg031_formatter(monkeypatch)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            ["generate", "--env", "dev", "--max-workers", "2", "--no-bundle"],
        )

        assert result.exit_code != 0, (
            f"Expected non-zero exit on LHP-CFG-031; got {result.exit_code}:\n"
            f"{result.output}"
        )
        assert (
            "LHP-CFG-031" in result.output
        ), f"Expected LHP-CFG-031 in error output:\n{result.output}"
        output_root = project_root / "generated" / "dev"
        py_files = _py_files_under(output_root)
        assert py_files == [], (
            f"Expected ZERO generated .py files on a Black-parse abort; "
            f"found: {[str(p) for p in py_files]}"
        )


# NOTE: the per-pipeline-start submission-order test was RETIRED here. It tested
# the deleted pipeline-batched pool runner firing a per-pipeline start callback
# once per pipeline in submission order. Stage-2's flat engine fans out per
# FLOWGROUP (no per-pipeline submission order), so the contract under test no
# longer exists; the provisional per-pipeline start callback was subsequently
# removed end-to-end (facade/orchestrator/executor/CLI).
