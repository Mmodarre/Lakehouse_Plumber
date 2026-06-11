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

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli

# Marker embedded in one flowgroup's generated source (via its table name)
# so the wrapper can single it out across the spawn boundary.
_CFG031_MARKER = "cfg031_unparseable_target"


class _Cfg031CodeGenerator:
    """Picklable code-generator wrapper that emits UN-PARSEABLE Python for ONE fg.

    Drives the REAL syntax guard the worker runs in place of the relocated
    formatting pass: :func:`lhp.core.codegen.formatter.assert_generated_python_valid`
    (a plain ``ast.parse``). The worker calls
    ``code_generator.generate_flowgroup_code(...)`` and then validates the
    result; by RETURNING syntactically-invalid source (``def (:``) for the
    marked flowgroup, this makes the REAL guard raise ``LHP-CFG-031`` — the
    guard is NOT mocked, so the honest root-cause path (real ast.parse ->
    real CFG-031 -> worker catch -> failure DTO -> gate -> facade raise) is
    exercised end to end.

    The worker pool runs under ``ProcessPoolExecutor(mp_context="spawn")`` and
    re-imports this module fresh in each child, so the injected collaborator
    must be a TOP-LEVEL (picklable) object shipped on the
    ``_FlowgroupWorkerState`` — a parent-process ``monkeypatch`` would not
    cross the spawn boundary. It wraps the REAL ``CodeGenerationService`` (also
    picklable, already shipped on the state) and delegates verbatim for every
    flowgroup whose generated source lacks :data:`_CFG031_MARKER`, so only the
    one marked flowgroup is corrupted.
    """

    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped

    def generate_flowgroup_code(self, *args, **kwargs) -> str:
        code = self._wrapped.generate_flowgroup_code(*args, **kwargs)
        if _CFG031_MARKER in code:
            # Replace the (valid) generated source with un-parseable Python so
            # the worker's REAL ast.parse guard raises LHP-CFG-031.
            return "def (:\n"
        return code


def _patch_worker_state_with_cfg031_codegen(monkeypatch) -> None:
    """Make the generate worker pool emit un-parseable Python for ONE flowgroup.

    Patches ``ActionOrchestrator._build_generate_worker_state`` to wrap the
    real ``code_generator`` on the (frozen) ``_FlowgroupWorkerState`` in
    :class:`_Cfg031CodeGenerator`. The state — wrapped generator included — is
    what the engine pickles to each spawned worker via ``initializer=``. The
    REAL syntax guard then trips on the corrupted output, so the worker
    surfaces a genuine ``LHP-CFG-031`` rather than an injected one.
    """
    from lhp.core.coordination.orchestrator import ActionOrchestrator

    original = ActionOrchestrator._build_generate_worker_state

    def _patched(self, env, include_tests, **kwargs):
        # ``**kwargs`` forwards builder keywords added after this shim was
        # written (e.g. ``table_renames``) so the wrapper tracks the real
        # signature (§6.2).
        state = original(self, env, include_tests, **kwargs)
        return dataclasses.replace(
            state, code_generator=_Cfg031CodeGenerator(state.code_generator)
        )

    monkeypatch.setattr(ActionOrchestrator, "_build_generate_worker_state", _patched)


def _build_multipipeline_project(project_root: Path, pipeline_names) -> None:
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


def _add_snapshot_cdc_missing_function_flowgroup(
    project_root: Path, pipeline_name: str
) -> None:
    """Add ONE snapshot_cdc flowgroup whose source_function omits ``function``.

    The flowgroup is otherwise well-formed (valid streaming_table target,
    snapshot_cdc mode, keys, scd type) and even references a REAL source
    file on disk — so the ONLY defect is the missing
    ``source_function.function`` key.

    This proves the GENERATE path rejects the missing field at validate-time
    (``SnapshotCdcConfigValidator`` via ``ConfigValidator.validate_flowgroup``,
    run unconditionally in the per-flowgroup worker BEFORE codegen) rather
    than via any generate-side presence guard — the redundant CONFIG/002
    raise that used to live in ``snapshot_cdc_source_function.resolve_source_function``
    was deleted. If the snapshot source-function resolver were ever reached,
    it would be on the codegen branch (which never runs because validation
    fails first), and the file IS present, so a "file not found" IO error
    cannot be confused for the validator error.
    """
    fg_dir = project_root / "pipelines" / pipeline_name
    fg_dir.mkdir(parents=True, exist_ok=True)

    # A real, syntactically valid source file so the ONLY defect is the
    # missing 'function' key (rules out an IO/file-not-found false positive).
    funcs_dir = project_root / "functions"
    funcs_dir.mkdir(parents=True, exist_ok=True)
    (funcs_dir / "snap_funcs.py").write_text(
        "from typing import Optional, Tuple\n"
        "from pyspark.sql import DataFrame\n\n\n"
        "def my_snapshot(latest_version: Optional[int])"
        " -> Optional[Tuple[DataFrame, int]]:\n"
        "    return None\n",
        encoding="utf-8",
    )

    flowgroup = {
        "pipeline": pipeline_name,
        "flowgroup": f"{pipeline_name}_fg",
        "actions": [
            {
                "name": "write_snap",
                "type": "write",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": "snap_target",
                    "create_table": True,
                    "mode": "snapshot_cdc",
                    "snapshot_cdc_config": {
                        "source_function": {
                            # 'function' deliberately omitted; 'file' exists.
                            "file": "functions/snap_funcs.py",
                        },
                        "keys": ["id"],
                        "stored_as_scd_type": 2,
                    },
                },
            },
        ],
    }
    with open(fg_dir / f"{pipeline_name}_fg.yaml", "w") as f:
        yaml.dump(flowgroup, f)


def _py_files_under(output_root: Path):
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
        # ``--show-details`` expands per-pipeline detail; the per-pipeline rows
        # render on a successful run regardless, so each name appears in output.
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "4",
                "--no-bundle",
                "--show-details",
            ],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        for name in self.PIPELINES:
            assert name in result.output, (
                f"Pipeline {name} missing from CLI output:\n{result.output}"
            )

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
            assert par_file.read_bytes() == seq_file.read_bytes(), (
                f"Bytewise diff for {name} between parallel and sequential runs"
            )

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

        assert result.exit_code != 0, (
            f"Expected non-zero exit on a failing flowgroup; got "
            f"{result.exit_code}:\n{result.output}"
        )

        py_files = _py_files_under(output_root)
        assert py_files == [], (
            f"Expected ZERO generated .py files on all-or-nothing failure; "
            f"found: {[str(p) for p in py_files]}"
        )

        assert sentinel.exists(), (
            "A gate failure must NOT wipe the pre-existing output tree; the "
            "sentinel file was deleted."
        )

        assert "p_broken" in result.output, (
            f"Failing pipeline 'p_broken' missing from output:\n{result.output}"
        )
        assert "not_a_real_action_type" in result.output, (
            f"Offending action type missing from error output:\n{result.output}"
        )

    @pytest.mark.unit
    def test_snapshot_cdc_missing_source_function_function_rejected_at_generate(
        self, runner, tmp_path, monkeypatch
    ):
        """GENERATE rejects a snapshot_cdc flowgroup missing source_function.function.

        Safety basis for deleting the redundant CONFIG/002 presence guard in
        ``snapshot_cdc_source_function.resolve_source_function``: the
        snapshot-CDC validator (reached via
        ``ConfigValidator.validate_flowgroup`` → ``WriteActionValidator``)
        runs UNCONDITIONALLY in the per-flowgroup worker
        (``core/coordination/_flowgroup_pool.py`` → ``process_flowgroup``)
        BEFORE the codegen branch, in BOTH ``validate`` and ``generate``
        modes. So a missing ``source_function.function`` is rejected at
        validate-time during ``generate`` and the codegen resolver
        (``resolve_source_function``) is NEVER reached.

        Asserts:
        * non-zero exit (the aggregated validator error fails the gate);
        * the failure is the VALIDATION error (``LHP-VAL-007`` / the
          ``source_function must have 'function'`` validator message), NOT
          the deleted ``LHP-CFG-002`` presence guard;
        * ZERO ``.py`` files written — proving codegen never ran (had it
          run, ``resolve_source_function`` would have been the only other
          place this could fail, and the file IS present on disk).
        """
        project_root = tmp_path
        _build_multipipeline_project(project_root, ["p_alpha", "p_beta"])
        _add_snapshot_cdc_missing_function_flowgroup(project_root, "p_snap_bad")

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            ["generate", "--env", "dev", "--max-workers", "2", "--no-bundle"],
        )

        assert result.exit_code != 0, (
            f"Expected non-zero exit on a snapshot_cdc flowgroup missing "
            f"source_function.function; got {result.exit_code}:\n{result.output}"
        )

        assert "LHP-CFG-002" not in result.output, (
            "Rejection must come from the snapshot-CDC validator, NOT the "
            f"deleted generate-side CONFIG/002 guard:\n{result.output}"
        )
        assert (
            "LHP-VAL-007" in result.output
            or "source_function must have 'function'" in result.output
        ), (
            "Expected the snapshot-CDC validator error (LHP-VAL-007 / "
            f"missing 'function') in output:\n{result.output}"
        )

        output_root = project_root / "generated" / "dev"
        py_files = _py_files_under(output_root)
        assert py_files == [], (
            f"Expected ZERO generated .py files (validation gates before "
            f"codegen); found: {[str(p) for p in py_files]}"
        )

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
        assert seq_result.exit_code == 0, (
            f"max-workers=1 run failed {seq_result.exit_code}: {seq_result.output}"
        )

        monkeypatch.chdir(par_root)
        par_result = runner.invoke(
            cli,
            ["generate", "--env", "dev", "--max-workers", "8", "--no-bundle"],
        )
        assert par_result.exit_code == 0, (
            f"max-workers=8 run failed {par_result.exit_code}: {par_result.output}"
        )

        seq_out = seq_root / "generated" / "dev"
        par_out = par_root / "generated" / "dev"

        # Relative paths so the tmp-dir prefixes don't affect the comparison.
        seq_rel = {p.relative_to(seq_out) for p in _py_files_under(seq_out)}
        par_rel = {p.relative_to(par_out) for p in _py_files_under(par_out)}
        assert seq_rel, "Expected generated .py files; found none for max-workers=1"
        assert seq_rel == par_rel, (
            "File set differs between --max-workers 1 and 8:\n"
            f"  only in w1: {sorted(str(p) for p in seq_rel - par_rel)}\n"
            f"  only in w8: {sorted(str(p) for p in par_rel - seq_rel)}"
        )

        for rel in sorted(seq_rel, key=str):
            seq_bytes = (seq_out / rel).read_bytes()
            par_bytes = (par_out / rel).read_bytes()
            assert seq_bytes == par_bytes, (
                f"Bytewise diff for {rel} between --max-workers 1 and 8"
            )

    def test_N4_unparseable_generated_python_cfg031_aborts_with_zero_files(
        self, runner, tmp_path, monkeypatch
    ):
        """N4: a flowgroup whose generated Python does not parse aborts the run.

        When ONE flowgroup's generated source fails the worker's ``ast.parse``
        syntax guard, the worker surfaces ``LHP-CFG-031`` as a
        ``FlowgroupOutcome`` failure (workers never raise), the all-or-nothing
        gate aggregates it, and the run aborts with ZERO files written. The
        other (valid) pipeline must NOT be committed.

        ``LHP-CFG-031`` by design only fires on an actual LHP generator/template
        bug — config seams sanitize user text before it enters generated source.
        So the test corrupts the GENERATOR OUTPUT (not the guard):
        :class:`_Cfg031CodeGenerator` wraps the real ``CodeGenerationService``
        on the worker state and returns un-parseable Python for the one marked
        flowgroup, so the REAL guard raises a REAL ``LHP-CFG-031``.
        """
        project_root = tmp_path
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
                        # Marker leaks into the generated source -> the wrapper
                        # corrupts this fg's output -> the real guard raises 031.
                        "table": _CFG031_MARKER,
                        "create_table": True,
                    },
                },
            ],
        }
        with open(bad_dir / "p_cfg031_fg.yaml", "w") as f:
            yaml.dump(bad_fg, f)

        _patch_worker_state_with_cfg031_codegen(monkeypatch)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            ["generate", "--env", "dev", "--max-workers", "2", "--no-bundle"],
        )

        assert result.exit_code != 0, (
            f"Expected non-zero exit on LHP-CFG-031; got {result.exit_code}:\n"
            f"{result.output}"
        )
        assert "LHP-CFG-031" in result.output, (
            f"Expected LHP-CFG-031 in error output:\n{result.output}"
        )
        output_root = project_root / "generated" / "dev"
        py_files = _py_files_under(output_root)
        assert py_files == [], (
            f"Expected ZERO generated .py files on a syntax-guard abort; "
            f"found: {[str(p) for p in py_files]}"
        )
