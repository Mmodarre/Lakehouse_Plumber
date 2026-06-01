"""End-to-end tests for the ``--no-format`` / ``apply_formatting`` capability.

Covers the path from the Click CLI down to the terminal ruff-format pass and
the ``lhp.yaml`` ``apply_formatting`` key, verifying:

  * ``--no-format`` skips the terminal formatting pass (the generated file is
    left UN-formatted) while the in-worker ``ast.parse`` validity guard still
    fires (a flowgroup that generates invalid Python still raises
    ``LHP-CFG-031``).
  * The default run (no flag) still formats (``apply_formatting`` defaults
    ``True``).
  * The ``lhp.yaml`` ``apply_formatting: false`` key is honored when no flag is
    given, and ``--no-format`` overrides ``apply_formatting: true`` on the CLI.

Detection of "was the terminal ruff-format pass applied?" reuses the spawn-safe
collaborator-injection pattern from ``test_generate_command_parallel`` (the
worker pool runs under ``ProcessPoolExecutor(mp_context="spawn")``, so the
injected wrapper must be a TOP-LEVEL picklable object shipped on the
``_FlowgroupWorkerState`` — a parent ``monkeypatch`` cannot cross spawn). Here
the wrapper returns VALID but deliberately badly-formatted Python so that ruff
WOULD reformat it: the default path reformats it (clean), ``--no-format`` leaves
it verbatim, and the two outputs differ — proving the gate is wired without
depending on whatever the real generator happens to emit.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli
from tests.test_generate_command_parallel import (
    _CFG031_MARKER,
    _build_multipipeline_project,
    _patch_worker_state_with_cfg031_codegen,
    _py_files_under,
)

# Valid-but-unformatted Python that ruff reformats (no spaces around ``=`` /
# in the signature). ``ast.parse`` accepts it, so the CFG-031 guard passes; the
# terminal ruff-format pass would rewrite it, so its presence verbatim proves
# the pass was SKIPPED.
_UNFORMATTED_SOURCE = "x=1\ndef  f( a ,b ):\n    return  a+b\n"


class _UnformattedCodeGenerator:
    """Picklable wrapper emitting VALID but un-formatted Python for ONE fg.

    Mirrors ``_Cfg031CodeGenerator`` but returns syntactically-valid source
    that ruff WOULD reformat (so the ``ast.parse`` guard passes). Only the
    flowgroup whose generated source carries :data:`_CFG031_MARKER` is
    replaced; every other flowgroup delegates verbatim. Shipped on the
    ``_FlowgroupWorkerState`` so it survives the spawn boundary.
    """

    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped

    def generate_flowgroup_code(self, *args, **kwargs) -> str:
        code = self._wrapped.generate_flowgroup_code(*args, **kwargs)
        if _CFG031_MARKER in code:
            return _UNFORMATTED_SOURCE
        return code


def _patch_worker_state_with_unformatted_codegen(monkeypatch) -> None:
    """Make the generate worker emit valid-but-unformatted Python for ONE fg.

    Wraps the real ``code_generator`` on the (frozen) ``_FlowgroupWorkerState``
    in :class:`_UnformattedCodeGenerator`, exactly as
    :func:`_patch_worker_state_with_cfg031_codegen` does for the invalid-source
    case. The state — wrapped generator included — is what the engine pickles to
    each spawned worker.
    """
    from lhp.core.coordination.orchestrator import ActionOrchestrator

    original = ActionOrchestrator._build_generate_worker_state

    def _patched(self, env, include_tests):
        state = original(self, env, include_tests)
        return dataclasses.replace(
            state, code_generator=_UnformattedCodeGenerator(state.code_generator)
        )

    monkeypatch.setattr(ActionOrchestrator, "_build_generate_worker_state", _patched)


def _build_marked_unformatted_project(project_root: Path) -> None:
    """One pipeline whose generated source is corrupted into unformatted Python.

    Reuses :func:`_build_multipipeline_project` for the project scaffold, then
    adds a flowgroup whose write-target table name carries
    :data:`_CFG031_MARKER`, so :class:`_UnformattedCodeGenerator` singles it out
    and replaces its generated source with :data:`_UNFORMATTED_SOURCE`.
    """
    _build_multipipeline_project(project_root, [])
    fg_dir = project_root / "pipelines" / "p_unfmt"
    fg_dir.mkdir(parents=True)
    fg = {
        "pipeline": "p_unfmt",
        "flowgroup": "p_unfmt_fg",
        "actions": [
            {
                "name": "load_p_unfmt",
                "type": "load",
                "target": "v_p_unfmt_raw",
                "source": {
                    "type": "cloudfiles",
                    "path": "${landing_path}/p_unfmt",
                    "format": "json",
                },
            },
            {
                "name": "write_p_unfmt",
                "type": "write",
                "source": "v_p_unfmt_raw",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": _CFG031_MARKER,
                    "create_table": True,
                },
            },
        ],
    }
    with open(fg_dir / "p_unfmt_fg.yaml", "w") as f:
        yaml.dump(fg, f)


def _set_apply_formatting_key(project_root: Path, value: bool) -> None:
    """Rewrite ``lhp.yaml`` to carry an explicit ``apply_formatting`` key."""
    (project_root / "lhp.yaml").write_text(
        f"name: test_no_format_project\nversion: '1.0'\napply_formatting: {str(value).lower()}\n"
    )


def _generated_file(project_root: Path) -> Path:
    return project_root / "generated" / "dev" / "p_unfmt" / "p_unfmt_fg.py"


@pytest.mark.unit
class TestGenerateNoFormat:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_default_run_formats_generated_output(self, runner, tmp_path, monkeypatch):
        """(b) Default run (no flag) formats: the injected unformatted source is
        rewritten by the terminal ruff-format pass (``apply_formatting`` defaults True).
        """
        project_root = tmp_path
        _build_marked_unformatted_project(project_root)
        _patch_worker_state_with_unformatted_codegen(monkeypatch)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli, ["generate", "--env", "dev", "--max-workers", "1", "--no-bundle"]
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        content = _generated_file(project_root).read_text()
        # ruff normalizes ``x=1`` -> ``x = 1``; the verbatim unformatted source
        # must NOT survive a default (formatting) run.
        assert _UNFORMATTED_SOURCE not in content, (
            "Default run left the injected source UN-formatted; the terminal "
            f"ruff-format pass did not run.\n{content!r}"
        )
        assert (
            "x = 1" in content
        ), f"Expected ruff-formatted ``x = 1`` after a default run:\n{content!r}"

    def test_no_format_skips_formatting(self, runner, tmp_path, monkeypatch):
        """(a) ``--no-format`` skips the terminal ruff-format pass: the injected
        unformatted source is written VERBATIM (differs from a default run).
        """
        project_root = tmp_path
        _build_marked_unformatted_project(project_root)
        _patch_worker_state_with_unformatted_codegen(monkeypatch)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "1",
                "--no-bundle",
                "--no-format",
            ],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        content = _generated_file(project_root).read_text()
        assert content == _UNFORMATTED_SOURCE, (
            "Expected the UN-formatted source verbatim under --no-format; the "
            f"terminal ruff-format pass appears to have run.\n{content!r}"
        )

    def test_no_format_still_runs_cfg031_guard(self, runner, tmp_path, monkeypatch):
        """(a) ``--no-format`` does NOT bypass the in-worker validity guard: a
        flowgroup generating invalid Python still raises ``LHP-CFG-031`` and
        writes ZERO files.
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
                        "table": _CFG031_MARKER,
                        "create_table": True,
                    },
                },
            ],
        }
        with open(bad_dir / "p_cfg031_fg.yaml", "w") as f:
            yaml.dump(bad_fg, f)

        # Injects REAL invalid generated source -> the worker's REAL ast.parse
        # guard raises a REAL LHP-CFG-031, even with --no-format.
        _patch_worker_state_with_cfg031_codegen(monkeypatch)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "2",
                "--no-bundle",
                "--no-format",
            ],
        )
        assert result.exit_code != 0, (
            f"Expected non-zero exit on LHP-CFG-031 even with --no-format; got "
            f"{result.exit_code}:\n{result.output}"
        )
        assert (
            "LHP-CFG-031" in result.output
        ), f"Expected LHP-CFG-031 in error output under --no-format:\n{result.output}"
        py_files = _py_files_under(project_root / "generated" / "dev")
        assert py_files == [], (
            "Expected ZERO generated .py files on a syntax-guard abort under "
            f"--no-format; found: {[str(p) for p in py_files]}"
        )

    def test_lhp_yaml_apply_formatting_false_honored(
        self, runner, tmp_path, monkeypatch
    ):
        """(c) ``apply_formatting: false`` in ``lhp.yaml`` skips formatting when
        no CLI flag is given.
        """
        project_root = tmp_path
        _build_marked_unformatted_project(project_root)
        _set_apply_formatting_key(project_root, False)
        _patch_worker_state_with_unformatted_codegen(monkeypatch)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli, ["generate", "--env", "dev", "--max-workers", "1", "--no-bundle"]
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        content = _generated_file(project_root).read_text()
        assert content == _UNFORMATTED_SOURCE, (
            "Expected UN-formatted source verbatim with lhp.yaml "
            f"apply_formatting: false and no flag.\n{content!r}"
        )

    def test_cli_no_format_overrides_lhp_yaml_true(self, runner, tmp_path, monkeypatch):
        """(c) ``--no-format`` overrides ``apply_formatting: true`` in
        ``lhp.yaml`` (CLI flag wins): formatting is skipped.
        """
        project_root = tmp_path
        _build_marked_unformatted_project(project_root)
        _set_apply_formatting_key(project_root, True)
        _patch_worker_state_with_unformatted_codegen(monkeypatch)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "1",
                "--no-bundle",
                "--no-format",
            ],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        content = _generated_file(project_root).read_text()
        assert content == _UNFORMATTED_SOURCE, (
            "Expected --no-format to OVERRIDE lhp.yaml apply_formatting: true; "
            f"formatting appears to have run.\n{content!r}"
        )

    def test_lhp_yaml_apply_formatting_true_formats(
        self, runner, tmp_path, monkeypatch
    ):
        """(c) ``apply_formatting: true`` in ``lhp.yaml`` with no flag formats —
        the complement of the override case, confirming the key is read (not
        ignored) in the True direction too.
        """
        project_root = tmp_path
        _build_marked_unformatted_project(project_root)
        _set_apply_formatting_key(project_root, True)
        _patch_worker_state_with_unformatted_codegen(monkeypatch)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli, ["generate", "--env", "dev", "--max-workers", "1", "--no-bundle"]
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        content = _generated_file(project_root).read_text()
        assert _UNFORMATTED_SOURCE not in content and "x = 1" in content, (
            "Expected ruff-formatted output with lhp.yaml apply_formatting: "
            f"true and no flag.\n{content!r}"
        )
