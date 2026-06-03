"""Empty-pipelines guard in ``ValidateCommand.execute``: emit a warning and
exit 0 (asymmetric with generate, which raises ``LHPConfigError-014``).
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.commands.validate_command import ValidateCommand
from lhp.cli.main import cli


def _bare_project(project_root: Path) -> None:
    """Minimum on-disk project so ``ensure_project_root`` and
    ``ProjectConfigLoader`` succeed; flowgroup discovery is monkeypatched.
    """
    (project_root / "presets").mkdir(parents=True, exist_ok=True)
    (project_root / "templates").mkdir(parents=True, exist_ok=True)
    (project_root / "substitutions").mkdir(parents=True, exist_ok=True)
    (project_root / "pipelines").mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text(
        "name: test_validate_empty_project\nversion: '1.0'\n"
    )
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump({"dev": {"catalog": "dev_catalog"}}, f)


class TestValidateEmptyPipelinesGuard:
    def test_empty_pipelines_warns_and_exits_zero(self, monkeypatch):
        monkeypatch.setattr(
            ValidateCommand,
            "_determine_pipelines_to_validate",
            lambda self, pipeline, orchestrator: ([], []),
        )

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _bare_project(project_root)

            cwd = os.getcwd()
            try:
                os.chdir(project_root)
                result = runner.invoke(cli, ["validate", "--env", "dev"])
            finally:
                os.chdir(cwd)

        assert result.exit_code == 0, (
            f"CLI exited {result.exit_code} (expected 0).\noutput:\n{result.output}"
        )

        assert "no pipelines found to validate" in result.output.lower(), (
            f"Expected empty-pipelines warning in output. output:\n{result.output}"
        )

        matches = re.findall(
            r"no pipelines found to validate", result.output, re.IGNORECASE
        )
        assert len(matches) == 1, (
            f"Expected the empty-pipelines warning exactly once, "
            f"got {len(matches)}.\noutput:\n{result.output}"
        )

        # CliRunner reports SystemExit(0) via result.exception even on success.
        assert result.exception is None or isinstance(result.exception, SystemExit), (
            f"Unexpected exception: {result.exception!r}"
        )

    def test_deprecation_scan_runs_when_pipelines_empty_but_flowgroups_exist(
        self, monkeypatch
    ):
        """Deprecation scan must run when ``pipelines_to_validate`` is empty
        but ``all_flowgroups`` is non-empty (e.g. ``--pipeline foo`` filter
        resolves to no flowgroups). Previously the scan sat inside the
        ``if not empty_no_op:`` gate.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _bare_project(project_root)

            # Path must exist on disk: ``emit_deprecation_warning_if_needed``
            # calls ``Path.read_text`` and skips on ``OSError``.
            bare_yaml = project_root / "fake_flowgroup.yaml"
            bare_yaml.write_text(
                "pipeline: ignored\nflowgroup: ignored\n"
                "actions:\n"
                "  - name: load_table\n"
                "    type: load\n"
                "    target: {catalog}.{schema}.table\n",
                encoding="utf-8",
            )

            fake_fg = SimpleNamespace(pipeline="ignored", file_path=bare_yaml)

            monkeypatch.setattr(
                ValidateCommand,
                "_determine_pipelines_to_validate",
                lambda self, pipeline, orchestrator: ([], [fake_fg]),
            )

            runner = CliRunner()
            cwd = os.getcwd()
            try:
                os.chdir(project_root)
                result = runner.invoke(cli, ["validate", "--env", "dev"])
            finally:
                os.chdir(cwd)

        assert result.exit_code == 0, (
            f"CLI exited {result.exit_code} (expected 0).\noutput:\n{result.output}"
        )

        assert "bare {token} substitution syntax is deprecated" in result.output, (
            "Expected deprecation warning in output when "
            "pipelines_to_validate is empty but all_flowgroups is non-empty. "
            f"output:\n{result.output}"
        )
