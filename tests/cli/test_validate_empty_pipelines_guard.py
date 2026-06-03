"""Empty-project ``lhp validate`` behaviour (ratified spec §6.6).

A project with no flowgroups validates to a clean ``ValidationCompleted`` and
exits 0 (asymmetric with ``generate``, which still raises ``LHP-CFG-014`` on a
truly empty project). The deprecation scan still runs over any flowgroup files
that are present, surfacing ``LHP-DEPR-001`` for the bare ``{token}`` syntax.

These are thin CliRunner tests against the ``cli`` group object: the old
``ValidateCommand`` class and its ``_determine_pipelines_to_validate`` seam were
removed in the CLI rebuild, so the empty-worklist state is driven through real
on-disk fixtures rather than a monkeypatched method.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import yaml
from click.testing import CliRunner

from lhp.cli.main import cli


def _bare_project(project_root: Path) -> None:
    """Minimum on-disk project so ``resolve_project_root`` and the facade
    succeed; the ``pipelines/`` tree is left empty (no flowgroups).
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
    def test_empty_project_exits_zero(self):
        """No flowgroups -> validate completes cleanly and exits 0 (ratified)."""
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

        # The event-stream renderer reports a clean validate stage and a
        # zero-count summary; nothing was validated and no error is raised.
        assert "0 validated" in result.output, (
            f"Expected a zero-count validation summary. output:\n{result.output}"
        )

        # CliRunner reports SystemExit(0) via result.exception even on success.
        from click.exceptions import Exit as ClickExit

        assert result.exception is None or isinstance(
            result.exception, (SystemExit, ClickExit)
        ), f"Unexpected exception: {result.exception!r}"

    def test_deprecation_scan_runs_for_present_flowgroups(self):
        """The deprecation scan surfaces ``LHP-DEPR-001`` for a flowgroup file
        that uses the bare ``{token}`` substitution syntax, while validate still
        exits 0 (a deprecation is a warning, not a failure).
        """
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            _bare_project(project_root)

            # A flowgroup whose write target uses the deprecated bare {token}
            # syntax. ``emit_deprecation_warning_if_needed`` reads the file
            # from disk, so it must exist on disk (it does).
            (project_root / "pipelines" / "fg.yaml").write_text(
                "pipeline: p\nflowgroup: fg\n"
                "actions:\n"
                "  - name: load_table\n"
                "    type: load\n"
                "    source:\n"
                "      type: sql\n"
                "      sql: 'SELECT 1'\n"
                "    target: v_raw\n"
                "  - name: write_table\n"
                "    type: write\n"
                "    source: v_raw\n"
                "    write_target:\n"
                "      type: streaming_table\n"
                "      database: '{catalog}.bronze'\n"
                "      table: t\n",
                encoding="utf-8",
            )

            cwd = os.getcwd()
            try:
                os.chdir(project_root)
                result = runner.invoke(cli, ["validate", "--env", "dev"])
            finally:
                os.chdir(cwd)

        assert result.exit_code == 0, (
            f"CLI exited {result.exit_code} (expected 0).\noutput:\n{result.output}"
        )

        assert "LHP-DEPR-001" in result.output, (
            "Expected the bare {token} deprecation (LHP-DEPR-001) in output. "
            f"output:\n{result.output}"
        )
        assert "substitution syntax is deprecated" in result.output, (
            f"Expected the deprecation message text in output. output:\n{result.output}"
        )
