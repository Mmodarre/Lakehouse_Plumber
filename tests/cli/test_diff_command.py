"""Acceptance tests for the ``lhp diff`` command.

Invokes the command object directly (not through ``main.py``, which is
import-red until the CLI rebuild lands). Click 8.4 captures stdout / stderr
separately, so bare ``CliRunner()`` (no ``mix_stderr`` — removed in 8.2) gives
``result.stdout`` / ``result.stderr`` distinctly.

Project shape: one flowgroup per pipeline under ``pipelines/<name>/`` (the layout
the generation discoverer requires). The worker pool is ``spawn``-based, so the
top-level builders below must be importable from the spawned child — they are.

The plan-only primitive narrows by a pipeline-name WORKLIST: a single name via
``pipeline_filter`` (``-p``) OR the full discovered set via ``pipeline_fields``
(``core.codegen.build_generation_plan``: "passing neither plans nothing"). The
``diff`` command derives that worklist itself — with ``-p`` it forwards
``pipeline_filter``; WITHOUT ``-p`` it enumerates every discovered pipeline and
forwards ``pipeline_fields`` so the WHOLE project is planned. The whole-project
tests below assert exactly that: a ``diff`` with no ``-p`` plans every pipeline
(so an in-sync multi-pipeline tree reports no changes and a single modified file
shows as ``~ modified``, NOT the whole tree collapsing to ``- orphan`` because
the plan came back empty — the regression this command's worklist fix guards).

The on-disk ``generated/<env>`` tree is materialised by driving the SAME
``plan_generation`` stream the command drives and writing each planned file's
(already ruff-formatted) content to disk, giving a byte-identical in-sync
baseline. The drift cases then mutate / delete / add files and assert the
presenter's ``~`` / ``+`` / ``-`` lines plus the ``--exit-code`` behaviour.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Sequence

import pytest
import yaml
from click.testing import CliRunner

from lhp.api import collect_response
from lhp.cli._app_context import build_facade
from lhp.cli.commands.diff_command import diff_command


def _write_project(root: Path, pipeline_names) -> None:
    """Write a minimal generation-ready LHP project under ``root``.

    One flowgroup per pipeline at ``pipelines/<name>/<name>_fg.yaml`` (cloudfiles
    load -> sql transform -> streaming-table write) plus a ``dev`` substitution
    file supplying the ``${...}`` tokens those actions reference.
    """
    (root / "lhp.yaml").write_text("name: diff_test_project\nversion: '1.0'\n")
    (root / "substitutions").mkdir(parents=True, exist_ok=True)
    subs = {
        "dev": {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
        }
    }
    (root / "substitutions" / "dev.yaml").write_text(yaml.safe_dump(subs))

    for name in pipeline_names:
        fg_dir = root / "pipelines" / name
        fg_dir.mkdir(parents=True, exist_ok=True)
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
        (fg_dir / f"{name}_fg.yaml").write_text(yaml.safe_dump(flowgroup))


def _materialize_planned_tree(
    root: Path,
    *,
    pipeline: Optional[str] = None,
    pipeline_fields: Sequence[str] = (),
    env: str = "dev",
) -> Dict[str, Path]:
    """Drive ``plan_generation`` for the given worklist and write every planned file.

    Mirrors the command's worklist semantics: pass ``pipeline`` for a single-``-p``
    plan, or ``pipeline_fields`` for the whole-project plan. Returns
    ``{posix-relative-path: absolute-path}`` of what was written; the written bytes
    equal the plan's content, so a subsequent ``diff`` over the same worklist sees
    no changes.
    """
    facade = build_facade(root)
    plan = collect_response(
        facade.generation.plan_generation(
            env, pipeline_filter=pipeline, pipeline_fields=pipeline_fields
        )
    )
    output_location = plan.output_location
    assert output_location is not None
    written: Dict[str, Path] = {}
    for pf in plan.files:
        dest = output_location / pf.path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(pf.content, encoding="utf-8")
        written[Path(pf.path).as_posix()] = dest
    return written


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.mark.unit
@pytest.mark.slow
class TestDiffCommand:
    def test_in_sync_tree_exits_zero_and_reports_no_changes(self, runner: CliRunner):
        """A byte-identical ``generated/dev`` tree -> exit 0, 'No changes'."""
        with runner.isolated_filesystem():
            root = Path.cwd()
            _write_project(root, ["p_one"])
            written = _materialize_planned_tree(root, pipeline="p_one")
            assert written, "expected the plan to produce at least one file"

            result = runner.invoke(
                diff_command,
                ["--env", "dev", "--no-progress", "-p", "p_one"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, result.stderr
            assert "No changes" in result.stdout

    def test_in_sync_tree_with_exit_code_flag_still_exits_zero(self, runner: CliRunner):
        """``--exit-code`` on an in-sync tree is a no-op -> exit 0."""
        with runner.isolated_filesystem():
            root = Path.cwd()
            _write_project(root, ["p_one"])
            _materialize_planned_tree(root, pipeline="p_one")

            result = runner.invoke(
                diff_command,
                ["--env", "dev", "--no-progress", "-p", "p_one", "--exit-code"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0, result.stderr

    def test_empty_tree_reports_would_create(self, runner: CliRunner):
        """No on-disk tree -> every planned file is a '+' would-create."""
        with runner.isolated_filesystem():
            root = Path.cwd()
            _write_project(root, ["p_one"])

            result = runner.invoke(
                diff_command,
                ["--env", "dev", "--no-progress", "-p", "p_one"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, result.stderr
            assert "would-create" in result.stdout

    def test_whole_project_in_sync_reports_no_changes(self, runner: CliRunner):
        """No ``-p`` plans the WHOLE project; an in-sync two-pipeline tree -> exit 0.

        Regression guard: before the worklist fix, ``diff`` without ``-p`` forwarded
        ``pipeline_filter=None`` and ``pipeline_fields=()`` to the plan primitive,
        which "plans nothing". The plan came back EMPTY, so every on-disk file was
        an orphan and an in-sync tree wrongly reported changes. With the fix the
        command enumerates both pipelines, the plan matches disk, and we get a clean
        'No changes' / exit 0.
        """
        with runner.isolated_filesystem():
            root = Path.cwd()
            _write_project(root, ["p_one", "p_two"])
            written = _materialize_planned_tree(
                root, pipeline_fields=("p_one", "p_two")
            )
            # The whole-project plan must span BOTH pipelines (not just one).
            assert any("p_one" in path for path in written), written
            assert any("p_two" in path for path in written), written

            result = runner.invoke(
                diff_command,
                ["--env", "dev", "--no-progress", "--exit-code"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, result.stderr
            assert "No changes" in result.stdout
            # Nothing must read as an orphan: the plan was NOT empty.
            assert "orphan" not in result.stdout

    def test_whole_project_drift_shows_modified_not_orphans(self, runner: CliRunner):
        """No ``-p``: ONE modified file shows as '~ modified', siblings stay in sync.

        This is the core bug assertion. Two pipelines are planned project-wide and
        written to disk; ONE planned file is then mutated. The fixed command plans
        BOTH pipelines, so the mutated file shows as ``~ modified`` while the OTHER
        pipeline's file lines up with the plan and does NOT appear at all. Under the
        old empty-plan bug, the modified file would have been reported as ``- orphan``
        (alongside every other on-disk file) and NEVER as ``~ modified``.
        """
        with runner.isolated_filesystem():
            root = Path.cwd()
            _write_project(root, ["p_one", "p_two"])
            written = _materialize_planned_tree(
                root, pipeline_fields=("p_one", "p_two")
            )

            # Mutate exactly one planned file belonging to p_one.
            p_one_paths = sorted(p for p in written if "p_one" in p)
            assert p_one_paths, written
            target = written[p_one_paths[0]]
            target.write_text(
                target.read_text(encoding="utf-8") + "\n# drift\n", encoding="utf-8"
            )

            result = runner.invoke(
                diff_command,
                ["--env", "dev", "--no-progress", "--exit-code"],
                catch_exceptions=False,
            )

            assert result.exit_code == 1, result.stderr
            assert "modified" in result.stdout
            # The mutated file must be the modified one, by path.
            assert p_one_paths[0] in result.stdout
            # The sibling pipeline's files are in sync -> no orphan / would-create.
            assert "orphan" not in result.stdout
            assert "would-create" not in result.stdout

    def test_whole_project_orphan_shows_as_orphan(self, runner: CliRunner):
        """No ``-p``: an on-disk file with no planned counterpart -> '- orphan'."""
        with runner.isolated_filesystem():
            root = Path.cwd()
            _write_project(root, ["p_one"])
            written = _materialize_planned_tree(root, pipeline_fields=("p_one",))

            any_planned = written[sorted(written)[0]]
            (any_planned.parent / "orphan_file.py").write_text(
                "# orphan\n", encoding="utf-8"
            )

            result = runner.invoke(
                diff_command,
                ["--env", "dev", "--no-progress", "--exit-code"],
                catch_exceptions=False,
            )

            assert result.exit_code == 1, result.stderr
            assert "orphan" in result.stdout
            assert "orphan_file.py" in result.stdout
            # The planned files match disk, so nothing reads as modified.
            assert "modified" not in result.stdout

    def test_drift_without_exit_code_flag_exits_zero(self, runner: CliRunner):
        """Drift without ``--exit-code`` still renders the diff but exits 0."""
        with runner.isolated_filesystem():
            root = Path.cwd()
            _write_project(root, ["p_one"])
            written = _materialize_planned_tree(root, pipeline="p_one")

            target = written[sorted(written)[0]]
            target.write_text(
                target.read_text(encoding="utf-8") + "\n# drift\n", encoding="utf-8"
            )

            result = runner.invoke(
                diff_command,
                ["--env", "dev", "--no-progress", "-p", "p_one"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, result.stderr
            assert "modified" in result.stdout

    def test_show_details_emits_unified_diff(self, runner: CliRunner):
        """``--show-details`` expands a modified file into a unified diff."""
        with runner.isolated_filesystem():
            root = Path.cwd()
            _write_project(root, ["p_one"])
            written = _materialize_planned_tree(root, pipeline="p_one")

            target = written[sorted(written)[0]]
            target.write_text(
                target.read_text(encoding="utf-8") + "\n# drift-detail\n",
                encoding="utf-8",
            )

            result = runner.invoke(
                diff_command,
                ["--env", "dev", "--no-progress", "-p", "p_one", "--show-details"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0, result.stderr
            assert "@@" in result.stdout
            assert "# drift-detail" in result.stdout
