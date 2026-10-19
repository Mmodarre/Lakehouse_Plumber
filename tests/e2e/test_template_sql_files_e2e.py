"""End-to-end tests for template parameters in ``sql_path`` files.

The fixture project covers the three places a template SQL file is read:

- ``gold_mv_template`` points a materialized view at ``sql/gold/<table>.sql``.
  ``orders_by_period_mv.sql`` loops over the ``group_by`` list parameter and
  names its input with ``{{ silver_schema }}`` (defaulted to the
  ``${silver_schema}`` token) next to ``${catalog}``.
- ``gold_steps_template`` reads an SQL load file and an SQL transform file;
  the load file branches on a boolean parameter default.

Template expansion renders the files; substitution and generation then treat
the result like the template's own inline SQL.

  TSF-1  Golden path: the rendered materialized view, load and transform match
         their baselines byte-for-byte and contain no Jinja.
  TSF-2  An undefined name in a SQL file stops generation and validation with
         LHP-CFG-071 naming the file.
  TSF-3  An unresolved ${token} in the rendered SQL fails validation with
         LHP-CFG-010 naming the file the SQL was rendered from.
  TSF-4  ``lhp dag`` extracts the rendered table reference, not the raw
         ``{{ }}`` text.
  TSF-5  ``lhp dag`` still completes when a file fails to render.
"""

import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli

SQL_FILE = "sql/gold/orders_by_period_mv.sql"
TRANSFORM_FILE = "sql/gold/customer_segments_transform.sql"
GENERATED = (
    Path("gold_load") / "orders_by_period_mv.py",
    Path("gold_load") / "customer_segments.py",
)


@pytest.mark.e2e
class TestTemplateSqlFilesE2E:
    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.baseline_dir = self.project_root / "generated_baseline" / "dev"
        for working_dir in (
            self.generated_dir,
            self.project_root / "resources" / "lhp",
        ):
            if working_dir.exists():
                shutil.rmtree(working_dir)
            working_dir.mkdir(parents=True)

        yield

        os.chdir(self.original_cwd)

    def _run(self, *args: str) -> tuple:
        result = CliRunner().invoke(
            cli,
            [*args, "--env", "dev", "--pipeline-config", "config/pipeline_config.yaml"],
        )
        return result.exit_code, result.output

    def _rewrite_sql(self, old: str, new: str, file: str = SQL_FILE) -> None:
        sql_file = self.project_root / file
        text = sql_file.read_text()
        assert old in text
        sql_file.write_text(text.replace(old, new))

    def test_TSF1_rendered_sql_files_match_baselines(self):
        exit_code, output = self._run("generate")
        assert exit_code == 0, f"Generation failed:\n{output}"

        for relative in GENERATED:
            generated = self.generated_dir / relative
            baseline = self.baseline_dir / relative
            assert (
                hashlib.sha256(generated.read_bytes()).hexdigest()
                == hashlib.sha256(baseline.read_bytes()).hexdigest()
            ), f"Baseline mismatch for {relative}:\n{generated.read_text()}"
            code = generated.read_text()
            assert "{{" not in code and "{%" not in code
        view, steps = (self.generated_dir / relative for relative in GENERATED)
        assert "GROUP BY o.order_year, o.order_month" in view.read_text()
        assert "WHERE c.__end_at IS NULL" in steps.read_text()
        assert "FROM v_customer_segments_raw" in steps.read_text()

    @pytest.mark.parametrize("command", ["generate", "validate"])
    def test_TSF2_undefined_name_in_sql_file_stops_the_command(self, command):
        self._rewrite_sql("{{ entity }}", "{{ entity_name }}", TRANSFORM_FILE)

        exit_code, output = self._run(command, "--show-details")

        assert exit_code != 0
        assert "LHP-CFG-071" in output
        assert TRANSFORM_FILE in output

    def test_TSF3_unresolved_token_names_the_rendered_file(self):
        self._rewrite_sql("${catalog}", "${missing_catalog}")

        exit_code, output = self._run("validate", "--show-details")

        assert exit_code != 0
        assert "LHP-CFG-010" in output
        assert f"rendered from {SQL_FILE}" in output

    def test_TSF4_dag_extracts_the_rendered_table(self):
        result = CliRunner().invoke(cli, ["dag", "-b"])
        assert result.exit_code == 0, f"dag failed:\n{result.output}"

        deps_json = (
            self.project_root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
        )
        gold = json.loads(deps_json.read_text())["pipelines"]["gold_load"]
        assert "${catalog}.${silver_schema}.orders" in gold["external_sources"]
        assert not any("?" in source for source in gold["external_sources"])

    def test_TSF5_dag_completes_when_a_file_fails_to_render(self):
        self._rewrite_sql("{{ entity }}", "{{ entity_name }}", TRANSFORM_FILE)

        result = CliRunner().invoke(cli, ["dag", "-b"])

        assert result.exit_code == 0, f"dag failed:\n{result.output}"
        deps_json = (
            self.project_root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
        )
        gold = json.loads(deps_json.read_text())["pipelines"]["gold_load"]
        assert "${catalog}.${silver_schema}.orders" in gold["external_sources"]
