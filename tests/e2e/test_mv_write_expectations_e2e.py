"""E2E tests for data-quality expectations on materialized views (#344).

The ``mv_write_expectations`` flowgroup in ``gold_load`` covers both shapes:

* Shape 1: ``write_target.expectations_file`` on a self-contained SQL MV.
* Shape 2: a ``data_quality`` action whose only consumer is a pass-through MV;
  its rules fuse onto that MV, which reads the DQ action's source view.

Generation runs through the real ``lhp generate`` path, including the spawned
flowgroup workers, so this also proves read-mode inheritance and fusion fire
there. Negative paths edit the temp copy of the project at runtime.
"""

import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli

_FLOWGROUP_REL = Path("pipelines") / "04_gold" / "mv_write_expectations.yaml"
_DQ_FILE_REL = Path("expectations") / "mv_write_expectations_dq.json"


@pytest.mark.e2e
class TestMvWriteExpectationsE2E:
    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"

        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

        yield
        os.chdir(self.original_cwd)

    def _invoke(self, command: str, *extra: str) -> tuple:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                command,
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
                *extra,
            ],
        )
        return result.exit_code, result.output

    def run_generate(self) -> tuple:
        return self._invoke("generate")

    def run_validate(self) -> tuple:
        # --show-details prints the underlying error codes, not just the wrapper
        return self._invoke("validate", "--show-details")

    def _generated_code(self) -> str:
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"
        generated_file = self.generated_dir / "gold_load" / "mv_write_expectations.py"
        assert generated_file.exists(), "mv_write_expectations.py should be generated"
        return generated_file.read_text()

    @staticmethod
    def _function_block(code: str, name: str) -> str:
        """The decorators and body of the top-level function ``name``.

        Generated top-level blocks are separated by two blank lines.
        """
        for chunk in code.split("\n\n\n"):
            if f"\ndef {name}():" in f"\n{chunk}":
                return chunk
        raise AssertionError(f"function {name} not found in generated code")

    def _load_flowgroup(self) -> dict:
        return yaml.safe_load((self.project_root / _FLOWGROUP_REL).read_text())

    def _save_flowgroup(self, content: dict) -> None:
        (self.project_root / _FLOWGROUP_REL).write_text(
            yaml.dump(content, default_flow_style=False)
        )

    @staticmethod
    def _action(content: dict, name: str) -> dict:
        return next(a for a in content["actions"] if a["name"] == name)

    def _assert_validate_fails_with(self, code: str) -> None:
        exit_code, output = self.run_validate()
        assert exit_code != 0, f"Validation should fail with {code}: {output}"
        assert code in output, f"Expected {code} in validate output: {output}"

    # ------------------------------------------------------------------
    # Positive paths
    # ------------------------------------------------------------------

    def test_validate_passes(self):
        exit_code, output = self.run_validate()
        assert exit_code == 0, f"Validation failed: {output}"

    def test_baseline_hash(self):
        code = self._generated_code()
        generated_file = self.generated_dir / "gold_load" / "mv_write_expectations.py"
        baseline_file = (
            self.project_root
            / "generated_baseline"
            / "dev"
            / "gold_load"
            / "mv_write_expectations.py"
        )
        assert baseline_file.exists(), "Baseline file should exist"
        assert code, "Generated file should not be empty"

        generated_hash = hashlib.sha256(generated_file.read_bytes()).hexdigest()
        baseline_hash = hashlib.sha256(baseline_file.read_bytes()).hexdigest()
        assert generated_hash == baseline_hash, (
            f"Hash mismatch: {generated_file.name} ({generated_hash[:12]}) != "
            f"{baseline_file.name} ({baseline_hash[:12]})"
        )

    def test_both_mvs_carry_fail_expectations(self):
        code = self._generated_code()
        for mv in ("mv_expect_sql_gate", "mv_expect_fused"):
            block = self._function_block(code, mv)
            assert "@dp.expect_all_or_fail(" in block, f"{mv} lacks expect_all_or_fail"

    def test_fused_mv_reads_load_view_and_carries_all_actions(self):
        code = self._generated_code()
        fused = self._function_block(code, "mv_expect_fused")

        assert 'spark.read.table("v_mv_expect_orders")' in fused
        assert "v_mv_expect_orders_checked" not in fused
        assert (
            '@dp.expect_all({"fused_customer_key_present": '
            '"customer_key IS NOT NULL"})' in fused
        )
        assert (
            '@dp.expect_all_or_drop({"fused_price_not_negative": '
            '"total_price >= 0"})' in fused
        )
        assert (
            '@dp.expect_all_or_fail({"fused_order_key_present": '
            '"order_key IS NOT NULL"})' in fused
        )

    def test_dq_view_still_emitted_and_reads_batch(self):
        code = self._generated_code()
        dq_view = self._function_block(code, "v_mv_expect_orders_checked")

        assert 'spark.read.table("v_mv_expect_orders")' in dq_view
        assert "readStream" not in dq_view
        assert "@dp.temporary_view()" in dq_view
        assert "readStream" not in code

    def test_sql_gate_mv_carries_warn_and_fail(self):
        code = self._generated_code()
        gate = self._function_block(code, "mv_expect_sql_gate")

        assert '@dp.expect_all({"gate_price_not_negative": "total_price >= 0"})' in gate
        assert (
            '@dp.expect_all_or_fail({"gate_amount_reconciled": "amount_reconciled"})'
            in gate
        )
        assert "@dp.expect_all_or_drop" not in gate

    # ------------------------------------------------------------------
    # Negative paths (the temp copy is edited at runtime)
    # ------------------------------------------------------------------

    def test_stream_dq_over_batch_load_fails_val_070(self):
        content = self._load_flowgroup()
        self._action(content, "check_mv_expect_orders")["readMode"] = "stream"
        self._save_flowgroup(content)

        self._assert_validate_fails_with("LHP-VAL-070")

    def test_action_level_expectations_file_on_mv_write_fails_cfg_073(self):
        content = self._load_flowgroup()
        write = self._action(content, "write_mv_expect_sql_gate")
        write["expectations_file"] = "expectations/mv_write_expectations.json"
        self._save_flowgroup(content)

        self._assert_validate_fails_with("LHP-CFG-073")

    def test_unknown_expectation_action_fails_val_006(self):
        dq_file = self.project_root / _DQ_FILE_REL
        rules = json.loads(dq_file.read_text())
        rules["total_price >= 0"]["action"] = "explode"
        dq_file.write_text(json.dumps(rules, indent=2))

        self._assert_validate_fails_with("LHP-VAL-006")

    def test_duplicate_expectation_name_on_fused_mv_fails_val_071(self):
        duplicate_file = self.project_root / "expectations" / "mv_dup_names.json"
        duplicate_file.write_text(
            json.dumps(
                {
                    "version": "1.0",
                    "expectations": [
                        {
                            "name": "fused_order_key_present",
                            "constraint": "order_key > 0",
                            "failureAction": "warn",
                        }
                    ],
                },
                indent=2,
            )
        )
        content = self._load_flowgroup()
        write = self._action(content, "write_mv_expect_fused")
        write["write_target"]["expectations_file"] = "expectations/mv_dup_names.json"
        self._save_flowgroup(content)

        self._assert_validate_fails_with("LHP-VAL-071")
