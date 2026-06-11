"""E2E test for deterministic per-pipeline wheel packaging.

Exercises the whole wheel-mode generate path end-to-end and locks a baseline.
One pipeline (``sample_python_func_pipeline``) opts into wheel packaging via a
dedicated ``config/pipeline_config_wheel.yaml`` (``packaging: wheel``); the
project's ``lhp.yaml`` declares the ``wheel.artifact_volume``. Every other
pipeline stays in source mode through the unchanged ``config/pipeline_config.yaml``,
so this scenario is fully isolated from the standard ``generated_baseline/`` and
``resources_baseline/`` comparisons (see ``test_wheel_mode_other_pipelines_unchanged``).

Wheel-mode output contract verified here (WHEEL_PACKAGING_SPEC R2/R6/R8/R9):

  * the wheeled pipeline's dir under ``generated/dev/<pipeline>/`` contains ONLY
    the ``<import_pkg>_runner.py`` runner — NO loose flowgroup ``.py`` and NO
    top-level ``custom_python_functions/`` (those travel inside the wheel);
  * ``generated/dev/_wheels/<pipeline>/dist/<...>.whl`` IS built on disk but is
    a content-addressed binary that is gitignored and NOT part of the baseline;
  * ``resources/lhp/<pipeline>.pipeline.yml`` gains the wheel reference under
    ``environment.dependencies`` and carries NO ``packaging:`` key;
  * ``resources/lhp/_wheels.bundle.yml`` is emitted with NO ``artifacts:`` block —
    only the resolved ``targets.dev.workspace.artifact_path`` + ``sync.exclude``
    (each wheel is a prebuilt local library reference DAB uploads + rewrites itself).

Baselines were authored manually and verified against the feature contract —
never formatted. The runner and ``_wheels.bundle.yml`` are byte-stable and
hash-compared directly. The resource YAML's wheel reference embeds the LHP tool
version in the ``.whl`` filename; that single version token is normalized to the
live ``get_version()`` before comparison so a version bump does not false-fail
the otherwise byte-identical baseline (the content hash stays exact).
"""

import hashlib
import os
import re
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli

# The fixture pipeline converted to wheel mode by config/pipeline_config_wheel.yaml.
WHEEL_PIPELINE = "sample_python_func_pipeline"
# import_package_name("sample_python_func_pipeline") is already a valid identifier.
RUNNER_FILENAME = f"{WHEEL_PIPELINE}_runner.py"
# A source-mode pipeline used to prove other pipelines are untouched in wheel mode.
SOURCE_PIPELINE = "acmi_edw_silver"


@pytest.mark.e2e
class TestWheelPackagingE2E:
    """E2E coverage of per-pipeline wheel packaging through the real generate path."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Create an isolated copy of the fixture project for each test."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.pipeline_dir = self.generated_dir / WHEEL_PIPELINE

        # Dedicated wheel-mode baselines (kept separate from the shared
        # source-mode generated_baseline/ and resources_baseline/).
        self.wheel_generated_baseline = (
            self.project_root / "generated_baseline_wheel" / "dev" / WHEEL_PIPELINE
        )
        self.wheel_resources_baseline = (
            self.project_root / "resources_baseline_wheel" / "lhp"
        )
        # Source-mode baseline used by the no-contamination assertion.
        self.source_generated_baseline = (
            self.project_root / "generated_baseline" / "dev" / SOURCE_PIPELINE
        )

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def run_generate_wheel(self) -> tuple:
        """Run 'lhp generate --env dev' with the wheel-mode pipeline config."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config_wheel.yaml",
            ],
        )
        return result.exit_code, result.output

    def run_inspect_wheel(self, *args: str) -> tuple:
        """Run 'lhp inspect-wheel <args>' through the same in-process CliRunner.

        Reuses the exact CLI-invocation mechanism as ``run_generate_wheel`` so
        the worktree CLI (which is the one ``lhp.cli.main:cli`` resolves to under
        the test's ``PYTHONPATH``) is exercised in-process — no subprocess, no
        reliance on the installed ``lhp`` binary.
        """
        runner = CliRunner()
        result = runner.invoke(cli, ["inspect-wheel", *args])
        return result.exit_code, result.output

    @staticmethod
    def _parse_listed_arcnames(output: str) -> set:
        """Extract the listed ``.py`` module arcnames from inspect-wheel output.

        The list presenter renders a non-TTY tab-separated table under the
        deterministic test console (``force_terminal=False``): a title line, a
        ``Module\\tSize (bytes)`` header, one ``<arcname>\\t<size>`` row per
        module, and a ``Total modules: N`` footer. The arcname is the first
        tab-delimited field; keeping only first-fields that end in ``.py`` drops
        the title (no tab), the header (``Module``), and the footer naturally.
        """
        arcnames = set()
        for line in output.splitlines():
            if "\t" not in line:
                continue
            first = line.split("\t", 1)[0].strip()
            if first.endswith(".py"):
                arcnames.add(first)
        return arcnames

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical, error otherwise."""

        def get_hash(f: Path) -> str:
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
            )
        return ""

    def _normalize_wheel_version(self, text: str) -> str:
        """Rewrite ANY version token in a ``.whl`` filename to a fixed placeholder,
        so a baseline frozen at any version compares equal to a freshly generated
        reference at any other version while the content hash stays exact.

        PEP 440-normalized versions start with a digit and contain no hyphens, so
        ``-\\d[^-]*-py3-none-any\\.whl`` matches exactly the
        ``-<version>-py3-none-any.whl`` segment and cannot consume the preceding
        content hash (no hyphens in the hash) or any other path component. The hash
        and the rest of the reference remain in the normalized text and are still
        compared byte-for-byte.
        """
        return re.sub(r"-\d[^-]*-py3-none-any\.whl", "-VERSION-py3-none-any.whl", text)

    # ------------------------------------------------------------------ #
    # Runner-only pipeline dir + wheel artifact built off to the side      #
    # ------------------------------------------------------------------ #

    def test_wheel_mode_pipeline_dir_contains_only_runner(self):
        """The wheeled pipeline dir holds ONLY the runner — no flowgroup .py, no
        custom_python_functions/ (R6: those ship inside the wheel)."""
        exit_code, output = self.run_generate_wheel()
        assert exit_code == 0, f"Generation failed: {output}"

        assert self.pipeline_dir.is_dir(), "Wheeled pipeline dir should be generated"

        files = sorted(
            p.relative_to(self.pipeline_dir).as_posix()
            for p in self.pipeline_dir.rglob("*")
            if p.is_file()
        )
        assert files == [RUNNER_FILENAME], (
            f"Wheeled pipeline dir must contain only the runner; found: {files}"
        )

        # Explicit negative checks for the source-mode artifacts that must be gone.
        assert not (self.pipeline_dir / "python_func_flowgroup.py").exists(), (
            "Loose flowgroup .py must NOT exist in wheel mode (it ships in the wheel)"
        )
        assert not (self.pipeline_dir / "custom_python_functions").exists(), (
            "custom_python_functions/ must NOT exist in the pipeline dir in wheel "
            "mode (it ships top-level inside the wheel)"
        )

    def test_wheel_mode_runner_matches_baseline(self):
        """The generated runner is byte-identical to the manually-authored baseline."""
        exit_code, output = self.run_generate_wheel()
        assert exit_code == 0, f"Generation failed: {output}"

        generated = self.pipeline_dir / RUNNER_FILENAME
        baseline = self.wheel_generated_baseline / RUNNER_FILENAME
        assert generated.exists(), "Runner should be generated"
        assert baseline.exists(), "Runner baseline should exist"

        diff = self._compare_file_hashes(generated, baseline)
        assert diff == "", f"Runner baseline mismatch: {diff}"

        # The runner references only the import-package name (R9): no flowgroup
        # name, no content hash, no wheel version leak into its bytes.
        content = generated.read_text()
        assert f'importlib.import_module("{WHEEL_PIPELINE}")' in content
        assert "py3-none-any.whl" not in content
        assert "python_func_flowgroup" not in content

    def test_wheel_mode_wheel_artifact_built_but_not_committed_to_pipeline_dir(self):
        """The .whl is built under the _wheels/ staging tree (off to the side) and
        never lands in the synced pipeline dir or the baseline."""
        exit_code, output = self.run_generate_wheel()
        assert exit_code == 0, f"Generation failed: {output}"

        dist_dir = self.generated_dir / "_wheels" / WHEEL_PIPELINE / "dist"
        wheels = sorted(dist_dir.glob("*.whl"))
        assert len(wheels) == 1, (
            f"Exactly one wheel should be built under {dist_dir}; found: {wheels}"
        )
        whl = wheels[0]
        # Deterministic identity components in the filename, with the ``lhp_`` brand
        # prefix that marks LHP-generated wheels.
        assert whl.name.startswith(f"lhp_{WHEEL_PIPELINE}_dev_"), (
            f"Wheel filename should carry lhp_<pipeline>_<env>_<hash>: {whl.name}"
        )
        assert whl.name.endswith("-py3-none-any.whl")

        # The wheel is NOT inside the synced pipeline dir ...
        assert not list(self.pipeline_dir.rglob("*.whl")), (
            "No .whl may live under the synced pipeline dir"
        )
        # ... and is NOT part of the committed baseline.
        assert not list(self.wheel_generated_baseline.rglob("*.whl")), (
            "No .whl may be committed to the wheel baseline (gitignored binary)"
        )

    # ------------------------------------------------------------------ #
    # Bundle resource wiring                                               #
    # ------------------------------------------------------------------ #

    def test_wheel_mode_resource_yaml_has_wheel_ref_and_no_packaging_key(self):
        """The pipeline resource YAML gains the wheel reference under
        environment.dependencies and carries no LHP-internal 'packaging:' key.

        Compared to the manually-authored baseline with the (environment-coupled)
        LHP version token in the .whl filename normalized on both sides.
        """
        exit_code, output = self.run_generate_wheel()
        assert exit_code == 0, f"Generation failed: {output}"

        generated = self.resources_dir / f"{WHEEL_PIPELINE}.pipeline.yml"
        baseline = self.wheel_resources_baseline / f"{WHEEL_PIPELINE}.pipeline.yml"
        assert generated.exists(), "Wheeled pipeline resource YAML should be generated"
        assert baseline.exists(), "Wheeled pipeline resource YAML baseline should exist"

        gen_text = generated.read_text()

        # Structural facts: the wheel ref is present as a FILE-RELATIVE LOCAL path
        # (DAB classifies it as a prebuilt wheel, uploads + rewrites it itself — so
        # NO absolute /Volumes/ path leaks into the resource YAML), and no
        # 'packaging:' key leaked into the rendered YAML.
        assert "environment:" in gen_text
        assert "dependencies:" in gen_text
        assert "/Volumes/" not in gen_text, (
            "The wheel ref must be a local ../../generated/... path, not an "
            "absolute /Volumes/ path (DAB uploads + rewrites the prebuilt wheel)"
        )
        assert (
            f"../../generated/dev/_wheels/{WHEEL_PIPELINE}/dist/lhp_{WHEEL_PIPELINE}_dev_"
            in gen_text
        )
        assert gen_text.rstrip().count("py3-none-any.whl") == 1
        assert "packaging:" not in gen_text, (
            "The LHP-internal 'packaging' toggle must be stripped from the resource YAML"
        )

        # Byte-comparison with only the LHP version token normalized.
        assert self._normalize_wheel_version(gen_text) == self._normalize_wheel_version(
            baseline.read_text()
        ), "Wheel resource YAML differs from baseline (beyond the version token)"

    def test_wheel_mode_wheels_bundle_matches_baseline(self):
        """resources/lhp/_wheels.bundle.yml is emitted and byte-identical to baseline.

        It carries no version token, so a direct hash comparison is stable.
        """
        exit_code, output = self.run_generate_wheel()
        assert exit_code == 0, f"Generation failed: {output}"

        generated = self.resources_dir / "_wheels.bundle.yml"
        baseline = self.wheel_resources_baseline / "_wheels.bundle.yml"
        assert generated.exists(), "_wheels.bundle.yml should be emitted in wheel mode"
        assert baseline.exists(), "_wheels.bundle.yml baseline should exist"

        diff = self._compare_file_hashes(generated, baseline)
        assert diff == "", f"_wheels.bundle.yml baseline mismatch: {diff}"

        # Spot-check the load-bearing wiring (resolved volume + sync-exclude) and
        # the absence of any 'artifacts:' block. The header comment now documents
        # why there is no artifacts block (the word "artifacts" appears in prose),
        # so the no-artifacts check must parse the YAML and assert no top-level
        # 'artifacts' KEY — a naive substring check would false-positive on the
        # comment.
        content = generated.read_text()
        doc = yaml.safe_load(content)
        assert "artifacts" not in doc, (
            "No top-level 'artifacts:' block: each wheel is a prebuilt local "
            "library reference that DAB uploads + rewrites itself"
        )
        assert "artifact_path: /Volumes/acme_edw_dev/edw_raw/artifacts" in content
        assert "../../generated/${bundle.target}/_wheels/**" in content

    # ------------------------------------------------------------------ #
    # No contamination of source-mode pipelines                           #
    # ------------------------------------------------------------------ #

    def test_wheel_mode_other_pipelines_unchanged(self):
        """A source-mode pipeline is byte-identical to its standard baseline even
        when another pipeline in the same run is wheel-packaged (R8 isolation)."""
        exit_code, output = self.run_generate_wheel()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_dir = self.generated_dir / SOURCE_PIPELINE
        baseline_dir = self.source_generated_baseline
        assert generated_dir.is_dir(), f"{SOURCE_PIPELINE} should still be generated"
        assert baseline_dir.is_dir(), f"{SOURCE_PIPELINE} baseline should exist"

        differences = []
        for baseline_file in baseline_dir.rglob("*"):
            if not baseline_file.is_file():
                continue
            rel = baseline_file.relative_to(baseline_dir)
            generated_file = generated_dir / rel
            if not generated_file.exists():
                differences.append(f"MISSING: {rel}")
                continue
            diff = self._compare_file_hashes(generated_file, baseline_file)
            if diff:
                differences.append(f"CHANGED: {rel}")
        # And no extra files appeared on the generated side.
        for generated_file in generated_dir.rglob("*"):
            if not generated_file.is_file():
                continue
            rel = generated_file.relative_to(generated_dir)
            if not (baseline_dir / rel).exists():
                differences.append(f"EXTRA: {rel}")

        assert not differences, (
            f"Source-mode pipeline {SOURCE_PIPELINE} drifted in a wheel-mode run: "
            f"{differences}"
        )

    def test_inspect_wheel_list_matches_extract(self, tmp_path):
        """After generate, 'lhp inspect-wheel' list mode and --extract agree.

        Exercises the consumer-side wheel reader end-to-end against the real
        wheel built by the generate step (content-addressed, gitignored — no
        baseline involved): the set of ``.py`` module arcnames LISTED must equal
        the set of ``.py`` files EXTRACTED to disk (relative to the extract dir),
        since extraction preserves the in-wheel structure (writes each member at
        ``<extract_dir>/<arcname>``).
        """
        exit_code, output = self.run_generate_wheel()
        assert exit_code == 0, f"Generation failed: {output}"

        list_code, list_output = self.run_inspect_wheel(WHEEL_PIPELINE, "-e", "dev")
        assert list_code == 0, f"inspect-wheel list failed: {list_output}"
        listed = self._parse_listed_arcnames(list_output)
        assert listed, (
            f"inspect-wheel listed no .py modules; raw output:\n{list_output}"
        )

        extract_dir = tmp_path / "wheel_extract"
        extract_code, extract_output = self.run_inspect_wheel(
            WHEEL_PIPELINE, "-e", "dev", "--extract", str(extract_dir)
        )
        assert extract_code == 0, f"inspect-wheel --extract failed: {extract_output}"

        extracted = {
            p.relative_to(extract_dir).as_posix()
            for p in extract_dir.rglob("*.py")
            if p.is_file()
        }
        assert listed == extracted, (
            "inspect-wheel listed modules differ from extracted .py files.\n"
            f"  listed only: {sorted(listed - extracted)}\n"
            f"  extracted only: {sorted(extracted - listed)}"
        )
