"""Tests for the ``lhp init`` command (``lhp.cli.commands.init_command``).

Uses ``CliRunner()`` (Click 8.4 removed the ``mix_stderr`` kwarg) so the
success report (stdout) and the error panel (stderr) can be asserted
independently.
"""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from lhp.cli.commands.init_command import init


def test_init_fresh_dir_succeeds_and_shows_tree() -> None:
    """Fresh cwd -> exit 0, project scaffolded, created tree reported."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["demo_project"])

        assert result.exit_code == 0, (
            f"exit {result.exit_code}; stderr:\n{result.stderr}"
        )
        # Project was actually scaffolded into the cwd.
        assert Path("lhp.yaml").exists()
        # Success report names the project and the created structure.
        assert "Initialized LHP" in result.stdout
        assert "lhp.yaml" in result.stdout
        # Next-step guidance is present.
        assert "lhp validate" in result.stdout
        assert "lhp generate" in result.stdout


def test_init_default_is_bundle_and_shows_deploy_step() -> None:
    """Default (no --no-bundle) scaffolds a bundle project incl. databricks.yml."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["demo_project"])

        assert result.exit_code == 0, (
            f"exit {result.exit_code}; stderr:\n{result.stderr}"
        )
        assert Path("databricks.yml").exists()
        assert "databricks bundle deploy" in result.stdout


def test_init_no_bundle_skips_bundle_files() -> None:
    """--no-bundle scaffolds a plain project without databricks.yml."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["--no-bundle", "demo_project"])

        assert result.exit_code == 0, (
            f"exit {result.exit_code}; stderr:\n{result.stderr}"
        )
        assert Path("lhp.yaml").exists()
        assert not Path("databricks.yml").exists()
        assert "databricks bundle deploy" not in result.stdout


def test_init_sample_with_no_bundle_is_usage_error() -> None:
    """--sample --no-bundle -> exit 2 (usage error), nothing scaffolded."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["--sample", "--no-bundle", "demo_project"])

        assert result.exit_code == 2, (
            f"exit {result.exit_code}; stdout:\n{result.stdout}"
        )
        flat_stderr = " ".join(result.stderr.replace("│", " ").split())
        assert "--sample cannot be combined with --no-bundle" in flat_stderr
        assert "requires Declarative Automation Bundles support" in flat_stderr
        # Fail-fast: the guard fires before any facade call / filesystem write.
        assert not Path("lhp.yaml").exists()


def test_init_sample_scaffolds_sample_tree() -> None:
    """--sample -> exit 0 with the sample-only tree scaffolded into cwd.

    Presence-only asserts: a later task owns the template *content*, so
    these checks must stay green across content-only template changes.
    """
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["--sample", "demo_project"])

        assert result.exit_code == 0, (
            f"exit {result.exit_code}; stderr:\n{result.stderr}"
        )
        # lhp.yaml is rendered from lhp.yaml.j2 with the project name baked in.
        assert Path("lhp.yaml").exists()
        assert "demo_project" in Path("lhp.yaml").read_text(encoding="utf-8")
        assert Path("pipelines/02_silver/dim_customer.yaml").exists()
        assert Path("transforms/normalize_tz.py").exists()
        assert Path("notebooks/data_prep.py").exists()
        # Bundle subtree is flattened to the project root.
        assert Path("databricks.yml").exists()
        assert Path("resources/sample_job.yml").exists()
        # Inactive secret-syntax example ships as .txt (never parsed by LHP).
        assert Path("examples_optional/jdbc_with_secret.yaml.txt").exists()


def test_init_sample_leaves_no_template_suffixes() -> None:
    """--sample -> no scaffolded file keeps a .j2 or .tmpl template suffix."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["--sample", "demo_project"])

        assert result.exit_code == 0, (
            f"exit {result.exit_code}; stderr:\n{result.stderr}"
        )
        leftovers = [
            str(p)
            for p in Path(".").rglob("*")
            if p.is_file() and p.suffix in (".j2", ".tmpl")
        ]
        assert leftovers == [], f"unrendered template files: {leftovers}"


def test_init_plain_does_not_emit_sample_paths() -> None:
    """Plain init (no --sample) -> none of the sample-only paths appear."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(init, ["demo_project"])

        assert result.exit_code == 0, (
            f"exit {result.exit_code}; stderr:\n{result.stderr}"
        )
        assert Path("lhp.yaml").exists()
        assert not Path("notebooks").exists()
        assert not Path("examples_optional").exists()
        assert not Path("functions").exists()
        assert not Path("resources/sample_job.yml").exists()


def test_init_existing_lhp_yaml_raises_io_007() -> None:
    """Pre-existing lhp.yaml -> exit 1 with LHP-IO-007 surfaced on stderr."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("lhp.yaml").write_text("name: existing\n")

        result = runner.invoke(init, ["demo_project"])

        assert result.exit_code == 1, (
            f"exit {result.exit_code}; stdout:\n{result.stdout}"
        )
        assert "LHP-IO-007" in result.stderr
