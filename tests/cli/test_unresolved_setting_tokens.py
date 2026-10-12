"""CLI acceptance tests for #298: unresolved ``${tokens}`` in bundle and monitoring config.

A plain ``${name}`` left after substitution in a value LHP writes out fails
BOTH ``lhp validate`` and ``lhp generate`` with the same ``LHP-CFG-010`` panel,
before anything is written: the bundle resource settings (the ``event_log``
injected from ``lhp.yaml``, every ``pipeline_config.yaml`` value) when bundle
output is enabled, and the monitoring notebook's ``checkpoint_path`` and
event-log references with or without bundle output. Defined tokens resolve;
dotted Databricks references pass; the monitoring job config keeps its
``${bundle_target}``.

Thin ``CliRunner`` tests over real on-disk projects; the check is unit-tested
under ``tests/core/validators/`` and ``tests/api/``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from conftest import strip_ansi

from lhp.cli.commands.generate_command import generate
from lhp.cli.commands.validate_command import validate_command

pytestmark = pytest.mark.unit

_PC = ["-pc", "config/pipeline_config.yaml"]
_NO_BUNDLE = ["--no-bundle"]
_FLAGS = ["--no-progress", "--show-details"]

_FLOWGROUP = """pipeline: repro
flowgroup: repro_fg
actions:
  - name: write_orders
    type: write
    write_target:
      type: materialized_view
      catalog: c
      schema: s
      table: orders
      sql: "SELECT 1 AS id"
"""

_PIPELINE_CONFIG = """project_defaults:
  catalog: "${catalog}"
  schema: "${schema}"
  serverless: true
"""


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _event_log(schema: str) -> str:
    return f'event_log:\n  catalog: "${{catalog}}"\n  schema: "{schema}"\n'


def _monitoring(checkpoint_path: str) -> str:
    return (
        "monitoring:\n"
        f'  checkpoint_path: "{checkpoint_path}"\n'
        "  job_config_path: config/monitoring_job_config.yaml\n"
    )


def _project(
    root: Path, lhp_extra: str = "", pipeline_config: str = _PIPELINE_CONFIG
) -> None:
    _write(root / "lhp.yaml", 'name: tok_repro\nversion: "1.0"\n' + lhp_extra)
    _write(
        root / "substitutions" / "dev.yaml",
        "dev:\n  catalog: main\n  schema: bronze\n",
    )
    for sub in ("presets", "templates"):
        (root / sub).mkdir(exist_ok=True)
    _write(root / "pipelines" / "repro" / "p.yaml", _FLOWGROUP)
    _write(root / "config" / "pipeline_config.yaml", pipeline_config)
    _write(
        root / "config" / "monitoring_job_config.yaml",
        "max_concurrent_runs: 1\ntags:\n  environment: ${bundle_target}\n",
    )
    _write(
        root / "databricks.yml",
        "bundle:\n  name: tok_repro\ntargets:\n  dev:\n    default: true\n",
    )


def _flat(text: str) -> str:
    return " ".join(strip_ansi(text).replace("│", " ").split())


def _error_panel(stderr: str) -> str:
    """The rendered ``LHP-...`` error panel (first one), flattened."""
    lines = strip_ansi(stderr).splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("╭─ LHP-"))
    end = next(i for i in range(start, len(lines)) if lines[i].startswith("╰"))
    return _flat("\n".join(lines[start : end + 1]))


def _validate(*options: str):
    return CliRunner().invoke(validate_command, ["--env", "dev", *options, *_FLAGS])


def _generate(*options: str):
    return CliRunner().invoke(generate, ["-e", "dev", *options, *_FLAGS])


def _assert_same_cfg_010(options, title: str, *expected: str) -> None:
    """Both commands fail on the same CFG-010, and both name the place.

    ``lhp validate`` prints a project-level preflight issue as one
    ``Error [<code>]: <title>`` line, so the title carries the token, file and
    key; ``lhp generate`` raises it as the full panel under the same title.
    """
    validated, generated = _validate(*options), _generate(*options)
    assert validated.exit_code == 1, validated.stderr
    assert generated.exit_code == 1, generated.stderr
    assert f"Error [LHP-CFG-010]: {title}" in _flat(validated.stderr)
    panel = _error_panel(generated.stderr)
    assert "LHP-CFG-010" in panel
    assert title in panel
    for text in (*expected, "substitutions/dev.yaml"):
        assert text in panel
    assert "Spark substitutes an empty string" not in panel


def _resource(root: Path) -> dict:
    text = (root / "resources" / "lhp" / "repro.pipeline.yml").read_text()
    return yaml.safe_load(text)["resources"]["pipelines"]["repro_pipeline"]


def test_undefined_event_log_token_fails_validate_and_generate(tmp_path, monkeypatch):
    _project(tmp_path, _event_log("${undefined_evt}"))
    monkeypatch.chdir(tmp_path)

    _assert_same_cfg_010(
        _PC,
        "Unresolved substitution token(s) ${undefined_evt} in lhp.yaml (event_log)",
        "File: lhp.yaml",
        "Field: event_log",
    )
    assert not (tmp_path / "resources" / "lhp" / "repro.pipeline.yml").exists()
    assert not (tmp_path / "generated").exists()


def test_validate_summary_names_the_file_key_and_token(tmp_path, monkeypatch):
    _project(tmp_path, _event_log("${undefined_evt}"))
    monkeypatch.chdir(tmp_path)

    result = _validate(*_PC)

    assert result.exit_code == 1, result.stderr
    flat = _flat(result.stderr)
    for text in ("LHP-CFG-010", "${undefined_evt}", "lhp.yaml", "(event_log)"):
        assert text in flat


def test_defined_event_log_token_is_substituted(tmp_path, monkeypatch):
    _project(tmp_path, _event_log("${schema}"))
    monkeypatch.chdir(tmp_path)

    assert _validate(*_PC).exit_code == 0
    result = _generate(*_PC)
    assert result.exit_code == 0, result.stderr
    assert _resource(tmp_path)["event_log"]["schema"] == "bronze"


def test_undefined_pipeline_config_token_fails_validate_and_generate(
    tmp_path, monkeypatch
):
    _project(
        tmp_path,
        pipeline_config=_PIPELINE_CONFIG
        + '  configuration:\n    source_root: "/Volumes/${undefined_cfg}"\n',
    )
    monkeypatch.chdir(tmp_path)

    _assert_same_cfg_010(
        _PC,
        "Unresolved substitution token(s) ${undefined_cfg} in "
        "config/pipeline_config.yaml (configuration.source_root)",
        "Pipeline: repro",
    )


def test_dotted_bundle_references_pass_through(tmp_path, monkeypatch):
    _project(
        tmp_path,
        pipeline_config=_PIPELINE_CONFIG
        + "  configuration:\n"
        + '    target: "${bundle.target}"\n'
        + '    owner: "${var.owner}"\n',
    )
    monkeypatch.chdir(tmp_path)

    assert _validate(*_PC).exit_code == 0
    result = _generate(*_PC)
    assert result.exit_code == 0, result.stderr
    configuration = _resource(tmp_path)["configuration"]
    assert configuration["target"] == "${bundle.target}"
    assert configuration["owner"] == "${var.owner}"


def test_bundle_without_pipeline_config_stops_at_cfg_023(tmp_path, monkeypatch):
    """Without ``-pc`` a bundle project never reaches the bundle output."""
    _project(tmp_path, _event_log("${undefined_evt}"))
    monkeypatch.chdir(tmp_path)

    for result in (_validate(), _generate()):
        assert result.exit_code == 1, result.stderr
        flat = _flat(result.stderr)
        assert "LHP-CFG-023" in flat
        assert "LHP-CFG-010" not in flat
    assert not (tmp_path / "generated").exists()


@pytest.mark.parametrize("options", [_NO_BUNDLE, _PC], ids=["no_bundle", "bundle"])
def test_undefined_checkpoint_path_token_fails_validate_and_generate(
    tmp_path, monkeypatch, options
):
    _project(
        tmp_path,
        _event_log("_meta") + _monitoring("/Volumes/${catalog}/${undefined_ckpt}/c"),
    )
    monkeypatch.chdir(tmp_path)

    _assert_same_cfg_010(
        options,
        "Unresolved substitution token(s) ${undefined_ckpt} in lhp.yaml "
        "(monitoring.checkpoint_path)",
        "Field: monitoring.checkpoint_path",
    )
    assert not (tmp_path / "monitoring").exists()


def test_monitoring_job_config_keeps_its_bundle_target_token(tmp_path, monkeypatch):
    _project(tmp_path, _event_log("_meta") + _monitoring("/Volumes/${catalog}/m/c"))
    monkeypatch.chdir(tmp_path)

    assert _validate(*_NO_BUNDLE).exit_code == 0
    result = _generate(*_NO_BUNDLE)
    assert result.exit_code == 0, result.stderr
    notebook = (tmp_path / "monitoring" / "dev" / "union_event_logs.py").read_text()
    assert '"/Volumes/main/m/c"' in notebook
    job = (
        tmp_path / "resources" / "tok_repro_event_log_monitoring.job.yml"
    ).read_text()
    assert "${bundle_target}" in job
