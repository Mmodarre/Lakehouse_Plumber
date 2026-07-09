"""Acceptance tests for ``lhp substitutions`` (command + presenter).

The command test runs against the e2e fixture project and asserts the
resolved token listing reaches stdout. The secret-reference form is exercised
at the presenter level with a hand-built view: ``build_substitution_view``
constructs a substitution manager but never runs substitution, so its
``secret_references`` tuple is always empty (the manager only populates it
while substituting payloads). The default secret scope, by contrast, is read
at YAML-load time and is asserted end-to-end.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from click.testing import CliRunner
from conftest import capture_lhp_console
from rich.console import Console

from lhp.api.views import SecretReferenceView, SubstitutionView
from lhp.cli.commands.substitutions_command import substitutions_command
from lhp.cli.presenters import substitutions_presenter

FIXTURE_PROJECT = (
    Path(__file__).resolve().parents[1] / "e2e" / "fixtures" / "testing_project"
)


@pytest.mark.unit
def test_substitutions_lists_tokens_against_fixture(monkeypatch):
    """Resolved tokens and the default secret scope reach stdout for the fixture."""
    monkeypatch.chdir(FIXTURE_PROJECT)
    # Runs in-place in the tracked fixture: the parse cache must not write
    # .lhp/cache/ shards into it (same for the two tests below).
    monkeypatch.setenv("LHP_NO_CACHE", "1")
    runner = CliRunner()
    with capture_lhp_console(width=120) as buf:
        result = runner.invoke(substitutions_command, ["-e", "dev"])
    output = buf.getvalue()

    assert result.exit_code == 0, result.output
    # A representative flat token and its resolved value.
    assert "${catalog}" in output
    assert "acme_edw_dev" in output
    # A token whose value is expanded from another token.
    assert "/Volumes/acme_edw_dev/edw_raw/incoming_volume" in output
    # The default secret scope is read at YAML-load time and surfaced.
    assert "dev_secrets" in output


@pytest.mark.unit
def test_substitutions_default_env_is_dev(monkeypatch):
    """Omitting ``-e`` resolves the ``dev`` environment."""
    monkeypatch.chdir(FIXTURE_PROJECT)
    monkeypatch.setenv("LHP_NO_CACHE", "1")
    runner = CliRunner()
    with capture_lhp_console(width=120) as buf:
        result = runner.invoke(substitutions_command, [])
    output = buf.getvalue()

    assert result.exit_code == 0, result.output
    assert "environment: dev" in output
    assert "${catalog}" in output


@pytest.mark.unit
def test_substitutions_missing_env_is_not_an_error(monkeypatch):
    """A missing ``substitutions/<env>.yaml`` reports an empty context, not a failure."""
    monkeypatch.chdir(FIXTURE_PROJECT)
    monkeypatch.setenv("LHP_NO_CACHE", "1")
    runner = CliRunner()
    with capture_lhp_console(width=120) as buf:
        result = runner.invoke(substitutions_command, ["-e", "does_not_exist"])
    output = buf.getvalue()

    assert result.exit_code == 0, result.output
    assert "environment: does_not_exist" in output


@pytest.mark.unit
def test_presenter_renders_secret_reference_form():
    """A ``SecretReferenceView`` renders as ``${secret:scope/key}``."""
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, no_color=True, width=120)
    view = SubstitutionView(
        env="dev",
        tokens={"catalog": "acme_edw_dev"},
        raw_mappings={"catalog": "acme_edw_dev"},
        secret_references=(
            SecretReferenceView(scope="dev_db_secrets", key="password"),
        ),
        default_secret_scope="dev_secrets",
    )

    substitutions_presenter.render(view, console=console)
    output = buf.getvalue()

    assert "${secret:dev_db_secrets/password}" in output
    assert "${catalog}" in output
    assert "acme_edw_dev" in output
    assert "Default secret scope: dev_secrets" in output


@pytest.mark.unit
def test_presenter_renders_nested_map_as_tree():
    """A nested ``dict`` value in ``raw_mappings`` renders under a Maps section."""
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, no_color=True, width=120)
    view = SubstitutionView(
        env="dev",
        tokens={"catalog": "acme_edw_dev", "table_props": "{'delta': 'true'}"},
        raw_mappings={"catalog": "acme_edw_dev", "table_props": {"delta": "true"}},
    )

    substitutions_presenter.render(view, console=console)
    output = buf.getvalue()

    assert "Maps" in output
    assert "table_props" in output
    assert 'delta: "true"' in output
    # The nested-map key must not also appear as a flat scalar token.
    assert "${table_props}" not in output
