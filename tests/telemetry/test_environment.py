"""Tests for :mod:`lhp.telemetry._environment`.

The detection rules read an injected ``environ`` rather than ``os.environ``,
so a CI vendor or coding agent can be simulated without the host's own
variables (this suite runs under GitHub Actions, which sets ``CI`` and
``GITHUB_ACTIONS``) leaking into the assertions.
"""

import platform
import sys
from dataclasses import FrozenInstanceError
from typing import Optional

import pytest

from lhp.telemetry._environment import (
    EnvironmentFacts,
    agent,
    arch,
    capture,
    ci_vendor,
    databricks_runtime,
    install_kind,
    interactive,
    lhp_version,
    os_name,
    python_version,
)


class _FakeDistribution:
    """Stand-in for ``importlib.metadata.Distribution``.

    ``read_text`` returns ``None`` for a file the distribution does not carry,
    which is exactly how the real implementation reports a missing
    ``direct_url.json``.
    """

    def __init__(self, payload: Optional[str]) -> None:
        self._payload = payload

    def read_text(self, filename: str) -> Optional[str]:
        assert filename == "direct_url.json"
        return self._payload


def _distribution_returning(payload: Optional[str]):
    return lambda name: _FakeDistribution(payload)


# ci_vendor


@pytest.mark.unit
@pytest.mark.parametrize(
    ("variable", "expected"),
    [
        ("GITHUB_ACTIONS", "github_actions"),
        ("GITLAB_CI", "gitlab_ci"),
        ("TF_BUILD", "azure_devops"),
        ("JENKINS_URL", "jenkins"),
        ("CIRCLECI", "circleci"),
        ("BUILDKITE", "buildkite"),
        ("TEAMCITY_VERSION", "teamcity"),
        ("BITBUCKET_BUILD_NUMBER", "bitbucket"),
        ("CODEBUILD_BUILD_ID", "codebuild"),
        ("TRAVIS", "travis"),
        ("DRONE", "drone"),
        ("CI", "other_ci"),
    ],
)
def test_ci_vendor_recognises_each_vendor(variable: str, expected: str) -> None:
    assert ci_vendor({variable: "true"}) == expected


@pytest.mark.unit
@pytest.mark.parametrize("variable", ["TF_BUILD", "CODEBUILD_BUILD_ID"])
def test_ci_vendor_detects_vendors_that_do_not_set_ci(variable: str) -> None:
    # Azure DevOps and CodeBuild do not export ``CI``, so a check that only
    # looked at ``CI`` would report these builds as developer machines.
    assert ci_vendor({variable: "True"}) != "none"


@pytest.mark.unit
def test_ci_vendor_prefers_the_specific_vendor_over_generic_ci() -> None:
    assert ci_vendor({"CI": "true", "GITLAB_CI": "true"}) == "gitlab_ci"


@pytest.mark.unit
@pytest.mark.parametrize("value", ["", "0", "false", "no"])
def test_ci_vendor_ignores_falsy_values(value: str) -> None:
    assert ci_vendor({"CI": value, "GITHUB_ACTIONS": value}) == "none"


@pytest.mark.unit
def test_ci_vendor_defaults_to_none() -> None:
    assert ci_vendor({}) == "none"


# agent


@pytest.mark.unit
@pytest.mark.parametrize(
    ("variable", "expected"),
    [
        ("CLAUDECODE", "claude_code"),
        ("CURSOR_AGENT", "cursor"),
        ("CURSOR_EDITOR", "cursor"),
        ("GEMINI_CLI", "gemini_cli"),
        ("CODEX_SANDBOX", "codex"),
        ("GITHUB_COPILOT_CLI_MODE", "copilot_cli"),
        ("AGENT", "other"),
        ("AI_AGENT", "other"),
    ],
)
def test_agent_matrix(variable: str, expected: str) -> None:
    assert agent({variable: "1"}) == expected


@pytest.mark.unit
def test_agent_prefers_the_named_agent_over_the_generic_marker() -> None:
    assert agent({"AGENT": "1", "CLAUDECODE": "1"}) == "claude_code"


@pytest.mark.unit
def test_agent_ignores_falsy_values() -> None:
    assert agent({"CLAUDECODE": "0"}) == "none"


@pytest.mark.unit
def test_agent_defaults_to_none() -> None:
    assert agent({}) == "none"


# os_name / arch / python_version


@pytest.mark.unit
@pytest.mark.parametrize(
    ("platform_value", "expected"),
    [
        ("linux", "linux"),
        ("darwin", "macos"),
        ("win32", "windows"),
        ("cygwin", "windows"),
        ("freebsd14", "other"),
    ],
)
def test_os_name(monkeypatch, platform_value: str, expected: str) -> None:
    monkeypatch.setattr(sys, "platform", platform_value)
    assert os_name() == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("machine", "expected"),
    [
        ("x86_64", "x86_64"),
        ("AMD64", "x86_64"),
        ("amd64", "x86_64"),
        ("arm64", "arm64"),
        ("aarch64", "arm64"),
        ("ppc64le", "other"),
        ("", "other"),
    ],
)
def test_arch(monkeypatch, machine: str, expected: str) -> None:
    monkeypatch.setattr(platform, "machine", lambda: machine)
    assert arch() == expected


@pytest.mark.unit
def test_python_version_is_major_minor_only() -> None:
    expected = f"{sys.version_info.major}.{sys.version_info.minor}"
    assert python_version() == expected


# install_kind


@pytest.mark.unit
def test_install_kind_is_wheel_when_direct_url_is_absent() -> None:
    assert install_kind(distribution=_distribution_returning(None)) == "wheel"


@pytest.mark.unit
def test_install_kind_is_editable_when_dir_info_says_so() -> None:
    payload = '{"url": "file:///src", "dir_info": {"editable": true}}'
    assert install_kind(distribution=_distribution_returning(payload)) == "editable"


@pytest.mark.unit
def test_install_kind_is_wheel_when_dir_info_is_not_editable() -> None:
    payload = '{"url": "file:///src", "dir_info": {"editable": false}}'
    assert install_kind(distribution=_distribution_returning(payload)) == "wheel"


@pytest.mark.unit
def test_install_kind_is_wheel_when_editable_is_absent_from_dir_info() -> None:
    payload = '{"url": "file:///src", "dir_info": {}}'
    assert install_kind(distribution=_distribution_returning(payload)) == "wheel"


@pytest.mark.unit
def test_install_kind_is_unknown_for_malformed_json() -> None:
    assert install_kind(distribution=_distribution_returning("{not json")) == "unknown"


@pytest.mark.unit
def test_install_kind_is_unknown_when_dir_info_is_missing() -> None:
    payload = '{"url": "https://example.invalid/pkg.whl"}'
    assert install_kind(distribution=_distribution_returning(payload)) == "unknown"


@pytest.mark.unit
def test_install_kind_is_unknown_when_the_distribution_is_not_installed() -> None:
    def _raise(name: str):
        raise ModuleNotFoundError(name)

    assert install_kind(distribution=_raise) == "unknown"


@pytest.mark.unit
def test_install_kind_never_reads_the_url_field() -> None:
    # Only ``dir_info.editable`` is consulted; a direct URL is a path-like
    # value and must never reach the payload.
    payload = '{"url": "file:///Users/someone/secret/project", "dir_info": {"editable": true}}'
    assert install_kind(distribution=_distribution_returning(payload)) == "editable"


# databricks_runtime / interactive / lhp_version


@pytest.mark.unit
def test_databricks_runtime_is_presence_based() -> None:
    assert databricks_runtime({"DATABRICKS_RUNTIME_VERSION": "15.4"}) is True
    assert databricks_runtime({"DATABRICKS_RUNTIME_VERSION": ""}) is True
    assert databricks_runtime({}) is False


@pytest.mark.unit
def test_interactive_reflects_stderr(monkeypatch) -> None:
    class _Tty:
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(sys, "stderr", _Tty())
    assert interactive() is True


@pytest.mark.unit
def test_interactive_is_false_when_stderr_cannot_answer(monkeypatch) -> None:
    monkeypatch.setattr(sys, "stderr", None)
    assert interactive() is False


@pytest.mark.unit
def test_lhp_version_delegates_to_the_shared_version_helper() -> None:
    from lhp.utils.version import get_version

    assert lhp_version() == get_version()


# capture


@pytest.mark.unit
def test_capture_returns_frozen_facts_for_the_injected_environment() -> None:
    facts = capture({"GITLAB_CI": "true", "CLAUDECODE": "1"})
    assert isinstance(facts, EnvironmentFacts)
    assert facts.ci_vendor == "gitlab_ci"
    assert facts.agent == "claude_code"
    assert facts.databricks_runtime is False
    assert facts.python == python_version()
    assert facts.lhp_version == lhp_version()


@pytest.mark.unit
def test_capture_facts_are_immutable() -> None:
    facts = capture({})
    with pytest.raises(FrozenInstanceError):
        facts.ci_vendor = "github_actions"  # type: ignore[misc]


@pytest.mark.unit
def test_capture_defaults_to_the_process_environment(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    assert capture().databricks_runtime is True
