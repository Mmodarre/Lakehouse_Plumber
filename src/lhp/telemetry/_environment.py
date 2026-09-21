"""Environment detection for the telemetry envelope.

Every value produced here is a bounded enum or a boolean — never a variable's
value, a path or a hostname. Detection reads an injected ``environ`` so the
rules are testable without the host's own CI and agent variables leaking in;
only :func:`capture` defaults to the process environment.
"""

from __future__ import annotations

import importlib.metadata
import json
import logging
import os
import platform
import sys
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Tuple

from lhp.utils.env_flags import env_truthy
from lhp.utils.version import get_version

logger = logging.getLogger(__name__)

# Ordered so a specific vendor always wins over the generic ``CI`` marker that
# most of them also export. Azure DevOps (``TF_BUILD``) and CodeBuild do NOT
# export ``CI``, which is why the table is keyed on vendor variables rather
# than gated behind one.
_CI_VENDORS: Tuple[Tuple[str, str], ...] = (
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
)

# Named agents first; the generic markers fall through to ``other``.
_AGENTS: Tuple[Tuple[Tuple[str, ...], str], ...] = (
    (("CLAUDECODE",), "claude_code"),
    (("CURSOR_AGENT", "CURSOR_EDITOR"), "cursor"),
    (("GEMINI_CLI",), "gemini_cli"),
    (("CODEX_SANDBOX",), "codex"),
    (("GITHUB_COPILOT_CLI_MODE",), "copilot_cli"),
    (("AGENT", "AI_AGENT"), "other"),
)

_OS_NAMES = {
    "linux": "linux",
    "darwin": "macos",
    "win32": "windows",
    "cygwin": "windows",
}
_ARCH_NAMES = {
    "x86_64": "x86_64",
    "amd64": "x86_64",
    "arm64": "arm64",
    "aarch64": "arm64",
}

_DISTRIBUTION_NAME = "lakehouse-plumber"
_DIRECT_URL_FILE = "direct_url.json"


@dataclass(frozen=True)
class EnvironmentFacts:
    """The envelope's environment block, captured once per process."""

    lhp_version: str
    python: str
    os: str
    arch: str
    install_kind: str
    ci_vendor: str
    agent: str
    databricks_runtime: bool
    interactive: bool


def ci_vendor(environ: Mapping[str, str]) -> str:
    """Name the CI provider running this invocation, or ``"none"``."""
    for variable, vendor in _CI_VENDORS:
        if env_truthy(environ, variable):
            return vendor
    return "none"


def agent(environ: Mapping[str, str]) -> str:
    """Name the coding agent driving this invocation, or ``"none"``."""
    for variables, name in _AGENTS:
        if any(env_truthy(environ, variable) for variable in variables):
            return name
    return "none"


def os_name() -> str:
    """Report the operating-system family as one of four bounded values."""
    return _OS_NAMES.get(sys.platform, "other")


def arch() -> str:
    """Report the CPU architecture as one of three bounded values."""
    return _ARCH_NAMES.get(platform.machine().lower(), "other")


def python_version() -> str:
    """Report the interpreter version as ``major.minor`` only."""
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def lhp_version() -> str:
    """Report the installed LHP version.

    Delegates to the one version reader LHP owns; the telemetry client does
    not carry a second copy of the metadata lookup.
    """
    return get_version()


def databricks_runtime(environ: Mapping[str, str]) -> bool:
    """Report whether this process runs on a Databricks cluster.

    Presence of the variable is the signal — the runtime version itself is a
    value and is never collected.
    """
    return "DATABRICKS_RUNTIME_VERSION" in environ


def interactive() -> bool:
    """Report whether stderr is a terminal."""
    try:
        return bool(sys.stderr.isatty())
    except Exception:  # a replaced, closed or absent stderr cannot answer
        return False


def install_kind(
    distribution: Callable[[str], Any] = importlib.metadata.distribution,
) -> str:
    """Classify the install as ``wheel``, ``editable`` or ``unknown``.

    PEP 610 records an editable install in the distribution's
    ``direct_url.json`` as ``dir_info.editable``. That one field is read and
    nothing else — in particular never ``url``, which is a local filesystem
    path. A distribution with no ``direct_url.json`` was installed from an
    index, so it is a ``wheel``; a file that cannot be read or parsed, or one
    describing a direct URL with no ``dir_info``, says nothing trustworthy and
    yields ``unknown``.
    """
    try:
        raw = distribution(_DISTRIBUTION_NAME).read_text(_DIRECT_URL_FILE)
    except Exception:  # metadata may be missing entirely (source tree, zipapp)
        logger.debug("Could not read distribution metadata", exc_info=True)
        return "unknown"

    if raw is None:
        return "wheel"

    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):  # a malformed record is not evidence
        logger.debug("Could not parse %s", _DIRECT_URL_FILE, exc_info=True)
        return "unknown"

    dir_info = payload.get("dir_info") if isinstance(payload, dict) else None
    if not isinstance(dir_info, dict):
        return "unknown"
    return "editable" if dir_info.get("editable") is True else "wheel"


def capture(environ: Optional[Mapping[str, str]] = None) -> EnvironmentFacts:
    """Capture every environment fact the envelope carries."""
    resolved = os.environ if environ is None else environ
    return EnvironmentFacts(
        lhp_version=lhp_version(),
        python=python_version(),
        os=os_name(),
        arch=arch(),
        install_kind=install_kind(),
        ci_vendor=ci_vendor(resolved),
        agent=agent(resolved),
        databricks_runtime=databricks_runtime(resolved),
        interactive=interactive(),
    )
