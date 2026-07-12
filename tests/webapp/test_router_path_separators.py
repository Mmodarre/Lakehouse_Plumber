"""Router path helpers must emit POSIX (forward-slash) separators.

The frontend files API and :mod:`lhp.webapp.services.file_io` key files by
forward-slash paths regardless of the host OS (``file_watcher`` already uses
``.relative_to(root).as_posix()``). Four router helpers project an absolute
source path to a project-relative one; if any emit OS-native separators, then
on Windows the designer dirty-guard, the sandbox file-tree filter, filename
display, and the SSE cache-key all break.

These tests drive the helpers with :class:`pathlib.PureWindowsPath` so a native
backslash separator is meaningful even when the suite runs on POSIX — that is
the only way a ``str(path)`` regression is observable off Windows. The helpers
are separator-agnostic at runtime (they only call ``relative_to`` / ``as_posix``).
"""

from __future__ import annotations

from pathlib import PureWindowsPath

import pytest

from lhp.webapp.routers.blueprints import _relative_source as blueprints_relative_source
from lhp.webapp.routers.flowgroups import _relative_source as flowgroups_relative_source
from lhp.webapp.routers.project import _relative_path
from lhp.webapp.routers.tables import _relative_source as tables_relative_source

pytestmark = pytest.mark.webapp

# Each helper takes (path, project_root) and returns a project-relative string.
RELATIVE_HELPERS = [
    pytest.param(flowgroups_relative_source, id="flowgroups"),
    pytest.param(blueprints_relative_source, id="blueprints"),
    pytest.param(tables_relative_source, id="tables"),
    pytest.param(_relative_path, id="project"),
]

ROOT = PureWindowsPath(r"C:\proj")
NESTED = PureWindowsPath(r"C:\proj\pipelines\domain_a_bronze\fg.yaml")


@pytest.mark.parametrize("helper", RELATIVE_HELPERS)
def test_nested_path_uses_forward_slashes(helper) -> None:
    """A nested source path is emitted with '/' and never a backslash."""
    result = helper(NESTED, ROOT)
    assert result == "pipelines/domain_a_bronze/fg.yaml"
    assert "\\" not in result


@pytest.mark.parametrize(
    "helper",
    [
        pytest.param(blueprints_relative_source, id="blueprints"),
        pytest.param(_relative_path, id="project"),
    ],
)
def test_path_outside_root_still_has_no_backslash(helper) -> None:
    """The outside-root fallback (returns the full path) is POSIX too."""
    outside = PureWindowsPath(r"D:\elsewhere\other.yaml")
    result = helper(outside, ROOT)
    assert "\\" not in result
    assert result == "D:/elsewhere/other.yaml"
