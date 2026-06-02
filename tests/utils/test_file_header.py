"""Tests for :mod:`lhp.utils.file_header`.

Covers the cross-platform-determinism guarantees of the file-header helpers:

- :func:`helper_header_path` must always emit a POSIX (forward-slash) path in
  the ``# LHP-SOURCE:`` header, regardless of the OS-native separator of the
  host's :class:`pathlib.Path`. The helper composes
  ``Path(module_path).parent / rel_path`` and the bug is that the pre-fix copier
  used ``str(...)`` on that composition, which emits backslashes on Windows.

  On a POSIX host this is subtle: ``Path(...)`` resolves to ``PosixPath`` and a
  ``PosixPath`` join re-homes the segments of *any* ``rel_path`` (even a
  :class:`PureWindowsPath`) into POSIX form, so ``str()`` and ``as_posix()``
  agree and a direct call cannot tell the fixed code from the pre-fix code. To
  make the regression observable on macOS, the test rebinds the module-level
  ``Path`` name to :class:`PureWindowsPath` for the duration of the call, which
  faithfully simulates the Windows host where the bug occurs: the pre-fix
  ``str()`` would then surface backslashes, while the fixed ``.as_posix()`` does
  not.
- :func:`write_normalized` is the single funnel for LHP-generated files: it
  must normalize line endings to ``\\n``, strip trailing whitespace per line,
  ensure a trailing newline, and always write UTF-8.
"""

from pathlib import Path, PureWindowsPath

import pytest

import lhp.utils.file_header as file_header
from lhp.utils.file_header import helper_header_path, write_normalized


@pytest.mark.unit
def test_helper_header_path_renders_posix_separators_on_windows_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The header path is forward-slash even when the host ``Path`` is Windows.

    The helper composes ``Path(module_path).parent / rel_path``. On a Windows
    host ``Path`` is ``WindowsPath`` and ``str(...)`` of that composition yields
    backslash separators; the fix routes the result through ``.as_posix()`` so
    the emitted ``# LHP-SOURCE:`` header is byte-identical across platforms.

    macOS cannot instantiate a ``WindowsPath``, and a ``PosixPath`` join
    re-homes a ``PureWindowsPath`` tail into POSIX form -- so calling the helper
    directly on macOS renders forward slashes under *both* the fixed
    ``.as_posix()`` and the pre-fix ``str()``, making a direct call a tautology
    that proves nothing. Rebinding the module's ``Path`` to ``PureWindowsPath``
    simulates the Windows host: under the pre-fix ``str()`` body the result
    would contain backslashes (this assertion would go red); the fixed
    ``.as_posix()`` body keeps it forward-slash (green).
    """
    monkeypatch.setattr(file_header, "Path", PureWindowsPath)

    result = helper_header_path(
        "py_functions/entry.py", PureWindowsPath("sub/pkg/helper.py")
    )

    # Guard: under the simulated Windows host, the pre-fix str() composition --
    # exactly Path(module_path).parent / rel_path with Path == PureWindowsPath --
    # genuinely renders backslashes; this is the byte the fix must eliminate.
    pre_fix_composition = PureWindowsPath(
        "py_functions/entry.py"
    ).parent / PureWindowsPath("sub/pkg/helper.py")
    assert str(pre_fix_composition) == "py_functions\\sub\\pkg\\helper.py"

    assert "/" in result
    assert "\\" not in result
    assert result == "py_functions/sub/pkg/helper.py"


@pytest.mark.unit
def test_helper_header_path_posix_rel_path_unchanged() -> None:
    """A plain POSIX ``rel_path`` joins under the entry's directory verbatim."""
    result = helper_header_path("py_functions/entry.py", Path("a/b/c.py"))

    assert result == "py_functions/a/b/c.py"
    assert "\\" not in result


@pytest.mark.unit
def test_write_normalized_converts_crlf_and_strips_trailing_ws(tmp_path: Path) -> None:
    """CRLF / CR line endings collapse to ``\\n`` and trailing whitespace drops."""
    target = tmp_path / "out.py"

    write_normalized(target, "a = 1   \r\nb = 2\t\rc = 3")

    # Read raw bytes so the on-disk line endings are asserted exactly.
    raw = target.read_bytes()
    assert b"\r" not in raw
    assert raw == b"a = 1\nb = 2\nc = 3\n"


@pytest.mark.unit
def test_write_normalized_writes_utf8(tmp_path: Path) -> None:
    """Non-ASCII content is always written as UTF-8, not the platform locale."""
    target = tmp_path / "out.py"

    write_normalized(target, 'note = "em dash — here"')

    raw = target.read_bytes()
    # U+2014 EM DASH encodes to the 3-byte UTF-8 sequence e2 80 94.
    assert b"\xe2\x80\x94" in raw
    assert raw.decode("utf-8") == 'note = "em dash — here"\n'
