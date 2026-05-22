"""Module-level Rich Console singletons for LHP CLI output.

- ``console`` writes to stdout for primary user output.
- ``err_console`` writes to stderr for errors and warnings.

Both honor Rich's TTY detection and the ``NO_COLOR`` env var. For tests, the
``lhp.cli.console`` module is patched per-test by the autouse fixture in
``tests/conftest.py`` so that color and width are deterministic.
"""

from rich.console import Console

console: Console = Console()
err_console: Console = Console(stderr=True)
