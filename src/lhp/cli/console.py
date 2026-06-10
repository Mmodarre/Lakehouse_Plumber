"""Module-level Rich Console singletons for LHP CLI output.

Patched per-test by the autouse fixture in ``tests/conftest.py`` so that color
and width are deterministic.
"""

from rich.console import Console

console: Console = Console()
err_console: Console = Console(stderr=True)
