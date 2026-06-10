"""Thin CLI surface test: the ``--max-workers`` flag exists on both commands.

This is the ONLY genuinely CLI-layer slice of the former
``tests/test_max_workers_wiring.py``: that the ``generate`` and ``validate``
Click command objects expose ``--max-workers`` so the parsed value can reach the
facade. The actual worker-pool resolution (env var, clamps, auto-detect) is
engine behavior and lives in
``tests/core/coordination/test_max_workers_wiring.py``.

The command objects are imported directly (``lhp.cli.commands.*``), mirroring
the rest of ``tests/cli/``. The old class-based ``GenerateCommand.execute`` /
``ValidateCommand.execute`` signature probes were dropped with those classes.
"""

from __future__ import annotations

import pytest

from lhp.cli.commands.generate_command import generate
from lhp.cli.commands.validate_command import validate_command


@pytest.mark.unit
class TestCLIMaxWorkersFlag:
    def test_generate_has_max_workers_option(self):
        """The ``lhp generate`` Click command exposes --max-workers."""
        flag_names = {p.name for p in generate.params if hasattr(p, "name")}
        assert "max_workers" in flag_names

    def test_validate_has_max_workers_option(self):
        """The ``lhp validate`` Click command exposes --max-workers."""
        flag_names = {p.name for p in validate_command.params if hasattr(p, "name")}
        assert "max_workers" in flag_names
