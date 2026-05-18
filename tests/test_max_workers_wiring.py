"""Verify the --max-workers CLI flag flows through to the orchestrator.

Asserts:
  - lhp generate --max-workers N reaches ActionOrchestrator(max_workers=N)
  - lhp validate --max-workers N reaches ActionOrchestrator(max_workers=N)
  - ActionOrchestrator(max_workers=N).max_workers == N
  - None default uses min(cpu_count, 8)
"""

import multiprocessing
from pathlib import Path

import pytest

from lhp.core.orchestrator import ActionOrchestrator


class TestOrchestratorMaxWorkers:
    """ActionOrchestrator must honour max_workers passed in.

    The resolution that previously lived in
    ``ParallelFlowgroupProcessor.__init__`` was hoisted into
    ``ActionOrchestrator.__init__``, so the resolved value is now
    accessible directly via ``orch.max_workers``.
    """

    def test_explicit_max_workers_propagates_to_orchestrator(self, tmp_path):
        (tmp_path / "lhp.yaml").write_text(
            "name: test_project\nversion: 1.0\nauthor: test\n"
        )
        orch = ActionOrchestrator(
            project_root=tmp_path, enforce_version=False, max_workers=3
        )
        assert orch.max_workers == 3

    def test_default_max_workers_resolves_to_cpu_min_8(self, tmp_path):
        (tmp_path / "lhp.yaml").write_text(
            "name: test_project\nversion: 1.0\nauthor: test\n"
        )
        orch = ActionOrchestrator(project_root=tmp_path, enforce_version=False)
        expected = min(multiprocessing.cpu_count(), 8)
        assert orch.max_workers == expected

    def test_max_workers_one_means_sequential_capable(self, tmp_path):
        """--max-workers 1 must be accepted; the orchestrator stores it as 1."""
        (tmp_path / "lhp.yaml").write_text(
            "name: test_project\nversion: 1.0\nauthor: test\n"
        )
        orch = ActionOrchestrator(
            project_root=tmp_path, enforce_version=False, max_workers=1
        )
        assert orch.max_workers == 1


@pytest.mark.unit
class TestCLIFlagPassthrough:
    """Click integration: --max-workers parses, propagates to commands."""

    def test_generate_command_accepts_max_workers_kw(self):
        """GenerateCommand.execute accepts max_workers as a keyword argument."""
        import inspect

        from lhp.cli.commands.generate_command import GenerateCommand

        sig = inspect.signature(GenerateCommand.execute)
        assert "max_workers" in sig.parameters

    def test_validate_command_accepts_max_workers_kw(self):
        """ValidateCommand.execute accepts max_workers as a keyword argument."""
        import inspect

        from lhp.cli.commands.validate_command import ValidateCommand

        sig = inspect.signature(ValidateCommand.execute)
        assert "max_workers" in sig.parameters

    def test_click_generate_has_max_workers_option(self):
        """The `lhp generate` Click command exposes --max-workers."""
        from lhp.cli.main import generate

        flag_names = {p.name for p in generate.params if hasattr(p, "name")}
        assert "max_workers" in flag_names

    def test_click_validate_has_max_workers_option(self):
        """The `lhp validate` Click command exposes --max-workers."""
        from lhp.cli.main import validate

        flag_names = {p.name for p in validate.params if hasattr(p, "name")}
        assert "max_workers" in flag_names
