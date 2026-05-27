"""Pickle invariants for :mod:`tests.fakes`.

The fakes in :mod:`tests.fakes` exist so that worker-boundary tests can run
under :class:`concurrent.futures.ProcessPoolExecutor`, which serialises
``initargs`` and arguments via :mod:`pickle`. ``unittest.mock.MagicMock``
fails that boundary; the fakes here must succeed.

Each test below confirms a fresh instance of one fake survives a
``pickle.dumps`` / ``pickle.loads`` round-trip with its public surface
intact. If a fake later acquires a non-picklable attribute, this file is the
canary.
"""

from __future__ import annotations

import pickle

import pytest

from tests.fakes import (
    CallRecord,
    FakeCodeFormatter,
    FakeCodeGenerator,
    FakeFlowgroupResolutionService,
    FakeProjectConfig,
    FakeSubstitutionManager,
    FakeTemplate,
    FakeTemplateEngine,
)


@pytest.mark.unit
class TestFakesPickle:
    """Every fake is picklable when freshly constructed."""

    def test_call_record_pickles(self) -> None:
        original = CallRecord(args=("hello",), kwargs={"x": 1})
        restored = pickle.loads(pickle.dumps(original))
        assert restored.args == ("hello",)
        assert restored.kwargs == {"x": 1}

    def test_fake_flowgroup_processor_pickles(self) -> None:
        fake = FakeFlowgroupResolutionService()
        restored = pickle.loads(pickle.dumps(fake))
        assert isinstance(restored, FakeFlowgroupResolutionService)
        assert restored.calls == []

    def test_fake_flowgroup_processor_pickles_with_call_history(self) -> None:
        fake = FakeFlowgroupResolutionService()
        fake.process_flowgroup("fg", env="dev", include_tests=False)
        restored = pickle.loads(pickle.dumps(fake))
        assert len(restored.calls) == 1
        assert restored.calls[0].args == ("fg",)
        assert restored.calls[0].kwargs == {"env": "dev", "include_tests": False}

    def test_fake_substitution_manager_pickles(self) -> None:
        fake = FakeSubstitutionManager(env="prod", skip_validation=True)
        restored = pickle.loads(pickle.dumps(fake))
        assert restored.env == "prod"
        assert restored.skip_validation is True
        assert restored.secret_references == []
        assert restored.mappings == {}

    def test_fake_template_pickles(self) -> None:
        fake = FakeTemplate(presets=["bronze_defaults"], actions=[])
        restored = pickle.loads(pickle.dumps(fake))
        assert restored.presets == ["bronze_defaults"]
        assert restored.actions == []

    def test_fake_template_engine_pickles(self) -> None:
        fake = FakeTemplateEngine(template=FakeTemplate(), rendered_actions=[])
        restored = pickle.loads(pickle.dumps(fake))
        assert isinstance(restored, FakeTemplateEngine)
        assert restored.get_template("anything") is not None

    def test_fake_code_generator_pickles(self) -> None:
        restored = pickle.loads(pickle.dumps(FakeCodeGenerator()))
        assert isinstance(restored, FakeCodeGenerator)

    def test_fake_code_formatter_pickles(self) -> None:
        restored = pickle.loads(pickle.dumps(FakeCodeFormatter()))
        assert isinstance(restored, FakeCodeFormatter)

    def test_fake_project_config_pickles(self) -> None:
        fake = FakeProjectConfig(name="proj", test_reporting={"enabled": True})
        restored = pickle.loads(pickle.dumps(fake))
        assert restored.name == "proj"
        assert restored.test_reporting == {"enabled": True}
