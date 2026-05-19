"""Initializer-pattern contract: state populates via initargs and workers read it.

Companion to ``tests/test_pipeline_delta_serialization.py`` — that test pins
the worker-return-path pickle invariant; this one pins the worker-arg-path
invariant (``initargs`` carry the captured state, and the state dataclasses
themselves pickle across the spawn boundary).
"""

import pickle
from pathlib import Path

import pytest

from lhp.core import pipeline_executor as pe
from lhp.core.pipeline_executor import (
    _GenerateWorkerState,
    _ValidateWorkerState,
    _init_generate_worker,
    _init_validate_worker,
)
from tests.fakes import (
    FakeCodeFormatter,
    FakeCodeGenerator,
    FakeFlowgroupProcessor,
    FakeProjectConfig,
    FakeSubstitutionManager,
)


@pytest.fixture(autouse=True)
def _isolate_module_state():
    """Each test gets fresh ``_validate_state`` / ``_generate_state`` globals.

    Worker-state globals leak across tests under in-process pytest if not
    reset — the initializer is module-level, so tests share the binding.
    """
    pe._validate_state = None
    pe._generate_state = None
    try:
        yield
    finally:
        pe._validate_state = None
        pe._generate_state = None


@pytest.mark.unit
class TestValidateWorkerStateContract:
    def test_state_pickles_across_spawn_boundary(self):
        """``_ValidateWorkerState`` round-trips through pickle.

        The ``ProcessPoolExecutor`` ``initargs=`` seam pickles each arg in
        the main process and unpickles in each worker. Any non-picklable
        field would surface here.
        """
        state = _ValidateWorkerState(
            processor=FakeFlowgroupProcessor(),
            substitution_managers={"p1": FakeSubstitutionManager()},
            include_tests=False,
        )
        loaded = pickle.loads(pickle.dumps(state))
        assert loaded.include_tests is False
        assert "p1" in loaded.substitution_managers
        assert isinstance(loaded.processor, FakeFlowgroupProcessor)

    def test_initializer_populates_module_state(self):
        """``_init_validate_worker`` assigns ``pe._validate_state``."""
        state = _ValidateWorkerState(
            processor=FakeFlowgroupProcessor(),
            substitution_managers={},
            include_tests=True,
        )
        assert pe._validate_state is None
        _init_validate_worker(0, state)
        assert pe._validate_state is state

    def test_state_is_frozen(self):
        """Worker state mutation is a bug — frozen dataclass surfaces it."""
        state = _ValidateWorkerState(
            processor=FakeFlowgroupProcessor(),
            substitution_managers={},
            include_tests=True,
        )
        with pytest.raises(Exception):  # FrozenInstanceError, but exact type isn't load-bearing
            state.include_tests = False  # type: ignore[misc]


@pytest.mark.unit
class TestGenerateWorkerStateContract:
    def test_state_pickles_across_spawn_boundary(self, tmp_path):
        """``_GenerateWorkerState`` round-trips through pickle."""
        state = _GenerateWorkerState(
            processor=FakeFlowgroupProcessor(),
            code_generator=FakeCodeGenerator(),
            formatter=FakeCodeFormatter(),
            substitution_managers={"p1": FakeSubstitutionManager()},
            pipeline_output_dirs={"p1": tmp_path / "p1"},
            project_config=FakeProjectConfig(),
            blueprint_provenance=None,
            environment="prod",
            state_dir=tmp_path / ".lhp_state",
            project_root=tmp_path,
            include_tests=False,
            build_state=True,
        )
        loaded = pickle.loads(pickle.dumps(state))
        assert loaded.environment == "prod"
        assert loaded.build_state is True
        assert loaded.include_tests is False
        assert loaded.project_root == tmp_path

    def test_initializer_populates_module_state(self, tmp_path):
        """``_init_generate_worker`` assigns ``pe._generate_state``."""
        state = _GenerateWorkerState(
            processor=FakeFlowgroupProcessor(),
            code_generator=FakeCodeGenerator(),
            formatter=FakeCodeFormatter(),
            substitution_managers={},
            pipeline_output_dirs={},
            project_config=FakeProjectConfig(),
            blueprint_provenance=None,
            environment="dev",
            state_dir=None,
            project_root=tmp_path,
            include_tests=True,
            build_state=False,
        )
        assert pe._generate_state is None
        _init_generate_worker(0, state)
        assert pe._generate_state is state

    def test_state_is_frozen(self, tmp_path):
        """Worker state mutation is a bug — frozen dataclass surfaces it."""
        state = _GenerateWorkerState(
            processor=FakeFlowgroupProcessor(),
            code_generator=FakeCodeGenerator(),
            formatter=FakeCodeFormatter(),
            substitution_managers={},
            pipeline_output_dirs={},
            project_config=FakeProjectConfig(),
            blueprint_provenance=None,
            environment="dev",
            state_dir=None,
            project_root=tmp_path,
            include_tests=True,
            build_state=False,
        )
        with pytest.raises(Exception):
            state.environment = "prod"  # type: ignore[misc]


@pytest.mark.unit
class TestWorkerEntryPointContract:
    def test_validate_one_fg_fails_loudly_when_initializer_did_not_run(self):
        """Worker raises rather than silently using a stale or empty state.

        The plan calibration is explicit: don't add a defensive ``if state
        is None`` fallback. An AssertionError (or AttributeError under
        ``python -O``) surfaces an initializer-wiring bug.
        """
        from lhp.models.config import FlowGroup, FlowGroupContext

        fg = FlowGroup(
            pipeline="p1",
            flowgroup="fg_a",
            actions=[],
        )
        ctx = FlowGroupContext(flowgroup=fg, source_yaml=None)
        assert pe._validate_state is None

        # No initializer run → state is None → assert fires.
        with pytest.raises((AssertionError, AttributeError)):
            pe._validate_one_fg(ctx)

    def test_generate_one_pipeline_fails_loudly_when_initializer_did_not_run(self):
        """Mirror of the validate-side contract."""
        assert pe._generate_state is None
        with pytest.raises((AssertionError, AttributeError)):
            pe._generate_one_pipeline("p1", [])
