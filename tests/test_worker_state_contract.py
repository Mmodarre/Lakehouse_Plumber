"""Worker-side state-manager contract: workers never receive a ProjectStateManager.

This file replaces ``test_pipeline_executor_no_state_in_workers.py`` (deleted as
part of the parallelisation refactor). The intent is unchanged: the worker entry
must accept only a ``state_dir: Path`` and ``build_state: bool``, never a
``StateManager`` / ``ProjectStateManager`` / cross-pipeline state object. The
invariant is checkable via ``__annotations__`` introspection on the public
worker symbols.
"""

from __future__ import annotations

import inspect
import typing
from pathlib import Path

from lhp.core.pipeline_executor import _process_pipeline_for_generate
from lhp.core.pipeline_processor import PipelineProcessor
from lhp.core.state.pipeline_state_manager import PipelineStateManager


FORBIDDEN_TYPES = {"ProjectStateManager", "StateManager"}


def _annotation_names(annotation) -> set:
    """Collect the string names of every type referenced by an annotation."""
    if annotation is inspect.Parameter.empty or annotation is None:
        return set()
    names: set = set()
    if isinstance(annotation, str):
        names.add(annotation)
        return names
    if hasattr(annotation, "__name__"):
        names.add(annotation.__name__)
    args = typing.get_args(annotation)
    for arg in args:
        names |= _annotation_names(arg)
    return names


class TestWorkerSignatureExcludesProjectStateManager:
    def test_process_pipeline_for_generate_signature(self):
        """No parameter on ``_process_pipeline_for_generate`` is annotated as
        a cross-pipeline state manager. Only ``state_dir: Path`` + ``build_state``
        are present for state plumbing.
        """
        sig = inspect.signature(_process_pipeline_for_generate)
        offenders = []
        for name, param in sig.parameters.items():
            ann_names = _annotation_names(param.annotation)
            if ann_names & FORBIDDEN_TYPES:
                offenders.append((name, ann_names))
        assert not offenders, (
            f"Worker entry must NOT accept a project-level state manager. "
            f"Offenders: {offenders}"
        )
        # Positive contract: state_dir + build_state are present.
        assert "state_dir" in sig.parameters
        assert "build_state" in sig.parameters

    def test_pipeline_processor_init_signature(self):
        """Same invariant on ``PipelineProcessor.__init__``."""
        sig = inspect.signature(PipelineProcessor.__init__)
        offenders = []
        for name, param in sig.parameters.items():
            if name == "self":
                continue
            ann_names = _annotation_names(param.annotation)
            if ann_names & FORBIDDEN_TYPES:
                offenders.append((name, ann_names))
        assert not offenders, (
            f"PipelineProcessor must NOT accept a project-level state manager. "
            f"Offenders: {offenders}"
        )
        assert "state_dir" in sig.parameters


class TestProcessorUsesPipelineStateManager:
    def test_processor_state_manager_attribute_typed_correctly(self, tmp_path):
        """When ``build_state=True`` the processor owns a PipelineStateManager.

        Constructs a minimal PipelineProcessor without invoking workers and
        verifies ``processor.state_manager`` is a :class:`PipelineStateManager`
        (or ``None`` when ``build_state=False`` — see the negative case below).
        """
        from lhp.core.pipeline_processor import ProcessingContext
        from unittest.mock import Mock

        ctx = ProcessingContext(
            processor=Mock(),
            code_generator=Mock(),
            formatter=Mock(),
            substitution_mgr=Mock(),
            include_tests=False,
        )
        pp = PipelineProcessor(
            pipeline_name="p1",
            environment="dev",
            output_dir=tmp_path / "out",
            state_dir=tmp_path / ".lhp_state",
            project_root=tmp_path,
            project_config=Mock(),
            context=ctx,
            build_state=True,
        )
        assert isinstance(pp.state_manager, PipelineStateManager)

    def test_processor_state_manager_none_when_no_state(self, tmp_path):
        """``build_state=False`` leaves ``processor.state_manager`` as None."""
        from lhp.core.pipeline_processor import ProcessingContext
        from unittest.mock import Mock

        ctx = ProcessingContext(
            processor=Mock(),
            code_generator=Mock(),
            formatter=Mock(),
            substitution_mgr=Mock(),
            include_tests=False,
        )
        pp = PipelineProcessor(
            pipeline_name="p1",
            environment="dev",
            output_dir=tmp_path / "out",
            state_dir=None,
            project_root=tmp_path,
            project_config=Mock(),
            context=ctx,
            build_state=False,
        )
        assert pp.state_manager is None
