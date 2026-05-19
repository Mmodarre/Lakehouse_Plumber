"""Test-reporting hook generation, callable from a worker.

Takes a worker-local :class:`PipelineStateManager` (env + pipeline
baked in) and calls the 2-arg
``track_pipeline_artifact(path, artifact_type)`` shape directly.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from ...models.config import FlowGroup, ProjectConfig
    from ...utils.smart_file_writer import SmartFileWriter
    from ...utils.substitution import EnhancedSubstitutionManager
    from ..state.pipeline_state_manager import PipelineStateManager


logger = logging.getLogger(__name__)


def generate_test_reporting_hook(
    *,
    pipeline_name: str,
    flowgroups: List["FlowGroup"],
    output_dir: Path,
    project_config: "ProjectConfig",
    project_root: Path,
    state_manager: Optional["PipelineStateManager"],
    smart_writer: "SmartFileWriter",
    include_tests: bool,
    substitution_mgr: Optional["EnhancedSubstitutionManager"] = None,
) -> int:
    """Generate the per-pipeline test reporting hook artifact, if configured.

    Guards (in order):

      1. ``include_tests`` must be True — the hook references test-result
         tables that only exist when test actions are emitted.
      2. ``project_config.test_reporting`` must be set; otherwise there
         is no provider configuration to wire the hook against.
      3. :meth:`TestReportingHookGenerator.generate` may still return
         ``None`` (no flowgroup carried a ``test_id``); that is treated
         as "nothing to track" — returns 0.

    On success the generator writes three files via ``smart_writer``:
      - ``<output_dir>/test_reporting_hook.py``
      - ``<output_dir>/test_reporting_providers/<provider_stem>.py``
      - ``<output_dir>/test_reporting_providers/__init__.py``

    When ``state_manager`` is non-None we register all three as pipeline
    artifacts so the next ``lhp generate`` knows they belong to this
    pipeline (and the cleanup service won't treat them as orphans).

    Args:
        pipeline_name: Pipeline this hook is for. Embedded in the hook
            header and used as the artifact's pipeline scope.
        flowgroups: Already-processed flowgroups for the pipeline; the
            generator walks them to build the ``test_id`` map.
        output_dir: Pipeline output directory. Hook + providers land here.
        project_config: Project config; ``project_config.test_reporting``
            decides whether anything is emitted at all.
        project_root: Project root (forwarded to the generator for
            template/provider resolution).
        state_manager: Worker-local :class:`PipelineStateManager` (or None).
            When None, artifacts are still written but not tracked —
            equivalent to today's ``--no-state`` flag.
        smart_writer: Per-pipeline :class:`SmartFileWriter`. The generator
            calls ``write_if_changed`` on it.
        include_tests: Caller's ``include_tests`` flag. Hook generation
            is skipped entirely when False.
        substitution_mgr: Optional substitution manager for the provider
            module copy step.

    Returns:
        Number of pipeline artifacts registered with ``state_manager``
        (0 when no hook was generated, OR when ``state_manager`` is None).
        :class:`~lhp.core.pipeline_processor.PipelineProcessor` consumes
        this count for its ``PipelineDelta.artifacts_count`` rollup.
    """
    if not include_tests:
        return 0

    if project_config is None or project_config.test_reporting is None:
        return 0

    # Deferred import: the generator pulls in Jinja / formatter machinery
    # that callers without a hook config never need.
    from .tst_reporting_hook_generator import HOOK_FILENAME, TestReportingHookGenerator

    generator = TestReportingHookGenerator(project_config, project_root)
    content = generator.generate(
        processed_flowgroups=flowgroups,
        pipeline_name=pipeline_name,
        output_dir=output_dir,
        smart_writer=smart_writer,
        substitution_mgr=substitution_mgr,
    )

    if not content:
        # No flowgroup carried a test_id; nothing was written.
        return 0

    if state_manager is None:
        # Skip artifact tracking when state is disabled.
        return 0

    config = project_config.test_reporting
    provider_stem = Path(config.module_path).stem

    state_manager.track_pipeline_artifact(
        output_dir / HOOK_FILENAME,
        "test_reporting_hook",
    )
    state_manager.track_pipeline_artifact(
        output_dir / "test_reporting_providers" / f"{provider_stem}.py",
        "test_reporting_provider",
    )
    state_manager.track_pipeline_artifact(
        output_dir / "test_reporting_providers" / "__init__.py",
        "test_reporting_init",
    )
    return 3
