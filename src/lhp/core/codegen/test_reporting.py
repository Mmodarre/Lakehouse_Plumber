"""Test-reporting hook generation, callable from a worker."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from lhp.models import FlowGroup, ProjectConfig
    from ..processing.substitution import EnhancedSubstitutionManager


logger = logging.getLogger(__name__)


def generate_test_reporting_hook(
    *,
    pipeline_name: str,
    flowgroups: List["FlowGroup"],
    output_dir: Path,
    project_config: "ProjectConfig",
    project_root: Path,
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

    On success the generator writes three files directly to disk:
      - ``<output_dir>/test_reporting_hook.py``
      - ``<output_dir>/test_reporting_providers/<provider_stem>.py``
      - ``<output_dir>/test_reporting_providers/__init__.py``

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
        include_tests: Caller's ``include_tests`` flag. Hook generation
            is skipped entirely when False.
        substitution_mgr: Optional substitution manager for the provider
            module copy step.

    Returns:
        Number of pipeline artifacts written (0 when no hook was generated).
        :class:`~lhp.core.coordination.processor.PipelineProcessor` consumes
        this count for its ``PipelineDelta.artifacts_count`` rollup.
    """
    if not include_tests:
        return 0

    if project_config is None or project_config.test_reporting is None:
        return 0

    # Deferred import: the generator pulls in Jinja / formatter machinery
    # that callers without a hook config never need.
    from .tst_reporting_hook_generator import TestReportingHookGenerator

    generator = TestReportingHookGenerator(project_config, project_root)
    content = generator.generate(
        processed_flowgroups=flowgroups,
        pipeline_name=pipeline_name,
        output_dir=output_dir,
        substitution_mgr=substitution_mgr,
    )

    if not content:
        # No flowgroup carried a test_id; nothing was written.
        return 0

    return 3
