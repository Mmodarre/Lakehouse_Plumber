"""Test-reporting hook generation, callable from a worker."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from lhp.models import FlowGroup, ProjectConfig

    from ..processing.substitution import EnhancedSubstitutionManager
    from .tst_reporting_hook_generator import TestReportingHookGenerator


logger = logging.getLogger(__name__)


def _make_generator(
    *,
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    include_tests: bool,
) -> Optional["TestReportingHookGenerator"]:
    """Build the generator, or return None when the hook guards fail.

    Shared by :func:`generate_test_reporting_hook` and
    :func:`build_test_reporting_hook_files`. Encodes the first two guards:

      1. ``include_tests`` must be True — the hook references test-result
         tables that only exist when test actions are emitted.
      2. ``project_config.test_reporting`` must be set; otherwise there is no
         provider configuration to wire the hook against.

    The third guard (no flowgroup carried a ``test_id``) lives in the generator
    itself, which returns ``None`` in that case.
    """
    if not include_tests:
        return None

    if project_config is None or project_config.test_reporting is None:
        return None

    # Deferred import: the generator pulls in Jinja / formatter machinery
    # that callers without a hook config never need.
    from .tst_reporting_hook_generator import TestReportingHookGenerator

    return TestReportingHookGenerator(project_config, project_root)


def build_test_reporting_hook_files(
    *,
    pipeline_name: str,
    flowgroups: List["FlowGroup"],
    project_config: Optional["ProjectConfig"],
    project_root: Path,
    include_tests: bool,
    substitution_mgr: Optional["EnhancedSubstitutionManager"] = None,
) -> Optional[Dict[str, str]]:
    """Build the per-pipeline test-reporting hook's files IN MEMORY, if configured.

    Mirrors :func:`generate_test_reporting_hook`'s signature minus ``output_dir``
    and the writing: applies the same three guards (``include_tests``,
    ``test_reporting`` present, ≥1 ``test_id``) and returns the same three files
    :func:`generate_test_reporting_hook` would write, keyed by their
    source-relative path::

        {
            "_test_reporting_hook.py": <hook content>,
            "test_reporting_providers/__init__.py": "",
            "test_reporting_providers/<provider_stem>.py": <provider content>,
        }

    The content is PRE-normalization (exactly what source mode passes to
    ``write_normalized``), so the wheel-mode caller (task C2b) can split this
    dict into its two packager buckets and get byte-identical members.

    Returns ``None`` when the hook is not configured or no flowgroup carries a
    ``test_id``. Does NOT touch disk. May raise ``LHPError`` on a duplicate
    ``test_id`` target or a missing provider module / config file (same as
    :func:`generate_test_reporting_hook`).
    """
    generator = _make_generator(
        project_config=project_config,
        project_root=project_root,
        include_tests=include_tests,
    )
    if generator is None:
        return None

    return generator.build_hook_files(
        processed_flowgroups=flowgroups,
        pipeline_name=pipeline_name,
        substitution_mgr=substitution_mgr,
    )


def generate_test_reporting_hook(
    *,
    pipeline_name: str,
    flowgroups: List["FlowGroup"],
    output_dir: Path,
    project_config: Optional["ProjectConfig"],
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
      - ``<output_dir>/_test_reporting_hook.py``
      - ``<output_dir>/test_reporting_providers/<provider_stem>.py``
      - ``<output_dir>/test_reporting_providers/__init__.py``

    Returns:
        Number of pipeline artifacts written (0 when no hook was generated).
        The per-pipeline commit step (:mod:`~lhp.core.coordination._commit`)
        consumes this count for its ``PipelineDelta.artifacts_count`` rollup.
    """
    generator = _make_generator(
        project_config=project_config,
        project_root=project_root,
        include_tests=include_tests,
    )
    if generator is None:
        return 0

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
