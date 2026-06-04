"""Targeted unit test for ``ActionOrchestrator.format_output_tree``.

``format_output_tree`` is the thin coordination-layer delegate that runs the
single terminal ``ruff format`` pass over the generated output tree (B1
relocated it off the execution service's delta-generator and onto the
orchestrator). This test proves the delegate actually shells out to the bundled
ruff and rewrites a deliberately UNFORMATTED generated file in place — it does
NOT mock the subprocess (unlike ``tests/core/codegen/test_formatter.py``, which
unit-tests the underlying ``format_generated_tree`` against a mocked ruff). The
point here is the END-TO-END delegate: orchestrator method → real ruff → file
on disk becomes formatted.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from lhp.core.coordination.layers import build_facade_orchestrator

# A syntactically valid but deliberately UN-formatted module. ruff (pinned to
# line-length=88 / py311 / --isolated by the formatter) normalizes the spacing
# around ``=`` and collapses the runs of blank lines, so the post-format text is
# strictly different from this input.
_UNFORMATTED = "x   =   1\n\n\n\ny   =   2\n"
_RUFF_FORMATTED = "x = 1\n\n\ny = 2\n"


@pytest.mark.unit
def test_format_output_tree_formats_generated_files_in_place():
    """The delegate runs real ruff and rewrites an unformatted file in place."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        (project_root / "lhp.yaml").write_text(
            "name: fmt_delegate_proj\nversion: '1.0'\n", encoding="utf-8"
        )
        orchestrator = build_facade_orchestrator(project_root, enforce_version=False)

        output_dir = project_root / "generated" / "dev"
        output_dir.mkdir(parents=True)
        target = output_dir / "flowgroup.py"
        target.write_text(_UNFORMATTED, encoding="utf-8")

        # Pre-condition: the file is genuinely unformatted (so the assertion
        # below cannot pass vacuously).
        assert target.read_text(encoding="utf-8") == _UNFORMATTED

        orchestrator.format_output_tree(output_dir)

        assert target.read_text(encoding="utf-8") == _RUFF_FORMATTED
