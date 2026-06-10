"""Deterministic per-pipeline wheel packaging.

Pure identity math (``identity``), a byte-reproducible archive assembler
(``wheel_builder``), the runner renderer (``runner``), and the composing
:class:`PipelinePackager` seam with its :class:`WheelPackageResult` transport.
See ``TARGET_ARCHITECTURE.md`` §5bis and ``WHEEL_PACKAGING_SPEC.md``.
"""

from __future__ import annotations

from .identity import (
    content_hash,
    distribution_name,
    import_package_name,
    wheel_filename,
)
from .packager import PipelinePackager, WheelPackageResult

__all__ = [
    "PipelinePackager",
    "WheelPackageResult",
    "content_hash",
    "distribution_name",
    "import_package_name",
    "wheel_filename",
]
