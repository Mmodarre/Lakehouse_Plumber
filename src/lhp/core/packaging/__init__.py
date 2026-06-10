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
from .wheel_reader import (
    extract_wheel_py_modules,
    list_wheel_py_modules,
    locate_pipeline_wheel,
)

__all__ = [
    "PipelinePackager",
    "WheelPackageResult",
    "content_hash",
    "distribution_name",
    "extract_wheel_py_modules",
    "import_package_name",
    "list_wheel_py_modules",
    "locate_pipeline_wheel",
    "wheel_filename",
]
