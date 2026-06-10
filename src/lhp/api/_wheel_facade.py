"""Private module — implementation of the public :class:`WheelFacade`.

Underscore-prefixed: not part of the import surface; external callers
MUST import :class:`WheelFacade` from :mod:`lhp.api` (re-exported via
:mod:`lhp.api.facade`). Wraps the primitive-returning core reader
:mod:`lhp.core.packaging.wheel_reader` and converts its tuples/paths
into the frozen public DTOs (:class:`WheelContentsView`,
:class:`WheelModuleView`, :class:`WheelExtractionResult`). The core
reader MUST NOT import the public API (``core -> api`` is a forbidden
upward edge), so primitive-to-DTO conversion is the facade's job.

:stability: internal
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from lhp.api.responses import WheelExtractionResult
from lhp.api.views import WheelContentsView, WheelModuleView

if TYPE_CHECKING:
    # Internal orchestrator type, referenced only as a quoted annotation
    # below; never named directly in the public API surface (§1.10, §9.13).
    _Orchestrator = Any


class WheelFacade:
    """Inspection / extraction operations on a BUILT per-pipeline wheel.

    Two public methods, both returning frozen DTOs:

    - ``list_modules`` — enumerate the ``.py`` members of a wheel.
    - ``extract_modules`` — extract the ``.py`` members to a directory.

    Both accept the wheel via exactly one selector form: either a direct
    ``wheel_path`` OR a ``pipeline`` (which requires ``env`` so the
    facade can locate the single wheel LHP built under
    ``generated/<env>/_wheels/<pipeline>/dist/``). Supplying both or
    neither — or ``pipeline`` without ``env`` — is programmer misuse and
    raises :class:`ValueError`.

    :stability: provisional
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _resolve_selector(
        wheel_path: Optional[Path],
        pipeline: Optional[str],
        env: Optional[str],
    ) -> None:
        """Validate the "exactly one form" selector contract (programmer misuse).

        Exactly one of ``wheel_path`` or ``pipeline`` must be supplied;
        when ``pipeline`` is given, ``env`` is required. Raises
        :class:`ValueError` on any violation — this is API misuse, not a
        domain failure, so it carries no structured ``LHP-*`` code (the
        codebase convention for invalid-argument combinations; see
        ``core/coordination/orchestrator.py``).
        """
        if (wheel_path is None) == (pipeline is None):
            raise ValueError(
                "Provide exactly one of `wheel_path` or `pipeline` "
                "(got both or neither)."
            )
        if pipeline is not None and env is None:
            raise ValueError("`env` is required when `pipeline` is provided.")

    def list_modules(
        self,
        *,
        wheel_path: Optional[Path] = None,
        pipeline: Optional[str] = None,
        env: Optional[str] = None,
    ) -> WheelContentsView:
        """List the ``.py`` modules packaged inside a built wheel.

        Supply the wheel via exactly one selector form: a direct
        ``wheel_path``, or a ``pipeline`` + ``env`` pair (which locates
        the single wheel built under
        ``generated/<env>/_wheels/<pipeline>/dist/``). In path-mode the
        returned view's ``pipeline`` / ``env`` are ``None``.

        :stability: provisional
        :raises ValueError: if the selector contract is violated — both
            or neither of ``wheel_path`` / ``pipeline``, or ``pipeline``
            without ``env``.
        :raises lhp.errors.LHPError: ``LHP-IO-022`` (wheel file not
            found), ``LHP-IO-023`` (not a ``.whl`` file), ``LHP-IO-024``
            (corrupt archive), and — in pipeline mode — ``LHP-GEN-001``
            when zero or more than one wheel is built for the pipeline.
        """
        from lhp.core.packaging import wheel_reader

        self._resolve_selector(wheel_path, pipeline, env)
        if wheel_path is not None:
            resolved = wheel_path
        else:
            # ``_resolve_selector`` guarantees both are set in this branch;
            # the asserts narrow ``Optional[str]`` -> ``str`` for the call.
            assert pipeline is not None and env is not None
            resolved = wheel_reader.locate_pipeline_wheel(
                self._orchestrator.project_root, pipeline, env
            )

        members = wheel_reader.list_wheel_py_modules(resolved)
        modules = tuple(
            WheelModuleView(arcname=arcname, size_bytes=size)
            for arcname, size in members
        )
        return WheelContentsView(
            wheel_path=resolved,
            pipeline=pipeline,
            env=env,
            modules=modules,
            module_count=len(modules),
        )

    def extract_modules(
        self,
        output_dir: Path,
        *,
        wheel_path: Optional[Path] = None,
        pipeline: Optional[str] = None,
        env: Optional[str] = None,
    ) -> WheelExtractionResult:
        """Extract the ``.py`` modules from a built wheel into ``output_dir``.

        Supply the wheel via exactly one selector form: a direct
        ``wheel_path``, or a ``pipeline`` + ``env`` pair (which locates
        the single wheel built under
        ``generated/<env>/_wheels/<pipeline>/dist/``). The in-wheel
        directory structure is preserved; ``output_dir`` (and parents)
        is created if missing.

        :stability: provisional
        :raises ValueError: if the selector contract is violated — both
            or neither of ``wheel_path`` / ``pipeline``, or ``pipeline``
            without ``env``.
        :raises lhp.errors.LHPError: ``LHP-IO-022`` (wheel file not
            found), ``LHP-IO-023`` (not a ``.whl`` file), ``LHP-IO-024``
            (corrupt archive), ``LHP-IO-005`` (permission denied writing
            a destination under ``output_dir``), and — in pipeline mode —
            ``LHP-GEN-001`` when zero or more than one wheel is built for
            the pipeline.
        """
        from lhp.core.packaging import wheel_reader

        self._resolve_selector(wheel_path, pipeline, env)
        if wheel_path is not None:
            resolved = wheel_path
        else:
            # ``_resolve_selector`` guarantees both are set in this branch;
            # the asserts narrow ``Optional[str]`` -> ``str`` for the call.
            assert pipeline is not None and env is not None
            resolved = wheel_reader.locate_pipeline_wheel(
                self._orchestrator.project_root, pipeline, env
            )

        written = wheel_reader.extract_wheel_py_modules(resolved, output_dir)
        return WheelExtractionResult(
            wheel_path=resolved,
            output_dir=output_dir,
            written_paths=written,
            written_count=len(written),
        )
