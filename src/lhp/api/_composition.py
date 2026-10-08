"""Shared internal construction for the canonical API service graph."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional


def build_api_service_graph(
    project_root: Path,
    *,
    pipeline_config_path: Optional[str],
    enforce_version: bool,
    max_workers: Optional[int],
    no_cache: bool,
) -> Any:
    """Build the same registered graph for the facade and editor composition.

    ``Any`` is internal because core's volatile coordinator type cannot occur
    in the versioned public API annotation (constitution §1.10/§4.8).

    :stability: provisional
    """
    from lhp.core.coordination.layers import build_facade_orchestrator
    from lhp.generators.registration import register_all

    register_all()
    return build_facade_orchestrator(
        project_root,
        pipeline_config_path=pipeline_config_path,
        enforce_version=enforce_version,
        max_workers=max_workers,
        no_cache=no_cache,
    )
