"""Project scaffolding — separate from the runtime facade per §1.12.

``LakehousePlumberBootstrap`` performs one-time project initialization
(creating ``lhp.yaml``, the directory tree, optional Asset Bundle
files). Construction is parameter-free; no project root is required.

:stability: provisional
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Tuple

from lhp.api.responses import InitProjectResult

logger = logging.getLogger(__name__)


class LakehousePlumberBootstrap:
    """Public entry point for one-time project scaffolding.

    Separated from :class:`LakehousePlumberApplicationFacade` because
    scaffolding produces a project root rather than consuming one.
    Stateless — the constructor takes no arguments.

    :stability: stable
    """

    def __init__(self) -> None:
        return

    def init_project(
        self,
        target_dir: Path,
        *,
        bundle: bool = True,
        project_name: Optional[str] = None,
    ) -> InitProjectResult:
        """Scaffold a new LHP project at ``target_dir``.

        Args:
            target_dir: Directory to create the project in. If the
                directory exists and is non-empty, the call returns an
                :class:`InitProjectResult` with ``success=False``
                rather than raising. The directory is created if
                absent.
            bundle: If True, also generate Databricks Asset Bundle
                scaffolding (``databricks.yml`` + ``bundle/`` resource
                dir).
            project_name: Explicit project name to bake into template
                substitutions (``lhp.yaml``, ``databricks.yml``, etc.).
                Defaults to ``target_dir.name`` when omitted — the
                historical behaviour for callers that scaffold into a
                directory whose name already matches the project name.

        Returns:
            A frozen :class:`InitProjectResult` describing the outcome.

        :raises: None — :class:`LHPError` and :class:`OSError` failures
            during scaffolding are caught and surfaced on the result's
            ``error_message`` / ``error_code`` fields (§4.8).
        """
        # Lazy imports keep this module cheap to import from
        # ``lhp.api`` even when the caller never invokes
        # ``init_project``.
        from lhp.core.loaders.init_template_context import InitTemplateContext
        from lhp.core.loaders.init_template_loader import InitTemplateLoader
        from lhp.errors.types import LHPError

        target_dir = Path(target_dir).resolve()
        resolved_project_name = project_name or target_dir.name
        logger.debug(
            f"Bootstrap init_project: target={target_dir} bundle={bundle} "
            f"project_name={resolved_project_name}"
        )

        # Reject non-empty existing directory before mutating anything.
        if target_dir.exists() and any(target_dir.iterdir()):
            logger.debug(f"Refusing to scaffold into non-empty directory: {target_dir}")
            return InitProjectResult(
                success=False,
                target_dir=target_dir,
                created_files=(),
                created_dirs=(),
                bundle_enabled=bundle,
                error_message=(f"Target directory is not empty: {target_dir}"),
                error_code="LHP-IO-007",
            )

        created_target_dir = not target_dir.exists()
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            new_dirs: list[Path] = []
            if created_target_dir:
                new_dirs.append(target_dir)

            pre_paths = _walk_paths(target_dir)

            new_dirs.extend(_create_directory_tree(target_dir, bundle=bundle))

            context = InitTemplateContext.create(
                project_name=resolved_project_name,
                bundle_enabled=bundle,
                author="",
            )
            InitTemplateLoader().create_project_files(  # type: ignore[no-untyped-call]
                target_dir, context
            )

            post_paths = _walk_paths(target_dir)
            delta = post_paths - pre_paths

            created_files = tuple(sorted(p for p in delta if p.is_file()))
            created_dirs_from_loader = [
                p for p in delta if p.is_dir() and p not in new_dirs
            ]
            created_dirs = tuple(sorted(set(new_dirs) | set(created_dirs_from_loader)))

            return InitProjectResult(
                success=True,
                target_dir=target_dir,
                created_files=created_files,
                created_dirs=created_dirs,
                bundle_enabled=bundle,
            )

        except LHPError as lhp_err:
            logger.exception(f"LHP project scaffolding failed at {target_dir}")
            return InitProjectResult(
                success=False,
                target_dir=target_dir,
                created_files=(),
                created_dirs=(),
                bundle_enabled=bundle,
                error_message=lhp_err.title,
                error_code=lhp_err.code,
            )
        except OSError as exc:
            logger.exception(
                f"Filesystem error during project scaffolding at {target_dir}"
            )
            return InitProjectResult(
                success=False,
                target_dir=target_dir,
                created_files=(),
                created_dirs=(),
                bundle_enabled=bundle,
                error_message=str(exc),
                error_code=None,
            )


def _walk_paths(root: Path) -> "set[Path]":
    if not root.exists():
        return set()
    return {p.resolve() for p in root.rglob("*")}


def _create_directory_tree(project_path: Path, *, bundle: bool) -> list[Path]:
    """Returns only directories created by this call; existing dirs are not listed."""
    created: list[Path] = []

    directories: Tuple[str, ...] = (
        "presets",
        "templates",
        "pipelines",
        "substitutions",
        "schemas",
        "expectations",
        "generated",
        "config",
    )

    for dir_name in directories:
        dir_path = project_path / dir_name
        if not dir_path.exists():
            dir_path.mkdir()
            created.append(dir_path.resolve())

    if bundle:
        resources_dir = project_path / "resources"
        resources_lhp_dir = resources_dir / "lhp"
        if not resources_dir.exists():
            resources_dir.mkdir()
            created.append(resources_dir.resolve())
        if not resources_lhp_dir.exists():
            resources_lhp_dir.mkdir(parents=True, exist_ok=True)
            created.append(resources_lhp_dir.resolve())

    return created
