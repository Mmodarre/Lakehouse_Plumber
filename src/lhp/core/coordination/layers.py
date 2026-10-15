"""Composition root for the application facade.

Hosts :func:`build_facade_orchestrator`, the internal helper that
:meth:`lhp.api.facade.LakehousePlumberApplicationFacade.for_project`
delegates to. Keeping the orchestrator class name (and the
service-graph wiring) outside :mod:`lhp.api` enforces the constitutional
invariant that ``ActionOrchestrator`` is never named inside the public
API surface (§1.10, §9.13).
"""

import functools
from pathlib import Path
from typing import Optional


def build_facade_orchestrator(
    project_root: Path,
    *,
    pipeline_config_path: Optional[str] = None,
    enforce_version: bool = True,
    max_workers: Optional[int] = None,
    no_cache: bool = False,
):
    """Build a fully-wired orchestrator for the application facade.

    Centralises service-graph construction so no individual service
    reaches into another's private attributes. Builds
    :class:`ConfigValidator` once and threads it into both
    :class:`ValidationService` and :class:`FlowgroupResolutionService`
    from the same level — closes the §9.24 leak.

    Imports are local so this module stays cheap to import and so
    ``lhp.api.facade`` can call us through a single lazy import without
    pulling in the core/coordination graph at module-import time.
    """
    from lhp.core.codegen import OperationalMetadataService, check_tags_file_tokens
    from lhp.core.coordination.orchestrator import ActionOrchestrator
    from lhp.core.coordination.validation_service import ValidationService
    from lhp.core.loaders import ProjectConfigLoader, load_sql_file
    from lhp.core.processing import TemplateEngine
    from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
    from lhp.core.registry import ActionRegistry
    from lhp.core.validators import (
        ConfigValidator,
        ExternalFileTokenValidator,
        OperationalMetadataExpressionValidator,
        SecretValidator,
    )
    from lhp.presets.preset_manager import PresetManager

    project_config = ProjectConfigLoader(project_root).load_project_config()
    template_engine = TemplateEngine(project_root / "templates")
    preset_manager = PresetManager(project_root / "presets")
    config_validator = ConfigValidator(project_root, project_config)
    secret_validator = SecretValidator()
    # The validator checks only what generation renders (the registry's
    # generator declarations) via the render path's own column resolver.
    operational_metadata_validator = OperationalMetadataExpressionValidator(
        project_config,
        ActionRegistry(),
        OperationalMetadataService().resolve_selected_columns,
    )
    # Reads each sql_path / tags_file through the loader and the tagging-hook
    # check generation uses, so validate and generate reject the same tokens.
    file_token_validator = ExternalFileTokenValidator(
        project_root,
        load_sql_file,
        functools.partial(
            check_tags_file_tokens,
            project_config=project_config,
            project_root=project_root,
        ),
    )

    validation_service = ValidationService(
        project_root=project_root,
        project_config=project_config,
        config_validator=config_validator,
    )
    flowgroup_resolver = FlowgroupResolutionService(
        template_engine=template_engine,
        preset_manager=preset_manager,
        config_validator=config_validator,
        secret_validator=secret_validator,
        operational_metadata_validator=operational_metadata_validator,
        file_token_validator=file_token_validator,
    )

    return ActionOrchestrator(
        project_root,
        enforce_version=enforce_version,
        pipeline_config_path=pipeline_config_path,
        max_workers=max_workers,
        flowgroup_resolver=flowgroup_resolver,
        validation_service=validation_service,
        config_validator=config_validator,
        no_cache=no_cache,
    )
