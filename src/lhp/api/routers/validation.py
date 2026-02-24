import asyncio
import logging
from pathlib import Path

import yaml
from fastapi import APIRouter, Depends, HTTPException

from lhp.api.dependencies import (
    get_discoverer,
    get_facade,
    get_orchestrator,
    get_project_root_adaptive,
)
from lhp.api.schemas.validation import (
    ValidateRequest,
    ValidateResponse,
    ValidateYAMLRequest,
)
from lhp.core.layers import (
    LakehousePlumberApplicationFacade,
    PipelineValidationRequest,
)
from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/validate", tags=["validation"])


@router.post("", response_model=ValidateResponse)
async def validate_all(
    req: ValidateRequest,
    facade: LakehousePlumberApplicationFacade = Depends(get_facade),
    discoverer: FlowgroupDiscoverer = Depends(get_discoverer),
) -> ValidateResponse:
    """#59: Validate all pipelines for an environment."""
    flowgroups = await asyncio.to_thread(discoverer.discover_all_flowgroups)
    pipelines = list({fg.pipeline for fg in flowgroups})

    all_errors = []
    all_warnings = []
    validated = []

    for pipeline_id in pipelines:
        request = PipelineValidationRequest(
            pipeline_identifier=pipeline_id,
            environment=req.environment,
        )
        response = await asyncio.to_thread(facade.validate_pipeline, request)
        all_errors.extend(response.errors)
        all_warnings.extend(response.warnings)
        validated.append(pipeline_id)

    return ValidateResponse(
        success=len(all_errors) == 0,
        errors=all_errors,
        warnings=all_warnings,
        validated_pipelines=validated,
    )


@router.post("/pipeline/{name}", response_model=ValidateResponse)
async def validate_pipeline(
    name: str,
    req: ValidateRequest,
    facade: LakehousePlumberApplicationFacade = Depends(get_facade),
) -> ValidateResponse:
    """#60: Validate a specific pipeline."""
    request = PipelineValidationRequest(
        pipeline_identifier=name,
        environment=req.environment,
    )
    response = await asyncio.to_thread(facade.validate_pipeline, request)

    return ValidateResponse(
        success=response.success,
        errors=response.errors,
        warnings=response.warnings,
        validated_pipelines=response.validated_pipelines,
        error_message=response.error_message,
    )


@router.post("/flowgroup/{name}", response_model=ValidateResponse)
async def validate_flowgroup(
    name: str,
    env: str = "dev",
    orchestrator: ActionOrchestrator = Depends(get_orchestrator),
) -> ValidateResponse:
    """#61: Validate a single flowgroup.

    Uses the orchestrator's validator to check a specific flowgroup against
    the given environment's substitutions.
    """
    try:
        errors, warnings = await asyncio.to_thread(
            orchestrator.validate_pipeline_by_field,
            pipeline_field=name,
            env=env,
        )
        return ValidateResponse(
            success=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            validated_pipelines=[name],
        )
    except Exception as e:
        return ValidateResponse(
            success=False,
            errors=[str(e)],
            warnings=[],
            validated_pipelines=[],
            error_message=str(e),
        )


@router.post("/yaml", response_model=ValidateResponse)
async def validate_yaml(
    req: ValidateYAMLRequest,
    project_root: Path = Depends(get_project_root_adaptive),
) -> ValidateResponse:
    """#62: Validate arbitrary YAML content against the LHP schema.

    Parses the provided YAML string and attempts to construct a FlowGroup
    model from it. If the YAML is structurally valid, runs full flowgroup
    validation via ConfigValidator.
    """
    from pydantic import ValidationError as PydanticValidationError

    from lhp.core.project_config_loader import ProjectConfigLoader
    from lhp.core.validator import ConfigValidator
    from lhp.models.config import FlowGroup

    try:
        parsed = yaml.safe_load(req.content)
        if not isinstance(parsed, dict):
            return ValidateResponse(
                success=False,
                errors=["YAML content must be a mapping (dict), not a scalar or list"],
                warnings=[],
                validated_pipelines=[],
            )

        # Try to construct a FlowGroup from the parsed dict
        try:
            flowgroup = FlowGroup(**parsed)
        except PydanticValidationError as e:
            errors = [
                f"{err['loc'][-1] if err['loc'] else 'root'}: {err['msg']}"
                for err in e.errors()
            ]
            return ValidateResponse(
                success=False,
                errors=errors,
                warnings=[],
                validated_pipelines=[],
            )

        config_loader = ProjectConfigLoader(project_root)
        project_config = await asyncio.to_thread(config_loader.load_project_config)
        validator = ConfigValidator(project_root, project_config)

        errors = await asyncio.to_thread(validator.validate_flowgroup, flowgroup)
        return ValidateResponse(
            success=len(errors) == 0,
            errors=[str(e) for e in errors] if errors else [],
            warnings=[],
            validated_pipelines=[],
        )
    except yaml.YAMLError as e:
        return ValidateResponse(
            success=False,
            errors=[f"YAML syntax error: {e}"],
            warnings=[],
            validated_pipelines=[],
        )
