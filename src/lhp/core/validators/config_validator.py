from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

from lhp.models import Action, ActionType, FlowGroup

from ...errors import LHPError
from ._base import ValidationError
from .field.config_field import ConfigFieldValidator

# Distinct from the sibling :class:`.field.config_field.ConfigFieldValidator`,
# which performs strict per-field schema validation on individual
# configuration objects. ``ConfigValidator`` is the per-flowgroup aggregator
# that composes the action validators (LoadActionValidator,
# TransformActionValidator, WriteActionValidator, TestActionValidator) to
# validate one flowgroup's actions end to end. Cross-flowgroup validators
# (TableCreationValidator, CdcFanInCompatibilityValidator) are composed
# exclusively by :class:`...coordination.validation_service.ValidationService`
# where the full flowgroup set is available (§9.24).


# NOTE: ``ActionRegistry`` and ``DependencyResolver`` are imported lazily
# inside ``__init__`` (see below). The submodule path
# ``lhp.core.dependencies.dependency_resolver`` must NOT be collapsed to
# the package-level ``..dependencies`` import. Module-level imports here
# would close cycles via:
#   - validators -> registry -> generators.load -> validators.field.kafka_options
#   - config_validator -> dependencies/__init__ -> dependencies.service ->
#     coordination/__init__ -> coordination.validation_service ->
#     config_validator
# under cold imports.

logger = logging.getLogger(__name__)

from . import (  # noqa: E402  # placed below docstring/comment to keep cold-import cycle notes adjacent
    LoadActionValidator,
    TestActionValidator,
    TransformActionValidator,
    WriteActionValidator,
)

if TYPE_CHECKING:
    pass


class ConfigValidator:
    def __init__(self, project_root=None, project_config=None):
        from ..dependencies.dependency_resolver import DependencyResolver
        from ..registry import ActionRegistry

        self.logger = logging.getLogger(__name__)
        self.project_root = project_root
        self.project_config = project_config
        self.action_registry = ActionRegistry()
        self.dependency_resolver = DependencyResolver()
        self.field_validator = ConfigFieldValidator()

        self.load_validator = LoadActionValidator(
            self.action_registry, self.field_validator
        )
        self.transform_validator = TransformActionValidator(
            self.action_registry,
            self.field_validator,
            self.project_root,
            self.project_config,
        )
        self.write_validator = WriteActionValidator(
            self.action_registry, self.field_validator
        )
        self.test_validator = TestActionValidator(
            self.action_registry, self.field_validator
        )

    def validate_flowgroup(self, flowgroup: FlowGroup) -> List[ValidationError]:
        errors = []

        if not flowgroup.pipeline:
            errors.append("FlowGroup must have a 'pipeline' name")

        if not flowgroup.flowgroup:
            errors.append("FlowGroup must have a 'flowgroup' name")

        if not flowgroup.actions:
            errors.append("FlowGroup must have at least one action")

        action_names = set()
        target_names = set()

        for i, action in enumerate(flowgroup.actions):
            action_errors = self.validate_action(action, i)
            errors.extend(action_errors)

            if action.name in action_names:
                errors.append(f"Duplicate action name: '{action.name}'")
            action_names.add(action.name)

            if action.target and action.target in target_names:
                errors.append(
                    f"Duplicate target name: '{action.target}' in action '{action.name}'"
                )
            if action.target:
                target_names.add(action.target)

        if flowgroup.actions:
            try:
                dependency_errors = self.dependency_resolver.validate_relationships(
                    flowgroup.actions
                )
                errors.extend(dependency_errors)
            except LHPError as e:
                logger.debug(f"Dependency validation error: {e.title}")
                errors.append(e)
            except Exception as e:
                logger.debug(f"Dependency validation error: {e}")
                errors.append(str(e))

        if flowgroup.use_template and not flowgroup.template_parameters:
            self.logger.warning(
                f"FlowGroup uses template '{flowgroup.use_template}' but no parameters provided"
            )

        return errors

    def validate_action(self, action: Action, index: int) -> List[ValidationError]:
        errors = []
        prefix = f"Action[{index}] '{action.name}'"

        if not action.name:
            errors.append(f"Action[{index}]: Missing 'name' field")
            return errors  # Can't continue without name

        if not action.type:
            errors.append(f"{prefix}: Missing 'type' field")
            return errors  # Can't continue without type

        try:
            action_dict = action.model_dump()
            self.field_validator.validate_action_fields(action_dict, action.name)
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except Exception as e:
            errors.append(str(e))
            return errors

        if action.type == ActionType.LOAD:
            errors.extend(self.load_validator.validate(action, prefix))

        elif action.type == ActionType.TRANSFORM:
            errors.extend(self.transform_validator.validate(action, prefix))

        elif action.type == ActionType.WRITE:
            errors.extend(self.write_validator.validate(action, prefix))

        elif action.type == ActionType.TEST:
            errors.extend(self.test_validator.validate(action, prefix))

        else:
            errors.append(f"{prefix}: Unknown action type '{action.type}'")

        return errors

    def validate_duplicate_pipeline_flowgroup(
        self, flowgroups: List[FlowGroup]
    ) -> List[str]:
        errors = []
        seen_combinations = set()

        for flowgroup in flowgroups:
            combination_key = f"{flowgroup.pipeline}.{flowgroup.flowgroup}"

            if combination_key in seen_combinations:
                errors.append(
                    f"Duplicate pipeline+flowgroup combination: '{combination_key}'. "
                    f"Each pipeline+flowgroup combination must be unique across all YAML files."
                )
            else:
                seen_combinations.add(combination_key)

        return errors
