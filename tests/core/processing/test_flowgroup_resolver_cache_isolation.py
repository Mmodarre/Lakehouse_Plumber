"""Regression: ``process_flowgroup`` must not mutate the input ``FlowGroup``
(the cached copy held by :class:`CachingYAMLParser`) during template
expansion or test-action filtering. Constitution §6.8 / §8.1.
"""

import tempfile
from pathlib import Path

import pytest

from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.validators import ConfigValidator, SecretValidator
from lhp.models import Action, ActionType, FlowGroup
from lhp.presets.preset_manager import PresetManager
from tests.fakes import FakeTemplate, FakeTemplateEngine
from tests.helpers import wrap_in_ctx as _ctx_of


@pytest.mark.unit
class TestProcessFlowgroupCacheIsolation:
    """Process must never mutate the caller's FlowGroup or its actions list."""

    @staticmethod
    def _substitution_mgr() -> EnhancedSubstitutionManager:
        """Substitution manager with no mappings — substitution is a no-op."""
        return EnhancedSubstitutionManager()

    def test_process_flowgroup_does_not_mutate_input_actions_when_template_expands(
        self,
    ):
        """Guards site #1: ``flowgroup.actions.extend(template_actions)``
        mutated the cached FlowGroup returned by CachingYAMLParser.
        """
        fake_template_engine = FakeTemplateEngine(
            template=FakeTemplate(presets=None),
            rendered_actions=[
                Action(
                    name="template_load",
                    type=ActionType.LOAD,
                    source={"type": "sql", "sql": "SELECT 1 as id"},
                    target="v_template",
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            service = FlowgroupResolutionService(
                template_engine=fake_template_engine,
                preset_manager=PresetManager(presets_dir=Path(tmpdir)),
                config_validator=ConfigValidator(),
                secret_validator=SecretValidator(),
            )

            fg = FlowGroup(
                pipeline="test_pipeline",
                flowgroup="template_fg",
                use_template="some_template",
                actions=[
                    Action(
                        name="inline_write",
                        type=ActionType.WRITE,
                        source="v_template",
                        write_target={
                            "type": "streaming_table",
                            "database": "catalog.schema",
                            "table": "target",
                        },
                    ),
                ],
            )

            original_action_count = len(fg.actions)
            original_actions_list = fg.actions

            ctx = _ctx_of(fg)
            result_ctx = service.process_flowgroup(
                ctx, self._substitution_mgr(), include_tests=True
            )

            result_action_names = {a.name for a in result_ctx.flowgroup.actions}
            assert "template_load" in result_action_names
            assert "inline_write" in result_action_names

            assert len(fg.actions) == original_action_count, (
                "input flowgroup was mutated (actions list grew)"
            )
            assert fg.actions is original_actions_list, (
                "actions list identity changed — mutation detected"
            )
            assert result_ctx.flowgroup is not fg, (
                "result must be a new instance (model_copy rebind)"
            )

    def test_process_flowgroup_does_not_mutate_input_actions_when_test_filter_drops_actions(
        self,
    ):
        """Guards site #2: ``flowgroup.actions = [...]`` filter-reassignment
        mutated the cached FlowGroup.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FlowgroupResolutionService(
                template_engine=None,  # not exercised (no use_template set)
                preset_manager=PresetManager(presets_dir=Path(tmpdir)),
                config_validator=ConfigValidator(),
                secret_validator=SecretValidator(),
            )

            fg = FlowGroup(
                pipeline="test_pipeline",
                flowgroup="mixed_fg",
                actions=[
                    Action(
                        name="load_data",
                        type=ActionType.LOAD,
                        source={"type": "sql", "sql": "SELECT 1 as id"},
                        target="v_data",
                    ),
                    Action(
                        name="test_uniqueness",
                        type=ActionType.TEST,
                        test_type="uniqueness",
                        source="v_data",
                        columns=["id"],
                        on_violation="fail",
                    ),
                    Action(
                        name="write_data",
                        type=ActionType.WRITE,
                        source="v_data",
                        write_target={
                            "type": "streaming_table",
                            "database": "catalog.schema",
                            "table": "target",
                        },
                    ),
                ],
            )

            original_action_count = len(fg.actions)
            original_actions_list = fg.actions

            ctx = _ctx_of(fg)
            result_ctx = service.process_flowgroup(
                ctx, self._substitution_mgr(), include_tests=False
            )

            result_types = [a.type for a in result_ctx.flowgroup.actions]
            assert ActionType.TEST not in result_types
            assert len(result_ctx.flowgroup.actions) == 2

            assert len(fg.actions) == original_action_count, (
                "input flowgroup was mutated (actions list shrank)"
            )
            assert fg.actions is original_actions_list, (
                "actions list identity changed — mutation detected"
            )
            assert any(a.type == ActionType.TEST for a in fg.actions), (
                "input flowgroup lost its TEST action — mutation detected"
            )
            assert result_ctx.flowgroup is not fg, (
                "result must be a new instance (model_copy rebind)"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
