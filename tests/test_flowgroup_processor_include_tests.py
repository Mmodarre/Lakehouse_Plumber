"""Tests for FlowgroupResolutionService include_tests filtering behavior.

Unit tests verify that include_tests=False filters test actions from flowgroups
before expensive processing (presets, substitution, validation). Integration tests
verify the parameter threads correctly through orchestrator.validate_pipelines.
"""

import tempfile
from pathlib import Path

import pytest

from lhp.core.processing import TemplateEngine
from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.validators import ConfigValidator, SecretValidator
from lhp.models import Action, ActionType, FlowGroup
from lhp.presets.preset_manager import PresetManager
from tests.fakes import (
    FakeFlowgroupResolutionService,
    FakeSubstitutionManager,
    FakeTemplate,
    FakeTemplateEngine,
)
from tests.helpers import process_unwrap as _process
from tests.helpers import wrap_in_ctx as _ctx_of

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def processor():
    """Create a FlowgroupResolutionService with real dependencies."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield FlowgroupResolutionService(
            template_engine=TemplateEngine(),
            preset_manager=PresetManager(presets_dir=Path(tmpdir)),
            config_validator=ConfigValidator(),
            secret_validator=SecretValidator(),
        )


@pytest.fixture
def substitution_mgr():
    """Create a substitution manager with no mappings."""
    return EnhancedSubstitutionManager()


@pytest.fixture
def mixed_flowgroup():
    """Flowgroup with LOAD + TEST actions."""
    return FlowGroup(
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


@pytest.fixture
def test_only_flowgroup():
    """Flowgroup with only TEST actions."""
    return FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_only_fg",
        actions=[
            Action(
                name="test_uniqueness",
                type=ActionType.TEST,
                test_type="uniqueness",
                source="some_table",
                columns=["id"],
                on_violation="fail",
            ),
            Action(
                name="test_completeness",
                type=ActionType.TEST,
                test_type="completeness",
                source="some_table",
                required_columns=["id", "name"],
                on_violation="warn",
            ),
        ],
    )


# ============================================================================
# Unit Tests — FlowgroupResolutionService.process_flowgroup
# ============================================================================


@pytest.mark.unit
class TestProcessFlowgroupIncludeTests:
    """Test include_tests filtering in FlowgroupResolutionService.process_flowgroup."""

    def test_filters_test_actions_when_false(
        self, processor, substitution_mgr, mixed_flowgroup
    ):
        """include_tests=False removes TEST actions, keeps LOAD/WRITE."""
        result = _process(
            processor, mixed_flowgroup, substitution_mgr, include_tests=False
        )

        action_types = [a.type for a in result.actions]
        assert ActionType.TEST not in action_types
        assert ActionType.LOAD in action_types
        assert ActionType.WRITE in action_types
        assert len(result.actions) == 2

    def test_keeps_test_actions_when_true(
        self, processor, substitution_mgr, mixed_flowgroup
    ):
        """include_tests=True preserves all actions including TEST."""
        result = _process(
            processor, mixed_flowgroup, substitution_mgr, include_tests=True
        )

        action_types = [a.type for a in result.actions]
        assert ActionType.TEST in action_types
        assert len(result.actions) == 3

    def test_default_keeps_test_actions(
        self, processor, substitution_mgr, mixed_flowgroup
    ):
        """Default (no include_tests arg) preserves TEST actions for backward compat."""
        result = _process(processor, mixed_flowgroup, substitution_mgr)

        action_types = [a.type for a in result.actions]
        assert ActionType.TEST in action_types
        assert len(result.actions) == 3

    def test_test_only_skips_validation_when_false(
        self, processor, substitution_mgr, test_only_flowgroup
    ):
        """Test-only flowgroup with include_tests=False returns without LHPValidationError.

        ConfigValidator rejects zero-action flowgroups, but when the zero-actions
        state comes from include_tests=False filtering, validation is skipped.
        """
        # This should NOT raise — zero actions is expected when filtering
        result = _process(
            processor, test_only_flowgroup, substitution_mgr, include_tests=False
        )

        assert len(result.actions) == 0

    def test_test_only_validates_when_true(
        self, processor, substitution_mgr, test_only_flowgroup
    ):
        """Test-only flowgroup with include_tests=True is validated normally."""
        # Should succeed since the test actions are valid
        result = _process(
            processor, test_only_flowgroup, substitution_mgr, include_tests=True
        )

        assert len(result.actions) == 2
        assert all(a.type == ActionType.TEST for a in result.actions)

    def test_filters_template_generated_test_actions(self, substitution_mgr):
        """include_tests=False filters test actions generated by templates.

        Validates the filter placement rationale: filter is after template
        expansion so template-generated test actions are also caught.
        """
        # Fake template engine returns one TEST and one LOAD action; the
        # filter under test must drop the TEST action regardless of where it
        # originated (template expansion vs. inline declaration).
        fake_template_engine = FakeTemplateEngine(
            template=FakeTemplate(presets=None),
            rendered_actions=[
                Action(
                    name="template_test",
                    type=ActionType.TEST,
                    test_type="row_count",
                    source=["table_a", "table_b"],
                    tolerance=0,
                    on_violation="fail",
                ),
                Action(
                    name="template_load",
                    type=ActionType.LOAD,
                    source={"type": "sql", "sql": "SELECT 1"},
                    target="v_template",
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            processor = FlowgroupResolutionService(
                template_engine=fake_template_engine,
                preset_manager=PresetManager(presets_dir=Path(tmpdir)),
                config_validator=ConfigValidator(),
                secret_validator=SecretValidator(),
            )

            flowgroup = FlowGroup(
                pipeline="test_pipeline",
                flowgroup="template_fg",
                use_template="test_template",
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

            result = _process(
                processor, flowgroup, substitution_mgr, include_tests=False
            )

            # Template-generated test action should be filtered out
            action_types = [a.type for a in result.actions]
            assert ActionType.TEST not in action_types
            # The inline write and template load should remain
            assert len(result.actions) == 2
            action_names = {a.name for a in result.actions}
            assert "template_load" in action_names
            assert "inline_write" in action_names


# ============================================================================
# Integration Tests — Parameter threading through orchestrator
# ============================================================================


@pytest.mark.integration
class TestValidatePipelineIncludeTests:
    """Test include_tests threading through validate_pipelines."""

    @staticmethod
    def _create_project_with_invalid_test(tmp_path):
        """Create a minimal project with a valid load/write and an invalid test action."""
        (tmp_path / "lhp.yaml").write_text("project_name: test\n")
        (tmp_path / "substitutions").mkdir()
        (tmp_path / "substitutions" / "dev.yaml").write_text("{}")
        pipelines_dir = tmp_path / "pipelines" / "test_pipeline"
        pipelines_dir.mkdir(parents=True)

        # Write a flowgroup with a valid load/write plus a test action missing
        # required fields (uniqueness requires 'columns')
        (pipelines_dir / "test_fg.yaml").write_text("""pipeline: test_pipeline
flowgroup: test_fg
actions:
  - name: load_data
    type: load
    source:
      type: sql
      sql: "SELECT 1 as id"
    target: v_data
  - name: write_data
    type: write
    source: v_data
    write_target:
      type: streaming_table
      database: catalog.schema
      table: target
  - name: bad_test
    type: test
    test_type: uniqueness
    source: v_data
""")

    def test_validate_skips_test_actions_when_false(self, tmp_path):
        """validate_pipelines(include_tests=False) skips test action errors.

        The per-field ``validate_pipeline_by_field`` shim (which returned a
        single flat ``(errors, warnings)`` tuple) was consolidated into the
        plural ``ActionOrchestrator.validate_pipelines`` (keyword-scoped via
        ``pipeline_filter``), which returns one ``PipelineValidationOutcome``
        per pipeline. That DTO splits diagnostics into a string-projection
        ``errors`` tuple AND a structured ``lhp_errors`` tuple of live
        ``LHPError`` instances (config/action validation errors like
        ``LHP-VAL-007`` land in ``lhp_errors``), so the legacy single-tuple
        assertion is replaced by a combined view across both channels
        (``src/lhp/core/coordination/orchestrator.py:590,593,596``;
        ``executor.py:101,104-119``).
        """
        from lhp.core.coordination.layers import build_facade_orchestrator

        self._create_project_with_invalid_test(tmp_path)
        orchestrator = build_facade_orchestrator(tmp_path)

        outcomes = orchestrator.validate_pipelines(
            pipeline_filter="test_pipeline", env="dev", include_tests=False
        )
        outcome = outcomes[0]
        all_errors = list(outcome.errors) + list(outcome.lhp_errors)
        assert (
            len(all_errors) == 0 and outcome.success is True
        ), f"Expected no errors with include_tests=False, got: {all_errors}"

    def test_validate_catches_test_actions_when_true(self, tmp_path):
        """validate_pipelines(include_tests=True) catches test action errors.

        Uses a separate orchestrator to avoid shared-state issues with
        discover_all_flowgroups caching.
        """
        from lhp.core.coordination.layers import build_facade_orchestrator

        self._create_project_with_invalid_test(tmp_path)
        orchestrator = build_facade_orchestrator(tmp_path)

        outcomes = orchestrator.validate_pipelines(
            pipeline_filter="test_pipeline", env="dev", include_tests=True
        )
        outcome = outcomes[0]
        # Structured config-validation errors (LHP-VAL-007 for the missing
        # ``columns`` field) land on ``lhp_errors``, not the string-projection
        # ``errors`` tuple — assert across both channels.
        all_errors = list(outcome.errors) + list(outcome.lhp_errors)
        assert (
            len(all_errors) > 0 and outcome.success is False
        ), "Expected validation errors with include_tests=True for missing columns"

    def test_validate_passes_include_tests_through_chain(self, monkeypatch):
        """Worker forwards include_tests to processor.process_flowgroup.

        The system under test is the consolidated single-wave worker
        (:func:`._flowgroup_pool._process_one_flowgroup`). The worker reads its
        collaborators from the module-global ``_flowgroup_state`` (populated by
        ``_init_flowgroup_worker`` in a spawned worker); here we set that global
        directly to a :class:`_FlowgroupWorkerState` wrapping picklable fakes,
        run the worker in-process in ``validate`` mode, and assert it threaded
        ``include_tests`` (carried on the state) onto
        ``processor.process_flowgroup``.
        """
        from lhp.core.coordination import _flowgroup_pool as fp
        from lhp.core.coordination._flowgroup_pool import (
            _FlowgroupWorkerState,
            _process_one_flowgroup,
        )

        fg = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="simple_fg",
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    source={"type": "sql", "sql": "SELECT 1 as id"},
                    target="v_data",
                ),
            ],
        )
        fake_processor = FakeFlowgroupResolutionService()
        state = _FlowgroupWorkerState(
            processor=fake_processor,
            substitution_managers={"test_pipeline": FakeSubstitutionManager()},
            include_tests=False,
            code_generator=None,  # unused in validate mode
            pipeline_output_dirs={"test_pipeline": None},
            environment="dev",
        )
        monkeypatch.setattr(fp, "_flowgroup_state", state)

        outcome = _process_one_flowgroup(_ctx_of(fg), mode="validate")

        # Worker did not raise and ran the resolver exactly once.
        assert outcome.success is True
        assert len(fake_processor.calls) == 1
        # include_tests (from the worker state) threaded onto the resolver call.
        assert fake_processor.calls[0].kwargs.get("include_tests") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
