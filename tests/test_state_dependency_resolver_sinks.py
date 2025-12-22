"""Tests for StateDependencyResolver sink external file tracking."""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock

from lhp.core.state_dependency_resolver import StateDependencyResolver
from lhp.models.config import Action, ActionType, FlowGroup


class TestSinkExternalFileDependencyTracking:
    """Test external file dependency tracking for sink actions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        
        # Create pipelines directory
        self.pipelines_dir = self.project_root / "pipelines"
        self.pipelines_dir.mkdir()
        
        # Create test external files
        self.batch_handlers_dir = self.project_root / "batch_handlers"
        self.batch_handlers_dir.mkdir()
        
        self.foreachbatch_handler = self.batch_handlers_dir / "merge_handler.py"
        self.foreachbatch_handler.write_text("df.write.saveAsTable('target')")
        
        self.custom_sink_module = self.batch_handlers_dir / "custom_sink.py"
        self.custom_sink_module.write_text("class MySink: pass")
        
        self.resolver = StateDependencyResolver(self.project_root)
    
    def _create_yaml_file(self, flowgroup: FlowGroup) -> Path:
        """Create a YAML file for the flowgroup."""
        import yaml
        
        yaml_path = self.pipelines_dir / "test.yaml"
        
        # Convert flowgroup to dict
        flowgroup_dict = {
            "pipeline": flowgroup.pipeline,
            "flowgroup": flowgroup.flowgroup,
            "actions": []
        }
        
        for action in flowgroup.actions:
            action_dict = {
                "name": action.name,
                "type": action.type.value,
                "source": action.source,
                "write_target": action.write_target
            }
            flowgroup_dict["actions"].append(action_dict)
        
        with open(yaml_path, 'w') as f:
            yaml.dump(flowgroup_dict, f)
        
        return yaml_path

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_foreachbatch_sink_module_path_tracked(self):
        """Test that ForEachBatch sink module_path is tracked as a dependency."""
        # Create action with ForEachBatch sink
        action = Action(
            name="test_foreachbatch",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "module_path": "batch_handlers/merge_handler.py"
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[action]
        )
        
        # Create YAML file
        self._create_yaml_file(flowgroup)
        
        # Resolve dependencies
        dependencies = self.resolver.resolve_file_dependencies(
            Path("pipelines/test.yaml"),
            environment="dev",
            pipeline="test_pipeline",
            flowgroup_name="test_flowgroup"
        )
        
        # Verify the batch handler file is tracked
        assert "batch_handlers/merge_handler.py" in dependencies
        dep_info = dependencies["batch_handlers/merge_handler.py"]
        assert dep_info.type == "external_file"
        assert dep_info.checksum != ""
        assert dep_info.path == "batch_handlers/merge_handler.py"

    def test_custom_sink_module_path_tracked(self):
        """Test that custom sink module_path is tracked as a dependency."""
        # Create action with custom sink
        action = Action(
            name="test_custom_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "my_custom_sink",
                "module_path": "batch_handlers/custom_sink.py",
                "custom_sink_class": "MySink"
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[action]
        )
        
        # Create YAML file
        self._create_yaml_file(flowgroup)
        
        # Resolve dependencies
        dependencies = self.resolver.resolve_file_dependencies(
            Path("pipelines/test.yaml"),
            environment="dev",
            pipeline="test_pipeline",
            flowgroup_name="test_flowgroup"
        )
        
        # Verify the custom sink file is tracked
        assert "batch_handlers/custom_sink.py" in dependencies
        dep_info = dependencies["batch_handlers/custom_sink.py"]
        assert dep_info.type == "external_file"
        assert dep_info.checksum != ""
        assert dep_info.path == "batch_handlers/custom_sink.py"

    def test_sink_module_path_change_detected(self):
        """Test that changes to sink module_path files trigger staleness detection."""
        # Create action with ForEachBatch sink
        action = Action(
            name="test_foreachbatch",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "module_path": "batch_handlers/merge_handler.py"
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[action]
        )
        
        # Create YAML file
        self._create_yaml_file(flowgroup)
        
        # Get initial dependencies
        initial_deps = self.resolver.resolve_file_dependencies(
            Path("pipelines/test.yaml"),
            environment="dev",
            pipeline="test_pipeline",
            flowgroup_name="test_flowgroup"
        )
        
        initial_checksum = initial_deps["batch_handlers/merge_handler.py"].checksum
        
        # Modify the batch handler file
        self.foreachbatch_handler.write_text("df.write.saveAsTable('target_modified')")
        
        # Get updated dependencies
        updated_deps = self.resolver.resolve_file_dependencies(
            Path("pipelines/test.yaml"),
            environment="dev",
            pipeline="test_pipeline",
            flowgroup_name="test_flowgroup"
        )
        
        updated_checksum = updated_deps["batch_handlers/merge_handler.py"].checksum
        
        # Verify checksum changed
        assert initial_checksum != updated_checksum

    def test_multiple_sink_types_tracked(self):
        """Test that both ForEachBatch and custom sinks are tracked in same flowgroup."""
        # Create actions with both sink types
        foreachbatch_action = Action(
            name="test_foreachbatch",
            type=ActionType.WRITE,
            source="v_data1",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "batch_sink",
                "module_path": "batch_handlers/merge_handler.py"
            }
        )
        
        custom_action = Action(
            name="test_custom",
            type=ActionType.WRITE,
            source="v_data2",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "custom_sink",
                "module_path": "batch_handlers/custom_sink.py",
                "custom_sink_class": "MySink"
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[foreachbatch_action, custom_action]
        )
        
        # Create YAML file
        self._create_yaml_file(flowgroup)
        
        # Resolve dependencies
        dependencies = self.resolver.resolve_file_dependencies(
            Path("pipelines/test.yaml"),
            environment="dev",
            pipeline="test_pipeline",
            flowgroup_name="test_flowgroup"
        )
        
        # Verify both files are tracked
        assert "batch_handlers/merge_handler.py" in dependencies
        assert "batch_handlers/custom_sink.py" in dependencies

    def test_sink_without_module_path_not_tracked(self):
        """Test that sinks without module_path (e.g., inline batch_handler) don't add dependencies."""
        # Create action with inline batch_handler
        action = Action(
            name="test_foreachbatch_inline",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "inline_sink",
                "batch_handler": "df.write.saveAsTable('target')"
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[action]
        )
        
        # Resolve dependencies
        dependencies = self.resolver.resolve_file_dependencies(
            Path("pipelines/test.yaml"),
            environment="dev",
            pipeline="test_pipeline",
            flowgroup_name="test_flowgroup"
        )
        
        # Verify no batch handler file is tracked (inline code doesn't have a file)
        batch_handler_files = [
            path for path in dependencies.keys() 
            if "batch_handlers" in path
        ]
        assert len(batch_handler_files) == 0

    def test_other_sink_types_not_tracked(self):
        """Test that delta and kafka sinks don't trigger module_path tracking."""
        # Create actions with delta and kafka sinks
        delta_action = Action(
            name="test_delta",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "delta_sink"
            }
        )
        
        kafka_action = Action(
            name="test_kafka",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_sink",
                "topic": "test_topic"
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[delta_action, kafka_action]
        )
        
        # Resolve dependencies
        dependencies = self.resolver.resolve_file_dependencies(
            Path("pipelines/test.yaml"),
            environment="dev",
            pipeline="test_pipeline",
            flowgroup_name="test_flowgroup"
        )
        
        # Verify no sink module files are tracked
        sink_files = [
            path for path in dependencies.keys() 
            if "batch_handlers" in path or "sinks" in path
        ]
        assert len(sink_files) == 0


class TestSinkTemplateDependencyTracking:
    """Test sink external file dependency tracking from templates."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        
        # Create templates directory
        self.templates_dir = self.project_root / "templates"
        self.templates_dir.mkdir()
        
        # Create batch handlers directory
        self.batch_handlers_dir = self.project_root / "batch_handlers"
        self.batch_handlers_dir.mkdir()
        
        # Create test external file
        self.handler_file = self.batch_handlers_dir / "template_handler.py"
        self.handler_file.write_text("df.write.saveAsTable('target')")
        
        self.resolver = StateDependencyResolver(self.project_root)

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_template_foreachbatch_sink_tracked(self):
        """Test that ForEachBatch sink module_path in templates is tracked."""
        # Test the template action extraction directly
        template_actions = [
            {
                "type": "write",
                "name": "template_batch_sink",
                "source": "v_data",
                "write_target": {
                    "type": "sink",
                    "sink_type": "foreachbatch",
                    "sink_name": "template_sink",
                    "module_path": "batch_handlers/template_handler.py"
                }
            }
        ]
        
        files = self.resolver._extract_external_files_from_template_actions(template_actions)
        
        # Verify the batch handler file is extracted
        assert "batch_handlers/template_handler.py" in files

    def test_template_custom_sink_tracked(self):
        """Test that custom sink module_path in templates is tracked."""
        template_actions = [
            {
                "type": "write",
                "name": "template_custom_sink",
                "source": "v_data",
                "write_target": {
                    "type": "sink",
                    "sink_type": "custom",
                    "sink_name": "template_custom",
                    "module_path": "batch_handlers/template_handler.py",
                    "custom_sink_class": "MySink"
                }
            }
        ]
        
        files = self.resolver._extract_external_files_from_template_actions(template_actions)
        
        # Verify the custom sink file is extracted
        assert "batch_handlers/template_handler.py" in files

    def test_template_inline_batch_handler_not_tracked(self):
        """Test that inline batch_handler in templates doesn't add file dependencies."""
        template_actions = [
            {
                "type": "write",
                "name": "template_inline_sink",
                "source": "v_data",
                "write_target": {
                    "type": "sink",
                    "sink_type": "foreachbatch",
                    "sink_name": "inline_sink",
                    "batch_handler": "df.write.saveAsTable('target')"
                }
            }
        ]
        
        files = self.resolver._extract_external_files_from_template_actions(template_actions)
        
        # Verify no batch handler file is extracted
        batch_handler_files = [f for f in files if "batch_handlers" in f]
        assert len(batch_handler_files) == 0

