"""Test for BurrowBuilder-style formatting in generated code."""

import pytest
from pathlib import Path
from lhp.core.orchestrator import ActionOrchestrator
from lhp.models.config import FlowGroup, Action, ActionType


def test_burrowbuilder_style_formatting(tmp_path):
    """Test that generated code includes BurrowBuilder-style section headers."""
    # Create project structure
    (tmp_path / "pipelines" / "test").mkdir(parents=True)
    (tmp_path / "substitutions").mkdir()
    (tmp_path / "templates").mkdir()
    (tmp_path / "presets").mkdir()
    
    # Create test flowgroup
    flowgroup_dict = {
        "pipeline": "test_pipeline",
        "flowgroup": "test_flowgroup",
        "actions": [
            {
                "name": "load_data",
                "type": "load",
                "target": "v_source",
                "source": {
                    "type": "cloudfiles",
                    "path": "/mnt/data/*.csv",
                    "format": "csv"
                }
            },
            {
                "name": "transform_data",
                "type": "transform",
                "transform_type": "sql",
                "target": "v_transformed",
                "source": "v_source",
                "sql": "SELECT * FROM v_source WHERE active = true"
            },
            {
                "name": "write_data",
                "type": "write",
                "source": "v_transformed",
                "write_target": {
                    "type": "streaming_table",
                    "database": "silver",
                    "table": "output",
                    "create_table": True
                }
            }
        ]
    }
    
    # Save flowgroup
    import yaml
    flowgroup_file = tmp_path / "pipelines" / "test" / "test_flowgroup.yaml"
    with open(flowgroup_file, 'w') as f:
        yaml.dump(flowgroup_dict, f)
    
    # Create minimal substitution file
    sub_file = tmp_path / "substitutions" / "dev.yaml"
    with open(sub_file, 'w') as f:
        yaml.dump({"environment": {"dev": {}}}, f)
    
    # Generate pipeline
    orchestrator = ActionOrchestrator(tmp_path)
    generated_files = orchestrator.generate_pipeline("test", "dev")
    
    # Verify formatting
    assert len(generated_files) == 1
    code = list(generated_files.values())[0]
    
    # Check for section headers
    assert "# ============================================================================" in code
    assert "# SOURCE VIEWS" in code
    assert "# TRANSFORMATION VIEWS" in code
    assert "# TARGET TABLES" in code
    
    # Check for pipeline configuration
    assert "# Pipeline Configuration" in code
    assert 'PIPELINE_ID = "test_pipeline"' in code
    assert 'FLOWGROUP_ID = "test_flowgroup"' in code
    
    # Verify sections appear in correct order
    source_idx = code.index("# SOURCE VIEWS")
    transform_idx = code.index("# TRANSFORMATION VIEWS")
    target_idx = code.index("# TARGET TABLES")
    
    assert source_idx < transform_idx < target_idx
    
    # Verify each section has proper header formatting
    lines = code.split('\n')
    for i, line in enumerate(lines):
        if "# SOURCE VIEWS" in line or "# TRANSFORMATION VIEWS" in line or "# TARGET TABLES" in line:
            # Check line before is separator
            assert lines[i-1] == "# " + "=" * 76
            # Check line after is separator
            assert lines[i+1] == "# " + "=" * 76


def test_formatting_with_minimal_pipeline(tmp_path):
    """Test formatting with minimal pipeline (load + write only)."""
    # Create project structure
    (tmp_path / "pipelines" / "test").mkdir(parents=True)
    (tmp_path / "substitutions").mkdir()
    (tmp_path / "templates").mkdir()
    (tmp_path / "presets").mkdir()
    
    # Create flowgroup with only load and write actions (no transforms)
    flowgroup_dict = {
        "pipeline": "test_pipeline",
        "flowgroup": "test_flowgroup",
        "actions": [
            {
                "name": "load_customers",
                "type": "load",
                "target": "v_customers",
                "source": {
                    "type": "delta",
                    "table": "bronze.customers"
                }
            },
            {
                "name": "write_customers",
                "type": "write",
                "source": "v_customers",
                "write_target": {
                    "type": "streaming_table",
                    "database": "silver",
                    "table": "customers",
                    "create_table": True
                }
            }
        ]
    }
    
    # Save and generate
    import yaml
    flowgroup_file = tmp_path / "pipelines" / "test" / "test_flowgroup.yaml"
    with open(flowgroup_file, 'w') as f:
        yaml.dump(flowgroup_dict, f)
    
    sub_file = tmp_path / "substitutions" / "dev.yaml"
    with open(sub_file, 'w') as f:
        yaml.dump({"environment": {"dev": {}}}, f)
    
    orchestrator = ActionOrchestrator(tmp_path)
    generated_files = orchestrator.generate_pipeline("test", "dev")
    
    code = list(generated_files.values())[0]
    
    # Should have SOURCE VIEWS section
    assert "# SOURCE VIEWS" in code
    # Should NOT have TRANSFORMATION VIEWS section
    assert "# TRANSFORMATION VIEWS" not in code
    # Should have TARGET TABLES section
    assert "# TARGET TABLES" in code 