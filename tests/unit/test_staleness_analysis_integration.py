"""Integration tests for staleness analysis with multi-flowgroup files."""

import pytest
import logging
from pathlib import Path


def test_staleness_analysis_with_multi_flowgroup_files(tmp_path):
    """End-to-end test: staleness analysis should work with multi-flowgroup files."""
    # Setup project structure
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    
    # Create multi-flowgroup file
    multi_fg = pipelines_dir / "ingestions.yaml"
    multi_fg.write_text("""
pipeline: raw_ingestion
flowgroup: brand_ingestion
actions:
  - name: load_brand
    type: load
    source: {type: delta, path: /brand}
---
pipeline: raw_ingestion
flowgroup: carrier_ingestion
actions:
  - name: load_carrier
    type: load
    source: {type: delta, path: /carrier}
""")
    
    # Initialize services
    from lhp.core.orchestrator import ActionOrchestrator
    from lhp.core.state_manager import StateManager
    
    orchestrator = ActionOrchestrator(tmp_path)
    state_manager = StateManager(tmp_path)
    
    # Run staleness analysis (should not raise errors or warnings)
    analysis = orchestrator.analyze_generation_requirements(
        env="dev",
        pipeline_names=["raw_ingestion"],
        include_tests=False,
        force=False,
        state_manager=state_manager
    )
    
    # Verify analysis completed successfully
    assert analysis is not None
    assert "raw_ingestion" in analysis.pipelines_needing_generation


def test_staleness_analysis_does_not_warn_on_multi_flowgroup_files(tmp_path, caplog):
    """Verify no warnings are logged for multi-flowgroup files during analysis."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    
    multi_fg = pipelines_dir / "multi.yaml"
    multi_fg.write_text("""
pipeline: test_pipeline
flowgroup: fg1
actions: []
---
pipeline: test_pipeline
flowgroup: fg2
actions: []
""")
    
    from lhp.core.orchestrator import ActionOrchestrator
    from lhp.core.state_manager import StateManager
    
    orchestrator = ActionOrchestrator(tmp_path)
    state_manager = StateManager(tmp_path)
    
    with caplog.at_level(logging.WARNING):
        orchestrator.analyze_generation_requirements(
            env="dev",
            pipeline_names=["test_pipeline"],
            include_tests=False,
            force=False,
            state_manager=state_manager
        )
    
    # Should have no warnings about YAML parsing
    yaml_warnings = [rec.message for rec in caplog.records 
                     if "Could not parse YAML" in rec.message or "expected a single document" in rec.message]
    assert len(yaml_warnings) == 0

