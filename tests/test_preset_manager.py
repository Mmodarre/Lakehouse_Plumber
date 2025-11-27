"""Tests for preset management functionality of LakehousePlumber."""

import pytest
from pathlib import Path
import tempfile
from lhp.presets.preset_manager import PresetManager


class TestPresetManager:
    """Test preset management and inheritance."""
    
    def test_preset_loading(self):
        """Test loading presets from directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            presets_dir = Path(temp_dir)
            
            # Create test preset
            (presets_dir / "bronze.yaml").write_text("""
name: bronze
version: "1.0"
description: Bronze layer preset
defaults:
  quality: bronze
  checkpoint: true
""")
            
            mgr = PresetManager(presets_dir)
            assert "bronze" in mgr.presets
            
            preset = mgr.get_preset("bronze")
            assert preset.name == "bronze"
            assert preset.defaults["quality"] == "bronze"
    
    def test_preset_inheritance(self):
        """Test preset inheritance resolution."""
        with tempfile.TemporaryDirectory() as temp_dir:
            presets_dir = Path(temp_dir)
            
            # Create base preset
            (presets_dir / "base.yaml").write_text("""
name: base
version: "1.0"
defaults:
  quality: base
  checkpoint: false
  common_setting: true
""")
            
            # Create child preset
            (presets_dir / "bronze.yaml").write_text("""
name: bronze
version: "1.0"
extends: base
defaults:
  quality: bronze
  checkpoint: true
""")
            
            mgr = PresetManager(presets_dir)
            
            # Resolve inheritance
            config = mgr._resolve_preset_inheritance("bronze")
            
            # Child overrides parent
            assert config["quality"] == "bronze"
            assert config["checkpoint"] is True
            # Parent settings are inherited
            assert config["common_setting"] is True
    
    def test_preset_chain_resolution(self):
        """Test resolving a chain of presets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            presets_dir = Path(temp_dir)
            
            # Create presets
            (presets_dir / "preset1.yaml").write_text("""
name: preset1
defaults:
  setting1: value1
  setting2: original
""")
            
            (presets_dir / "preset2.yaml").write_text("""
name: preset2
defaults:
  setting2: overridden
  setting3: value3
""")
            
            mgr = PresetManager(presets_dir)
            
            # Resolve chain
            config = mgr.resolve_preset_chain(["preset1", "preset2"])
            
            assert config["setting1"] == "value1"
            assert config["setting2"] == "overridden"  # preset2 overrides preset1
            assert config["setting3"] == "value3"


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 