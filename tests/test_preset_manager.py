import tempfile
from pathlib import Path

import pytest

from lhp.presets.preset_manager import PresetManager


class TestPresetManager:
    def test_preset_loading(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            presets_dir = Path(temp_dir)

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

            preset = mgr.presets["bronze"]
            assert preset.name == "bronze"
            assert preset.defaults["quality"] == "bronze"

    def test_preset_inheritance(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            presets_dir = Path(temp_dir)

            (presets_dir / "base.yaml").write_text("""
name: base
version: "1.0"
defaults:
  quality: base
  checkpoint: false
  common_setting: true
""")

            (presets_dir / "bronze.yaml").write_text("""
name: bronze
version: "1.0"
extends: base
defaults:
  quality: bronze
  checkpoint: true
""")

            mgr = PresetManager(presets_dir)

            config = mgr._resolve_preset_inheritance("bronze")

            assert config["quality"] == "bronze"
            assert config["checkpoint"] is True
            assert config["common_setting"] is True

    def test_preset_chain_resolution(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            presets_dir = Path(temp_dir)

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

            config = mgr.resolve_preset_chain(["preset1", "preset2"])

            assert config["setting1"] == "value1"
            assert config["setting2"] == "overridden"
            assert config["setting3"] == "value3"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
