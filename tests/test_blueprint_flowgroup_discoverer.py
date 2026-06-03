"""Unit tests for FlowgroupDiscoveryService.register_synthetic_sources.

Synthetic flowgroups must resolve to their blueprint path, and synthetic entries
must override conflicting on-disk entries when the same (pipeline, flowgroup)
appears in both.
"""

from pathlib import Path

import pytest

from lhp.core.discovery.flowgroup_discoverer import FlowgroupDiscoveryService
from lhp.models import FlowGroup

pytestmark = pytest.mark.unit


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_register_synthetic_sources_populates_index(tmp_path):
    bp_path = _write(
        tmp_path / "blueprints" / "erp.yaml",
        "name: erp\nparameters: []\nflowgroups: []\n",
    )
    disco = FlowgroupDiscoveryService(tmp_path)
    disco.register_synthetic_sources({("apac_sg_raw", "apac_sg_orders"): bp_path})
    fg = FlowGroup(pipeline="apac_sg_raw", flowgroup="apac_sg_orders", actions=[])
    found = disco.find_source_yaml_for_flowgroup(fg)
    assert found == bp_path


def test_register_synthetic_with_empty_input_is_noop(tmp_path):
    disco = FlowgroupDiscoveryService(tmp_path)
    # Should not raise and should not initialize the index.
    disco.register_synthetic_sources({})
    fg = FlowGroup(pipeline="x", flowgroup="y", actions=[])
    # Index built lazily on first lookup; result must be None when nothing exists.
    assert disco.find_source_yaml_for_flowgroup(fg) is None


def test_synthetic_overrides_disk_entry_with_same_tuple(tmp_path):
    """When a (pipeline, flowgroup) tuple appears in both the on-disk index
    and the synthetic provenance map, the synthetic blueprint path wins."""
    # Disk file with conflicting (pipeline, flowgroup).
    disk_file = _write(
        tmp_path / "pipelines" / "x.yaml",
        """
pipeline: shared_pipeline
flowgroup: shared_fg
actions:
  - name: load
    type: load
    source:
      type: sql
      sql: "SELECT 1"
    target: v_a
""",
    )
    bp_path = _write(
        tmp_path / "blueprints" / "erp.yaml",
        "name: erp\nparameters: []\nflowgroups: []\n",
    )
    disco = FlowgroupDiscoveryService(tmp_path)
    # Force eager build of disk index by looking up first.
    fg = FlowGroup(pipeline="shared_pipeline", flowgroup="shared_fg", actions=[])
    pre = disco.find_source_yaml_for_flowgroup(fg)
    assert pre == disk_file
    # Now register a synthetic source — must override.
    disco.register_synthetic_sources({("shared_pipeline", "shared_fg"): bp_path})
    post = disco.find_source_yaml_for_flowgroup(fg)
    assert post == bp_path
