"""Discovery ordering guard: the no-include-patterns rglob fallback is sorted.

``Path.rglob`` yields files in OS directory order (and the fallback globs
``*.yaml`` before ``*.yml``, grouping by extension), so downstream consumers —
the dependency graph builder in particular — used to see a nondeterministic
flowgroup order. The fallback now sorts its result; the include-patterns
branch was already sorted via ``utils/file_pattern_matcher.py``.

The fixture deliberately includes a ``.yml`` file that lexicographically
precedes a ``.yaml`` sibling in the same directory: without the sort, every
``.yml`` file trails ALL ``.yaml`` files regardless of readdir order, so the
assertion fails deterministically on a regression.
"""

from pathlib import Path

import pytest
import yaml

from lhp.core.discovery.flowgroup_discoverer import FlowgroupDiscoveryService


def _write_flowgroup(path: Path, pipeline: str, name: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = {
        "pipeline": pipeline,
        "flowgroup": name,
        "actions": [
            {
                "name": f"t_{name}",
                "type": "transform",
                "source": "raw.src",
                "target": f"v_{name}",
            }
        ],
    }
    with open(path, "w") as f:
        yaml.dump(doc, f)
    return path


@pytest.mark.integration
def test_discovery_returns_sorted_files(tmp_path):
    pipelines_dir = tmp_path / "pipelines"
    created = [
        _write_flowgroup(path, pipeline, name)
        for path, pipeline, name in [
            (pipelines_dir / "zeta" / "customers.yaml", "zeta", "zeta_customers"),
            (pipelines_dir / "alpha" / "zoo.yaml", "alpha", "alpha_zoo"),
            (pipelines_dir / "zeta" / "apples.yml", "zeta", "zeta_apples"),
            (pipelines_dir / "alpha" / "beta.yaml", "alpha", "alpha_beta"),
            (pipelines_dir / "mango.yaml", "mango", "mango_root"),
        ]
    ]

    # No config_loader -> no include patterns -> the rglob fallback under test.
    service = FlowgroupDiscoveryService(tmp_path)

    pairs = service.discover_all_flowgroups_with_paths()
    discovered = [path for _, path in pairs]
    assert set(discovered) == set(created)
    assert discovered == sorted(created)

    assert service._iter_pipeline_yaml_files() == sorted(created)
