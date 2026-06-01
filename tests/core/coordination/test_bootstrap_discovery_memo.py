"""Integration: the disk file-walk runs exactly once per invocation.

T1 made :meth:`FlowgroupBootstrapService.discover_all_flowgroups` memoize at
the bootstrap boundary (a guarded ``self._discovery_cache`` tuple returned by
identity). The bootstrap boundary is hit multiple times across one
``validate``/``generate`` invocation — at minimum the shared project preflight
(:func:`lhp.api._preflight._run_project_preflight`) and the flat-engine worklist
builder (:func:`lhp.core.coordination.flowgroup_worklist_builder.build_flowgroup_worklist`),
plus CLI listing / bundle-asset checks in the real CLI. Because all sub-facades
share ONE orchestrator (and therefore one bootstrap) within an invocation, the
memo is shared and the underlying disk file-walk must run only once.

This module spies on the disk *sink* — not the memoized boundary — to prove
that. The sink is
:meth:`FlowgroupDiscoveryService.discover_all_flowgroups_with_paths`, which is
where the ``rglob`` over ``pipelines/`` actually happens. Its call chain from
the boundary is:

    bootstrap.discover_all_flowgroups()            # memoized (T1)
      -> discovery.discover_flowgroups(filter=None)
      -> discovery.discover_all_flowgroups()       # populates source-path index
      -> discovery.discover_all_flowgroups_with_paths()   # <-- disk walk

:stability: provisional
"""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from lhp.api import LakehousePlumberApplicationFacade, collect_response
from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.core.discovery.flowgroup_discoverer import FlowgroupDiscoveryService


def _build_multipipeline_project(tmpdir, pipeline_names):
    """Build a multi-pipeline project with one flowgroup per pipeline.

    Copied verbatim from ``tests/test_orchestrator.py`` so the construction
    idiom (and the on-disk files the discovery walk must find) is identical.
    """
    project_root = Path(tmpdir)
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "presets").mkdir()
    (project_root / "templates").mkdir()
    (project_root / "substitutions").mkdir()
    for name in pipeline_names:
        (project_root / "pipelines" / name).mkdir(parents=True)

    substitutions = {
        "dev": {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
        }
    }
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(substitutions, f)

    for name in pipeline_names:
        flowgroup = {
            "pipeline": name,
            "flowgroup": f"{name}_fg",
            "actions": [
                {
                    "name": f"load_{name}",
                    "type": "load",
                    "target": f"v_{name}_raw",
                    "source": {
                        "type": "cloudfiles",
                        "path": "${landing_path}/" + name,
                        "format": "json",
                    },
                },
                {
                    "name": f"clean_{name}",
                    "type": "transform",
                    "transform_type": "sql",
                    "source": f"v_{name}_raw",
                    "target": f"v_{name}_clean",
                    "sql": f"SELECT * FROM v_{name}_raw",
                },
                {
                    "name": f"write_{name}",
                    "type": "write",
                    "source": f"v_{name}_clean",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "${catalog}",
                        "schema": "${bronze_schema}",
                        "table": name,
                        "create_table": True,
                    },
                },
            ],
        }
        with open(project_root / "pipelines" / name / f"{name}_fg.yaml", "w") as f:
            yaml.dump(flowgroup, f)

    return project_root


def _spy_on_disk_walk():
    """Patch the disk sink with an autospec mock that wraps the real method.

    ``autospec=True`` makes the mock forward ``(self, ...)`` to ``wraps``;
    passing the original *unbound function* as ``wraps`` preserves real
    discovery behavior while the mock counts every call. Patching the class
    (not an instance) catches whichever ``FlowgroupDiscoveryService`` instance
    the orchestrator constructs internally (§8.8 — spy a real boundary, do not
    fake the service we own).
    """
    original = FlowgroupDiscoveryService.discover_all_flowgroups_with_paths
    return patch.object(
        FlowgroupDiscoveryService,
        "discover_all_flowgroups_with_paths",
        autospec=True,
        wraps=original,
    )


@pytest.mark.integration
def test_validate_walks_disk_once(tmp_path):
    """A full facade ``validate_pipelines`` invocation walks disk exactly once.

    The facade fans the bootstrap boundary out to preflight + the validate
    work-unit pool (≥2 boundary calls), all sharing one orchestrator/bootstrap
    memo, so the disk sink fires once.
    """
    project_root = _build_multipipeline_project(
        tmp_path / "validate", ["v_alpha", "v_beta", "v_gamma"]
    )
    facade = LakehousePlumberApplicationFacade.for_project(
        project_root, enforce_version=False
    )

    with _spy_on_disk_walk() as disk_walk:
        response = collect_response(
            facade.validate_pipelines(
                pipeline_fields=["v_alpha", "v_beta", "v_gamma"],
                env="dev",
                include_tests=True,
            )
        )

    # Real wiring ran end-to-end: every pipeline validated cleanly.
    assert response.success, response
    assert disk_walk.call_count == 1, (
        "Disk file-walk must run exactly once across a full validate "
        f"invocation; ran {disk_walk.call_count} times. A count > 1 means a "
        "boundary call site bypassed the shared bootstrap memo (e.g. a "
        "discovery self-call not fed pre_discovered)."
    )


@pytest.mark.integration
def test_generate_walks_disk_once(tmp_path):
    """A full facade ``generate_pipelines`` invocation walks disk exactly once.

    Built with a FRESH facade (fresh orchestrator -> fresh memo) so the count
    is proven independently of the validate test above.
    """
    project_root = _build_multipipeline_project(
        tmp_path / "generate", ["g_alpha", "g_beta", "g_gamma"]
    )
    facade = LakehousePlumberApplicationFacade.for_project(
        project_root, enforce_version=False
    )

    with _spy_on_disk_walk() as disk_walk:
        response = collect_response(
            facade.generate_pipelines(
                pipeline_fields=["g_alpha", "g_beta", "g_gamma"],
                env="dev",
                output_dir=project_root / "generated",
            )
        )

    # Real wiring ran end-to-end: every pipeline generated a file.
    assert response.success, response
    for name in ("g_alpha", "g_beta", "g_gamma"):
        pr = response.pipeline_responses.get(name)
        assert (
            pr is not None and pr.generated_filenames
        ), f"Pipeline {name} produced no output: {pr}"

    assert disk_walk.call_count == 1, (
        "Disk file-walk must run exactly once across a full generate "
        f"invocation; ran {disk_walk.call_count} times. A count > 1 means a "
        "boundary call site bypassed the shared bootstrap memo (e.g. a "
        "discovery self-call not fed pre_discovered)."
    )


@pytest.mark.integration
def test_boundary_returns_stable_identity_guards_slice_cache(tmp_path):
    """The memoized boundary returns the SAME tuple by identity across calls.

    Substitution for a direct ``_pipeline_slice_cache`` assertion (the cache is
    invalidated at the start of every plural entry point and cleared after, so
    it cannot be read populated post-run). It guards the same trap:
    :meth:`ActionOrchestrator._lookup_pipeline_slice` keys its by-pipeline
    grouping on ``id(all_flowgroups)``. If the boundary returned a fresh tuple
    each call (a defensive copy), ``id()`` would change between the preflight
    call and the pool-builder call, cold-missing the slice cache on every
    pipeline. Identity stability is therefore load-bearing, and this asserts it
    directly.

    A raw orchestrator is built here (the sanctioned in-test seam for touching
    orchestrator internals, mirroring ``tests/test_orchestrator.py``); the
    facade deliberately hides ``_orchestrator`` (§1.10, §9.23), so this does
    NOT reach through the facade.
    """
    project_root = _build_multipipeline_project(
        tmp_path / "identity", ["s_alpha", "s_beta"]
    )
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)

    with _spy_on_disk_walk() as disk_walk:
        first = orchestrator.bootstrap.discover_all_flowgroups()
        second = orchestrator.bootstrap.discover_all_flowgroups()

    assert first is second, (
        "Bootstrap memo must return the cached tuple by identity; a fresh "
        "tuple each call would cold-miss ActionOrchestrator._pipeline_slice_cache "
        "(keyed on id(all_flowgroups))."
    )
    # And the identity stability comes for free from the same single disk walk.
    assert disk_walk.call_count == 1, (
        f"Two boundary calls triggered {disk_walk.call_count} disk walks; "
        "memo not shared across calls on the same instance."
    )
