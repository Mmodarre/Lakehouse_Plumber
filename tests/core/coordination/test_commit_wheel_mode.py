"""Contract pins for the wheel-mode COMMIT branch.

These tests pin :func:`lhp.core.coordination._commit.commit_pipeline`'s
``packaging_mode == "wheel"`` branch and its delegate
:func:`._commit._commit_wheel_pipeline`. The branch packages ONE pipeline into a
single deterministic ``.whl`` (written under the env-root ``_wheels/`` staging
tree) plus a single runner ``.py`` (written under the pipeline ``output_dir``),
and SKIPS every source-mode disk write (``_write_python_files``,
``_apply_copy_records``, the disk test-hook).

The four pins:

1. **Wheel mode** — exactly one runner ``.py`` (``<import_pkg>_runner.py``)
   directly under the pipeline dir; exactly one ``.whl`` under
   ``output_dir.parent / "_wheels" / <pipeline> / "dist" /``; NO loose flowgroup
   ``.py``; NO ``custom_python_functions/`` dir on disk (it rides INSIDE the
   wheel instead); the returned delta carries ``wheel_filename`` and
   ``files_written == 1`` / ``generated_filenames == (<runner>,)``.
2. **Mode isolation** — the SAME inputs committed in ``"source"`` mode produce
   loose flowgroup ``.py`` and a ``custom_python_functions/`` dir on disk, and
   ``wheel_filename is None``.
3. **Dry-run** — ``output_dir is None`` with ``packaging_mode == "wheel"`` writes
   NOTHING (no wheel, no runner) and does not crash.
4. **Stale-wheel regression** — a ``.whl`` planted under
   ``generated/<env>/_wheels/<pipeline>/dist/`` before a fresh
   :func:`._commit.commit_generate_results` run is GONE afterward, because the
   single up-front env-root wipe (``_wipe_env_output_dir`` →
   ``shutil.rmtree(env_root)``) removed the whole ``_wheels/`` subtree.

The ``FlowgroupOutcome.ok(...)`` + ``CopiedModuleRecord`` construction (a
flowgroup module via ``formatted_code`` plus a ``custom_python_functions`` copy
record whose ``dest_path`` is under ``output_dir / "custom_python_functions"``)
mirrors ``tests/core/packaging/test_packager.py``'s ``_outcome`` helper.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.core.coordination._commit import commit_generate_results, commit_pipeline
from lhp.core.coordination._pool import _PipelinePoolResult
from lhp.core.packaging import import_package_name
from lhp.models.processing import (
    CopiedModuleRecord,
    FlowgroupOutcome,
    PipelineDelta,
)

# A real-world pipeline name that is NOT a valid Python identifier, so the
# import-package sanitization (leading-digit ``p_`` prefix) is exercised.
_PIPELINE = "15_python_load"
_ENV = "dev"
_IMPORT_PKG = import_package_name(_PIPELINE)  # -> "p_15_python_load"
_RUNNER_FILENAME = f"{_IMPORT_PKG}_runner.py"

_FLOWGROUP_CODE = (
    "import dlt\n\n\n@dlt.table\ndef customers():\n    return spark.range(1)\n"
)
_HELPER_CODE = "# LHP-SOURCE: pipelines/helpers/util.py\ndef helper():\n    return 1\n"


class _FakeFlowGroup:
    """Minimal resolved-FlowGroup stand-in.

    ``_commit_wheel_pipeline`` collects ``o.resolved_flowgroup`` for the
    test-reporting hook; the hook's own guards short-circuit when tests are off
    (``include_tests=False``, no ``project_config``), so the object is never
    introspected here.
    """

    def __init__(self, pipeline: str, flowgroup: str) -> None:
        self.pipeline = pipeline
        self.flowgroup = flowgroup


def _outcome(output_dir: Path) -> FlowgroupOutcome:
    """One success outcome: a flowgroup module + a ``custom_python_functions``
    copy record (its ``dest_path`` under ``output_dir / "custom_python_functions"``).

    Mirrors ``tests/core/packaging/test_packager.py``'s ``_outcome``. In wheel
    mode the copy record rides INSIDE the wheel (top-level
    ``custom_python_functions/``); in source mode it is written to disk under the
    pipeline dir — the contrast the mode-isolation test asserts.
    """
    cpf_dir = output_dir / "custom_python_functions"
    return FlowgroupOutcome.ok(
        _PIPELINE,
        "customers",
        resolved_flowgroup=_FakeFlowGroup(_PIPELINE, "customers"),
        formatted_code=_FLOWGROUP_CODE,
        copy_records=[
            CopiedModuleRecord(
                source_path="pipelines/helpers/util.py",
                dest_path=cpf_dir / "util.py",
                content=_HELPER_CODE,
                module_path="pipelines.helpers.util",
                custom_functions_dir=cpf_dir,
            ),
        ],
    )


def _wheel_dir(output_dir: Path) -> Path:
    """The env-root staging dir the wheel is written under (C2 contract):
    ``output_dir.parent / "_wheels" / <pipeline> / "dist"``."""
    return output_dir.parent / "_wheels" / _PIPELINE / "dist"


@pytest.mark.unit
def test_wheel_mode_writes_runner_and_wheel_only(tmp_path: Path) -> None:
    """Wheel mode: exactly one runner ``.py`` under the pipeline dir + one
    ``.whl`` under the env-root ``_wheels/`` tree; NO loose flowgroup ``.py`` and
    NO ``custom_python_functions/`` dir on disk; delta carries ``wheel_filename``,
    ``files_written == 1``, ``generated_filenames == (<runner>,)``."""
    env_root = tmp_path / "generated" / _ENV
    output_dir = env_root / _PIPELINE

    delta = commit_pipeline(
        _PIPELINE,
        [_outcome(output_dir)],
        output_dir=output_dir,
        project_config=None,
        project_root=tmp_path,
        include_tests=False,
        env=_ENV,
        packaging_mode="wheel",
        substitution_mgr=None,
    )

    # Exactly one runner .py directly under the pipeline dir, named for the pkg.
    py_files = sorted(p.name for p in output_dir.glob("*.py"))
    assert py_files == [_RUNNER_FILENAME]
    assert (output_dir / _RUNNER_FILENAME).is_file()

    # No loose flowgroup module on disk (it lives inside the wheel instead).
    assert not (output_dir / "customers.py").exists()
    # No custom_python_functions/ dir on disk (it ships inside the wheel).
    assert not (output_dir / "custom_python_functions").exists()

    # Exactly one .whl under output_dir.parent / _wheels / <pipeline> / dist.
    wheels = sorted((_wheel_dir(output_dir)).glob("*.whl"))
    assert len(wheels) == 1
    assert wheels[0].name == delta.wheel_filename

    # The delta is the wheel-mode shape.
    assert isinstance(delta, PipelineDelta)
    assert delta.success is True
    assert delta.wheel_filename is not None
    assert delta.wheel_filename.endswith("-py3-none-any.whl")
    assert delta.files_written == 1
    assert delta.generated_filenames == (_RUNNER_FILENAME,)


@pytest.mark.unit
def test_source_mode_writes_loose_files_and_no_wheel(tmp_path: Path) -> None:
    """Mode isolation: the SAME inputs in ``"source"`` mode produce the loose
    flowgroup ``.py`` and a ``custom_python_functions/`` dir on disk, no wheel,
    and ``wheel_filename is None`` — the opposite outcome of wheel mode."""
    env_root = tmp_path / "generated" / _ENV
    output_dir = env_root / _PIPELINE

    delta = commit_pipeline(
        _PIPELINE,
        [_outcome(output_dir)],
        output_dir=output_dir,
        project_config=None,
        project_root=tmp_path,
        include_tests=False,
        env=_ENV,
        packaging_mode="source",
        substitution_mgr=None,
    )

    # Source mode writes the loose flowgroup module to disk.
    assert (output_dir / "customers.py").is_file()
    # ... and the custom_python_functions copy record onto disk.
    assert (output_dir / "custom_python_functions" / "util.py").is_file()
    # No runner is written in source mode.
    assert not (output_dir / _RUNNER_FILENAME).exists()
    # No wheel anywhere under the _wheels/ staging tree.
    assert not _wheel_dir(output_dir).exists()

    # The delta is the source-mode shape: wheel_filename unset, the flowgroup
    # module is the generated file (NOT a runner).
    assert delta.success is True
    assert delta.wheel_filename is None
    assert delta.generated_filenames == ("customers.py",)
    assert delta.files_written == 1


@pytest.mark.unit
def test_wheel_mode_dry_run_writes_no_wheel_or_runner(tmp_path: Path) -> None:
    """Dry-run: ``output_dir is None`` with ``packaging_mode == "wheel"`` produces
    NO wheel and NO runner, and does not crash.

    With ``output_dir is None`` the wheel-branch guard (``output_dir is not
    None``) is not entered, so ``commit_pipeline`` falls through to the
    source-mode body, whose ``_write_python_files`` / disk test-hook steps are
    dry-run no-ops. The wheel-specific contract is what's pinned here: no ``.whl``
    is staged and no runner ``.py`` is emitted.

    Scope note: a bare flowgroup outcome (no ``copy_records``) is used. Adding a
    ``custom_python_functions`` record would assert nothing about the WHEEL
    branch — ``_apply_copy_records`` writes copy records to their absolute
    ``dest_path`` even in source-mode dry-run (a pre-existing, wheel-independent
    behavior), so "the whole tree is empty" is not a wheel-mode invariant.
    """
    outcome = FlowgroupOutcome.ok(
        _PIPELINE,
        "customers",
        resolved_flowgroup=_FakeFlowGroup(_PIPELINE, "customers"),
        formatted_code=_FLOWGROUP_CODE,
    )

    delta = commit_pipeline(
        _PIPELINE,
        [outcome],
        output_dir=None,
        project_config=None,
        project_root=tmp_path,
        include_tests=False,
        env=_ENV,
        packaging_mode="wheel",
        substitution_mgr=None,
    )

    # No wheel and no runner were written anywhere under tmp_path.
    assert sorted(p.name for p in tmp_path.rglob("*.whl")) == []
    assert sorted(p.name for p in tmp_path.rglob("*_runner.py")) == []
    assert sorted(p for p in tmp_path.rglob("*") if p.is_file()) == []

    # Dry-run still synthesizes a success delta with no wheel. ``files_written``
    # is NOT asserted to be 0: source-mode dry-run intentionally still populates
    # the "would generate" filename tuple (see ``_collect_generated_filenames``),
    # so ``files_written == len(generated_filenames)`` even though zero bytes hit
    # disk — that is documented source-mode behavior, not the wheel contract.
    assert delta.success is True
    assert delta.wheel_filename is None


@pytest.mark.unit
def test_stale_wheel_removed_by_env_root_wipe(tmp_path: Path) -> None:
    """Stale-wheel regression: a ``.whl`` planted under
    ``generated/<env>/_wheels/<pipeline>/dist/`` before a fresh
    :func:`commit_generate_results` run is GONE afterward.

    Driven through the REAL ``commit_generate_results`` wipe path (not the
    ``_wipe_env_output_dir`` fallback): ``commit_generate_results`` calls
    ``_wipe_env_output_dir(output_dir)`` — where ``output_dir`` is the ENV ROOT
    ``generated/<env>`` — ONCE up front, which ``shutil.rmtree``s the whole env
    root, taking any prior ``_wheels/`` subtree with it. A fresh wheel-mode
    commit then re-stages a NEW wheel, proving the wipe ran before (not after)
    the per-pipeline writes.

    ``_PipelinePoolResult`` is a plain coordinator-side ``NamedTuple`` (never
    crosses the spawn boundary), so it is constructed directly here rather than
    driven through the full flowgroup pool / gate.
    """
    env_root = tmp_path / "generated" / _ENV
    output_dir = env_root / _PIPELINE

    # Plant a stale wheel exactly where a prior wheel-mode run would have left it.
    stale_dir = _wheel_dir(output_dir)
    stale_dir.mkdir(parents=True, exist_ok=True)
    stale_wheel = stale_dir / "stale_pkg-0.0.1-py3-none-any.whl"
    stale_wheel.write_bytes(b"PK\x03\x04 stale placeholder")
    assert stale_wheel.exists()  # guard: it really is there before the run

    pool_result = _PipelinePoolResult(
        pipeline=_PIPELINE,
        outcomes_in_order=(_outcome(output_dir),),
        cross_fg_issues=(),
        cross_fg_errors=(),
    )

    # Drain the generator: the wipe runs when the first delta is pulled, then
    # each pipeline is committed in input order.
    deltas = list(
        commit_generate_results(
            [pool_result],
            pipeline_output_dirs={_PIPELINE: output_dir},
            substitution_managers={_PIPELINE: None},
            include_tests=False,
            output_dir=env_root,  # the ENV ROOT — what gets wiped
            project_config=None,
            project_root=tmp_path,
            env=_ENV,
            packaging_modes={_PIPELINE: "wheel"},
        )
    )

    # The exact stale file is gone — the env-root wipe removed the _wheels/ tree.
    assert not stale_wheel.exists()

    # And the fresh wheel-mode commit re-staged a NEW wheel (distinct filename),
    # confirming the wipe preceded the per-pipeline writes rather than clobbering
    # them.
    assert len(deltas) == 1
    fresh_wheel_name = deltas[0].wheel_filename
    assert fresh_wheel_name is not None
    assert fresh_wheel_name != stale_wheel.name
    fresh_wheels = sorted(p.name for p in stale_dir.glob("*.whl"))
    assert fresh_wheels == [fresh_wheel_name]
