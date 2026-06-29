"""Contract test: top-level facade shortcuts must restate canonical signatures.

Per constitution §4.2, shortcut/wrapper methods on
:class:`LakehousePlumberApplicationFacade` must restate the canonical
signature of the sub-facade method they delegate to. ``**kwargs: Any``
defeats ``mypy --strict``, IDE autocomplete, and Sphinx docs — the
strict check passes vacuously and caller-side typos surface only at
runtime.

These tests guard against silent drift in two directions:

1. If the canonical sub-facade method gains a parameter that isn't
   mirrored on the shortcut, the signature-comparison test fails.
2. If either shortcut regresses to ``**kwargs``, the no-var-keyword
   test fails.
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from lhp.api.facade import (
    GenerationFacade,
    LakehousePlumberApplicationFacade,
    ValidationFacade,
)


def _params_excluding_self(method: Any) -> dict[str, tuple[Any, Any, Any]]:
    sig = inspect.signature(method)
    return {
        name: (p.kind, p.default, p.annotation)
        for name, p in sig.parameters.items()
        if name != "self"
    }


def test_generate_pipelines_shortcut_matches_canonical_signature() -> None:
    shortcut = _params_excluding_self(
        LakehousePlumberApplicationFacade.generate_pipelines
    )
    canonical = _params_excluding_self(GenerationFacade.generate_pipelines)
    assert shortcut == canonical, (
        "LakehousePlumberApplicationFacade.generate_pipelines must restate "
        "the canonical signature of GenerationFacade.generate_pipelines "
        "(constitution §4.2). Drift detected."
    )


def test_validate_pipelines_shortcut_matches_canonical_signature() -> None:
    shortcut = _params_excluding_self(
        LakehousePlumberApplicationFacade.validate_pipelines
    )
    canonical = _params_excluding_self(ValidationFacade.validate_pipelines)
    assert shortcut == canonical, (
        "LakehousePlumberApplicationFacade.validate_pipelines must restate "
        "the canonical signature of ValidationFacade.validate_pipelines "
        "(constitution §4.2). Drift detected."
    )


def test_generate_pipelines_shortcut_has_no_var_keyword() -> None:
    sig = inspect.signature(LakehousePlumberApplicationFacade.generate_pipelines)
    var_kw = [
        p for p in sig.parameters.values() if p.kind == inspect.Parameter.VAR_KEYWORD
    ]
    assert not var_kw, (
        "generate_pipelines must not use **kwargs (§4.2). Found: "
        f"{[p.name for p in var_kw]}"
    )


def test_validate_pipelines_shortcut_has_no_var_keyword() -> None:
    sig = inspect.signature(LakehousePlumberApplicationFacade.validate_pipelines)
    var_kw = [
        p for p in sig.parameters.values() if p.kind == inspect.Parameter.VAR_KEYWORD
    ]
    assert not var_kw, (
        "validate_pipelines must not use **kwargs (§4.2). Found: "
        f"{[p.name for p in var_kw]}"
    )


def test_sandbox_param_present_with_default_false_on_all_four_methods() -> None:
    """Every generate/validate surface accepts ``sandbox`` (default False).

    Keyword-only, defaulting to ``False`` so existing callers are unchanged
    (provisional stability permits the additive kwarg, §1.13).
    """
    methods = [
        GenerationFacade.generate_pipelines,
        ValidationFacade.validate_pipelines,
        LakehousePlumberApplicationFacade.generate_pipelines,
        LakehousePlumberApplicationFacade.validate_pipelines,
    ]
    for method in methods:
        params = inspect.signature(method).parameters
        assert "sandbox" in params, f"{method.__qualname__} is missing `sandbox`"
        param = params["sandbox"]
        assert param.kind == inspect.Parameter.KEYWORD_ONLY, (
            f"{method.__qualname__}: `sandbox` must be keyword-only"
        )
        assert param.default is False, (
            f"{method.__qualname__}: `sandbox` must default to False"
        )


class TestSandboxPipelineSelectionMutex:
    """``sandbox=True`` cannot be combined with a caller-supplied worklist.

    Sandbox scope comes from the personal profile, so
    ``pipeline_filter`` / ``pipeline_fields`` alongside ``sandbox=True`` is
    API misuse → plain :class:`ValueError` (no structured ``LHP-*`` code),
    raised on first iteration, before the orchestrator is touched (the
    dummy ``object()`` orchestrator below would explode on any attribute
    access). The shortcut methods delegate to these canonical sub-facade
    methods, so the enforcement covers all four surfaces.
    """

    def test_generate_sandbox_with_pipeline_filter_raises(self) -> None:
        facade = GenerationFacade(object())
        with pytest.raises(ValueError, match="sandbox"):
            next(
                facade.generate_pipelines(
                    pipeline_filter="some_pipeline",
                    env="dev",
                    output_dir=None,
                    sandbox=True,
                )
            )

    def test_generate_sandbox_with_pipeline_fields_raises(self) -> None:
        facade = GenerationFacade(object())
        with pytest.raises(ValueError, match="sandbox"):
            next(
                facade.generate_pipelines(
                    pipeline_fields=["p1", "p2"],
                    env="dev",
                    output_dir=None,
                    sandbox=True,
                )
            )

    def test_validate_sandbox_with_pipeline_filter_raises(self) -> None:
        facade = ValidationFacade(object())
        with pytest.raises(ValueError, match="sandbox"):
            next(
                facade.validate_pipelines(
                    pipeline_filter="some_pipeline",
                    env="dev",
                    sandbox=True,
                )
            )

    def test_validate_sandbox_with_pipeline_fields_raises(self) -> None:
        facade = ValidationFacade(object())
        with pytest.raises(ValueError, match="sandbox"):
            next(
                facade.validate_pipelines(
                    pipeline_fields=["p1"],
                    env="dev",
                    sandbox=True,
                )
            )
