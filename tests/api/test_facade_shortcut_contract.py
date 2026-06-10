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
