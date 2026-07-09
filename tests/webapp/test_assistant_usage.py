"""Unit tests for the pure usage-normalization / configured-cost module."""

from __future__ import annotations

import pytest

from lhp.webapp.services.assistant_usage import (
    compute_configured_cost,
    normalize_model_usage,
    normalize_usage,
)

pytestmark = pytest.mark.webapp

_ZERO = {
    "input_tokens": 0,
    "output_tokens": 0,
    "cache_read_input_tokens": 0,
    "cache_creation_input_tokens": 0,
}


# ---------------------------------------------------------------------------
# normalize_usage / normalize_model_usage
# ---------------------------------------------------------------------------


def test_normalize_usage_snake_case_passthrough() -> None:
    raw = {
        "input_tokens": 10,
        "output_tokens": 20,
        "cache_read_input_tokens": 30,
        "cache_creation_input_tokens": 40,
    }
    assert normalize_usage(raw) == raw


def test_normalize_usage_camel_case_model_usage_shape() -> None:
    raw = {
        "inputTokens": 1,
        "outputTokens": 2,
        "cacheReadInputTokens": 3,
        "cacheCreationInputTokens": 4,
    }
    assert normalize_usage(raw) == {
        "input_tokens": 1,
        "output_tokens": 2,
        "cache_read_input_tokens": 3,
        "cache_creation_input_tokens": 4,
    }


def test_normalize_usage_missing_keys_default_zero() -> None:
    assert normalize_usage({"input_tokens": 7}) == {**_ZERO, "input_tokens": 7}


def test_normalize_usage_ignores_unknown_and_non_numeric_keys() -> None:
    raw = {
        "input_tokens": 5,
        "service_tier": "standard",
        "webSearchRequests": 2,
        "output_tokens": "not-a-number",
    }
    assert normalize_usage(raw) == {**_ZERO, "input_tokens": 5}


def test_normalize_usage_none_and_empty() -> None:
    assert normalize_usage(None) == _ZERO
    assert normalize_usage({}) == _ZERO


def test_normalize_model_usage_normalizes_each_model() -> None:
    raw = {
        "claude-sonnet-5": {"inputTokens": 100, "outputTokens": 50},
        "claude-haiku-4-5": {"input_tokens": 10},
    }
    assert normalize_model_usage(raw) == {
        "claude-sonnet-5": {**_ZERO, "input_tokens": 100, "output_tokens": 50},
        "claude-haiku-4-5": {**_ZERO, "input_tokens": 10},
    }
    assert normalize_model_usage(None) == {}
    assert normalize_model_usage({}) == {}


# ---------------------------------------------------------------------------
# compute_configured_cost
# ---------------------------------------------------------------------------

_MTOK_USAGE = {
    "input_tokens": 1_000_000,
    "output_tokens": 1_000_000,
    "cache_read_input_tokens": 1_000_000,
    "cache_creation_input_tokens": 1_000_000,
}


def test_cost_exact_match_all_rates_given() -> None:
    pricing = {
        "claude-sonnet-5": {
            "input_per_mtok": 3.0,
            "output_per_mtok": 15.0,
            "cache_read_per_mtok": 0.3,
            "cache_write_per_mtok": 3.75,
        }
    }
    cost = compute_configured_cost({"claude-sonnet-5": _MTOK_USAGE}, pricing)
    assert cost == pytest.approx(3.0 + 15.0 + 0.3 + 3.75)


def test_cost_longest_prefix_match_wins() -> None:
    pricing = {
        "claude-": {"input_per_mtok": 100.0, "output_per_mtok": 100.0},
        "claude-sonnet-": {"input_per_mtok": 3.0, "output_per_mtok": 15.0},
    }
    usage = {
        "claude-sonnet-5-20260101": {
            **_MTOK_USAGE,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0,
        }
    }
    cost = compute_configured_cost(usage, pricing)
    assert cost == pytest.approx(3.0 + 15.0)


def test_cost_exact_match_beats_longer_prefix() -> None:
    pricing = {
        "claude-sonnet-5-20260101-extended": {
            "input_per_mtok": 99.0,
            "output_per_mtok": 99.0,
        },
        "claude-sonnet-5": {"input_per_mtok": 1.0, "output_per_mtok": 2.0},
    }
    usage = {
        "claude-sonnet-5": {
            **_ZERO,
            "input_tokens": 1_000_000,
            "output_tokens": 1_000_000,
        }
    }
    assert compute_configured_cost(usage, pricing) == pytest.approx(3.0)


def test_cost_cache_defaults_applied_when_rates_omitted() -> None:
    # cache_read = 0.1 x input, cache_write = 1.25 x input.
    pricing = {"m": {"input_per_mtok": 10.0, "output_per_mtok": 0.0}}
    usage = {
        "m": {
            **_ZERO,
            "cache_read_input_tokens": 1_000_000,
            "cache_creation_input_tokens": 1_000_000,
        }
    }
    assert compute_configured_cost(usage, pricing) == pytest.approx(1.0 + 12.5)


def test_cost_any_unpriced_model_returns_none_never_partial() -> None:
    pricing = {"claude-sonnet-": {"input_per_mtok": 3.0, "output_per_mtok": 15.0}}
    usage = {
        "claude-sonnet-5": {**_ZERO, "input_tokens": 100},
        "claude-haiku-4-5": {**_ZERO, "input_tokens": 100},
    }
    assert compute_configured_cost(usage, pricing) is None


def test_cost_empty_pricing_or_empty_usage_is_none() -> None:
    usage = {"m": {**_ZERO, "input_tokens": 1}}
    assert compute_configured_cost(usage, {}) is None
    assert compute_configured_cost({}, {"m": {"input_per_mtok": 1.0}}) is None


def test_cost_multiple_priced_models_sum() -> None:
    pricing = {
        "a": {"input_per_mtok": 1.0, "output_per_mtok": 2.0},
        "b": {"input_per_mtok": 10.0, "output_per_mtok": 20.0},
    }
    usage = {
        "a": {**_ZERO, "input_tokens": 1_000_000},
        "b": {**_ZERO, "output_tokens": 500_000},
    }
    assert compute_configured_cost(usage, pricing) == pytest.approx(1.0 + 10.0)
