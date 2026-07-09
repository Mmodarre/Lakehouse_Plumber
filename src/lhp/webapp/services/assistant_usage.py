"""Pure token-usage normalization and configured-cost math for the assistant.

The Claude Agent SDK reports usage in two shapes on every ``ResultMessage``:
the top-level ``usage`` dict uses snake_case keys (``input_tokens``,
``cache_read_input_tokens``, ...) while the per-model ``model_usage`` values
use camelCase (``inputTokens``, ``cacheReadInputTokens``, ...). Everything
downstream — frames, persistence, cost math — works on ONE canonical
snake_case shape produced here.

Pure functions only: no I/O, no FastAPI, no SQLite. Callers
(:mod:`~lhp.webapp.services.claude_sdk_translate`,
:mod:`~lhp.webapp.services.claude_sdk_chat`) do the wiring.

:stability: internal
"""

from __future__ import annotations

from typing import Any, Optional

#: Canonical snake_case token counters, in display order.
TOKEN_KEYS: tuple[str, ...] = (
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
)

#: camelCase aliases used by the SDK's per-model ``model_usage`` values.
_CAMEL_ALIASES = {
    "inputTokens": "input_tokens",
    "outputTokens": "output_tokens",
    "cacheReadInputTokens": "cache_read_input_tokens",
    "cacheCreationInputTokens": "cache_creation_input_tokens",
}

#: Cache-rate defaults (as multiples of the input rate) applied when a
#: priced model omits its cache rates.
_CACHE_READ_INPUT_MULTIPLE = 0.1
_CACHE_WRITE_INPUT_MULTIPLE = 1.25

_MTOK = 1_000_000


def normalize_usage(raw: Optional[dict[str, Any]]) -> dict[str, int]:
    """Normalize one usage dict (snake_case OR camelCase) to canonical shape.

    Missing counters default to 0; unknown extra keys (``service_tier``,
    ``webSearchRequests``, ...) and non-numeric values are ignored.
    """
    out = dict.fromkeys(TOKEN_KEYS, 0)
    if not raw:
        return out
    for key, value in raw.items():
        canonical = _CAMEL_ALIASES.get(key, key)
        if canonical in out and isinstance(value, (int, float)):
            out[canonical] = int(value)
    return out


def normalize_model_usage(
    raw: Optional[dict[str, Any]],
) -> dict[str, dict[str, int]]:
    """Normalize the SDK's per-model usage mapping to canonical values."""
    if not raw:
        return {}
    return {
        str(model): normalize_usage(usage if isinstance(usage, dict) else None)
        for model, usage in raw.items()
    }


def _resolve_price(
    model: str, pricing: dict[str, dict[str, Any]]
) -> Optional[dict[str, Any]]:
    """Exact model-id match first, then LONGEST-prefix match, else ``None``."""
    exact = pricing.get(model)
    if exact is not None:
        return exact
    best: Optional[str] = None
    for key in pricing:
        if model.startswith(key) and (best is None or len(key) > len(best)):
            best = key
    return pricing[best] if best is not None else None


def _rate(entry: dict[str, Any], key: str, default: float) -> float:
    value = entry.get(key)
    return float(value) if isinstance(value, (int, float)) else default


def compute_configured_cost(
    model_usage: dict[str, dict[str, int]],
    pricing: dict[str, dict[str, Any]],
) -> Optional[float]:
    """Cost in USD of ``model_usage`` under ``pricing``, or ``None``.

    ``model_usage`` is the NORMALIZED per-model mapping; ``pricing`` maps a
    model id or id-prefix to per-MTok rates (``input_per_mtok`` /
    ``output_per_mtok`` and optionally ``cache_read_per_mtok`` /
    ``cache_write_per_mtok`` — omitted cache rates default to 0.1x / 1.25x
    the input rate). Returns ``None`` — never a partial cost — when there is
    no usage, no pricing, or ANY used model lacks a matching price entry.
    """
    if not model_usage or not pricing:
        return None
    total = 0.0
    for model, usage in model_usage.items():
        entry = _resolve_price(model, pricing)
        if entry is None:
            return None
        input_rate = _rate(entry, "input_per_mtok", 0.0)
        output_rate = _rate(entry, "output_per_mtok", 0.0)
        cache_read_rate = _rate(
            entry, "cache_read_per_mtok", input_rate * _CACHE_READ_INPUT_MULTIPLE
        )
        cache_write_rate = _rate(
            entry, "cache_write_per_mtok", input_rate * _CACHE_WRITE_INPUT_MULTIPLE
        )
        total += (
            usage.get("input_tokens", 0) * input_rate
            + usage.get("output_tokens", 0) * output_rate
            + usage.get("cache_read_input_tokens", 0) * cache_read_rate
            + usage.get("cache_creation_input_tokens", 0) * cache_write_rate
        ) / _MTOK
    return total
