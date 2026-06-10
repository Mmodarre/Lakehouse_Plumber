"""Geometry tests for the status-bar fill (line 1, :mod:`._status_line`).

The flowgroup counter is provably monotonic; these tests pin the *rendering*
side of that promise — the bar's filled-cell count must be a pure, monotonic,
jitter-free function of ``done/total``:

- it never decreases as ``done`` increases (floor, not round — round() can
  regress or jump by two at narrow spans);
- it is identical whether or not the optional pipeline/elapsed trailers are
  present, and regardless of where ``done`` sits relative to powers of ten
  (the counter widens at the 10-boundary). Both would otherwise flex the bar
  span frame-to-frame; the span is reserved up front so it cannot.
"""

import pytest

from lhp.cli.presenters.event_stream._status_line import (
    _BAR_FULL,
    build_status_line,
)


def _filled_cells(
    *,
    done: int,
    total: int,
    width: int,
    pipeline: str | None,
    elapsed_s: float,
) -> int:
    """Count the filled (``█``) cells in the rendered status line's bar."""
    line = build_status_line(
        spinner="⠹",
        phase="generate",
        done=done,
        total=total,
        pipeline=pipeline,
        elapsed_s=elapsed_s,
        width=width,
    ).plain
    return line.count(_BAR_FULL)


# A range of console widths from cramped (bar at _MIN_BAR) to roomy, and a
# total that straddles the 10-boundary so the counter changes width mid-run.
_WIDTHS = [12, 16, 20, 24, 30, 40, 60, 80, 120, 200]
_TOTAL = 18


@pytest.mark.parametrize("width", _WIDTHS)
def test_fill_is_monotonic_non_decreasing_in_done(width):
    last = -1
    for done in range(_TOTAL + 1):  # 0..total inclusive
        filled = _filled_cells(
            done=done,
            total=_TOTAL,
            width=width,
            pipeline="silver_orders",
            elapsed_s=3.1,
        )
        assert filled >= last, (
            f"fill regressed at width={width}, done={done}: {filled} < {last}"
        )
        assert filled >= 0
        last = filled


@pytest.mark.parametrize("width", _WIDTHS)
def test_fill_never_overshoots_and_fills_at_completion(width):
    span_at_full = _filled_cells(
        done=_TOTAL, total=_TOTAL, width=width, pipeline=None, elapsed_s=0.0
    )
    # At done == total the bar is entirely filled; that count is the span, and
    # the fill at any earlier done can never exceed it.
    for done in range(_TOTAL + 1):
        filled = _filled_cells(
            done=done, total=_TOTAL, width=width, pipeline=None, elapsed_s=0.0
        )
        assert filled <= span_at_full, (
            f"fill {filled} overshot span {span_at_full} at width={width}, done={done}"
        )


@pytest.mark.parametrize("width", _WIDTHS)
@pytest.mark.parametrize("done", range(_TOTAL + 1))
def test_trailer_toggle_does_not_change_fill(width, done):
    # Render with and without trailers at the same done/total/width: the bar's
    # filled-cell count must be identical (the span is reserved up front, so
    # trailer presence cannot flex it).
    with_trailers = _filled_cells(
        done=done, total=_TOTAL, width=width, pipeline="silver_orders", elapsed_s=3.1
    )
    no_trailers = _filled_cells(
        done=done, total=_TOTAL, width=width, pipeline=None, elapsed_s=0.0
    )
    assert with_trailers == no_trailers, (
        f"trailer toggle changed fill at width={width}, done={done}: "
        f"{with_trailers} (with) != {no_trailers} (without)"
    )


@pytest.mark.parametrize("width", _WIDTHS)
def test_counter_widening_does_not_change_span(width):
    # The counter widens at the 10-boundary (9/18 -> 10/18). Because the span is
    # reserved at the counter's terminal width, the total bar span (filled +
    # empty) must be constant across the whole run.
    def span(done: int) -> int:
        line = build_status_line(
            spinner="⠹",
            phase="generate",
            done=done,
            total=_TOTAL,
            pipeline="silver_orders",
            elapsed_s=3.1,
            width=width,
        ).plain
        return line.count(_BAR_FULL) + line.count("░")

    spans = {span(done) for done in range(_TOTAL + 1)}
    assert len(spans) == 1, f"bar span flexed across done at width={width}: {spans}"
