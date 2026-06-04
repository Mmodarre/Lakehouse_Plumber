"""Tests for the deterministic flavor-word picker.

Covers the three behavioral guarantees the live status line relies on:
a fixed word at a fixed elapsed time, rotation at the period boundary, and
full reachability of the 30-word vocabulary. Also pins determinism (no RNG),
the all-ASCII/single-cell constraint, and the misuse guard.
"""

import pytest

from lhp.cli.presenters.event_stream._flavor_words import (
    _DEFAULT_PERIOD_S,
    _DEFAULT_SPINNER_PERIOD_S,
    _FLAVOR_WORDS,
    _ORDER,
    SPINNER_FRAMES,
    flavor_spinner_frame_for,
    flavor_word_count,
    flavor_word_for,
)


def test_vocabulary_is_thirty_unique_words():
    assert len(_FLAVOR_WORDS) == 30
    assert len(set(_FLAVOR_WORDS)) == 30


def test_order_is_a_permutation_of_the_vocabulary():
    assert tuple(sorted(_ORDER)) == tuple(range(len(_FLAVOR_WORDS)))


def test_fixed_word_at_fixed_elapsed():
    # Bucket 0 maps through _ORDER[0] == 7 to _FLAVOR_WORDS[7].
    assert flavor_word_for(0.0) == "pouring the foundation"
    # Anywhere inside the first period stays in bucket 0.
    assert flavor_word_for(0.5) == "pouring the foundation"
    assert flavor_word_for(_DEFAULT_PERIOD_S - 0.001) == "pouring the foundation"


def test_word_changes_at_the_period_boundary():
    before = flavor_word_for(_DEFAULT_PERIOD_S - 0.001)
    at_boundary = flavor_word_for(_DEFAULT_PERIOD_S)
    assert before != at_boundary
    # Bucket 1 maps through _ORDER[1] == 18.
    assert at_boundary == _FLAVOR_WORDS[18]
    # Still bucket 1 just past the boundary; rotates again one period later.
    assert flavor_word_for(_DEFAULT_PERIOD_S + 0.5) == at_boundary
    assert flavor_word_for(2 * _DEFAULT_PERIOD_S) == _FLAVOR_WORDS[_ORDER[2]]


def test_custom_period_controls_the_boundary():
    assert flavor_word_for(0.9, period_s=1.0) == flavor_word_for(0.0, period_s=1.0)
    assert flavor_word_for(1.0, period_s=1.0) != flavor_word_for(0.0, period_s=1.0)


def test_all_thirty_words_reachable_across_indices():
    seen = {flavor_word_for(i * _DEFAULT_PERIOD_S) for i in range(len(_FLAVOR_WORDS))}
    assert seen == set(_FLAVOR_WORDS)


def test_rotation_wraps_after_the_full_cycle():
    cycle = len(_ORDER)
    # One full cycle later returns to the same word, confirming the modulo wrap.
    assert flavor_word_for(0.0) == flavor_word_for(cycle * _DEFAULT_PERIOD_S)
    assert flavor_word_for(_DEFAULT_PERIOD_S) == flavor_word_for(
        (cycle + 1) * _DEFAULT_PERIOD_S
    )


def test_deterministic_repeated_calls():
    for elapsed in (0.0, 1.3, 5.0, 17.7, 61.2):
        assert flavor_word_for(elapsed) == flavor_word_for(elapsed)


def test_negative_elapsed_clamps_to_first_bucket():
    assert flavor_word_for(-5.0) == flavor_word_for(0.0)
    assert flavor_word_for(-0.001) == flavor_word_for(0.0)


def test_non_positive_period_raises_value_error():
    with pytest.raises(ValueError):
        flavor_word_for(1.0, period_s=0.0)
    with pytest.raises(ValueError):
        flavor_word_for(1.0, period_s=-2.0)


def test_words_are_ascii_and_single_line():
    for word in _FLAVOR_WORDS:
        assert word.isascii(), f"non-ASCII flavor word: {word!r}"
        assert "\n" not in word and "\t" not in word
        assert word == word.strip()


# ---------------------------------------------------------------------------
# Line-2 lead-in spinner: animated circle quadrants (U+25D0..U+25D3)
# ---------------------------------------------------------------------------
def test_spinner_frames_are_the_four_circle_quadrants():
    assert SPINNER_FRAMES == ("◐", "◓", "◑", "◒")


def test_fixed_spinner_frame_at_fixed_elapsed():
    # Frame 0 at elapsed 0, and anywhere inside the first period stays frame 0.
    assert flavor_spinner_frame_for(0.0) == SPINNER_FRAMES[0]
    assert flavor_spinner_frame_for(_DEFAULT_SPINNER_PERIOD_S / 2) == SPINNER_FRAMES[0]
    assert (
        flavor_spinner_frame_for(_DEFAULT_SPINNER_PERIOD_S - 0.001) == SPINNER_FRAMES[0]
    )


def test_spinner_frame_changes_at_the_period_boundary():
    before = flavor_spinner_frame_for(_DEFAULT_SPINNER_PERIOD_S - 0.001)
    at_boundary = flavor_spinner_frame_for(_DEFAULT_SPINNER_PERIOD_S)
    assert before != at_boundary
    # Frame 1 is the second quadrant; still frame 1 just past the boundary.
    assert at_boundary == SPINNER_FRAMES[1]
    assert flavor_spinner_frame_for(_DEFAULT_SPINNER_PERIOD_S * 1.5) == at_boundary
    assert flavor_spinner_frame_for(2 * _DEFAULT_SPINNER_PERIOD_S) == SPINNER_FRAMES[2]


def test_all_four_spinner_frames_reachable_and_wrap():
    seen = {
        flavor_spinner_frame_for(i * _DEFAULT_SPINNER_PERIOD_S)
        for i in range(len(SPINNER_FRAMES))
    }
    assert seen == set(SPINNER_FRAMES)
    # One full cycle later wraps back to frame 0 (modulo over four frames).
    assert flavor_spinner_frame_for(
        len(SPINNER_FRAMES) * _DEFAULT_SPINNER_PERIOD_S
    ) == flavor_spinner_frame_for(0.0)


def test_custom_spinner_period_controls_the_boundary():
    assert flavor_spinner_frame_for(0.9, period_s=1.0) == flavor_spinner_frame_for(
        0.0, period_s=1.0
    )
    assert flavor_spinner_frame_for(1.0, period_s=1.0) != flavor_spinner_frame_for(
        0.0, period_s=1.0
    )


def test_spinner_frame_is_deterministic():
    for elapsed in (0.0, 0.05, 0.3, 1.7, 6.2):
        assert flavor_spinner_frame_for(elapsed) == flavor_spinner_frame_for(elapsed)


def test_negative_elapsed_clamps_to_first_spinner_frame():
    assert flavor_spinner_frame_for(-5.0) == flavor_spinner_frame_for(0.0)
    assert flavor_spinner_frame_for(-0.001) == flavor_spinner_frame_for(0.0)


def test_non_positive_spinner_period_raises_value_error():
    with pytest.raises(ValueError):
        flavor_spinner_frame_for(1.0, period_s=0.0)
    with pytest.raises(ValueError):
        flavor_spinner_frame_for(1.0, period_s=-2.0)


def test_spinner_frames_are_single_line_no_emoji_decoration():
    for frame in SPINNER_FRAMES:
        assert len(frame) == 1, f"spinner frame is not one code point: {frame!r}"
        assert "\n" not in frame and "\t" not in frame


# ---------------------------------------------------------------------------
# Per-run phase offset: a fixed integer shifts the rotation deterministically
# (the seam the live region uses for its random per-RUN start). offset=0
# reproduces today's output exactly; the functions still draw NO randomness.
# ---------------------------------------------------------------------------
def test_word_count_matches_vocabulary_size():
    # The public count helper (used by the renderable to pick a per-run offset)
    # equals the vocabulary length — without exposing the private literal.
    assert flavor_word_count() == len(_FLAVOR_WORDS)


def test_word_offset_zero_reproduces_unshifted_rotation():
    # The default and an explicit offset=0 are byte-identical at every bucket:
    # the offset is purely additive, so 0 is the no-op that preserves history.
    for k in range(len(_ORDER) + 3):
        elapsed = k * _DEFAULT_PERIOD_S
        assert flavor_word_for(elapsed, offset=0) == flavor_word_for(elapsed)


def test_word_offset_shifts_phase_by_whole_buckets():
    # offset=k advances the STARTING bucket by k: the word at bucket b with
    # offset k equals the word at bucket (b + k) with offset 0. This pins the
    # offset as a pure phase shift over the fixed _ORDER walk, not a reshuffle.
    for offset in (1, 3, len(_ORDER) - 1):
        for bucket in range(5):
            elapsed = bucket * _DEFAULT_PERIOD_S
            shifted = (bucket + offset) * _DEFAULT_PERIOD_S
            assert flavor_word_for(elapsed, offset=offset) == flavor_word_for(shifted)


def test_word_offset_is_deterministic_for_fixed_inputs():
    # No RNG inside the function: identical (elapsed, offset) repeats exactly.
    for offset in (0, 2, 7, len(_ORDER)):
        for elapsed in (0.0, 1.3, 5.0, 17.7):
            assert flavor_word_for(elapsed, offset=offset) == flavor_word_for(
                elapsed, offset=offset
            )


def test_word_offset_wraps_modulo_the_cycle():
    # An offset of a full cycle is a no-op (mod len(_ORDER)); offset and
    # offset+cycle agree at every bucket.
    cycle = len(_ORDER)
    for bucket in range(4):
        elapsed = bucket * _DEFAULT_PERIOD_S
        assert flavor_word_for(elapsed, offset=2) == flavor_word_for(
            elapsed, offset=2 + cycle
        )


def test_spinner_offset_zero_reproduces_unshifted_rotation():
    for k in range(len(SPINNER_FRAMES) + 3):
        elapsed = k * _DEFAULT_SPINNER_PERIOD_S
        assert flavor_spinner_frame_for(elapsed, offset=0) == flavor_spinner_frame_for(
            elapsed
        )


def test_spinner_offset_shifts_phase_by_whole_frames():
    # offset=k advances the STARTING quadrant by k frames over the 4-frame cycle.
    for offset in (1, 2, 3):
        for frame_idx in range(4):
            elapsed = frame_idx * _DEFAULT_SPINNER_PERIOD_S
            shifted = (frame_idx + offset) * _DEFAULT_SPINNER_PERIOD_S
            assert flavor_spinner_frame_for(
                elapsed, offset=offset
            ) == flavor_spinner_frame_for(shifted)


def test_spinner_offset_is_deterministic_for_fixed_inputs():
    for offset in (0, 1, 2, 3, 4):
        for elapsed in (0.0, 0.05, 0.3, 1.7):
            assert flavor_spinner_frame_for(
                elapsed, offset=offset
            ) == flavor_spinner_frame_for(elapsed, offset=offset)


def test_offset_does_not_break_the_value_error_guard():
    # The period guard still fires regardless of offset (offset is orthogonal).
    with pytest.raises(ValueError):
        flavor_word_for(1.0, period_s=0.0, offset=3)
    with pytest.raises(ValueError):
        flavor_spinner_frame_for(1.0, period_s=-1.0, offset=2)
