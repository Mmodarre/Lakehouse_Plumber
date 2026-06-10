"""Deterministic flavor-word picker for the live status region.

While a long run waits between observable progress events, the live status
region's flavor line shows a rotating "flavor word" (a short plumbing/lakehouse
phrase) so the region reads as alive rather than stalled. This module owns ONE
responsibility:
given an elapsed wall-clock time (plus an optional fixed integer phase
``offset``), return which flavor word to show. It is a pure, deterministic
function of its arguments — it draws NO randomness itself — so the same inputs
always yield the same word, snapshot tests are stable, and two processes
rendering the same run agree. The per-run "look different every run" variation
lives entirely in the ``offset`` the caller supplies (the live region draws one
random ``offset`` once at construction — see :mod:`._live_renderable`); this
module never imports :mod:`random`.

Rotation is by fixed time buckets: the bucket index is ``int(elapsed_s /
period_s) + offset``, and that index (mod the word count) selects into a FIXED
permutation of the word tuple. The permutation is a hand-fixed literal, not a
shuffle, so word order is reviewable and reproducible without seeding any RNG;
``offset`` only shifts the *starting* point of that fixed walk (``offset=0``
reproduces the unshifted rotation).

Every word is ASCII and single-cell (no emoji, no combining marks), per
``cli/STYLE.md`` §4 and constitution §7.6, so the picked string occupies a
predictable width on its line within the live status region.

Dependency-light by intent: no ``rich`` import (this returns a plain ``str``;
styling is the caller's concern) and no ``lhp.errors`` import (the sole-bridge
invariant, constitution §9.5). A misuse of the period (non-positive) is a
programming error and raises a plain :class:`ValueError`.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# The frozen flavor-word vocabulary. Exactly 30 entries, all ASCII and
# single-cell (constitution §7.6 / STYLE.md §4 — no emoji, no decoration). The
# tuple is the canonical order; rotation visits it through _ORDER below, not in
# this declared order.
_FLAVOR_WORDS: tuple[str, ...] = (
    "laying bricks",
    "yamlizing",
    "connecting pipes",
    "priming the lakehouse",
    "tightening joints",
    "wrangling flowgroups",
    "soldering seams",
    "pouring the foundation",
    "routing the plumbing",
    "calibrating gauges",
    "sealing the gaskets",
    "mixing the mortar",
    "flushing the lines",
    "plumbing the depths",
    "stacking the medallion",
    "bronzing the tables",
    "silvering the views",
    "polishing the gold",
    "tuning the manifolds",
    "greasing the fittings",
    "threading the conduits",
    "bleeding the radiators",
    "torquing the bolts",
    "levelling the bedrock",
    "spinning the turbines",
    "unclogging the drains",
    "charging the capacitors",
    "weaving the lineage",
    "annealing the schemas",
    "ferrying the bytes",
)

# A FIXED permutation of the indices into _FLAVOR_WORDS. Hand-fixed (not a
# random shuffle) so the rotation order is deterministic and reviewable: a
# given bucket index always maps to the same word. Every index in
# range(len(_FLAVOR_WORDS)) appears exactly once, so the rotation visits all
# words before repeating (asserted as a startup invariant below).
_ORDER: tuple[int, ...] = (
    7,
    18,
    2,
    25,
    11,
    0,
    21,
    14,
    4,
    29,
    9,
    16,
    23,
    6,
    27,
    1,
    20,
    13,
    5,
    28,
    10,
    17,
    24,
    8,
    19,
    3,
    26,
    12,
    22,
    15,
)

# Startup invariants: keep the two literals in lockstep. A bad edit (wrong
# length, a duplicated or out-of-range index) fails fast at import rather than
# silently dropping a word or raising IndexError on a specific bucket.
assert len(_FLAVOR_WORDS) == 30, "flavor-word vocabulary must hold exactly 30 entries"
assert tuple(sorted(_ORDER)) == tuple(range(len(_FLAVOR_WORDS))), (
    "_ORDER must be a permutation of range(len(_FLAVOR_WORDS))"
)

# Default rotation period: at ~2 seconds per word the phrase changes often
# enough to read as live but not so fast that it flickers.
_DEFAULT_PERIOD_S = 2.0

# The lead-in spinner that prefixes the flavor word on line 2: four circle
# quadrants (U+25D0..U+25D3, Geometric Shapes block, Unicode category So),
# cycled in clockwise order. Each is single-cell — not an emoji — so the §7.6
# no-emoji rule holds and the picked frame occupies a predictable width on its
# line (see _status_line.build_flavor_line). A DIFFERENT shape from line 1's
# braille-dots spinner (same tick rhythm — see _DEFAULT_SPINNER_PERIOD_S).
SPINNER_FRAMES: tuple[str, ...] = ("◐", "◓", "◑", "◒")

# Frame-advance period for the line-2 spinner. Line 1's braille spinner is a
# rich.spinner.Spinner("dots"), whose frame interval (80 ms) is buried inside
# Rich's Spinner object — there is NO reusable LHP-level period constant to
# share. So the two spinners cannot literally share one constant; this 0.1 s
# (100 ms) tick is the closest clean local value, near Rich's 80 ms so the two
# shapes spin at a comparable cadence (a near-shared rhythm, different glyph).
_DEFAULT_SPINNER_PERIOD_S = 0.1


def flavor_word_count() -> int:
    """Return how many distinct flavor words the rotation cycles through.

    Exposed so a caller can pick a per-run starting ``offset`` for
    :func:`flavor_word_for` (drawing an int in ``range(flavor_word_count())``)
    without reaching into the private :data:`_FLAVOR_WORDS` literal. Any
    integer ``offset`` is valid — :func:`flavor_word_for` takes it modulo this
    count — but drawing within that range keeps the value readable and
    non-redundant. The RNG itself lives in the caller (:mod:`._live_renderable`),
    never here.

    :returns: The size of the flavor-word vocabulary (the rotation period in
        words).
    """
    return len(_FLAVOR_WORDS)


def flavor_word_for(
    elapsed_s: float, *, period_s: float = _DEFAULT_PERIOD_S, offset: int = 0
) -> str:
    """Return the flavor word to show after ``elapsed_s`` seconds of a run.

    Pure and deterministic: identical ``elapsed_s`` / ``period_s`` / ``offset``
    always yield the same word, and no randomness is involved (the caller owns
    any RNG — see :mod:`._live_renderable`). The bucket index is
    ``int(elapsed_s / period_s) + offset`` (the time component below one period
    — including negative clock skew — is clamped to ``0`` first), and that index
    modulo the word count selects into the fixed permutation :data:`_ORDER`, so
    the rotation walks every word before repeating.

    ``offset`` is a fixed integer phase shift added to the time bucket: it picks
    a different *starting* word without disturbing the rotation order or its
    determinism. ``offset=0`` reproduces the original output exactly. The live
    region passes a per-run random ``offset`` once so each run begins on a
    different word; tests pass a fixed ``offset`` to keep expected words pinned.

    :param elapsed_s: Wall-clock seconds since the run started.
    :param period_s: Seconds each word is shown before rotating; must be
        positive.
    :param offset: Integer phase shift added to the time bucket before the
        modulo; ``0`` reproduces the unshifted rotation.
    :returns: One entry from :data:`_FLAVOR_WORDS`.
    :raises ValueError: If ``period_s`` is not positive.
    """
    if period_s <= 0:
        raise ValueError(f"period_s must be positive, got {period_s!r}")
    bucket = int(elapsed_s / period_s)
    if bucket < 0:
        bucket = 0
    return _FLAVOR_WORDS[_ORDER[(bucket + offset) % len(_ORDER)]]


def flavor_spinner_frame_for(
    elapsed_s: float, *, period_s: float = _DEFAULT_SPINNER_PERIOD_S, offset: int = 0
) -> str:
    """Return the line-2 lead-in spinner frame after ``elapsed_s`` seconds.

    Pure and deterministic, mirroring :func:`flavor_word_for`: identical
    ``elapsed_s`` / ``period_s`` / ``offset`` always yield the same frame, with
    no randomness (the caller owns any RNG — see :mod:`._live_renderable`). The
    frame index is ``int(elapsed_s / period_s) + offset`` (the time component
    below one period — including negative clock skew — is clamped to ``0``
    first) modulo the four-frame cycle, so the circle quadrant rotates clockwise
    as the clock advances and wraps cleanly.

    ``offset`` is a fixed integer phase shift added to the time frame: it picks
    a different *starting* quadrant without disturbing the spin direction or its
    determinism. ``offset=0`` reproduces the original output exactly. The live
    region passes a per-run random ``offset`` once so each run begins on a
    different quadrant; tests pass a fixed ``offset`` to keep expected frames
    pinned.

    :param elapsed_s: Wall-clock seconds since the run started.
    :param period_s: Seconds each frame is shown before advancing; must be
        positive.
    :param offset: Integer phase shift added to the time frame before the
        modulo; ``0`` reproduces the unshifted rotation.
    :returns: One entry from :data:`SPINNER_FRAMES`.
    :raises ValueError: If ``period_s`` is not positive.
    """
    if period_s <= 0:
        raise ValueError(f"period_s must be positive, got {period_s!r}")
    frame = int(elapsed_s / period_s)
    if frame < 0:
        frame = 0
    return SPINNER_FRAMES[(frame + offset) % len(SPINNER_FRAMES)]
