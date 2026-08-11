"""Regression tests for the Yahoo/IDX discrepancy classifier.

1. (2026-08-09) A short coincidental repeat on the "moving" side was
   enough to trip the same low length threshold used for the "frozen"
   side, misclassifying a clean single-source freeze (ADMF, 289 days) as
   'both frozen' (category C) instead of 'yahoo frozen' (category B) —
   silently downgrading a real Yahoo data-quality defect into the
   zero-volume/suspension bucket, where it would never have been found.

2. (2026-08-11) MATERIALITY. The B-bucket audit found 10 tickers (ASRM
   x2, FASW, WICO, BRNA, DSSA, PEGE, TFCO, LCKM, TRUS, CTBN) where the
   "other side" ticked between 2-3 values on a handful of tiny-volume
   trades scattered through an otherwise fully zero-volume run —
   technically not bit-identical, so the old code called it "moved" and
   filed the ticker under B (real isolated Yahoo freeze) when it was
   really a thinly-traded/suspension-adjacent ticker that belongs in C.
   Same gap in the mirror direction (FASW 86d, WIKA 73d, FISH 153d,
   frozen_side='idx' with Yahoo "moving" on 1-2 real-volume days out of
   the whole run).

3. (2026-08-11) AT_FLOOR. BNBR spent 82% of 2020-01-02 to 2023-02-15 at
   exactly Rp50 — IDX's minimum tick price ("gocap") — with real, often
   huge, volume (up to 253M shares/day). That's genuine market
   microstructure, not staleness, but a naive "close never changes"
   check can't tell it apart from a real freeze. Worse: BBRM's IDX side
   sat bit-identical at 50 for a real 522-day run while Yahoo was
   independently frozen at an unrelated 67.7745 for the same window —
   without the floor exception this misfiled as category C ("both
   frozen, not a data-quality defect, folds into suspension"), silently
   burying a genuine 2+-year Yahoo staleness defect exactly the way the
   ADMF bug (case 1 above) did.
"""
from __future__ import annotations

import datetime as dt

from idx.jobs.classify_discrepancies import (
    IDX_FLOOR_PRICE,
    find_frozen_runs,
    find_regime_transitions,
)


def _row(day, yc, ic, yv, iv):
    return (dt.date(2024, 1, day), yc, ic, yv, iv)


def test_yahoo_frozen_with_coincidental_short_idx_repeat_is_not_misclassified_as_both():
    # idx repeats its value on days 1-2 by pure coincidence, then moves
    # every day after — must NOT count as "idx also frozen" just because
    # that 2-day coincidence clears the same threshold the real 10-day
    # yahoo freeze clears.
    entries = [
        _row(1, 100.0, 500.0, 0, 1000),
        _row(2, 100.0, 500.0, 0, 1200),  # idx coincidentally repeats once
        _row(3, 100.0, 510.0, 0, 900),
        _row(4, 100.0, 505.0, 0, 1100),
        _row(5, 100.0, 515.0, 0, 800),
        _row(6, 100.0, 520.0, 0, 1300),
        _row(7, 100.0, 512.0, 0, 950),
        _row(8, 100.0, 508.0, 0, 1400),
        _row(9, 100.0, 517.0, 0, 1000),
        _row(10, 100.0, 522.0, 0, 1100),
    ]
    runs = find_frozen_runs("TEST", entries)
    assert len(runs) == 1
    assert runs[0].frozen_side == "yahoo"
    assert runs[0].n_days == 10
    assert runs[0].other_side_moved is True


def test_both_frozen_at_matching_prices_is_not_a_discrepancy():
    entries = [_row(d, 100.0, 100.0, 0, 0) for d in range(1, 8)]
    runs = find_frozen_runs("TEST2", entries)
    assert len(runs) == 0  # both frozen at matching prices: not discrepant, no run reported


def test_both_frozen_at_different_prices_is_category_c():
    entries = [_row(d, 100.0, 105.0, 0, 0) for d in range(1, 8)]
    runs = find_frozen_runs("TEST3", entries)
    assert len(runs) == 1
    assert runs[0].frozen_side == "both"
    assert runs[0].other_side_moved is False


def test_thin_other_side_reclassifies_from_yahoo_to_both_at_default_threshold():
    # ASRM-shape: yahoo is genuinely, literally frozen at 100 the whole
    # window (zero volume throughout). idx nominally "moves" once (500 ->
    # 505 -> 500) but only on 1 of 10 days does it carry real volume — a
    # thin, isolated trade, not sustained trading. Under the default
    # materiality threshold (0.5) that 1/10 = 0.1 fraction must NOT count
    # as real activity, so this is 'both' inert (category C), not a real
    # isolated Yahoo freeze (category B).
    entries = [
        _row(1, 100.0, 500.0, 0, 0),
        _row(2, 100.0, 500.0, 0, 0),
        _row(3, 100.0, 505.0, 0, 50),  # one tiny real trade
        _row(4, 100.0, 500.0, 0, 0),
        _row(5, 100.0, 500.0, 0, 0),
        _row(6, 100.0, 500.0, 0, 0),
        _row(7, 100.0, 500.0, 0, 0),
        _row(8, 100.0, 500.0, 0, 0),
        _row(9, 100.0, 500.0, 0, 0),
        _row(10, 100.0, 500.0, 0, 0),
    ]
    runs = find_frozen_runs("ASRM_SHAPE", entries)
    assert len(runs) == 1
    assert runs[0].frozen_side == "both"
    assert runs[0].other_side_moved is False


def test_thin_other_side_stays_yahoo_when_threshold_lowered():
    # Same shape as above, but with the threshold dropped below the
    # observed 0.1 volume-day fraction, the same run must classify as a
    # real isolated Yahoo freeze — proves the parameter actually gates
    # the outcome, not just the default value.
    entries = [
        _row(1, 100.0, 500.0, 0, 0),
        _row(2, 100.0, 500.0, 0, 0),
        _row(3, 100.0, 505.0, 0, 50),
        _row(4, 100.0, 500.0, 0, 0),
        _row(5, 100.0, 500.0, 0, 0),
        _row(6, 100.0, 500.0, 0, 0),
        _row(7, 100.0, 500.0, 0, 0),
        _row(8, 100.0, 500.0, 0, 0),
        _row(9, 100.0, 500.0, 0, 0),
        _row(10, 100.0, 500.0, 0, 0),
    ]
    runs = find_frozen_runs("ASRM_SHAPE_LOW_THRESHOLD", entries, materiality_threshold=0.05)
    assert len(runs) == 1
    assert runs[0].frozen_side == "yahoo"
    assert runs[0].other_side_moved is True


def test_idx_pinned_at_floor_with_real_volume_is_not_category_c():
    # BBRM-shape: idx is bit-identical at exactly IDX_FLOOR_PRICE for the
    # whole window, but with real (materially-backed) volume most days —
    # that's genuine floor-pinned trading, not staleness. yahoo is
    # independently frozen at a completely unrelated value the whole
    # time (zero volume) — that IS a real, isolated Yahoo defect. Without
    # the AT_FLOOR exception this run would read as "both frozen" and
    # silently bury the Yahoo defect in category C.
    entries = [
        _row(1, 67.7745, IDX_FLOOR_PRICE, 0, 100),
        _row(2, 67.7745, IDX_FLOOR_PRICE, 0, 0),
        _row(3, 67.7745, IDX_FLOOR_PRICE, 0, 1100),
        _row(4, 67.7745, IDX_FLOOR_PRICE, 0, 0),
        _row(5, 67.7745, IDX_FLOOR_PRICE, 0, 900),
        _row(6, 67.7745, IDX_FLOOR_PRICE, 0, 800),
        _row(7, 67.7745, IDX_FLOOR_PRICE, 0, 0),
        _row(8, 67.7745, IDX_FLOOR_PRICE, 0, 1500),
        _row(9, 67.7745, IDX_FLOOR_PRICE, 0, 700),
        _row(10, 67.7745, IDX_FLOOR_PRICE, 0, 600),
    ]
    runs = find_frozen_runs("BBRM_SHAPE", entries)
    assert len(runs) == 1
    assert runs[0].frozen_side == "yahoo"
    assert runs[0].other_side_moved is True
    assert runs[0].at_floor is True


def test_idx_frozen_at_a_non_floor_price_gets_no_floor_exception():
    # Same shape as above, but idx is pinned at 55.0, not exactly
    # IDX_FLOOR_PRICE (50.0) — real volume alone must NOT be enough to
    # exempt a frozen run from category C. The exception is specifically
    # about the exchange's hard price floor, not "any constant price with
    # some volume".
    entries = [
        _row(1, 67.7745, 55.0, 0, 100),
        _row(2, 67.7745, 55.0, 0, 0),
        _row(3, 67.7745, 55.0, 0, 1100),
        _row(4, 67.7745, 55.0, 0, 0),
        _row(5, 67.7745, 55.0, 0, 900),
        _row(6, 67.7745, 55.0, 0, 800),
        _row(7, 67.7745, 55.0, 0, 0),
        _row(8, 67.7745, 55.0, 0, 1500),
        _row(9, 67.7745, 55.0, 0, 700),
        _row(10, 67.7745, 55.0, 0, 600),
    ]
    runs = find_frozen_runs("NON_FLOOR_SHAPE", entries)
    assert len(runs) == 1
    assert runs[0].frozen_side == "both"
    assert runs[0].at_floor is False


def test_yahoo_frozen_exactly_at_floor_value_gets_no_exception():
    # The floor exception is IDX-only (IDX is authoritative for
    # close_raw; Yahoo is not — CLAUDE.md SOURCE AUTHORITY). A Yahoo
    # close that happens to equal 50 must not be read as "Yahoo is
    # legitimately floor-pinned" — it is simply frozen, same as any
    # other value.
    entries = [
        _row(1, IDX_FLOOR_PRICE, 500.0, 100, 1000),
        _row(2, IDX_FLOOR_PRICE, 505.0, 0, 1200),
        _row(3, IDX_FLOOR_PRICE, 510.0, 200, 900),
        _row(4, IDX_FLOOR_PRICE, 515.0, 0, 1100),
        _row(5, IDX_FLOOR_PRICE, 520.0, 300, 800),
    ]
    runs = find_frozen_runs("YAHOO_AT_FLOOR_VALUE", entries)
    assert len(runs) == 1
    assert runs[0].frozen_side == "yahoo"
    assert runs[0].at_floor is False


def test_regime_transition_materiality_rejects_single_day_volume_spike():
    # Pre-regime: both sources agree (ratio ~1.0), real volume throughout.
    # Post-regime: ratio jumps to ~4.0 and STAYS there (no revert), but
    # only the very first post-regime day carries real volume on both
    # sides — the other 4 days of the "new regime" are silent. The old
    # single-day check would have called this a genuine corporate action
    # (category D); the window-materiality check must not, since 1/5 of
    # the post-regime is nowhere near sustained trading.
    entries = (
        [_row(d, 100.0, 100.0, 500, 500) for d in range(1, 6)]
        + [
            _row(6, 400.0, 100.0, 800, 700),  # transition day: real volume
            _row(7, 400.0, 100.0, 0, 0),
            _row(8, 400.0, 100.0, 0, 0),
            _row(9, 400.0, 100.0, 0, 0),
            _row(10, 400.0, 100.0, 0, 0),
        ]
    )
    transitions = find_regime_transitions("THIN_TRANSITION", entries)
    assert len(transitions) == 1
    t = transitions[0]
    assert t.yahoo_vol_frac_at_transition == 0.2
    assert t.idx_vol_frac_at_transition == 0.2
    # Caller (classify()) is responsible for applying the threshold to
    # these fractions; this test just locks in that the window-level
    # fraction is computed and exposed, not silently dropped.
