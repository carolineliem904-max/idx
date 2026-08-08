"""Regression test for a real bug found during the Yahoo/IDX discrepancy
investigation (2026-08-09): a short coincidental repeat on the "moving"
side was enough to trip the same low length threshold used for the
"frozen" side, misclassifying a clean single-source freeze (ADMF, 289
days) as 'both frozen' (category C) instead of 'yahoo frozen' (category
B) — silently downgrading a real Yahoo data-quality defect into the
zero-volume/suspension bucket, where it would never have been found.
"""
from __future__ import annotations

import datetime as dt

from idx.jobs.classify_discrepancies import find_frozen_runs


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
