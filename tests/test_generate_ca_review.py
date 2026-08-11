"""jobs/generate_ca_review.py — IDX ledger cross-check matching/agreement
logic. No network or DB calls; the fetch/write functions aren't covered
here.
"""
from __future__ import annotations

import datetime as dt

from idx.jobs.generate_ca_review import (
    annotate_with_ledger,
    build_ledger_index,
    find_ledger_match,
    ratios_agree,
)


def _ledger_row(ticker, listing_date, action_type_raw, shares_added, shares_after, ratio):
    return {
        "ticker": ticker,
        "listing_date": listing_date,
        "action_type_raw": action_type_raw,
        "action_type": "split",
        "shares_added": shares_added,
        "shares_after": shares_after,
        "ratio": ratio,
        "source": "idx_ledger",
    }


def test_find_ledger_match_prefers_ratioed_row_over_same_date_unrelated_event():
    # BBNI-shaped bug, found on the first real run: 2023-10-06 has a
    # partialDelisting row (no ratio) and a real stockSplit row (ratio
    # 2.0), same date, same |lag|=0. A naive "first at smallest |lag|"
    # tie-break silently picked partialDelisting, discarding a real,
    # confirmable agreement (price ratio_from=0.5 is the exact reciprocal
    # of ledger ratio=2.0).
    index = build_ledger_index(
        [
            _ledger_row("BBNI", dt.date(2023, 10, 6), "partialDelisting", 372_973_130.0, 36_924_339_786.0, None),
            _ledger_row("BBNI", dt.date(2023, 10, 6), "stockSplit", 18_359_314_591.0, 36_718_629_182.0, 2.0),
        ]
    )
    match = find_ledger_match("BBNI", dt.date(2023, 10, 6), index)
    assert match["action_type_raw"] == "stockSplit"
    assert match["ratio"] == 2.0


def test_find_ledger_match_prefers_larger_share_count_over_rounding_residual():
    # ISAT-shaped bug: two same-type rows on the same date, one a
    # degenerate rounding residual (JumlahSaham=0, ratio derives to a
    # meaningless 1.0 or None), the other the real split. Both have a
    # non-None ratio here (0.0 shares_added still derives *some* ratio,
    # unlike BBNI's None-typed row above), so the tie-break must fall
    # through to "largest |shares_added|" to find the real one.
    index = build_ledger_index(
        [
            _ledger_row("ISAT", dt.date(2024, 10, 14), "stockSplit", 0.0, 1.0, 1.0),  # residual
            _ledger_row("ISAT", dt.date(2024, 10, 14), "stockSplit", 5_000_000_000.0, 10_000_000_000.0, 2.0),
        ]
    )
    match = find_ledger_match("ISAT", dt.date(2024, 10, 14), index)
    assert match["ratio"] == 2.0
    assert match["shares_added"] == 5_000_000_000.0


def test_find_ledger_match_respects_window_and_picks_closest_lag():
    index = build_ledger_index(
        [
            _ledger_row("XYZ", dt.date(2024, 1, 1), "stockSplit", 100.0, 200.0, 2.0),
            _ledger_row("XYZ", dt.date(2024, 3, 1), "stockSplit", 100.0, 300.0, 3.0),
        ]
    )
    match = find_ledger_match("XYZ", dt.date(2024, 1, 5), index)
    assert match["ratio"] == 2.0
    assert match["lag_days"] == -4

    # Outside the window entirely -> no match.
    assert find_ledger_match("XYZ", dt.date(2020, 1, 1), index) is None


def test_ratios_agree_is_direction_agnostic():
    # price_ratio_from convention and the ledger's after/(after-added)
    # convention aren't guaranteed to land on the same side of 1.0 for
    # the same real event (e.g. 0.25 vs 4.0 for the same 1:4 split).
    assert ratios_agree(0.25, 4.0) is True
    assert ratios_agree(4.0, 4.0) is True
    assert ratios_agree(0.5, 2.0) is True
    assert ratios_agree(0.5, 3.0) is False


def test_annotate_with_ledger_verdicts():
    index = build_ledger_index(
        [
            _ledger_row("AGREE", dt.date(2024, 1, 1), "stockSplit", 100.0, 200.0, 2.0),
            _ledger_row("DISAGREE", dt.date(2024, 1, 1), "stockSplit", 100.0, 500.0, 5.0),
            _ledger_row("NOROW", dt.date(2024, 1, 1), "hmetd", 10.0, 20.0, None),
        ]
    )

    agree_row = annotate_with_ledger(
        {"ticker": "AGREE", "ex_date": dt.date(2024, 1, 1), "ratio_from": 0.5}, index
    )
    assert agree_row["ledger_match"]["agreement"] == "agree"

    disagree_row = annotate_with_ledger(
        {"ticker": "DISAGREE", "ex_date": dt.date(2024, 1, 1), "ratio_from": 0.5}, index
    )
    assert disagree_row["ledger_match"]["agreement"] == "disagree"

    unratioed_row = annotate_with_ledger(
        {"ticker": "NOROW", "ex_date": dt.date(2024, 1, 1), "ratio_from": 0.3333}, index
    )
    assert unratioed_row["ledger_match"]["agreement"] == "unratioed"

    no_match_row = annotate_with_ledger(
        {"ticker": "GHOST", "ex_date": dt.date(2024, 1, 1), "ratio_from": 0.5}, index
    )
    assert no_match_row["ledger_match"] is None
