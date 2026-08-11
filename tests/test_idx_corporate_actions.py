"""sources/idx_corporate_actions.py — ratio-derivation and row-mapping
correctness. No network calls; fetch_issued_history()/fetch_candidates()
are the only functions that touch the network and aren't covered here.
"""
from __future__ import annotations

import datetime as dt

from idx.sources.idx_corporate_actions import (
    CLEAN_RATIO_TYPES,
    derive_ratio,
    to_candidate_row,
)


def test_derive_ratio_matches_tpia_confirmed_1_for_4_split():
    # TPIA, 2022-08-23 stockSplit row, externally confirmed 1:4 by
    # Caroline. Must derive to exactly 4.0, not an approximation.
    ratio = derive_ratio(shares_added=64_883_658_819.0, shares_after=86_511_545_092.0)
    assert ratio == 4.0


def test_derive_ratio_matches_bbni_confirmed_1_for_2_split():
    ratio = derive_ratio(shares_added=18_359_314_591.0, shares_after=36_718_629_182.0)
    assert ratio == 2.0


def test_derive_ratio_none_on_missing_inputs():
    assert derive_ratio(None, 100.0) is None
    assert derive_ratio(100.0, None) is None


def test_derive_ratio_none_on_non_positive_before_shares():
    # shares_added >= shares_after implies a non-positive "before" count
    # — a degenerate/bad row, not a real ratio.
    assert derive_ratio(shares_added=100.0, shares_after=100.0) is None
    assert derive_ratio(shares_added=150.0, shares_after=100.0) is None


def test_to_candidate_row_derives_ratio_for_clean_types_only():
    split_row = to_candidate_row(
        {
            "KodeEmiten": "TPIA",
            "TanggalPencatatan": "2022-08-23T00:00:00",
            "JenisTindakan": "stockSplit",
            "JumlahSaham": 64_883_658_819.0,
            "JumlahSahamSetelahTindakan": 86_511_545_092.0,
        }
    )
    assert split_row["ratio"] == 4.0
    assert split_row["action_type"] == "split"
    assert split_row["listing_date"] == dt.date(2022, 8, 23)


def test_to_candidate_row_never_derives_ratio_for_rights_issues():
    # COCO's real 2026-07-28 hmetd row: share-count-derived ratio would
    # come out ~1.037, nowhere near the announced 1:3 factor 4 (~1.33) —
    # recording it would be presenting a subscription-rate artifact as
    # if it were the offer's terms. ratio must be None, unconditionally.
    hmetd_row = to_candidate_row(
        {
            "KodeEmiten": "COCO",
            "TanggalPencatatan": "2026-07-28T00:00:00",
            "JenisTindakan": "hmetd",
            "JumlahSaham": 505_189_568.0,
            "JumlahSahamSetelahTindakan": 14_237_823_696.0,
        }
    )
    assert hmetd_row["ratio"] is None
    assert hmetd_row["action_type"] == "rights"


def test_all_clean_ratio_types_map_to_a_known_action_type():
    # Guards against CLEAN_RATIO_TYPES and the action-type map silently
    # drifting apart (e.g. someone adds a type to one but not the other).
    for raw_type in CLEAN_RATIO_TYPES:
        row = to_candidate_row(
            {
                "KodeEmiten": "TEST",
                "TanggalPencatatan": "2024-01-01T00:00:00",
                "JenisTindakan": raw_type,
                "JumlahSaham": 100.0,
                "JumlahSahamSetelahTindakan": 200.0,
            }
        )
        assert row["action_type"] != "other"


def test_unknown_action_type_maps_to_other_and_gets_no_ratio():
    row = to_candidate_row(
        {
            "KodeEmiten": "TEST",
            "TanggalPencatatan": "2024-01-01T00:00:00",
            "JenisTindakan": "gabungUsaha",  # business combination — not in our vocabulary
            "JumlahSaham": 100.0,
            "JumlahSahamSetelahTindakan": 200.0,
        }
    )
    assert row["action_type"] == "other"
    assert row["ratio"] is None
