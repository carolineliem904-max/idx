"""IDX listing-activity ledger — ListingActivity/GetIssuedHistory endpoint.

Feasibility investigation, 2026-08-11 (see HANDOFF.md): Caroline asked
whether IDX publishes corporate-action announcements as structured data,
so ratio-derived candidates (jobs/generate_ca_review.py) could be
cross-checked against ground truth instead of standing alone. This
endpoint is real, reachable through the same curl_cffi(impersonate=
"chrome") + Referer pattern as sources/idx_company_list.py and
sources/idx_official.py, no extra auth, and goes back to 1911-01-01 —
far past GetStockSummary's 2020-01-02 wall.

It is a SHARE-COUNT LEDGER, not the full announcement schema originally
hoped for (no price, no cum/record date, no trading-start/end). Fields:
ticker, a listing date, an action-type code (Indonesian), and shares
added/outstanding-after. Two hard-won constraints from that
investigation, both load-bearing for how this module is used downstream
(jobs/generate_ca_review.py), not just documentation:

1. **`listing_date` (IDX's `TanggalPencatatan`) is NOT the ex-date.**
   Verified by matching 71 stockSplit/reverseStock/sahamBonus rows and
   40 hmetd/tanpaHmetd rows against independently-derived Track 2
   regime-transition dates (jobs/classify_discrepancies.py): stockSplit
   lag is ~0 days (n=58, median 0, range -4..0 — the two are
   effectively the same date). hmetd/sahamBonus/Dividen Saham lag is
   NOT zero — a consistent ~2.5-3 week listing delay (hmetd: n=40,
   median 19 days, range 11-84; sahamBonus: n=13, median 18 days;
   Dividen Saham: n=7, median 21 days). reverseStock has zero matched
   samples in our data (only 5 rows total, spread pre-2020) — assumed
   to behave like stockSplit on mechanical grounds (same "shares
   physically reissued on one date" shape) but NOT empirically
   confirmed; treat with the same caution as an unverified type until
   a real case turns up. This module never applies the median lag to
   invent an ex_date — that would be silently trading a known-wrong
   value for a plausible-looking one. `ex_date` stays unset here; it is
   only ever filled in by the caller from an independent source (a
   matching price-ratio transition, or manual confirmation).

2. **A derived ratio is only trustworthy for stockSplit / reverseStock /
   sahamBonus.** These three are mechanical, pro-rata, all-holders
   events — `JumlahSaham` (shares added) is the complete, exact
   consequence of the announced ratio, so `after / (after - added)`
   recovers that ratio exactly. Proven against two independent
   external confirmations: TPIA's 2022-08-23 stockSplit row derives to
   *exactly* 4.0 (Caroline confirmed 1:4); BBNI's 2023-10-06 rows
   include one clean *exactly* 2.0 split (Caroline confirmed 1:2).
   `hmetd`/`tanpaHmetd` (rights issues) do NOT get this treatment:
   `JumlahSaham` there reflects shares actually taken up/exercised, not
   the announced offer ratio — COCO's 2026-07-28 hmetd row derives to
   ~1.037x when the announced rights ratio was 1:3 factor 4 (~1.33x).
   Recording a "ratio" for a rights issue would be presenting a
   subscription-rate artifact as if it were the terms of the offer —
   worse than recording nothing. `ratio` is always None for every type
   outside CLEAN_RATIO_TYPES.

Coverage gap, also load-bearing: this ledger is not exhaustive. COCO's
externally-confirmed 2025-10-09 rights issue has NO row here at all —
checked every October 2025 row (6 total), none is COCO. Treat this
source as a cross-check, never as unconditional ground truth (same
posture as Yahoo's close_raw, CLAUDE.md SOURCE AUTHORITY) — where it
agrees with the price-ratio method that's high confidence, where either
side has something the other doesn't, that is itself the finding.

Second, distinct failure mode found on the first real cross-check run
(jobs/generate_ca_review.py, 2026-08-11): a ledger row can be PRESENT
for the right ticker and date and still be useless — ISAT's real
2024-10-14 ~1:4 split (confirmed independently: a 1158-trading-day
price-ratio regime, ratio_from=0.25) has TWO ledger rows on that exact
date, both with `JumlahSaham=0` (`shares_added=0`), i.e. both
placeholder/rounding-remainder entries, not the real share-count change.
`derive_ratio` correctly refuses to manufacture a number from these (one
derives a meaningless 1.0, the other None) — this surfaces as a genuine
cross-check DISAGREEMENT (ratio 1.0 vs. the real ~4.0), not a silent
false confirmation, which is the point of treating this as a cross-check
rather than ground truth. Compare to BBNI, where the same "multiple rows
on one date" shape (a `partialDelisting` row plus three `stockSplit`
rows, one of them also a `0->1` placeholder) DOES contain a real,
correct row (ratio exactly 2.0, confirmed) — `jobs/generate_ca_review.py`
's `find_ledger_match` has to pick the right one out of several
candidates, not just the closest by date; see that function's docstring
for the tie-break logic and the wrong-output-on-first-run story behind
it.
"""
from __future__ import annotations

import datetime as dt

import structlog
from curl_cffi import requests

log = structlog.get_logger()

BASE_URL = "https://www.idx.co.id/primary/ListingActivity/GetIssuedHistory"
REFERER = "https://www.idx.co.id/en/listed-companies/corporate-actions"
REQUEST_TIMEOUT_SECONDS = 30

# Only these three action types get a derived ratio — see module
# docstring point 2. Every other JenisTindakan value (ipo, delist,
# hmetd, tanpaHmetd, waran, ESOP, ...) always gets ratio=None.
CLEAN_RATIO_TYPES = {"stockSplit", "reverseStock", "sahamBonus"}

# IDX's raw JenisTindakan (Indonesian) -> corporate_actions.action_type
# vocabulary (models.py: split | reverse_split | dividend | rights |
# bonus | warrant). Anything not listed maps to 'other' — never guessed
# past what the source actually labeled.
_ACTION_TYPE_MAP = {
    "stockSplit": "split",
    "reverseStock": "reverse_split",
    "sahamBonus": "bonus",
    "Dividen Saham": "dividend",
    "hmetd": "rights",
    "tanpaHmetd": "rights",  # non-preemptive placement, still a rights-style dilution
    "waran": "warrant",
}


def fetch_issued_history() -> list[dict]:
    """One call, full history (1801 rows as of 2026-08-11) — same
    "IDX doesn't enforce `length` as real pagination" behavior already
    relied on in sources/idx_company_list.py."""
    resp = requests.get(
        BASE_URL,
        params={"start": 0, "length": 9999},
        impersonate="chrome",
        headers={"Referer": REFERER, "accept": "application/json, text/plain, */*"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    payload = resp.json()
    rows = payload.get("data", [])
    log.info(
        "idx_issued_history_fetched",
        records_total=payload.get("recordsTotal"),
        rows_returned=len(rows),
    )
    return rows


def derive_ratio(shares_added: float | None, shares_after: float | None) -> float | None:
    """after / (after - added) — the multiplicative factor applied to
    shares outstanding by this single action. Only meaningful for
    CLEAN_RATIO_TYPES; callers must gate on action type themselves (kept
    as a separate, independently-testable function rather than folded
    into to_candidate_row so its guards can be unit tested directly).
    Returns None on missing/non-positive inputs or a non-positive
    "before" share count — a bad or degenerate row, not a real ratio.
    """
    if shares_added is None or shares_after is None:
        return None
    shares_before = shares_after - shares_added
    if shares_before <= 0 or shares_after <= 0:
        return None
    return shares_after / shares_before


def to_candidate_row(raw: dict) -> dict:
    """Map one GetIssuedHistory record to a normalized candidate row.

    `ex_date` is deliberately absent here — see module docstring point 1.
    Callers that need one must derive it independently (e.g. a matching
    Track 2 transition date) and say so explicitly, never fall back to
    `listing_date`.
    """
    action_type_raw = raw.get("JenisTindakan")
    shares_added = raw.get("JumlahSaham")
    shares_after = raw.get("JumlahSahamSetelahTindakan")
    listing_date = None
    if raw.get("TanggalPencatatan"):
        listing_date = dt.date.fromisoformat(raw["TanggalPencatatan"][:10])

    ratio = derive_ratio(shares_added, shares_after) if action_type_raw in CLEAN_RATIO_TYPES else None

    return {
        "ticker": raw.get("KodeEmiten"),
        "listing_date": listing_date,
        "action_type_raw": action_type_raw,
        "action_type": _ACTION_TYPE_MAP.get(action_type_raw, "other"),
        "shares_added": shares_added,
        "shares_after": shares_after,
        "ratio": ratio,
        "source": "idx_ledger",
    }


def fetch_candidates() -> list[dict]:
    """Fetch + normalize the full ledger in one call."""
    return [to_candidate_row(r) for r in fetch_issued_history()]
