"""Generates review/corporate_actions_candidates.md — a human-review
artifact, NOT an ingestion job. Nothing here writes to corporate_actions.
Caroline reviews tier by tier and signs off explicitly before anything is
committed to the database (spec extension, Phase 2 follow-up, 2026-08-09).

Uses ONE consistent methodology throughout (Track 2 regime-transition
detection from jobs/classify_discrepancies.py) rather than mixing the
earlier rough single-pass ratio-clustering estimate (the original "106")
with the later, more rigorous per-transition method — so counts here may
differ slightly from earlier turns' estimates. That's the more rigorous
number superseding a rough one, not a new inconsistency.

IDX ledger cross-check (added 2026-08-11, see
sources/idx_corporate_actions.py and HANDOFF.md for the feasibility
investigation): every price-ratio-derived candidate below is annotated
against IDX's own ListingActivity/GetIssuedHistory ledger — an
independent, structured record of share-count-changing actions. This is
a CROSS-CHECK, not a second unconditional ground truth (the ledger has
its own real coverage gaps — see the source module docstring); where the
two agree that's high confidence, where either side has something the
other doesn't, that is itself reported, never silently resolved in
either direction. A new "Tier 0" section holds ledger-confirmed clean
splits/reverse-splits/bonus-shares with no matching price-ratio signal
at all (usually because the ticker's Yahoo/IDX pair never diverged
enough to trip Track 2, not because the event didn't happen) — still
confidence 4, since the ledger's ratio math is independently validated
against TPIA and BBNI (exact matches), just missing the second
(price-based) leg the other tiers have.

Runnable locally: python -m idx.jobs.generate_ca_review
"""
from __future__ import annotations

import datetime as dt

import structlog
import typer
from sqlalchemy import text

from idx.db.session import session_scope
from idx.jobs.classify_discrepancies import (
    DEFAULT_MATERIALITY_THRESHOLD,
    fetch_paired_series,
    find_regime_transitions,
    tick_size,
)
from idx.sources.idx_corporate_actions import CLEAN_RATIO_TYPES, fetch_candidates

log = structlog.get_logger()
app = typer.Typer(add_completion=False)

REPO_ROOT_REVIEW_PATH = "review/corporate_actions_candidates.md"

# Widest observed listing lag in the TanggalPencatatan-vs-ex_date
# investigation (LPCK, hmetd, 84 days) plus margin — see
# sources/idx_corporate_actions.py module docstring point 1. Used only
# to decide whether a ledger row and a price-derived transition are
# plausibly "the same event", never to compute an ex_date.
LEDGER_MATCH_WINDOW_DAYS = 90
# Two ratios (as multiplicative factors, direction-agnostic) count as
# agreeing if within this fraction of each other.
RATIO_AGREEMENT_TOL = 0.15

# Common real-world split/reverse-split ratios (1:N or N:1), tolerance 2%.
_CLEAN_FRACTIONS = [1 / n for n in (2, 3, 4, 5, 8, 10, 20, 25, 50, 100)] + [
    float(n) for n in (2, 3, 4, 5, 8, 10, 20, 25, 50, 100)
]


def is_clean_fraction(ratio: float, tol: float = 0.02) -> bool:
    return any(abs(ratio - f) <= tol * max(f, 1) for f in _CLEAN_FRACTIONS)


def guess_action_type(ratio_from: float, ratio_to: float) -> str:
    """ratio_to is always ~1.0 by construction (post-transition, sources
    agree) — the informative number is ratio_from, i.e. what Yahoo showed
    relative to IDX BEFORE the transition."""
    r = ratio_from
    if is_clean_fraction(r):
        return "split" if r < 1 else "reverse_split"
    if 0.5 < r < 1.0:
        return "bonus_share_or_rights"  # odd fraction below 1 — common bonus/rights signature
    return "unknown"  # do not guess past this — spec: leave 'unknown' rather than force a label


# --------------------------------------------------------------------------
# IDX ledger cross-check (sources/idx_corporate_actions.py)
# --------------------------------------------------------------------------


def build_ledger_index(ledger_rows: list[dict]) -> dict[str, list[dict]]:
    by_ticker: dict[str, list[dict]] = {}
    for row in ledger_rows:
        if row["ticker"] is None or row["listing_date"] is None:
            continue
        by_ticker.setdefault(row["ticker"], []).append(row)
    return by_ticker


def find_ledger_match(
    ticker: str, ex_date: dt.date, ledger_index: dict[str, list[dict]]
) -> dict | None:
    """Best ledger row for `ticker` within LEDGER_MATCH_WINDOW_DAYS of
    `ex_date`, or None.

    Not simply "closest by |lag|": IDX's ledger regularly carries more
    than one row for a ticker on the same listing date — a real split
    alongside an unrelated same-day event (BBNI 2023-10-06 has a
    `partialDelisting` row and THREE `stockSplit` rows), or alongside a
    degenerate rounding-residual row of its own type (ISAT 2024-10-14
    has two `stockSplit` rows, `JumlahSaham=0` on both — fractional-share
    remainders, not the real split). A naive "first row seen at the
    smallest |lag|" tie-break picked the `partialDelisting` row for BBNI
    (silently discarding real agreement — ratio_from=0.5 vs. the real
    ledger ratio=2.0, an exact reciprocal match) and the degenerate
    `0->1` row for ISAT (a false "disagree": ratio=1.0 vs. ratio_from
    1.9985 — a real ~2x split reported as a mismatch). Fixed by breaking
    ties in favor of (a) a ledger row that actually derives a ratio, then
    (b) the largest |shares_added| among those — the substantive change,
    not a same-day unrelated event or a rounding remainder. Found via a
    real, wrong output on the first full run of this cross-check, not
    hypothesized in advance — same "verify what looks clean" discipline
    as the rest of this codebase (CLAUDE.md SILENT SUCCESS).
    """
    candidates = [
        (row, (row["listing_date"] - ex_date).days)
        for row in ledger_index.get(ticker, [])
        if abs((row["listing_date"] - ex_date).days) <= LEDGER_MATCH_WINDOW_DAYS
    ]
    if not candidates:
        return None
    best, best_lag = min(
        candidates,
        key=lambda pair: (
            abs(pair[1]),
            pair[0]["ratio"] is None,
            -abs(pair[0]["shares_added"] or 0),
        ),
    )
    return {**best, "lag_days": best_lag}


def _ratio_factor(r: float) -> float:
    """Direction-agnostic magnitude: 0.25 and 4.0 both become 4.0. Needed
    because ratio_from's convention (Yahoo-relative-to-IDX, pre-
    transition) and the ledger's after/(after-minus-added) convention
    aren't guaranteed to land on the same side of 1.0 for the same real
    event."""
    r = abs(r)
    return max(r, 1 / r) if r > 1e-9 else float("inf")


def ratios_agree(price_ratio_from: float, ledger_ratio: float, tol: float = RATIO_AGREEMENT_TOL) -> bool:
    a, b = _ratio_factor(price_ratio_from), _ratio_factor(ledger_ratio)
    return abs(a - b) <= tol * b


def annotate_with_ledger(row: dict, ledger_index: dict[str, list[dict]]) -> dict:
    """Attaches a `ledger_match` key: None (no corroboration found — a
    finding, not a data gap to paper over), or a dict with the matched
    ledger row plus an `agreement` verdict ('agree' / 'disagree' /
    'unratioed' — the ledger row's type has no ratio to compare, e.g. a
    rights issue)."""
    match = find_ledger_match(row["ticker"], row["ex_date"], ledger_index)
    if match is None:
        row["ledger_match"] = None
        return row
    if match["ratio"] is None:
        verdict = "unratioed"
    elif ratios_agree(row["ratio_from"], match["ratio"]):
        verdict = "agree"
    else:
        verdict = "disagree"
    row["ledger_match"] = {**match, "agreement": verdict}
    return row


def find_all_discrepant_tickers(session) -> list[str]:
    rows = session.execute(
        text(
            """
            SELECT DISTINCT y.ticker
            FROM prices_daily_latest y
            JOIN prices_daily_latest i ON i.ticker = y.ticker AND i.date = y.date AND i.source = 'idx'
            WHERE y.source = 'yahoo' AND y.close_raw IS NOT NULL AND i.close_raw IS NOT NULL
            """
        )
    ).all()
    return [r.ticker for r in rows]


def evidence_window(entries: list[tuple], transition_date: dt.date) -> list[dict]:
    """+/-2 rows around the transition date, by position not calendar day
    (so it's always real trading days, not weekends)."""
    idx = next((i for i, e in enumerate(entries) if e[0] == transition_date), None)
    if idx is None:
        return []
    lo, hi = max(0, idx - 2), min(len(entries), idx + 3)
    return [
        {"date": e[0], "yahoo_close": e[1], "idx_close": e[2], "yahoo_vol": e[3], "idx_vol": e[4]}
        for e in entries[lo:hi]
    ]


MAX_RATIO_MAGNITUDE = 50  # ratio_from must be in [1/50, 50] — realistic split ratios don't exceed this
RATIO_TO_CONVERGENCE_TOL = 0.10  # ratio_to must be within 10% of 1.0 to count as "resolved"


def _is_implausible(ratio_from: float, ratio_to: float) -> str | None:
    """Returns a reason string if this transition shouldn't be trusted as
    a real, resolved split/adjustment, else None. Found via two real
    cases while building this file, not hypothesized in advance:

    - BCIC: ratio_from=11111 — Yahoo's close_raw was a garbage value
      (5,000,000, six days, no plausible relation to the real ~450-700
      IDX price) hiding behind what looked like a normal transition once
      it snapped back. A near-zero-denominator guard does NOT catch
      this — the denominator (IDX) was completely normal; the numerator
      (Yahoo) was corrupted. The actual invariant that catches it is a
      plausibility bound on the ratio itself.
    - DSSA: ratio_to=0.04, not ~1.0 — the "transition" found was IDX's
      own internal 10x rescaling, but Yahoo and IDX were ALREADY
      mismatched by a much larger, different factor before AND after it.
      The sources never actually converge, which the ratio_to~1.0
      invariant this whole methodology depends on (stated in the file
      header) silently failed to hold. Excluding on that failure, not
      just the magnitude, is what catches this one.
    """
    if not (1 / MAX_RATIO_MAGNITUDE <= ratio_from <= MAX_RATIO_MAGNITUDE):
        return f"ratio_from={ratio_from} outside plausible bound [{1/MAX_RATIO_MAGNITUDE:.3f}, {MAX_RATIO_MAGNITUDE}] — likely a bad data point, not a real adjustment"
    if abs(ratio_to - 1.0) > RATIO_TO_CONVERGENCE_TOL:
        return f"ratio_to={ratio_to} does not converge to ~1.0 — sources still disagree after this transition, methodology's core assumption doesn't hold here"
    return None


@app.command()
def main() -> None:
    with session_scope() as session:
        tickers = find_all_discrepant_tickers(session)
        by_ticker = fetch_paired_series(session, tickers)

    tier1, tier2, tier3, excluded = [], [], [], []

    for ticker, entries in by_ticker.items():
        for t in find_regime_transitions(ticker, entries):
            # Materiality (see classify_discrepancies.py, 2026-08-11 fix):
            # a single nonzero-volume day at the transition isn't proof of
            # sustained real trading in the new regime — require it across
            # the whole post-regime window, same threshold as Track 1.
            has_real_volume = (
                t.yahoo_vol_at_transition > 0
                and t.idx_vol_at_transition > 0
                and t.yahoo_vol_frac_at_transition >= DEFAULT_MATERIALITY_THRESHOLD
                and t.idx_vol_frac_at_transition >= DEFAULT_MATERIALITY_THRESHOLD
            )
            if not has_real_volume:
                continue

            row = {
                "ticker": ticker,
                "ex_date": t.transition_date,
                "ratio_from": t.pre_ratio,
                "ratio_to": t.post_ratio,
                "pre_regime_days": t.pre_regime_days,
                "clean_fraction": is_clean_fraction(t.pre_ratio),
                "action_type": guess_action_type(t.pre_ratio, t.post_ratio),
                "evidence": evidence_window(entries, t.transition_date),
                "reverts_later": t.reverts_later,
            }

            implausible_reason = _is_implausible(t.pre_ratio, t.post_ratio)
            if implausible_reason:
                row["reason"] = implausible_reason
                excluded.append(row)
            elif t.reverts_later:
                tier3.append(row)
            elif row["clean_fraction"]:
                tier1.append(row)
            else:
                tier2.append(row)

    # Tier 1: known-checkable tickers first (spot-checkable against real
    # events Caroline already knows), then by longest-standing regime
    # (more days at the pre-transition ratio = more confidence it was a
    # real, sustained state, not noise), confidence 4-5.
    KNOWN_CHECKABLE = {"BBCA", "BBNI", "AKRA"}
    tier1.sort(key=lambda r: (r["ticker"] not in KNOWN_CHECKABLE, -r["pre_regime_days"]))

    # Tier 2: by confidence proxy (regime length) descending, confidence 2-3.
    tier2.sort(key=lambda r: -r["pre_regime_days"])

    # Tier 3: chronological, NOT proposals — open questions only.
    tier3.sort(key=lambda r: (r["ticker"], r["ex_date"]))
    excluded.sort(key=lambda r: (r["ticker"], r["ex_date"]))

    # --- IDX ledger cross-check (sources/idx_corporate_actions.py) ---
    # Cross-check only, per instruction: annotate every price-derived row
    # with what the ledger independently says (or doesn't), surface
    # ledger-only clean events as Tier 0, and disagreements as their own
    # section. Never let the ledger silently overwrite the price-derived
    # numbers above.
    try:
        ledger_rows = fetch_candidates()
    except Exception as exc:  # noqa: BLE001 - a live external fetch; the
        # review file must still generate from the price-ratio method
        # alone if IDX's endpoint is down, not fail the whole job.
        log.warning("idx_ledger_fetch_failed", error=str(exc))
        ledger_rows = []

    ledger_index = build_ledger_index(ledger_rows)
    all_price_rows = tier1 + tier2 + tier3 + excluded
    for row in all_price_rows:
        annotate_with_ledger(row, ledger_index)

    disagreements = [r for r in all_price_rows if r["ledger_match"] and r["ledger_match"]["agreement"] == "disagree"]
    disagreements.sort(key=lambda r: (r["ticker"], r["ex_date"]))

    # Tier 0: ledger-confirmed clean-ratio events with NO price-derived
    # match at all — real events by IDX's own record, just missing the
    # second (price-ratio) leg the other tiers have (usually because that
    # ticker's Yahoo/IDX pair never diverged enough to trip Track 2, not
    # because the event didn't happen).
    matched_ledger_keys = {
        (m["ticker"], m["listing_date"]) for r in all_price_rows if (m := r["ledger_match"])
    }
    tier0 = [
        row
        for row in ledger_rows
        if row["action_type_raw"] in CLEAN_RATIO_TYPES
        and row["ratio"] is not None
        and (row["ticker"], row["listing_date"]) not in matched_ledger_keys
    ]
    tier0.sort(key=lambda r: (r["ticker"], r["listing_date"]))

    write_review_file(tier1, tier2, tier3, excluded, tier0, disagreements)
    log.info(
        "ca_review_generated",
        tier0=len(tier0), tier1=len(tier1), tier2=len(tier2), tier3=len(tier3),
        excluded=len(excluded), ledger_rows=len(ledger_rows), disagreements=len(disagreements),
    )
    print(
        f"Wrote {REPO_ROOT_REVIEW_PATH}: tier0={len(tier0)} tier1={len(tier1)} tier2={len(tier2)} "
        f"tier3={len(tier3)} excluded_implausible={len(excluded)} "
        f"ledger_rows_fetched={len(ledger_rows)} cross_check_disagreements={len(disagreements)}"
    )


def _confidence_for_tier1(row: dict) -> int:
    return 5 if row["pre_regime_days"] >= 200 else 4


def _confidence_for_tier2(row: dict) -> int:
    return 3 if row["pre_regime_days"] >= 200 else 2


def _fmt_evidence_table(evidence: list[dict]) -> str:
    lines = ["| date | yahoo_close | idx_close | yahoo_vol | idx_vol |", "|---|---|---|---|---|"]
    for e in evidence:
        lines.append(
            f"| {e['date']} | {e['yahoo_close']:.4f} | {e['idx_close']:.4f} | "
            f"{e['yahoo_vol']:,} | {e['idx_vol']:,} |"
        )
    return "\n".join(lines)


def _fmt_ledger_match(row: dict) -> str:
    """Renders the IDX ledger cross-check line for one price-derived row.
    Always present, even when there's no match — an absence is a finding
    (see COCO's 2025-10-09 rights issue, confirmed externally but with no
    ledger row at all), not something to leave implicit."""
    m = row.get("ledger_match")
    if m is None:
        return "- **IDX ledger cross-check: no corroborating row found** within ±90 days (gap, or event predates/postdates the ledger's coverage of this ticker)"
    lag = m["lag_days"]
    base = (
        f"- **IDX ledger cross-check: {m['action_type_raw']}** on {m['listing_date']} "
        f"(lag {lag:+d}d vs. this row's ex_date)"
    )
    if m["agreement"] == "agree":
        return base + f", ratio={m['ratio']:.4f} — **AGREES** with ratio_from"
    if m["agreement"] == "disagree":
        return base + f", ratio={m['ratio']:.4f} — **DISAGREES** with ratio_from, see Cross-check disagreements below"
    return base + " — ledger doesn't derive a ratio for this action type (rights issue or similar); event corroborated, ratio not comparable"


def write_review_file(
    tier1: list[dict],
    tier2: list[dict],
    tier3: list[dict],
    excluded: list[dict],
    tier0: list[dict] | None = None,
    disagreements: list[dict] | None = None,
) -> None:
    tier0 = tier0 or []
    disagreements = disagreements or []
    lines = []
    lines.append("# Corporate actions review — candidates for `corporate_actions`")
    lines.append("")
    lines.append(
        "**Nothing in this file has been applied to the database.** Every row is a "
        "proposal derived from Yahoo/IDX reconciliation (jobs/classify_discrepancies.py), "
        "not a confirmed fact. Review tier by tier; nothing gets committed to "
        "`corporate_actions` without explicit sign-off, tier by tier."
    )
    lines.append("")
    lines.append(
        "Methodology: a regime-transition is flagged when both sources show real, "
        "matching (non-zero) volume on the transition day and the ratio does not "
        "revert to its pre-transition value afterward. `ratio_from` is what Yahoo's "
        "close showed relative to IDX's BEFORE the transition (IDX raw is the tick-grid "
        "authority); `ratio_to` is always ~1.0 (both sources agree afterward, by "
        "construction of what counts as a transition here)."
    )
    lines.append(
        "Every price-derived row below also carries an **IDX ledger cross-check** line "
        "(sources/idx_corporate_actions.py, ListingActivity/GetIssuedHistory) — an "
        "independent, structured IDX record, not the price-ratio method's own output. "
        "It is a cross-check, not ground truth: it has real coverage gaps of its own "
        "(see the source module docstring), so 'no corroborating row found' is reported "
        "as a finding, not silently treated as disqualifying."
    )
    lines.append("")

    lines.append("## Tier 0 — IDX ledger-confirmed, no price-ratio corroboration (confidence 4)")
    lines.append("")
    lines.append(
        f"{len(tier0)} candidates. `stockSplit`/`reverseStock`/`sahamBonus` rows from IDX's "
        "own ListingActivity ledger with NO matching price-ratio transition — usually "
        "because this ticker's Yahoo/IDX pair never diverged enough to trip Track 2 "
        "detection, not because the event didn't happen. Ratio is derived directly from "
        "IDX's own before/after share counts (`after / (after - added)`), independently "
        "validated exactly against TPIA (4.0, confirmed 1:4) and BBNI (2.0, confirmed "
        "1:2) — see sources/idx_corporate_actions.py. No price evidence table exists for "
        "these (that's precisely what's missing), so `listing_date` is shown instead of "
        "`ex_date` — do not assume they're the same (see that module's docstring point 1: "
        "confirmed ~0-day lag for these three action types specifically, unlike rights "
        "issues)."
    )
    lines.append("")
    for row in tier0:
        lines.append(f"### {row['ticker']} — listing_date {row['listing_date']} — confidence 4/5")
        lines.append("")
        lines.append(
            f"- action_type_raw=**{row['action_type_raw']}**, ratio={row['ratio']:.4f}, "
            f"shares_added={row['shares_added']:,.0f}, shares_after={row['shares_after']:,.0f}"
        )
        lines.append("")

    lines.append("## Tier 1 — clean-fraction splits (confidence 4-5)")
    lines.append("")
    lines.append(
        f"{len(tier1)} candidates. Ratio is a clean reciprocal of a common split ratio "
        "(0.2=1:5, 0.5=1:2, 0.125=1:8, etc.), permanent, real matching volume at the "
        "transition. Known-checkable tickers (BBCA, BBNI, AKRA) sorted first."
    )
    lines.append("")
    for row in tier1:
        conf = _confidence_for_tier1(row)
        lines.append(f"### {row['ticker']} — ex_date {row['ex_date']} — confidence {conf}/5")
        lines.append("")
        lines.append(
            f"- ratio_from={row['ratio_from']}, ratio_to={row['ratio_to']}, "
            f"action_type=**{row['action_type']}**, pre-transition regime held "
            f"{row['pre_regime_days']} trading days"
        )
        lines.append(_fmt_ledger_match(row))
        lines.append("")
        lines.append(_fmt_evidence_table(row["evidence"]))
        lines.append("")

    lines.append("## Tier 2 — new corporate-action candidates (confidence 2-3)")
    lines.append("")
    lines.append(
        f"{len(tier2)} candidates. No individual verification yet. Ratio is NOT a clean "
        "split fraction — `action_type` is a best-effort label "
        "(`bonus_share_or_rights` for odd ratios between 0.5 and 1.0, `unknown` "
        "otherwise) and should be treated as a hint, not a conclusion."
    )
    lines.append("")
    for row in tier2:
        conf = _confidence_for_tier2(row)
        lines.append(f"### {row['ticker']} — ex_date {row['ex_date']} — confidence {conf}/5")
        lines.append("")
        lines.append(
            f"- ratio_from={row['ratio_from']}, ratio_to={row['ratio_to']}, "
            f"action_type=**{row['action_type']}** (best-effort, not confirmed), "
            f"pre-transition regime held {row['pre_regime_days']} trading days"
        )
        lines.append(_fmt_ledger_match(row))
        lines.append("")
        lines.append(_fmt_evidence_table(row["evidence"]))
        lines.append("")

    lines.append("## Tier 3 — ambiguous, self-reverting (OPEN QUESTIONS, not proposals)")
    lines.append("")
    lines.append(
        f"{len(tier3)} cases. Ratio changed with real matching volume but reverted back "
        "to its pre-transition value afterward — NOT a corporate action by definition "
        "(a genuine split/bonus/rights event doesn't undo itself). Listed for awareness "
        "only; do not seed corporate_actions from this section. NOTE (2026-08-11): "
        "Caroline confirmed at least one of these (COCO) is a real rights issue whose "
        "self-reverting price-ratio shape is exactly what a rights issue looks like "
        "(IDX adjusts to theoretical ex-rights price on ex-date, Yahoo on a different "
        "schedule, then reconverge) — 'self-reverting' does NOT mean 'not a corporate "
        "action', it means Track 2's method can't distinguish the two. The IDX ledger "
        "cross-check below is the tool for telling them apart case by case; do not "
        "dismiss any Tier 3 row without checking it first."
    )
    lines.append("")
    for row in tier3:
        lines.append(f"### {row['ticker']} — {row['ex_date']}")
        lines.append("")
        lines.append(f"- ratio_from={row['ratio_from']}, ratio_to={row['ratio_to']} (later reverted)")
        lines.append(_fmt_ledger_match(row))
        lines.append("")
        lines.append(_fmt_evidence_table(row["evidence"]))
        lines.append("")

    lines.append("## Excluded — implausible or unresolved (NOT proposals, NOT open questions — data artifacts)")
    lines.append("")
    lines.append(
        f"{len(excluded)} transitions that matched the real-volume + non-reverting shape "
        "but failed a plausibility check: found via two concrete cases while building "
        "this file (`BCIC` — Yahoo carried a literal 5,000,000 close_raw for 6 days, a "
        "corrupted value, not a real price; `DSSA` — the sources never actually converge "
        "to agreement after the detected transition, meaning it isn't a resolved "
        "adjustment at all, just a partial artifact of a deeper mismatch). Listed for "
        "transparency, not as candidates of any kind."
    )
    lines.append("")
    for row in excluded:
        lines.append(f"### {row['ticker']} — {row['ex_date']}")
        lines.append("")
        lines.append(
            f"- ratio_from={row['ratio_from']}, ratio_to={row['ratio_to']} — **{row['reason']}**"
        )
        lines.append(_fmt_ledger_match(row))
        lines.append("")
        lines.append(_fmt_evidence_table(row["evidence"]))
        lines.append("")

    lines.append("## Cross-check disagreements — investigate before trusting either source")
    lines.append("")
    lines.append(
        f"{len(disagreements)} cases where BOTH the price-ratio method and the IDX ledger "
        "found an event near the same date, but their ratios disagree by more than "
        f"{RATIO_AGREEMENT_TOL:.0%}. Not resolved in either direction here — each is a "
        "genuine open question: which source (if either) has the right ratio for this "
        "specific event."
    )
    lines.append("")
    for row in disagreements:
        m = row["ledger_match"]
        lines.append(f"### {row['ticker']} — price-derived ex_date {row['ex_date']} vs. ledger listing_date {m['listing_date']}")
        lines.append("")
        lines.append(
            f"- price-ratio: ratio_from={row['ratio_from']}, action_type={row['action_type']} "
            f"(tier: {'tier1' if row in tier1 else 'tier2' if row in tier2 else 'tier3' if row in tier3 else 'excluded'})"
        )
        lines.append(
            f"- IDX ledger: action_type_raw={m['action_type_raw']}, ratio={m['ratio']:.4f}, "
            f"lag={m['lag_days']:+d}d"
        )
        lines.append("")

    with open(REPO_ROOT_REVIEW_PATH, "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    app()
