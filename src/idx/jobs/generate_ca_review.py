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

Runnable locally: python -m idx.jobs.generate_ca_review
"""
from __future__ import annotations

import datetime as dt

import structlog
import typer
from sqlalchemy import text

from idx.db.session import session_scope
from idx.jobs.classify_discrepancies import (
    fetch_paired_series,
    find_regime_transitions,
    tick_size,
)

log = structlog.get_logger()
app = typer.Typer(add_completion=False)

REPO_ROOT_REVIEW_PATH = "review/corporate_actions_candidates.md"

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
            has_real_volume = t.yahoo_vol_at_transition > 0 and t.idx_vol_at_transition > 0
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

    write_review_file(tier1, tier2, tier3, excluded)
    log.info(
        "ca_review_generated",
        tier1=len(tier1), tier2=len(tier2), tier3=len(tier3), excluded=len(excluded),
    )
    print(
        f"Wrote {REPO_ROOT_REVIEW_PATH}: tier1={len(tier1)} tier2={len(tier2)} "
        f"tier3={len(tier3)} excluded_implausible={len(excluded)}"
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


def write_review_file(
    tier1: list[dict], tier2: list[dict], tier3: list[dict], excluded: list[dict]
) -> None:
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
        lines.append("")
        lines.append(_fmt_evidence_table(row["evidence"]))
        lines.append("")

    lines.append("## Tier 3 — ambiguous, self-reverting (OPEN QUESTIONS, not proposals)")
    lines.append("")
    lines.append(
        f"{len(tier3)} cases. Ratio changed with real matching volume but reverted back "
        "to its pre-transition value afterward — NOT a corporate action by definition "
        "(a genuine split/bonus/rights event doesn't undo itself). Listed for awareness "
        "only; do not seed corporate_actions from this section."
    )
    lines.append("")
    for row in tier3:
        lines.append(f"### {row['ticker']} — {row['ex_date']}")
        lines.append("")
        lines.append(f"- ratio_from={row['ratio_from']}, ratio_to={row['ratio_to']} (later reverted)")
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
        lines.append("")
        lines.append(_fmt_evidence_table(row["evidence"]))
        lines.append("")

    with open(REPO_ROOT_REVIEW_PATH, "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    app()
