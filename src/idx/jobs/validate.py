"""jobs/validate.py — data quality gate (spec §3.3).

Every check consults `known_issues` before reporting. A match SUPPRESSES
a finding from the "real failures" section but it is ALWAYS still printed,
in a separate "known, suppressed" section — never silently dropped. That
distinction is the whole point: suppression exists so the alert channel
doesn't cry wolf on already-understood data quirks, not so those quirks
disappear from view. Comment out a check instead and you've just made the
data worse without knowing it.

Called two ways:
- Inline, in-process, from jobs/daily.py (spec §3.2 step 4) — passes the
  run's own rolling window as the date range.
- Standalone, for an ad-hoc audit over an arbitrary range (defaults to
  the last 7 days if not given):
    python -m idx.jobs.validate run --start 2026-08-01 --end 2026-08-07
  Exits non-zero iff any NON-suppressed failure exists — scriptable.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

import structlog
import typer
from sqlalchemy import text

from idx.db.session import session_scope

log = structlog.get_logger()
app = typer.Typer(add_completion=False)

CLOSE_JUMP_THRESHOLD = 0.35  # spec §3.3
MISSING_BAR_THRESHOLD = 0.10  # spec §3.3
ZERO_VOLUME_LIQUIDITY_RANK = 300  # spec §3.3 "top 300"


@dataclass
class Finding:
    check_name: str
    detail: str
    ticker: str | None = None
    date: dt.date | None = None
    suppressed: bool = False
    suppression_reason: str | None = None


@dataclass
class ValidationReport:
    failures: list[Finding] = field(default_factory=list)
    suppressed: list[Finding] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return len(self.failures) > 0


# --------------------------------------------------------------------------
# Suppression
# --------------------------------------------------------------------------


def _load_known_issues(session) -> list[dict]:
    rows = session.execute(
        text(
            "SELECT scope, ticker, date_start, date_end, check_name, reason, review_by "
            "FROM known_issues"
        )
    ).mappings().all()
    today = dt.date.today()
    active = []
    for r in rows:
        if r["review_by"] is not None and r["review_by"] < today:
            continue  # expired suppression — must show up as a real failure again
        active.append(dict(r))
    return active


def _matches(issue: dict, finding: Finding) -> bool:
    if issue["check_name"] != finding.check_name:
        return False
    scope = issue["scope"]
    ticker_ok = scope in ("ticker", "both") and issue["ticker"] == finding.ticker
    range_ok = (
        scope in ("date_range", "both")
        and finding.date is not None
        and issue["date_start"] is not None
        and issue["date_end"] is not None
        and issue["date_start"] <= finding.date <= issue["date_end"]
    )
    if scope == "both":
        return ticker_ok and range_ok
    return ticker_ok or range_ok


def _apply_suppression(findings: list[Finding], known_issues: list[dict]) -> ValidationReport:
    report = ValidationReport()
    for finding in findings:
        match = next((i for i in known_issues if _matches(i, finding)), None)
        if match:
            finding.suppressed = True
            finding.suppression_reason = match["reason"]
            report.suppressed.append(finding)
        else:
            report.failures.append(finding)
    return report


# --------------------------------------------------------------------------
# Checks (spec §3.3)
# --------------------------------------------------------------------------


def check_zero_rows_on_trading_day(session, date_start: dt.date, date_end: dt.date) -> list[Finding]:
    rows = session.execute(
        text(
            """
            SELECT tc.date
            FROM trading_calendar tc
            WHERE tc.is_trading_day AND tc.date BETWEEN :start AND :end
              AND NOT EXISTS (
                  SELECT 1 FROM prices_daily_latest pd WHERE pd.date = tc.date
              )
            ORDER BY tc.date
            """
        ),
        {"start": date_start, "end": date_end},
    ).all()
    return [
        Finding(check_name="zero_rows_trading_day", date=r.date, detail=f"No prices_daily rows at all for {r.date}, a known trading day.")
        for r in rows
    ]


def check_missing_bar_pct(session, date_start: dt.date, date_end: dt.date) -> list[Finding]:
    """spec §3.3's ">10% of active tickers missing a bar" is an aggregate
    check with no per-ticker granularity, so a per-ticker known_issues
    suppression can never match it directly (Finding.ticker is None here).
    Without excluding chronically-missing tickers (known_issues,
    check_name='insufficient_yahoo_history') from the denominator, this
    check would fire — or sit permanently close to the threshold — every
    single day forever, exactly the "cry wolf" failure mode known_issues
    exists to prevent. So the exclusion happens here, in the query, not
    via the generic suppression path.
    """
    rows = session.execute(
        text(
            """
            WITH excluded_tickers AS (
                SELECT DISTINCT ticker FROM known_issues
                WHERE scope = 'ticker' AND check_name = 'insufficient_yahoo_history'
                  AND ticker IS NOT NULL
                  AND (review_by IS NULL OR review_by >= CURRENT_DATE)
            ),
            active_count AS (
                SELECT count(*) AS n FROM securities s
                WHERE s.is_active AND s.ticker NOT IN (SELECT ticker FROM excluded_tickers)
            ),
            per_day AS (
                SELECT tc.date, count(DISTINCT pd.ticker) AS n_present
                FROM trading_calendar tc
                LEFT JOIN prices_daily_latest pd
                  ON pd.date = tc.date AND pd.source = 'yahoo'
                  AND pd.ticker NOT IN (SELECT ticker FROM excluded_tickers)
                WHERE tc.is_trading_day AND tc.date BETWEEN :start AND :end
                GROUP BY tc.date
            )
            SELECT per_day.date, per_day.n_present, active_count.n AS n_active
            FROM per_day, active_count
            WHERE active_count.n > 0
              AND (active_count.n - per_day.n_present)::float / active_count.n > :threshold
            ORDER BY per_day.date
            """
        ),
        {"start": date_start, "end": date_end, "threshold": MISSING_BAR_THRESHOLD},
    ).all()
    return [
        Finding(
            check_name="missing_bar_pct",
            date=r.date,
            detail=f"{r.n_active - r.n_present}/{r.n_active} active tickers missing a Yahoo bar on {r.date} "
            f"({(r.n_active - r.n_present) / r.n_active:.1%} > {MISSING_BAR_THRESHOLD:.0%} threshold).",
        )
        for r in rows
    ]


def check_ohlc_sanity(session, date_start: dt.date, date_end: dt.date) -> list[Finding]:
    rows = session.execute(
        text(
            """
            SELECT ticker, date, source, open_raw, high_raw, low_raw, close_raw
            FROM prices_daily_latest
            WHERE date BETWEEN :start AND :end
              AND high_raw IS NOT NULL AND low_raw IS NOT NULL
              AND (
                  high_raw < low_raw
                  OR (close_raw IS NOT NULL AND (close_raw < low_raw OR close_raw > high_raw))
              )
            ORDER BY ticker, date
            """
        ),
        {"start": date_start, "end": date_end},
    ).all()
    return [
        Finding(
            check_name="ohlc_sanity",
            ticker=r.ticker,
            date=r.date,
            detail=f"{r.ticker} {r.date} ({r.source}): open={r.open_raw} high={r.high_raw} low={r.low_raw} close={r.close_raw} — high<low or close outside [low,high].",
        )
        for r in rows
    ]


def check_close_jump(session, date_start: dt.date, date_end: dt.date) -> list[Finding]:
    """close_raw change >35% day-over-day (spec §3.3) without a matching
    corporate_actions row (matched loosely: any action within +/-3 days —
    corporate action ex-dates and the market's reaction to them don't
    always land on the exact same trading day)."""
    rows = session.execute(
        text(
            """
            WITH ordered AS (
                SELECT ticker, source, date, close_raw,
                       LAG(close_raw) OVER (PARTITION BY ticker, source ORDER BY date) AS prev_close,
                       LAG(date) OVER (PARTITION BY ticker, source ORDER BY date) AS prev_date
                FROM prices_daily_latest
                WHERE close_raw IS NOT NULL
            )
            SELECT o.ticker, o.date, o.source, o.close_raw, o.prev_close,
                   (o.close_raw - o.prev_close) / o.prev_close AS pct_change
            FROM ordered o
            WHERE o.date BETWEEN :start AND :end
              AND o.prev_close IS NOT NULL AND o.prev_close != 0
              AND abs((o.close_raw - o.prev_close) / o.prev_close) > :threshold
              AND NOT EXISTS (
                  SELECT 1 FROM corporate_actions ca
                  WHERE ca.ticker = o.ticker AND ca.ex_date BETWEEN o.date - 3 AND o.date + 3
              )
            ORDER BY o.ticker, o.date
            """
        ),
        {"start": date_start, "end": date_end, "threshold": CLOSE_JUMP_THRESHOLD},
    ).all()
    return [
        Finding(
            check_name="close_jump_35pct",
            ticker=r.ticker,
            date=r.date,
            detail=f"{r.ticker} {r.date} ({r.source}): {r.prev_close} -> {r.close_raw} ({r.pct_change:+.1%}), no corporate_actions row within +/-3 days.",
        )
        for r in rows
    ]


def check_zero_volume_top300(session, date_start: dt.date, date_end: dt.date) -> list[Finding]:
    rows = session.execute(
        text(
            """
            WITH liquidity AS (
                SELECT ticker,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY volume) AS median_volume
                FROM prices_daily_latest
                WHERE source = 'yahoo' AND date >= :start - INTERVAL '20 days' AND date < :start
                GROUP BY ticker
            ),
            ranked AS (
                SELECT ticker, RANK() OVER (ORDER BY median_volume DESC NULLS LAST) AS liquidity_rank
                FROM liquidity
            )
            SELECT pd.ticker, pd.date
            FROM prices_daily_latest pd
            JOIN ranked r ON r.ticker = pd.ticker AND r.liquidity_rank <= :rank
            WHERE pd.source = 'yahoo' AND pd.date BETWEEN :start AND :end AND pd.volume = 0
            ORDER BY pd.ticker, pd.date
            """
        ),
        {"start": date_start, "end": date_end, "rank": ZERO_VOLUME_LIQUIDITY_RANK},
    ).all()
    return [
        Finding(
            check_name="zero_volume_top300",
            ticker=r.ticker,
            date=r.date,
            detail=f"{r.ticker} {r.date}: volume=0 despite ranking in the top {ZERO_VOLUME_LIQUIDITY_RANK} by trailing 20d median volume.",
        )
        for r in rows
    ]


def check_backdating(session) -> list[Finding]:
    """spec §3.3: created_at < valid_from/heard_at is impossible — it
    indicates backdating, which destroys the leakage guard. Not date-range
    scoped: these tables are small and every row matters, always."""
    findings = []
    rows = session.execute(
        text(
            "SELECT ticker, entity_id, role, valid_from, created_at FROM security_control "
            "WHERE created_at < valid_from"
        )
    ).all()
    findings += [
        Finding(
            check_name="backdating",
            ticker=r.ticker,
            detail=f"security_control {r.ticker}/{r.entity_id}/{r.role}: created_at={r.created_at} < valid_from={r.valid_from}.",
        )
        for r in rows
    ]
    rows = session.execute(
        text("SELECT id, ticker, heard_at, created_at FROM rumors WHERE created_at < heard_at")
    ).all()
    findings += [
        Finding(
            check_name="backdating",
            ticker=r.ticker,
            detail=f"rumors id={r.id} {r.ticker}: created_at={r.created_at} < heard_at={r.heard_at}.",
        )
        for r in rows
    ]
    return findings


def check_insufficient_history(session) -> list[Finding]:
    """Not one of spec §3.3's original 6 — added because Phase 1a found a
    real, recurring pattern spec's checks don't otherwise catch: IDX names
    under extended trading suspension (e.g. WSKT/Waskita Karya, mid debt
    restructuring), where Yahoo returns a single stale quote instead of
    real history. Whole-table, not date-scoped: this is a standing fact
    about a ticker's overall coverage, not something that happens "on" a
    particular day. Threshold (<=2 bars ever) matches exactly how Phase 1a
    originally identified this population, so seeded known_issues rows
    line up with what this actually finds."""
    rows = session.execute(
        text(
            """
            SELECT s.ticker, count(pd.date) AS n_bars
            FROM securities s
            LEFT JOIN prices_daily_latest pd ON pd.ticker = s.ticker AND pd.source = 'yahoo'
            WHERE s.is_active
            GROUP BY s.ticker
            HAVING count(pd.date) <= 2
            ORDER BY s.ticker
            """
        )
    ).all()
    return [
        Finding(
            check_name="insufficient_yahoo_history",
            ticker=r.ticker,
            detail=f"{r.ticker}: only {r.n_bars} Yahoo bar(s) ever, despite being marked active.",
        )
        for r in rows
    ]


def check_frozen_price_divergence(session, min_run_days: int = 5) -> list[Finding]:
    """Standing check, spec extension (2026-08-09), added because category
    B of the Yahoo/IDX reconciliation investigation is a distinct failure
    class from anything else in this file: `close_raw` identical across
    many consecutive days on ONE source while the other shows real
    volume/movement, with NO other visible defect — no gap, no null, no
    error. `ADMF` sat frozen at exactly 6100.0 for 289 days; `BNBR` for
    767. A model reads either as a genuinely quiet stock. Same
    silent-success failure mode as the ON CONFLICT DO NOTHING dead-tuple
    bug (Phase 2 Part A) and the reason the companion-audit-table design
    was rejected for the PK-widening decision (Phase 2 schema fix) — this
    is the third time that exact failure shape has shown up in this
    codebase.

    Deliberately NOT date-range scoped and NOT part of run_validation's
    default set daily.py calls every run — a Yahoo data freeze has no
    natural "today" boundary, it's a standing fact about the whole
    series, and rescanning full history on every daily run would be
    wasteful. Invoke via `python -m idx.jobs.validate full-history-audit`.
    """
    from idx.jobs.classify_discrepancies import fetch_paired_series, find_frozen_runs

    tickers = [
        r.ticker
        for r in session.execute(text("SELECT ticker FROM securities WHERE is_active")).all()
    ]
    by_ticker = fetch_paired_series(session, tickers)

    findings = []
    for ticker, entries in by_ticker.items():
        for run in find_frozen_runs(ticker, entries):
            if run.n_days < min_run_days:
                continue
            if run.frozen_side not in ("yahoo", "idx") or not run.other_side_moved:
                continue  # 'both frozen' is category C (thread #2 territory), not this check
            findings.append(
                Finding(
                    check_name="frozen_price_divergence",
                    ticker=ticker,
                    date=run.start,
                    detail=(
                        f"{ticker}: {run.frozen_side} close_raw frozen for {run.n_days} days "
                        f"({run.start}..{run.end}) while the other source shows real movement."
                    ),
                )
            )
    return findings


def run_validation(session, date_start: dt.date, date_end: dt.date) -> ValidationReport:
    findings: list[Finding] = []
    findings += check_zero_rows_on_trading_day(session, date_start, date_end)
    findings += check_missing_bar_pct(session, date_start, date_end)
    findings += check_ohlc_sanity(session, date_start, date_end)
    findings += check_close_jump(session, date_start, date_end)
    findings += check_zero_volume_top300(session, date_start, date_end)
    findings += check_backdating(session)  # whole-table, not date-scoped
    findings += check_insufficient_history(session)  # whole-table, not date-scoped

    known_issues = _load_known_issues(session)
    return _apply_suppression(findings, known_issues)


def print_report(report: ValidationReport) -> None:
    print(f"\n{'='*78}\nVALIDATION REPORT\n{'='*78}")
    print(f"\nFAILURES ({len(report.failures)}):")
    if not report.failures:
        print("  none")
    for f in report.failures:
        print(f"  [{f.check_name}] {f.detail}")

    print(f"\nKNOWN, SUPPRESSED ({len(report.suppressed)}):")
    if not report.suppressed:
        print("  none")
    for f in report.suppressed:
        print(f"  [{f.check_name}] {f.detail}  <- {f.suppression_reason}")
    print()


@app.command()
def run(
    start: dt.datetime = typer.Option(None, "--start", help="Default: 7 days ago."),
    end: dt.datetime = typer.Option(None, "--end", help="Default: today."),
) -> None:
    end_date = end.date() if end else dt.date.today()
    start_date = start.date() if start else end_date - dt.timedelta(days=7)

    with session_scope() as session:
        report = run_validation(session, start_date, end_date)

    print_report(report)
    if report.failed:
        raise typer.Exit(code=1)


@app.command("full-history-audit")
def full_history_audit(
    min_run_days: int = typer.Option(5, help="Minimum frozen-run length to report."),
) -> None:
    """Standing checks that need the whole series, not a rolling window —
    currently just check_frozen_price_divergence (category B). Run this
    periodically (manually, or its own scheduled job — not wired into
    daily.py's per-run set), not as part of the normal daily pass."""
    with session_scope() as session:
        findings = check_frozen_price_divergence(session, min_run_days=min_run_days)
        known_issues = _load_known_issues(session)
        report = _apply_suppression(findings, known_issues)

    print_report(report)
    if report.failed:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
