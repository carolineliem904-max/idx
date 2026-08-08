"""Alert rules for jobs/daily.py's own run, evaluated against a Notifier.

Kept separate from jobs/validate.py (which decides WHAT is wrong) and
jobs/reconcile.py (which decides WHAT disagrees) — this module only
decides which of those findings are alert-worthy and how to phrase them.
The dead man's switch is NOT here — see jobs/dead_mans_switch.py — because
it must run independently of daily.py to mean anything (a check that only
runs inside the job it's watching for absence can never fire).
"""
from __future__ import annotations

import datetime as dt

from idx.jobs.validate import ValidationReport
from idx.notify import Notifier

# First-guess defaults, not measured — tune once real daily-run history exists.
DISCREPANCY_ALERT_THRESHOLD = 5


def evaluate_daily_run(
    notifier: Notifier,
    run_date: dt.date,
    status: str,
    validation_report: ValidationReport,
    discrepancy_count: int,
    tickers_failed: int,
    rows_written: int,
) -> None:
    alerted = False

    if status in ("failed", "partial"):
        level = "error" if status == "failed" else "warning"
        notifier.notify(
            f"daily.py {status} for {run_date}",
            f"tickers_failed={tickers_failed}, rows_written={rows_written}. "
            f"See ingest_runs.error_summary for this run.",
            level=level,
        )
        alerted = True

    # Called out as its own alert (spec extension) even though it's also
    # one of validate.py's checks — a missing-bar spike deserves separate
    # visibility from, say, a single OHLC sanity finding.
    missing_bar_findings = [f for f in validation_report.failures if f.check_name == "missing_bar_pct"]
    if missing_bar_findings:
        notifier.notify(
            f">10% of active tickers missing a bar — {run_date}",
            "\n".join(f.detail for f in missing_bar_findings),
            level="error",
        )
        alerted = True

    other_failures = [f for f in validation_report.failures if f.check_name != "missing_bar_pct"]
    if other_failures:
        notifier.notify(
            f"{len(other_failures)} non-suppressed validator failure(s) — {run_date}",
            "\n".join(f.detail for f in other_failures[:20])
            + ("\n... (truncated)" if len(other_failures) > 20 else ""),
            level="warning",
        )
        alerted = True

    if discrepancy_count > DISCREPANCY_ALERT_THRESHOLD:
        notifier.notify(
            f"{discrepancy_count} new price discrepancies — {run_date}",
            f"Exceeds alert threshold of {DISCREPANCY_ALERT_THRESHOLD}. "
            f"See price_discrepancies table for detail.",
            level="warning",
        )
        alerted = True

    if not alerted:
        # Short summary on success too — so it's visible the pipeline is
        # alive, not just silent when something breaks.
        notifier.notify(
            f"daily.py OK — {run_date}",
            f"rows_written={rows_written}, tickers_failed=0, validation clean, "
            f"discrepancies={discrepancy_count} (within threshold).",
            level="info",
        )
