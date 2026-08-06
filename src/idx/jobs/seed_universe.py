"""Phase 1a — seed `securities` from IDX's current listed-company directory.

This populates the ACTIVE universe only (~962 tickers as of 2026-08). It is
NOT the full point-in-time universe (spec §0 principle 4) — delisted
tickers are a separate job, jobs/harvest_universe_history.py, because IDX's
company directory simply doesn't carry them. Anyone reading `securities`
after only this job has run is looking at a survivorship-biased snapshot;
see README "Data completeness" for the current, honest status.

Runnable locally: python -m idx.jobs.seed_universe [--dry-run]
"""
from __future__ import annotations

import datetime as dt

import structlog
import typer

from idx.db.models import IngestRun
from idx.db.session import session_scope
from idx.db.upserts import upsert_security
from idx.sources.idx_company_list import fetch_company_profiles, to_security_row

log = structlog.get_logger()

app = typer.Typer(add_completion=False)


@app.command()
def main(
    dry_run: bool = typer.Option(False, help="Fetch and log, but do not write to the database."),
) -> None:
    started_at = dt.datetime.now(dt.timezone.utc)
    profiles = fetch_company_profiles()
    rows = [to_security_row(p) for p in profiles]

    log.info("seed_universe_start", ticker_count=len(rows), dry_run=dry_run)

    if dry_run:
        log.info("dry_run_sample", sample=rows[:3])
        log.info("seed_universe_done", rows_written=0, dry_run=True)
        return

    with session_scope() as session:
        for row in rows:
            upsert_security(session, row)
        session.add(
            IngestRun(
                job_name="seed_universe",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                status="success",
                rows_written=len(rows),
                tickers_attempted=len(rows),
                tickers_failed=0,
                # error_summary is reserved for actual failures (spec §3.2
                # step 4 alerting reads it); the survivorship-bias caveat
                # belongs in README "Data completeness", not here.
                error_summary=None,
            )
        )

    log.warning(
        "seed_universe_done_survivorship_biased",
        rows_written=len(rows),
        note="Active universe only. Run jobs/harvest_universe_history.py + "
        "reconcile-delisted before treating `securities` as point-in-time-complete.",
    )


if __name__ == "__main__":
    app()
