"""Dead man's switch — spec extension, Phase 2.

Must run on its own independent schedule (see README "Local scheduling"),
NOT from inside jobs/daily.py: a check for "daily.py hasn't run" that only
executes as part of daily.py running can never fire when it matters. "A
dead scheduler looks exactly like success if we only alert on errors" —
this is the check for the scheduler itself being dead, not for a run
failing.

Runnable locally: python -m idx.jobs.dead_mans_switch
"""
from __future__ import annotations

import datetime as dt

import structlog
import typer
from sqlalchemy import text

from idx.db.session import session_scope
from idx.notify import get_notifier

log = structlog.get_logger()
app = typer.Typer(add_completion=False)

THRESHOLD_HOURS = 36


@app.command()
def main() -> None:
    with session_scope() as session:
        last_success = session.execute(
            text("SELECT max(finished_at) FROM ingest_runs WHERE job_name = 'daily' AND status = 'success'")
        ).scalar_one()

    notifier = get_notifier()
    now = dt.datetime.now(dt.timezone.utc)

    if last_success is None:
        notifier.notify(
            "DEAD MAN'S SWITCH: daily.py has never succeeded",
            "No ingest_runs row with job_name='daily' and status='success' exists at all.",
            level="error",
        )
        raise typer.Exit(code=1)

    age_hours = (now - last_success).total_seconds() / 3600
    if age_hours > THRESHOLD_HOURS:
        notifier.notify(
            f"DEAD MAN'S SWITCH: no successful daily.py run in {age_hours:.1f}h",
            f"Threshold is {THRESHOLD_HOURS}h. Last success: {last_success.isoformat()}. "
            f"Check launchd status (launchctl list | grep idx) and data/logs/.",
            level="error",
        )
        raise typer.Exit(code=1)

    log.info("dead_mans_switch_ok", hours_since_last_success=round(age_hours, 1))


if __name__ == "__main__":
    app()
