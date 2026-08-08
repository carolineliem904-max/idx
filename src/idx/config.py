"""Central config. Secrets come from environment variables only (see spec §8)."""
from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from dotenv import load_dotenv

# Explicit path, not bare load_dotenv()'s CWD-upward search: launchd (Part
# G) starts jobs with no guaranteed working directory, so CWD-relative
# discovery would silently fail to find .env and every DATABASE_URL lookup
# would raise — exactly the kind of failure that's invisible until the
# scheduled job actually runs unattended.
_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"  # src/idx/config.py -> repo root
load_dotenv(_ENV_PATH)


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )
    return url


# Business-scope decision (Phase 2, 2026-08-08): pre-2020 Yahoo history is
# exploration-only, not part of the production dataset. It stays in local
# Docker for research but is excluded from anything we'd deploy or treat
# as the production backup set. Single source of truth — reference this,
# don't hardcode the date elsewhere.
#
# Coincides with sources/idx_official.py::EARLIEST_AVAILABLE_DATE (IDX's
# own technical retention wall — GetStockSummary has no data before this
# date) but is a DIFFERENT, independent constant: one is a business scope
# decision, the other is a hard limit of an external API. Don't conflate
# them even though they're equal today — a future change to one should
# not silently change the other.
PRODUCTION_DATA_CUTOFF = dt.date(2020, 1, 2)

# Configurable, not hardcoded: which Docker container backup/restore
# tooling shells into when pg_dump/pg_restore/psql aren't on PATH directly
# (i.e. local dev, where Postgres only exists inside Docker). Irrelevant
# once DATABASE_URL points at real infrastructure with those binaries
# available directly — see jobs/backup.py.
def local_pg_container() -> str:
    return os.environ.get("IDX_PG_CONTAINER", "idx-postgres")
