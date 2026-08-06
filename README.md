# idx-data

IDX point-in-time-correct data ingestion pipeline. Spec: [idx-data-pipeline-spec.md](idx-data-pipeline-spec.md).

## Status

Phase 0 (schema + migrations + one ticker end-to-end) — done. `AMMN` has full
price history in Postgres, loaded idempotently via `jobs/bootstrap.py`.

## Local dev setup

```bash
# 1. Postgres (dev only — Railway is the system of record per spec §1)
docker run -d --name idx-postgres \
  -e POSTGRES_USER=idx -e POSTGRES_PASSWORD=idx_dev_local -e POSTGRES_DB=idx \
  -p 5432:5432 -v idx_postgres_data:/var/lib/postgresql/data postgres:16

# 2. Python env
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 3. Config
cp .env.example .env   # defaults already match the docker command above

# 4. Schema
alembic upgrade head

# 5. Backfill a ticker end-to-end
python -m idx.jobs.bootstrap --tickers AMMN
python -m idx.jobs.bootstrap --tickers AMMN --dry-run   # fetch + log, no DB writes

# Tests
pytest tests/ -q
```

`seed/securities_seed.csv` currently holds one row (AMMN). Phase 1 replaces
it with the full IDX listed-company list — nothing else changes.
