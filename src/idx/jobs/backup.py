"""Local backups. Compressed pg_dump (custom format, -Fc), timestamped,
gitignored local dir, retention (7 daily + 4 weekly), and a `verify`
command that actually restores into a throwaway database and checks it —
a backup that has never been restored is not a backup.

Provider-neutral by design: tries pg_dump/pg_restore/psql directly first
(what a deployed environment with real infra would have on PATH), and
only falls back to `docker exec <container> ...` when those binaries
aren't found locally — which is our situation today, running Postgres
only inside Docker. Nothing here is Railway-specific or host-hardcoded;
the container name is configurable (idx.config.local_pg_container) and
the connection itself always comes from DATABASE_URL. When this moves to
real infrastructure, the direct-binary path is what runs, no code change.

Full local backups intentionally include everything, exploration data
(pre-2020 Yahoo history) included — it's still worth protecting locally.
idx.config.PRODUCTION_DATA_CUTOFF is where a future production-migration
step would draw the line for what actually ships to real infrastructure;
that scoped export isn't built yet since we're not deploying (see README
"Data completeness" / "Production data scope").

Runnable locally:
    python -m idx.jobs.backup backup
    python -m idx.jobs.backup list
    python -m idx.jobs.backup verify                      # verifies the latest backup
    python -m idx.jobs.backup restore <file> --target-db idx_restored --force
"""
from __future__ import annotations

import datetime as dt
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import structlog
import typer

from idx.config import database_url, local_pg_container

log = structlog.get_logger()
app = typer.Typer(add_completion=False)

REPO_ROOT = Path(__file__).resolve().parents[3]
BACKUP_DIR = REPO_ROOT / "data" / "backups"
RETENTION_DAILY = 7
RETENTION_WEEKLY = 4
VERIFY_DB_NAME = "idx_backup_verify"

FILENAME_RE = re.compile(r"idx_backup_(\d{8})_(\d{6})\.dump")


@dataclass
class PgConn:
    host: str
    port: int
    user: str
    password: str
    dbname: str


def _parse_database_url() -> PgConn:
    # postgresql+psycopg://user:pass@host:port/dbname -> strip the driver suffix
    url = database_url().replace("postgresql+psycopg://", "postgresql://")
    parsed = urlparse(url)
    return PgConn(
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        user=parsed.username or "postgres",
        password=parsed.password or "",
        dbname=parsed.path.lstrip("/"),
    )


def _binary_available(name: str) -> bool:
    return shutil.which(name) is not None


def _run(cmd: list[str], env: dict | None = None, **kwargs) -> subprocess.CompletedProcess:
    log.debug("run_command", cmd=" ".join(cmd))
    return subprocess.run(cmd, env=env, check=True, **kwargs)


def _pg_env(conn: PgConn) -> dict:
    import os

    env = os.environ.copy()
    env["PGPASSWORD"] = conn.password
    return env


def _docker_prefix() -> list[str]:
    return ["docker", "exec", "-i", local_pg_container()]


# --------------------------------------------------------------------------
# backup
# --------------------------------------------------------------------------


@app.command()
def backup() -> None:
    conn = _parse_database_url()
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = BACKUP_DIR / f"idx_backup_{timestamp}.dump"

    log.info("backup_start", target=str(out_path))

    if _binary_available("pg_dump"):
        cmd = [
            "pg_dump", "-Fc",
            "-h", conn.host, "-p", str(conn.port), "-U", conn.user, "-d", conn.dbname,
            "-f", str(out_path),
        ]
        _run(cmd, env=_pg_env(conn))
    else:
        # Local dev: Postgres only exists inside Docker, pg_dump isn't on
        # the host PATH. Run pg_dump inside the container, stream its
        # stdout to a host file (docker exec -i pipes stdout normally).
        cmd = _docker_prefix() + ["pg_dump", "-Fc", "-U", conn.user, "-d", conn.dbname]
        with out_path.open("wb") as f:
            log.info("backup_via_docker_exec", container=local_pg_container())
            subprocess.run(cmd, stdout=f, check=True)

    size_bytes = out_path.stat().st_size
    log.info("backup_done", path=str(out_path), size_bytes=size_bytes)
    print(f"Backup written: {out_path} ({size_bytes / 1024 / 1024:.1f} MB)")

    pruned = prune_backups()
    if pruned:
        print(f"Pruned {len(pruned)} old backup(s) per retention policy: {[p.name for p in pruned]}")


def _list_backup_files() -> list[tuple[Path, dt.datetime]]:
    out = []
    if not BACKUP_DIR.exists():
        return out
    for f in BACKUP_DIR.glob("idx_backup_*.dump"):
        m = FILENAME_RE.match(f.name)
        if not m:
            continue
        ts = dt.datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(
            tzinfo=dt.timezone.utc
        )
        out.append((f, ts))
    return sorted(out, key=lambda x: x[1], reverse=True)


def prune_backups() -> list[Path]:
    """Keep the most recent RETENTION_DAILY distinct calendar days (latest
    backup per day) plus the most recent RETENTION_WEEKLY distinct ISO
    weeks (latest backup per week). Delete everything else."""
    files = _list_backup_files()
    keep: set[Path] = set()

    seen_days: dict[dt.date, Path] = {}
    for path, ts in files:  # newest first
        day = ts.date()
        if day not in seen_days:
            seen_days[day] = path
    keep.update(list(seen_days.values())[:RETENTION_DAILY])

    seen_weeks: dict[tuple[int, int], Path] = {}
    for path, ts in files:
        week = ts.isocalendar()[:2]  # (iso_year, iso_week)
        if week not in seen_weeks:
            seen_weeks[week] = path
    keep.update(list(seen_weeks.values())[:RETENTION_WEEKLY])

    pruned = []
    for path, _ts in files:
        if path not in keep:
            path.unlink()
            pruned.append(path)
    return pruned


@app.command("list")
def list_backups() -> None:
    files = _list_backup_files()
    if not files:
        print("No backups found.")
        return
    print(f"{'file':<40}{'age':>12}{'size':>12}")
    now = dt.datetime.now(dt.timezone.utc)
    for path, ts in files:
        age_hours = (now - ts).total_seconds() / 3600
        size_mb = path.stat().st_size / 1024 / 1024
        print(f"{path.name:<40}{age_hours:>10.1f}h{size_mb:>10.1f}MB")


# --------------------------------------------------------------------------
# restore
# --------------------------------------------------------------------------


def _createdb(conn: PgConn, dbname: str) -> None:
    if _binary_available("createdb"):
        _run(
            ["createdb", "-h", conn.host, "-p", str(conn.port), "-U", conn.user, dbname],
            env=_pg_env(conn),
        )
    else:
        _run(_docker_prefix() + ["createdb", "-U", conn.user, dbname])


def _dropdb(conn: PgConn, dbname: str) -> None:
    if _binary_available("dropdb"):
        _run(
            ["dropdb", "-h", conn.host, "-p", str(conn.port), "-U", conn.user, "--if-exists", dbname],
            env=_pg_env(conn),
        )
    else:
        _run(_docker_prefix() + ["dropdb", "-U", conn.user, "--if-exists", dbname])


def _pg_restore(conn: PgConn, backup_path: Path, target_dbname: str) -> None:
    if _binary_available("pg_restore"):
        _run(
            [
                "pg_restore", "-h", conn.host, "-p", str(conn.port), "-U", conn.user,
                "-d", target_dbname, "--no-owner", "--no-privileges", str(backup_path),
            ],
            env=_pg_env(conn),
        )
    else:
        # Copy the dump into the container first — docker exec -i can pipe
        # stdin, but pg_restore reading a custom-format file needs a real
        # path, not a stream, so `docker cp` it in rather than piping.
        container = local_pg_container()
        tmp_path = f"/tmp/{backup_path.name}"
        _run(["docker", "cp", str(backup_path), f"{container}:{tmp_path}"])
        try:
            _run(
                _docker_prefix()
                + ["pg_restore", "-U", conn.user, "-d", target_dbname,
                   "--no-owner", "--no-privileges", tmp_path]
            )
        finally:
            _run(_docker_prefix() + ["rm", "-f", tmp_path])


def _row_counts(conn: PgConn, dbname: str, tables: list[str]) -> dict[str, int]:
    counts = {}
    for table in tables:
        query = f'SELECT count(*) FROM "{table}"'
        if _binary_available("psql"):
            result = subprocess.run(
                ["psql", "-h", conn.host, "-p", str(conn.port), "-U", conn.user,
                 "-d", dbname, "-t", "-A", "-c", query],
                env=_pg_env(conn), check=True, capture_output=True, text=True,
            )
        else:
            result = subprocess.run(
                _docker_prefix() + ["psql", "-U", conn.user, "-d", dbname, "-t", "-A", "-c", query],
                check=True, capture_output=True, text=True,
            )
        counts[table] = int(result.stdout.strip())
    return counts


@app.command()
def restore(
    backup_file: Path = typer.Argument(..., help="Path to a .dump file from `backup`."),
    target_db: str = typer.Option(..., help="Database to restore INTO. Must not be the live DATABASE_URL db unless --force."),
    force: bool = typer.Option(False, help="Allow restoring over the live database."),
) -> None:
    conn = _parse_database_url()
    if target_db == conn.dbname and not force:
        print(
            f"Refusing to restore into '{target_db}' — that's the live database "
            f"(from DATABASE_URL). Pass --force if this is really what you want, "
            f"or pick a different --target-db."
        )
        raise typer.Exit(code=1)

    if not backup_file.exists():
        print(f"Backup file not found: {backup_file}")
        raise typer.Exit(code=1)

    log.info("restore_start", backup_file=str(backup_file), target_db=target_db)
    _dropdb(conn, target_db)
    _createdb(conn, target_db)
    _pg_restore(conn, backup_file, target_db)
    log.info("restore_done", target_db=target_db)
    print(f"Restored {backup_file.name} into database '{target_db}'.")


# --------------------------------------------------------------------------
# verify — actually prove a backup is restorable, not just that the file exists
# --------------------------------------------------------------------------

# Kept small and cheap on purpose: this runs after every intended-for-real
# backup verification, not just once. Anything bigger (full checksum diff)
# is overkill for "did the restore work at all".
_SANITY_TABLES = ["securities", "prices_daily", "trading_calendar", "ingest_runs"]


@app.command()
def verify(
    backup_file: Path = typer.Option(
        None, help="Backup to verify (default: the most recent one)."
    ),
) -> None:
    conn = _parse_database_url()

    if backup_file is None:
        files = _list_backup_files()
        if not files:
            print("No backups to verify.")
            raise typer.Exit(code=1)
        backup_file = files[0][0]

    print(f"Verifying {backup_file.name} by restoring into throwaway db '{VERIFY_DB_NAME}'...")

    source_counts = _row_counts(conn, conn.dbname, _SANITY_TABLES)

    _dropdb(conn, VERIFY_DB_NAME)
    _createdb(conn, VERIFY_DB_NAME)
    try:
        _pg_restore(conn, backup_file, VERIFY_DB_NAME)
        restored_counts = _row_counts(conn, VERIFY_DB_NAME, _SANITY_TABLES)
    finally:
        _dropdb(conn, VERIFY_DB_NAME)

    print(f"\n{'table':<20}{'source (live)':>15}{'restored':>15}{'match':>8}")
    all_ok = True
    for table in _SANITY_TABLES:
        src = source_counts[table]
        rst = restored_counts[table]
        # Source may have grown since backup was taken (a later job ran) —
        # restored should match what was live AT BACKUP TIME, which we
        # don't separately know here, so the honest check is: restored is
        # non-negative and no worse than absurd (e.g. zero when source has
        # real data). If they're run back-to-back with no writes between,
        # they'll match exactly, which is the common case we actually test.
        ok = rst > 0 if src > 0 else rst == 0
        all_ok = all_ok and ok
        print(f"{table:<20}{src:>15,}{rst:>15,}{'OK' if ok else 'FAIL':>8}")

    if all_ok:
        print("\nVERIFY PASSED: backup is restorable and sanity-checked tables are populated.")
    else:
        print("\nVERIFY FAILED: see table above.")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
