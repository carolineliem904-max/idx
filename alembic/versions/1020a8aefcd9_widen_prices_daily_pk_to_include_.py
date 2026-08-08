"""widen prices_daily PK to include ingested_at

Resolves a genuine contradiction between spec §2.1's schema (PK on
(ticker, date, source), holding at most one row per key) and spec §3.2's
daily-job behavior ("write a new row with a fresh ingested_at rather than
mutating... history is preserved for audit"). The old PK could not
physically hold two versions of the same trading day, so revision history
was structurally impossible — a real point-in-time leakage risk (spec
principle #1), not a cosmetic one. Autogenerate produced an empty
upgrade/downgrade here (PK-only changes on an existing column aren't
reliably diffed), so this migration is hand-written.

`prices_daily_latest` is the "current state" read path spec §3.2 itself
names ("the latest wins in views") — every non-audit consumer should query
this view, not the base table, unless it specifically wants full revision
history.

Revision ID: 1020a8aefcd9
Revises: 22955e9aa3a2
Create Date: 2026-08-08 21:02:49.410690

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '1020a8aefcd9'
down_revision: Union[str, Sequence[str], None] = '22955e9aa3a2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE prices_daily DROP CONSTRAINT prices_daily_pkey")
    op.execute(
        "ALTER TABLE prices_daily ADD CONSTRAINT prices_daily_pkey "
        "PRIMARY KEY (ticker, date, source, ingested_at)"
    )
    op.execute(
        """
        CREATE VIEW prices_daily_latest AS
        SELECT DISTINCT ON (ticker, date, source) *
        FROM prices_daily
        ORDER BY ticker, date, source, ingested_at DESC
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS prices_daily_latest")
    # Lossy by necessity: if any (ticker, date, source) has more than one
    # ingested_at version at this point, the old narrower PK cannot hold
    # all of them. Keep only the latest version per key before narrowing.
    op.execute(
        """
        DELETE FROM prices_daily p
        USING prices_daily p2
        WHERE p.ticker = p2.ticker AND p.date = p2.date AND p.source = p2.source
          AND p.ingested_at < p2.ingested_at
        """
    )
    op.execute("ALTER TABLE prices_daily DROP CONSTRAINT prices_daily_pkey")
    op.execute(
        "ALTER TABLE prices_daily ADD CONSTRAINT prices_daily_pkey "
        "PRIMARY KEY (ticker, date, source)"
    )
