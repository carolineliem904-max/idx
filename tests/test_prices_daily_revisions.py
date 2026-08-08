"""Tests for the widened prices_daily PK and its point-in-time read path.

This is the leakage guard spec principle #1 exists for. Before the PK was
widened to (ticker, date, source, ingested_at), prices_daily could not
physically hold more than one version of a trading day — these tests would
have been impossible to even set up, let alone pass.
"""
from __future__ import annotations

import datetime as dt

from sqlalchemy import text

from idx.db.models import PriceDaily, Security
from idx.db.queries import latest_price, price_as_of

TEST_TICKER = "ZZTEST"
TEST_SOURCE = "test"


def _seed_test_security(session):
    session.add(Security(ticker=TEST_TICKER, yahoo_symbol=f"{TEST_TICKER}.JK", is_active=False))
    session.flush()


def test_prices_daily_latest_returns_exactly_one_row_per_key(db_session):
    _seed_test_security(db_session)
    the_date = dt.date(2024, 1, 2)
    base = dt.datetime(2024, 1, 1, 9, 0, tzinfo=dt.timezone.utc)

    for i, close in enumerate([100, 101, 102]):
        db_session.add(
            PriceDaily(
                ticker=TEST_TICKER,
                date=the_date,
                source=TEST_SOURCE,
                ingested_at=base + dt.timedelta(hours=i),
                close_raw=close,
                open_raw=close,
                high_raw=close,
                low_raw=close,
            )
        )
    db_session.flush()

    count = db_session.execute(
        text(
            "SELECT count(*) FROM prices_daily_latest "
            "WHERE ticker = :t AND date = :d AND source = :s"
        ),
        {"t": TEST_TICKER, "d": the_date, "s": TEST_SOURCE},
    ).scalar_one()
    assert count == 1, "prices_daily_latest must collapse to one row per (ticker, date, source)"

    latest = latest_price(db_session, TEST_TICKER, the_date, TEST_SOURCE)
    assert latest is not None
    assert latest["close_raw"] == 102, "latest view must pick the highest ingested_at, not just any row"


def test_as_of_read_does_not_leak_a_future_revision(db_session):
    """The actual leakage guard: an as-of query strictly between the
    original ingest and a later correction must see the ORIGINAL value,
    never the correction. Seeing the correction early is exactly the
    failure mode spec principle #1 exists to prevent — a backtest that
    knows a value before it was actually knowable."""
    _seed_test_security(db_session)
    the_date = dt.date(2024, 1, 1)
    t1 = dt.datetime(2024, 6, 1, 9, 0, tzinfo=dt.timezone.utc)  # original ingest
    t2 = dt.datetime(2024, 9, 1, 9, 0, tzinfo=dt.timezone.utc)  # later correction

    db_session.add(
        PriceDaily(
            ticker=TEST_TICKER, date=the_date, source=TEST_SOURCE, ingested_at=t1,
            close_raw=100, open_raw=100, high_raw=100, low_raw=100,
        )
    )
    db_session.flush()
    db_session.add(
        PriceDaily(
            ticker=TEST_TICKER, date=the_date, source=TEST_SOURCE, ingested_at=t2,
            close_raw=105, open_raw=105, high_raw=105, low_raw=105,
        )
    )
    db_session.flush()

    before_anything = price_as_of(db_session, TEST_TICKER, the_date, TEST_SOURCE, t1 - dt.timedelta(days=1))
    assert before_anything is None

    between = price_as_of(db_session, TEST_TICKER, the_date, TEST_SOURCE, t1 + dt.timedelta(days=30))
    assert between is not None
    assert between["close_raw"] == 100, "must see the original value, not the not-yet-known correction"

    after = price_as_of(db_session, TEST_TICKER, the_date, TEST_SOURCE, t2 + dt.timedelta(days=1))
    assert after is not None
    assert after["close_raw"] == 105, "once the correction has actually happened, as-of should see it"

    # And the "current state" read (no as-of) should always show the latest.
    latest = latest_price(db_session, TEST_TICKER, the_date, TEST_SOURCE)
    assert latest["close_raw"] == 105
