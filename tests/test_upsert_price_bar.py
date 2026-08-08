"""upsert_price_bar must never write a new row for a value that hasn't
actually changed. jobs/daily.py re-fetches a rolling 7-day window every
run — if this regresses, every daily run would silently start doubling
(then tripling, ...) prices_daily forever, exactly as flagged when the PK
was widened."""
from __future__ import annotations

import datetime as dt

from sqlalchemy import text

from idx.db.models import Security
from idx.db.upserts import upsert_price_bar

TEST_TICKER = "ZZUPSERT"
TEST_SOURCE = "test"


def _seed_test_security(session):
    session.add(Security(ticker=TEST_TICKER, yahoo_symbol=f"{TEST_TICKER}.JK", is_active=False))
    session.flush()


def _row_count(session, date):
    return session.execute(
        text(
            "SELECT count(*) FROM prices_daily WHERE ticker = :t AND date = :d AND source = :s"
        ),
        {"t": TEST_TICKER, "d": date, "s": TEST_SOURCE},
    ).scalar_one()


def _base_values(date, close):
    return {
        "ticker": TEST_TICKER,
        "date": date,
        "source": TEST_SOURCE,
        "open_raw": close,
        "high_raw": close,
        "low_raw": close,
        "close_raw": close,
        "close_adj": close,
        "volume": 1000,
        "value_traded": 1000 * close,
        "frequency": 5,
    }


def test_first_write_is_new(db_session):
    _seed_test_security(db_session)
    the_date = dt.date(2024, 3, 1)
    outcome = upsert_price_bar(db_session, _base_values(the_date, 100))
    assert outcome == "new"
    assert _row_count(db_session, the_date) == 1


def test_identical_refetch_is_a_no_op(db_session):
    _seed_test_security(db_session)
    the_date = dt.date(2024, 3, 2)
    upsert_price_bar(db_session, _base_values(the_date, 100))
    assert _row_count(db_session, the_date) == 1

    # Simulate daily.py re-fetching the same rolling-window day again.
    outcome = upsert_price_bar(db_session, _base_values(the_date, 100))
    assert outcome == "unchanged"
    assert _row_count(db_session, the_date) == 1, "an unchanged re-fetch must not add a row"


def test_repeated_identical_refetches_stay_at_one_row(db_session):
    """The exact scenario that would otherwise silently double the table:
    a rolling window re-touching the same days on every run."""
    _seed_test_security(db_session)
    the_date = dt.date(2024, 3, 3)
    for _ in range(7):  # spec §3.2's rolling 7-day window, applied to one day
        upsert_price_bar(db_session, _base_values(the_date, 100))
    assert _row_count(db_session, the_date) == 1


def test_genuine_change_writes_a_new_row(db_session):
    _seed_test_security(db_session)
    the_date = dt.date(2024, 3, 4)
    upsert_price_bar(db_session, _base_values(the_date, 100))
    outcome = upsert_price_bar(db_session, _base_values(the_date, 105))
    assert outcome == "revised"
    assert _row_count(db_session, the_date) == 2
