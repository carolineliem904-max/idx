"""ORM models mirroring spec §2 exactly. All timestamps are UTC (spec §8).

Schema is defined in full here even though Phase 0 only populates
`securities` and `prices_daily` for one ticker — later phases populate the
rest without further migrations changing shape.
"""
from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# --------------------------------------------------------------------------
# 2.1 Core market data
# --------------------------------------------------------------------------


class Security(Base):
    __tablename__ = "securities"

    ticker: Mapped[str] = mapped_column(Text, primary_key=True)  # 'AMMN', no .JK
    yahoo_symbol: Mapped[str] = mapped_column(Text, nullable=False)  # 'AMMN.JK'
    name: Mapped[str | None] = mapped_column(Text)
    sector: Mapped[str | None] = mapped_column(Text)
    sub_industry: Mapped[str | None] = mapped_column(Text)
    listing_date: Mapped[dt.date | None] = mapped_column(Date)
    delisting_date: Mapped[dt.date | None] = mapped_column(Date)  # null = still listed
    board: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PriceDaily(Base):
    __tablename__ = "prices_daily"

    # PK is (ticker, date, source, ingested_at) — NOT (ticker, date, source)
    # as spec §2.1 originally read. That version could hold at most one row
    # per key, which is structurally incompatible with spec §3.2's revision
    # behavior ("write a new row with a fresh ingested_at rather than
    # mutating... history is preserved for audit") — the schema simply
    # couldn't hold two versions of the same trading day. Phase 0/1's
    # bootstrap upserts got away with overwriting in place because a
    # one-time backfill has no prior observation to preserve; jobs/daily.py
    # (Phase 2) is the first job where this actually matters, and getting
    # it wrong is a real point-in-time leakage bug (spec principle #1): an
    # as-of read between an original ingest and a later correction would
    # silently see the future-corrected value. Widened here; see the
    # `prices_daily_latest` view (created in the same migration, not an
    # ORM model — query it directly) for "current state" reads, and
    # db/queries.py for the as-of-aware read every point-in-time consumer
    # (including the Phase 4 feature builder) must use instead.
    ticker: Mapped[str] = mapped_column(Text, ForeignKey("securities.ticker"), primary_key=True)
    date: Mapped[dt.date] = mapped_column(Date, primary_key=True, nullable=False)
    source: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)  # 'yahoo' | 'idx' | 'sectors'
    ingested_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, nullable=False, server_default=func.now()
    )
    open_raw: Mapped[float | None] = mapped_column(Numeric(18, 4))
    high_raw: Mapped[float | None] = mapped_column(Numeric(18, 4))
    low_raw: Mapped[float | None] = mapped_column(Numeric(18, 4))
    close_raw: Mapped[float | None] = mapped_column(Numeric(18, 4))
    close_adj: Mapped[float | None] = mapped_column(Numeric(18, 6))
    volume: Mapped[int | None] = mapped_column(BigInteger)  # shares, not lots
    value_traded: Mapped[float | None] = mapped_column(Numeric(20, 2))  # IDR
    frequency: Mapped[int | None] = mapped_column(Integer)


class CorporateAction(Base):
    __tablename__ = "corporate_actions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(Text, ForeignKey("securities.ticker"))
    ex_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    action_type: Mapped[str] = mapped_column(Text, nullable=False)  # split | reverse_split | dividend | rights | bonus | warrant
    ratio_from: Mapped[float | None] = mapped_column(Numeric)
    ratio_to: Mapped[float | None] = mapped_column(Numeric)
    cash_amount: Mapped[float | None] = mapped_column(Numeric)
    source: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TradingCalendar(Base):
    __tablename__ = "trading_calendar"

    date: Mapped[dt.date] = mapped_column(Date, primary_key=True)
    is_trading_day: Mapped[bool] = mapped_column(Boolean, nullable=False)
    note: Mapped[str | None] = mapped_column(Text)  # 'Idul Fitri', 'cuti bersama', half-day, etc.


# --------------------------------------------------------------------------
# 2.2 Annotation layer
# --------------------------------------------------------------------------


class Entity(Base):
    __tablename__ = "entities"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)  # ultimate_owner | operator | broker | fund | family_group
    broker_code: Mapped[str | None] = mapped_column(Text)  # only for entity_type='broker'
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class SecurityControl(Base):
    __tablename__ = "security_control"
    __table_args__ = (
        UniqueConstraint("ticker", "entity_id", "role", "valid_from"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(Text, ForeignKey("securities.ticker"))
    entity_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("entities.id"))
    role: Mapped[str] = mapped_column(Text, nullable=False)  # ultimate_owner | nominee | suspected_operator | active_accumulator
    valid_from: Mapped[dt.date] = mapped_column(Date, nullable=False)
    valid_to: Mapped[dt.date | None] = mapped_column(Date)
    confidence: Mapped[int] = mapped_column(SmallInteger, nullable=False)  # 1..5
    evidence: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )  # WHEN YOU RECORDED IT


class Rumor(Base):
    __tablename__ = "rumors"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(Text, ForeignKey("securities.ticker"))
    heard_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)  # when it reached YOU
    source_channel: Mapped[str | None] = mapped_column(Text)  # telegram_grup_a | broker_call | twitter | private
    source_reliability: Mapped[int | None] = mapped_column(SmallInteger)  # 1..5
    direction: Mapped[str | None] = mapped_column(Text)  # bullish | bearish | neutral
    specificity: Mapped[str | None] = mapped_column(Text)  # vague | targeted_price | named_catalyst | dated_event
    body: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("entities.id"))
    resolved_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(Text)  # true | false | partial | unresolved
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class BrokerFlowDaily(Base):
    """Phase 3, from Sectors or IDX broker summary."""

    __tablename__ = "broker_flow_daily"

    ticker: Mapped[str] = mapped_column(Text, ForeignKey("securities.ticker"), primary_key=True)
    date: Mapped[dt.date] = mapped_column(Date, primary_key=True, nullable=False)
    broker_code: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    investor_type: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)  # domestic | foreign | all
    source: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    buy_volume: Mapped[int | None] = mapped_column(BigInteger)
    sell_volume: Mapped[int | None] = mapped_column(BigInteger)
    buy_value: Mapped[float | None] = mapped_column(Numeric(20, 2))
    sell_value: Mapped[float | None] = mapped_column(Numeric(20, 2))
    ingested_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# --------------------------------------------------------------------------
# 2.3 Audit
# --------------------------------------------------------------------------


class IngestRun(Base):
    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_name: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str | None] = mapped_column(Text)  # success | partial | failed
    rows_written: Mapped[int | None] = mapped_column(Integer)
    tickers_attempted: Mapped[int | None] = mapped_column(Integer)
    tickers_failed: Mapped[int | None] = mapped_column(Integer)
    error_summary: Mapped[str | None] = mapped_column(Text)


# --------------------------------------------------------------------------
# 2.4 Known issues — validator suppression (Phase 2, not in original spec)
# --------------------------------------------------------------------------


class KnownIssue(Base):
    """Suppression list jobs/validate.py consults. A suppressed finding is
    still REPORTED (in a separate "known, suppressed" section) — this table
    exists to stop the alert channel from crying wolf on already-understood
    data quirks, not to hide them. Must exist before real alerting does, or
    the channel gets ignored within a week and a genuine failure gets missed
    alongside it."""

    __tablename__ = "known_issues"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    scope: Mapped[str] = mapped_column(Text, nullable=False)  # ticker | date_range | both
    ticker: Mapped[str | None] = mapped_column(Text, ForeignKey("securities.ticker"))
    date_start: Mapped[dt.date | None] = mapped_column(Date)
    date_end: Mapped[dt.date | None] = mapped_column(Date)
    check_name: Mapped[str] = mapped_column(Text, nullable=False)  # matches a validate.py check
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    review_by: Mapped[dt.date | None] = mapped_column(Date)  # null = no review needed (permanent/historical fact)


# --------------------------------------------------------------------------
# 2.5 Cross-source reconciliation (Phase 2, not in original spec)
# --------------------------------------------------------------------------


class PriceDiscrepancy(Base):
    """Permanent canary for the next 2007-style upstream defect, and — per
    instruction — systematic per-ticker disagreement usually means an
    unhandled corporate action, which is exactly the leakage that ruins
    backtests quietly. Upserted per (ticker, date): re-checking the same
    day refreshes the row rather than duplicating it."""

    __tablename__ = "price_discrepancies"
    __table_args__ = (UniqueConstraint("ticker", "date"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(Text, ForeignKey("securities.ticker"), nullable=False)
    date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    yahoo_close: Mapped[float | None] = mapped_column(Numeric(18, 4))
    idx_close: Mapped[float | None] = mapped_column(Numeric(18, 4))
    diff_abs: Mapped[float | None] = mapped_column(Numeric(18, 4))
    diff_ticks: Mapped[float | None] = mapped_column(Numeric(10, 2))  # diff_abs / IDX tick size at that price
    detected_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    note: Mapped[str | None] = mapped_column(Text)
