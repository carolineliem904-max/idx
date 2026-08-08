"""Shared test fixtures.

`db_session` runs against the real local dev Postgres (no separate test DB
infra exists yet) but every test's writes live inside one transaction that
is always rolled back in teardown — nothing a test does is ever committed,
so this is safe to run against the same database Phase 0-1b data lives in.
"""
from __future__ import annotations

import pytest

from idx.db.session import get_sessionmaker


@pytest.fixture
def db_session():
    session = get_sessionmaker()()
    try:
        yield session
    finally:
        session.rollback()
        session.close()
