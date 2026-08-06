"""IDX listed-company directory (spec §3.1 step 1: "Seed from IDX's
listed-company list").

This is the CURRENT, ACTIVE universe only — idx.co.id's
ListedCompany/GetCompanyProfiles endpoint has no delisted-company mode and
no Status flag that varies (verified empirically: all 962 rows return
Status=0). Seeding from this endpoint alone is survivorship-biased by
construction (spec §0 principle 4). The delisted tail is a separate problem
solved by jobs/harvest_universe_history.py, not this module.

The endpoint sits behind Cloudflare; curl_cffi's browser impersonation gets
through where plain `requests` gets a 403. Reference: github.com/nichsedge/idx-bei.
"""
from __future__ import annotations

import datetime as dt

import structlog
from curl_cffi import requests

log = structlog.get_logger()

BASE_URL = "https://www.idx.co.id/primary/ListedCompany/GetCompanyProfiles"
REFERER = "https://www.idx.co.id/id/perusahaan-tercatat/profil-perusahaan/"
REQUEST_TIMEOUT_SECONDS = 30


def fetch_company_profiles() -> list[dict]:
    """One call with a large page size returns the full directory (~962
    rows as of 2026-08) — IDX doesn't actually enforce the `length` cap as
    real pagination here, per the idx-bei reference implementation."""
    resp = requests.get(
        BASE_URL,
        params={"start": 0, "length": 9999},
        impersonate="chrome",
        headers={"Referer": REFERER, "accept": "application/json, text/plain, */*"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    payload = resp.json()
    rows = payload.get("data", [])
    log.info(
        "idx_company_profiles_fetched",
        records_total=payload.get("recordsTotal"),
        rows_returned=len(rows),
    )
    return [r for r in rows if r.get("EfekEmiten_Saham") is True]


def to_security_row(profile: dict) -> dict:
    """Map one GetCompanyProfiles record to `securities` columns.

    Sektor/SubSektor (IDX-IC classification) are used for sector/sub_industry
    rather than the older Industri/SubIndustri fields IDX also returns —
    IDX-IC is the classification the exchange has used since 2021.
    """
    ticker = profile["KodeEmiten"]
    listing_date = None
    if profile.get("TanggalPencatatan"):
        listing_date = dt.date.fromisoformat(profile["TanggalPencatatan"][:10])
    return {
        "ticker": ticker,
        "yahoo_symbol": f"{ticker}.JK",
        "name": profile.get("NamaEmiten"),
        "sector": profile.get("Sektor"),
        "sub_industry": profile.get("SubSektor"),
        "listing_date": listing_date,
        "delisting_date": None,
        "board": profile.get("PapanPencatatan"),
        "is_active": True,
    }
