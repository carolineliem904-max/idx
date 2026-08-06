"""Central config. Secrets come from environment variables only (see spec §8)."""
from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )
    return url
