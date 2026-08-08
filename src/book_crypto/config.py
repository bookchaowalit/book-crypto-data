"""Runtime configuration for book-crypto-data."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", str(PROJECT_ROOT / "data")))
FIXTURES_DIR = Path(os.environ.get("FIXTURES_DIR", str(PROJECT_ROOT / "fixtures")))

REPO_NAME = 'book-crypto-data'
DOMAIN = 'crypto'
SCHEMA_VERSION = 'crypto.v1'
RECORDS_FILE = 'crypto_prices.csv'
HISTORY_FILE = 'crypto_history.csv'
TRENDING_FILE = 'crypto_trending.csv'
LINEAGE_FILE = 'lake_lineage.json'
ID_FIELDS = ['coin_id', 'currency']
ID_SEP = ':'

# Lake-first contract (Bronze envelope schema_version stays "1").
# Product envelope schema_version remains SCHEMA_VERSION (crypto.v1).
LAKE_SOURCE = REPO_NAME
LAKE_DOMAIN = "market"
LAKE_DATASET_PRICES = "crypto_prices"
LAKE_DATASET_TRENDING = "crypto_trending"
LAKE_BRONZE_SCHEMA_VERSION = "1"
LAKE_PRIVACY_CLASS = "public"
LAKE_RETENTION_CLASS = "operational"
LAKE_LINEAGE_PATH = DATA_DIR / LINEAGE_FILE


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


FREE_ONLY = env_bool("FREE_ONLY", True)
ALLOW_PAID_PROVIDERS = env_bool("ALLOW_PAID_PROVIDERS", False)
ALLOW_EXTERNAL_WRITES = env_bool("ALLOW_EXTERNAL_WRITES", False)
ALLOW_REFRESH = env_bool("ALLOW_REFRESH", False)
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN", "")
API_HOST = os.environ.get("API_HOST", "127.0.0.1")
API_PORT = int(os.environ.get("API_PORT", "8101"))
REQUEST_TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "15"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))
MAX_RESPONSE_BYTES = int(os.environ.get("MAX_RESPONSE_BYTES", str(2_000_000)))
STALE_AFTER_HOURS = float(os.environ.get("STALE_AFTER_HOURS", "24"))
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "20"))

# Local CSV/JSON under data/ are projections after a successful lake write.
# Set SOLO_EMPIRE_DATA_LAKE_URI / DATA_LAKE_URI to override the default path.
DATA_LAKE_URI = os.environ.get(
    "SOLO_EMPIRE_DATA_LAKE_URI",
    os.environ.get("DATA_LAKE_URI", ""),
)
SOLO_EMPIRE_ROOT = os.environ.get("SOLO_EMPIRE_ROOT", "")

# API read path is feature-flagged. Bronze Parquet remains the default and the
# durable contract; Iceberg is an optional catalog-backed query pilot.
LAKE_READ_MODE = os.environ.get("LAKE_READ_MODE", "parquet").strip().lower()
LAKE_READ_FALLBACK = os.environ.get("LAKE_READ_FALLBACK", "error").strip().lower()

# Silver parity/serving pilot. ``bronze`` is the production-safe default;
# ``compare`` reads Silver for diagnostics but still serves Bronze; ``silver``
# serves Silver only after the parity check is known to pass.
SILVER_READ_MODE = os.environ.get("SILVER_READ_MODE", "bronze").strip().lower()
