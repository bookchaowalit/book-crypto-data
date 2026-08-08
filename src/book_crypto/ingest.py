#!/usr/bin/env python3
"""
Scrape cryptocurrency prices via CoinGecko API (free, no auth required).

Lake-first flow:
    CoinGecko API response
      → Object Storage landing/ (exact bytes)
      → Bronze Parquet + manifest
      → local CSV projection under data/ (optional operational view)

Local CSV files are projections only. They must not be treated as the durable
source of truth. Shared procedure:
  learning/platform-engineering/app-cli-data-lake-playbook.md

Usage:
    python -m book_crypto.ingest
    python -m book_crypto.ingest --coins bitcoin,ethereum,solana
    python -m book_crypto.ingest --alert-threshold 3
    python -m book_crypto.ingest --vs-currency thb,usd
    python -m book_crypto.ingest --data-lake-uri /path/to/data/lake
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

def _require_httpx():
    try:
        import httpx as _httpx
    except ImportError:
        print("ERROR: httpx required for live ingestion. Install: pip install httpx")
        raise SystemExit(1)
    return _httpx


class _HttpxProxy:
    def __getattr__(self, name):
        return getattr(_require_httpx(), name)


httpx = _HttpxProxy()

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data"

# Free-only policy / local store (API-compatible local data)
try:
    from . import config as _dp_config
    from .policy import evaluate_provider, require_provider, require_external_writes, external_writes_allowed
    from .store import seed_fixtures
    from . import lake as _lake
except ImportError:  # pragma: no cover
    _dp_config = None
    _lake = None

    def evaluate_provider(name):
        class D:
            allowed = True
            status = "free"
            reason = ""
        return D()

    def require_provider(name):
        return evaluate_provider(name)

    def require_external_writes(action):
        return None

    def external_writes_allowed():
        return False

    def seed_fixtures(data_dir=None):
        return None


COINGECKO_BASE = "https://api.coingecko.com/api/v3"

DEFAULT_COINS = [
    "bitcoin", "ethereum", "solana", "binancecoin", "ripple",
    "cardano", "dogecoin", "polkadot", "avalanche-2", "chainlink",
    "polygon-matic", "litecoin", "uniswap", "stellar", "cosmos",
]

DEFAULT_CURRENCIES = ["usd", "thb"]


def fetch_prices(coins: list, currencies: list) -> tuple[bytes, dict]:
    """Fetch current prices from CoinGecko. Returns (raw_bytes, parsed_json)."""
    require_provider("coingecko_public")
    ids_str = ",".join(coins)
    curr_str = ",".join(currencies)
    url = f"{COINGECKO_BASE}/simple/price"
    params = {
        "ids": ids_str,
        "vs_currencies": curr_str,
        "include_24hr_change": "true",
        "include_24hr_vol": "true",
        "include_market_cap": "true",
        "include_last_updated_at": "true",
    }
    resp = httpx.get(url, params=params, timeout=30)
    resp.raise_for_status()
    # Prefer exact wire bytes for landing/; fall back to re-encoded body.
    raw = getattr(resp, "content", None) or json.dumps(resp.json()).encode("utf-8")
    return raw, resp.json()


def fetch_trending() -> tuple[bytes, list]:
    """Fetch trending coins. Returns (raw_bytes, normalized list)."""
    require_provider("coingecko_public")
    url = f"{COINGECKO_BASE}/search/trending"
    resp = httpx.get(url, timeout=30)
    resp.raise_for_status()
    raw = getattr(resp, "content", None) or json.dumps(resp.json()).encode("utf-8")
    data = resp.json()
    trending = [
        {
            "id": coin["item"]["id"],
            "name": coin["item"]["name"],
            "symbol": coin["item"]["symbol"],
            "market_cap_rank": coin["item"]["market_cap_rank"],
            "score": coin["item"]["score"],
        }
        for coin in data.get("coins", [])
    ]
    return raw, trending


def _projection_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def price_rows_for_csv(data: dict, currencies: list) -> list[dict[str, Any]]:
    """Build CSV projection rows from CoinGecko simple/price payload."""
    now = _projection_timestamp()
    rows = []
    for coin_id, info in data.items():
        if not isinstance(info, dict):
            continue
        for curr in currencies:
            rows.append(
                {
                    "coin_id": coin_id,
                    "currency": curr,
                    "price": info.get(curr, ""),
                    "change_24h_pct": round(info.get(f"{curr}_24h_change", 0) or 0, 2),
                    "volume_24h": info.get(f"{curr}_24h_vol", ""),
                    "market_cap": info.get(f"{curr}_market_cap", ""),
                    "updated_at": now,
                }
            )
    return rows


def history_rows_for_csv(data: dict, currencies: list) -> list[dict[str, Any]]:
    now = _projection_timestamp()
    rows = []
    for coin_id, info in data.items():
        if not isinstance(info, dict):
            continue
        for curr in currencies:
            rows.append(
                {
                    "date": now,
                    "coin_id": coin_id,
                    "currency": curr,
                    "price": info.get(curr, ""),
                    "change_24h_pct": round(info.get(f"{curr}_24h_change", 0) or 0, 2),
                    "market_cap": info.get(f"{curr}_market_cap", ""),
                }
            )
    return rows


def project_prices_csv(data: dict, currencies: list, output_dir: Path) -> Path:
    """Write latest snapshot CSV projection (after lake ingest)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    prices_file = output_dir / "crypto_prices.csv"
    rows = price_rows_for_csv(data, currencies)
    fieldnames = [
        "coin_id",
        "currency",
        "price",
        "change_24h_pct",
        "volume_24h",
        "market_cap",
        "updated_at",
    ]
    with open(prices_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Projected {len(rows)} rows → {prices_file}")
    return prices_file


def project_history_csv(data: dict, currencies: list, output_dir: Path) -> Path:
    """Append daily history CSV projection (after lake ingest)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    history_file = output_dir / "crypto_history.csv"
    rows = history_rows_for_csv(data, currencies)
    file_exists = history_file.exists()
    fieldnames = ["date", "coin_id", "currency", "price", "change_24h_pct", "market_cap"]
    with open(history_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    print(f"  Projected +{len(rows)} history rows → {history_file}")
    return history_file


def project_trending_csv(trending: list, output_dir: Path) -> Path:
    """Write trending CSV projection (after lake ingest)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    trending_file = output_dir / "crypto_trending.csv"
    now = _projection_timestamp()
    fieldnames = ["date", "id", "name", "symbol", "market_cap_rank", "score"]
    with open(trending_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for coin in trending:
            writer.writerow({"date": now, **coin})
    print(f"  Projected {len(trending)} trending coins → {trending_file}")
    return trending_file


# Backward-compatible aliases used by older call sites / tests.
def save_prices(data: dict, currencies: list, output_dir: Path):
    return project_prices_csv(data, currencies, output_dir)


def append_history(data: dict, currencies: list, output_dir: Path):
    return project_history_csv(data, currencies, output_dir)


def save_trending(trending: list, output_dir: Path):
    return project_trending_csv(trending, output_dir)


def print_alerts(data: dict, threshold: float, currencies: list):
    """Print alerts for coins with significant 24h moves."""
    alerts = []
    for coin_id, info in data.items():
        if not isinstance(info, dict):
            continue
        for curr in currencies:
            change = info.get(f"{curr}_24h_change", 0) or 0
            if abs(change) >= threshold:
                direction = "UP" if change > 0 else "DOWN"
                price = info.get(curr, "N/A")
                alerts.append(
                    {
                        "coin": coin_id,
                        "currency": curr,
                        "price": price,
                        "change": round(change, 2),
                        "direction": direction,
                    }
                )

    if alerts:
        print(f"\n  ALERTS ({threshold}% threshold):")
        for a in sorted(alerts, key=lambda x: abs(x["change"]), reverse=True):
            emoji = "+" if a["direction"] == "UP" else "-"
            print(
                f"    {emoji} {a['coin'].upper()}: {a['price']} "
                f"{a['currency'].upper()} ({a['change']:+.2f}%)"
            )
    else:
        print(f"\n  No alerts (all moves < {threshold}%)")

    return alerts


def _lake_ingest_prices(
    raw: bytes,
    data: dict,
    currencies: list,
    *,
    data_lake_uri: Optional[str],
    output_dir: Path,
) -> dict[str, Any]:
    if _lake is None or _dp_config is None:
        raise RuntimeError("Lake adapter unavailable in this runtime")
    records = _lake.price_records_from_api(data, currencies)
    result = _lake.ingest_to_lake(
        raw=raw,
        records=records,
        dataset=_dp_config.LAKE_DATASET_PRICES,
        data_lake_uri=data_lake_uri,
        metadata={"endpoint": "simple/price", "currencies": currencies},
    )
    _lake.write_lineage(result, dataset=_dp_config.LAKE_DATASET_PRICES, data_dir=output_dir)
    print(
        f"  Lake prices: run_id={result.get('run_id')} "
        f"records={result.get('record_count')} bronze={result.get('bronze_key')}"
    )
    return result


def _lake_ingest_trending(
    raw: bytes,
    trending: list,
    *,
    data_lake_uri: Optional[str],
    output_dir: Path,
) -> dict[str, Any]:
    if _lake is None or _dp_config is None:
        raise RuntimeError("Lake adapter unavailable in this runtime")
    records = _lake.trending_records_from_api(trending)
    if not records:
        raise _lake.LakeIngestError("Trending payload produced zero records")
    result = _lake.ingest_to_lake(
        raw=raw,
        records=records,
        dataset=_dp_config.LAKE_DATASET_TRENDING,
        data_lake_uri=data_lake_uri,
        metadata={"endpoint": "search/trending"},
    )
    _lake.write_lineage(result, dataset=_dp_config.LAKE_DATASET_TRENDING, data_dir=output_dir)
    print(
        f"  Lake trending: run_id={result.get('run_id')} "
        f"records={result.get('record_count')} bronze={result.get('bronze_key')}"
    )
    return result


def run_live_ingest(
    *,
    coins: list[str],
    currencies: list[str],
    output_dir: Path,
    alert_threshold: float = 5.0,
    fetch_trending_flag: bool = True,
    data_lake_uri: Optional[str] = None,
    project_csv: bool = True,
) -> dict[str, Any]:
    """Live free-only ingest: lake first, then optional CSV projection."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Crypto Price Scraper (lake-first)")
    print(f"  Coins: {len(coins)} | Currencies: {currencies}")
    if data_lake_uri:
        print(f"  Data lake: {data_lake_uri}")
    elif _lake is not None:
        print(f"  Data lake: {_lake.default_data_lake_uri()}")

    print("  Fetching prices...")
    raw_prices, data = fetch_prices(coins, currencies)
    print(f"  Got {len(data)} coins")

    # Durable write before any local projection.
    lake_prices = _lake_ingest_prices(
        raw_prices,
        data,
        currencies,
        data_lake_uri=data_lake_uri,
        output_dir=output_dir,
    )

    if project_csv:
        project_prices_csv(data, currencies, output_dir)
        project_history_csv(data, currencies, output_dir)

    lake_trending = None
    if fetch_trending_flag:
        print("  Fetching trending...")
        try:
            raw_trending, trending = fetch_trending()
            lake_trending = _lake_ingest_trending(
                raw_trending,
                trending,
                data_lake_uri=data_lake_uri,
                output_dir=output_dir,
            )
            if project_csv:
                project_trending_csv(trending, output_dir)
        except Exception as e:  # noqa: BLE001 - trending is best-effort after prices
            print(f"  Trending failed: {e}")

    print_alerts(data, alert_threshold, currencies)
    print("\n  Done (lake durable; CSV is projection only).")
    return {
        "coins": len(data),
        "lake_prices": lake_prices,
        "lake_trending": lake_trending,
        "projected_csv": project_csv,
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scrape crypto prices via CoinGecko (lake-first; CSV is projection)"
    )
    parser.add_argument(
        "--coins",
        default=",".join(DEFAULT_COINS),
        help="Comma-separated coin IDs (default: top 15)",
    )
    parser.add_argument(
        "--vs-currencies",
        default=",".join(DEFAULT_CURRENCIES),
        help="Comma-separated fiat currencies (default: usd,thb)",
    )
    parser.add_argument(
        "--alert-threshold",
        type=float,
        default=5.0,
        help="Alert on 24h change >= this %% (default: 5)",
    )
    parser.add_argument(
        "--trending",
        action="store_true",
        default=True,
        help="Also fetch trending coins",
    )
    parser.add_argument(
        "--no-trending",
        action="store_true",
        help="Skip trending coins",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help="Local projection directory (default: repo data/)",
    )
    parser.add_argument(
        "--data-lake-uri",
        default=None,
        help="Object storage / local lake URI (default: SOLO_EMPIRE_DATA_LAKE_URI or monorepo data/lake)",
    )
    parser.add_argument(
        "--no-project-csv",
        action="store_true",
        help="Skip local CSV projection after lake write",
    )
    parser.add_argument(
        "--fixture",
        action="store_true",
        help="Load offline fixtures into data/ (no upstream network, no lake write)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry-run: use fixtures / skip upstream network",
    )
    args = parser.parse_args(argv)

    coins = [c.strip() for c in args.coins.split(",") if c.strip()]
    currencies = [c.strip() for c in args.vs_currencies.split(",") if c.strip()]
    output_dir = Path(args.output_dir)

    if getattr(args, "fixture", False) or getattr(args, "dry_run", False):
        out = Path(args.output_dir)
        out.mkdir(parents=True, exist_ok=True)
        if _dp_config is not None:
            _dp_config.DATA_DIR = out
        seed_fixtures(out)
        print(
            "["
            + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            + "] Fixture mode: seeded local projection under "
            + str(out)
        )
        print("  No upstream providers contacted; no lake write.")
        return 0

    try:
        run_live_ingest(
            coins=coins,
            currencies=currencies,
            output_dir=output_dir,
            alert_threshold=args.alert_threshold,
            fetch_trending_flag=bool(args.trending and not args.no_trending),
            data_lake_uri=args.data_lake_uri,
            project_csv=not args.no_project_csv,
        )
    except Exception as exc:  # noqa: BLE001
        # Lake/runtime failures must not leave a false success path.
        if _lake is not None and isinstance(
            exc, (_lake.LakeUnavailable, _lake.LakeIngestError)
        ):
            print(f"ERROR: lake ingest failed before projection: {exc}", file=sys.stderr)
            return 2
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


class CryptoPriceScraper:
    """Wrapper class for scheduler compatibility (lake-first)."""

    def __init__(self, coins=None, vs_currencies=None, alert_threshold=5.0, **kwargs):
        self.coins = coins or DEFAULT_COINS
        self.currencies = vs_currencies or DEFAULT_CURRENCIES
        self.alert_threshold = alert_threshold
        self.data_lake_uri = kwargs.get("data_lake_uri")
        self.output_dir = Path(kwargs.get("output_dir") or OUTPUT_DIR)

    async def run(self, **kwargs):
        result = run_live_ingest(
            coins=self.coins,
            currencies=self.currencies,
            output_dir=Path(kwargs.get("output_dir") or self.output_dir),
            alert_threshold=self.alert_threshold,
            fetch_trending_flag=True,
            data_lake_uri=kwargs.get("data_lake_uri", self.data_lake_uri),
            project_csv=True,
        )
        return [{"source": "crypto", "count": result["coins"], "lake": True}]


if __name__ == "__main__":
    raise SystemExit(main())
