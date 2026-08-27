#!/usr/bin/env python3
"""Capture CoinGecko public crypto prices for the scheduler.

This repository is the collection producer. The separate ``book-crypto-data``
repository owns lake-first ingestion and its read-only API; this adapter only
captures the validated upstream response under this repository's ``data/``.
"""

from __future__ import annotations

import csv
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    import httpx
except ImportError as exc:  # pragma: no cover - requirements.txt supplies httpx
    raise RuntimeError("httpx is required for crypto price capture") from exc


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "exported"
COINGECKO_API = "https://api.coingecko.com/api/v3/simple/price"
DEFAULT_COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "binancecoin",
    "ripple",
    "cardano",
    "dogecoin",
    "polkadot",
    "avalanche-2",
    "chainlink",
]
DEFAULT_CURRENCIES = ["usd", "thb"]
MAX_COINS = 50
MAX_CURRENCIES = 10
PRICE_FIELDS = [
    "coin_id",
    "currency",
    "price",
    "change_24h_pct",
    "volume_24h",
    "market_cap",
    "updated_at",
]
HISTORY_FIELDS = [
    "date",
    "coin_id",
    "currency",
    "price",
    "change_24h_pct",
    "market_cap",
]
_COIN_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_CURRENCY_RE = re.compile(r"^[a-z]{3,5}$")


def _as_values(values: Iterable[str] | str | None, default: list[str]) -> list[str]:
    if values is None:
        values = default
    if isinstance(values, str):
        values = values.split(",")
    result: list[str] = []
    for value in values:
        normalized = str(value).strip().lower()
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def normalize_request(
    coins: Iterable[str] | str | None,
    currencies: Iterable[str] | str | None,
) -> tuple[list[str], list[str]]:
    """Normalize and bound provider request parameters."""

    normalized_coins = _as_values(coins, DEFAULT_COINS)
    normalized_currencies = _as_values(currencies, DEFAULT_CURRENCIES)
    if not normalized_coins or len(normalized_coins) > MAX_COINS:
        raise ValueError(f"coins must contain 1-{MAX_COINS} unique IDs")
    if not normalized_currencies or len(normalized_currencies) > MAX_CURRENCIES:
        raise ValueError(f"vs_currencies must contain 1-{MAX_CURRENCIES} unique values")
    if any(not _COIN_ID_RE.fullmatch(coin) for coin in normalized_coins):
        raise ValueError("coins contain an invalid CoinGecko ID")
    if any(not _CURRENCY_RE.fullmatch(currency) for currency in normalized_currencies):
        raise ValueError("vs_currencies contain an invalid currency code")
    return normalized_coins, normalized_currencies


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def validate_payload(
    payload: Any,
    coins: list[str],
    currencies: list[str],
) -> dict[str, dict[str, Any]]:
    """Fail closed on partial or malformed CoinGecko responses."""

    if not isinstance(payload, dict):
        raise ValueError("CoinGecko response must be a JSON object")
    normalized: dict[str, dict[str, Any]] = {}
    for coin in coins:
        info = payload.get(coin)
        if not isinstance(info, dict):
            raise ValueError(f"CoinGecko response is missing coin: {coin}")
        for currency in currencies:
            price = _finite_number(info.get(currency), f"{coin}.{currency}")
            if price <= 0:
                raise ValueError(f"{coin}.{currency} must be greater than zero")
            change_key = f"{currency}_24h_change"
            if info.get(change_key) is not None:
                _finite_number(info[change_key], f"{coin}.{change_key}")
        if info.get("last_updated_at") is not None:
            last_updated = info["last_updated_at"]
            if isinstance(last_updated, bool) or not isinstance(last_updated, int) or last_updated <= 0:
                raise ValueError(f"{coin}.last_updated_at must be a positive integer")
        normalized[coin] = info
    return normalized


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def price_rows(
    payload: dict[str, dict[str, Any]],
    currencies: list[str],
    observed_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for coin_id, info in payload.items():
        for currency in currencies:
            rows.append(
                {
                    "coin_id": coin_id,
                    "currency": currency,
                    "price": info[currency],
                    "change_24h_pct": round(float(info.get(f"{currency}_24h_change", 0) or 0), 2),
                    "volume_24h": info.get(f"{currency}_24h_vol", ""),
                    "market_cap": info.get(f"{currency}_market_cap", ""),
                    "updated_at": observed_at,
                }
            )
    return rows


def fetch_prices(coins: list[str], currencies: list[str]) -> tuple[bytes, dict[str, dict[str, Any]]]:
    """Fetch and validate one bounded public API response."""

    response = httpx.get(
        COINGECKO_API,
        params={
            "ids": ",".join(coins),
            "vs_currencies": ",".join(currencies),
            "include_24hr_change": "true",
            "include_24hr_vol": "true",
            "include_market_cap": "true",
            "include_last_updated_at": "true",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = validate_payload(response.json(), coins, currencies)
    raw = getattr(response, "content", b"") or json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return raw, payload


def write_raw(raw: bytes, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "crypto_prices_raw.json"
    path.write_bytes(raw)
    return path


def write_snapshot(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "crypto_prices.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PRICE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def append_history(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "crypto_history.csv"
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "date": row["updated_at"],
                    "coin_id": row["coin_id"],
                    "currency": row["currency"],
                    "price": row["price"],
                    "change_24h_pct": row["change_24h_pct"],
                    "market_cap": row["market_cap"],
                }
            )
    return path


def print_alerts(rows: list[dict[str, Any]], threshold: float) -> list[dict[str, Any]]:
    alerts = [row for row in rows if abs(float(row["change_24h_pct"])) >= threshold]
    for row in sorted(alerts, key=lambda item: abs(float(item["change_24h_pct"])), reverse=True):
        direction = "UP" if float(row["change_24h_pct"]) > 0 else "DOWN"
        print(
            f"  ALERT {direction}: {row['coin_id']} {row['currency']} "
            f"{row['change_24h_pct']:+.2f}%"
        )
    return alerts


class CryptoPriceScraper:
    """Scheduler adapter for bounded CoinGecko price capture."""

    def __init__(
        self,
        coins: Iterable[str] | str | None = None,
        vs_currencies: Iterable[str] | str | None = None,
        alert_threshold: float = 5.0,
        output_dir: str | Path | None = None,
        **_: Any,
    ) -> None:
        self.coins, self.currencies = normalize_request(coins, vs_currencies)
        self.alert_threshold = float(alert_threshold)
        if self.alert_threshold < 0:
            raise ValueError("alert_threshold must be non-negative")
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR

    async def run(self, **_: Any) -> list[dict[str, Any]]:
        raw, payload = fetch_prices(self.coins, self.currencies)
        observed_at = _utc_now()
        rows = price_rows(payload, self.currencies, observed_at)
        raw_path = write_raw(raw, self.output_dir)
        snapshot_path = write_snapshot(rows, self.output_dir)
        history_path = append_history(rows, self.output_dir)
        alerts = print_alerts(rows, self.alert_threshold)
        print(f"[crypto_prices] {len(rows)} rows -> {snapshot_path}")
        return [
            {
                "source": "crypto_prices",
                "count": len(rows),
                "alerts": len(alerts),
                "output": str(snapshot_path),
                "history": str(history_path),
                "raw": str(raw_path),
            }
        ]


if __name__ == "__main__":
    import asyncio

    asyncio.run(CryptoPriceScraper().run())
