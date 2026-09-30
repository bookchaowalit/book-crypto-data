#!/usr/bin/env python3
"""Lake-first OHLCV and funding-rate capture from Binance public market data.

Flow (one landing object per upstream page, exact bytes):
    Binance public klines / fundingRate
      -> landing/source=book-crypto-data/provider=binance_public_market_data/...
      -> bronze/domain=market/dataset=crypto_ohlcv | crypto_funding
      -> control/manifests/...

Safety:
- Keyless public endpoints only. No API key, request signing, account, or trade
  endpoint exists in this module (``binance_trading_api`` stays blocked).
- Only CLOSED bars are written; the still-forming bar is dropped so Bronze
  never contains a bar whose values can change later.
- Incremental by default: resumes after the latest bar already in Bronze.

Usage:
    python -m book_crypto.ohlcv --symbols BTCUSDT,ETHUSDT --interval 1h --days 730
    python -m book_crypto.ohlcv --symbols BTCUSDT --no-funding --json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from . import config
from . import lake as _lake
from .policy import require_provider

PROVIDER = "binance_public_market_data"
MARKETS = {
    "futures": {
        "venue": "binance_futures",
        "klines_url": "https://fapi.binance.com/fapi/v1/klines",
        "page_limit": 1500,
    },
    "spot": {
        "venue": "binance_spot",
        "klines_url": "https://api.binance.com/api/v3/klines",
        "page_limit": 1000,
    },
}
FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
FUNDING_PAGE_LIMIT = 1000
INTERVAL_MS = {
    "15m": 15 * 60_000,
    "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}
MAX_PAGES_PER_SERIES = 200

# fetch(url, params) -> exact response bytes
Fetcher = Callable[[str, dict[str, Any]], bytes]


def iso_utc(ms: int) -> str:
    return (
        datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_iso_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def http_fetch(url: str, params: dict[str, Any]) -> bytes:
    """Default fetcher: keyless GET with bounded retries on 429/timeouts."""
    require_provider(PROVIDER)
    import httpx

    last_error: Exception | None = None
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            resp = httpx.get(url, params=params, timeout=config.REQUEST_TIMEOUT_SECONDS)
        except httpx.TimeoutException as exc:
            last_error = exc
            time.sleep(min(2 ** attempt, 8))
            continue
        if resp.status_code in (418, 429):
            last_error = RuntimeError(f"rate limited (HTTP {resp.status_code})")
            time.sleep(min(2 ** (attempt + 1), 30))
            continue
        resp.raise_for_status()
        if len(resp.content) > config.MAX_RESPONSE_BYTES:
            raise RuntimeError("Upstream response exceeded size limit")
        return resp.content
    raise RuntimeError(f"Binance public request failed: {last_error}")


def kline_records(
    payload: list[list[Any]],
    *,
    venue: str,
    symbol: str,
    interval: str,
    now_ms: int,
) -> list[dict[str, Any]]:
    """Normalize a klines payload into Bronze records, keeping closed bars only."""
    records = []
    for row in payload:
        open_ms, close_ms = int(row[0]), int(row[6])
        if close_ms >= now_ms:
            continue  # bar still forming
        records.append(
            {
                "id": f"{venue}:{symbol}:{interval}:{open_ms}",
                "venue": venue,
                "symbol": symbol,
                "interval": interval,
                "open_time_ms": open_ms,
                "close_time_ms": close_ms,
                "open": str(row[1]),
                "high": str(row[2]),
                "low": str(row[3]),
                "close": str(row[4]),
                "volume": str(row[5]),
                "quote_volume": str(row[7]),
                "trades": int(row[8]),
                "event_time": iso_utc(open_ms),
            }
        )
    return records


def funding_records(payload: list[dict[str, Any]], *, symbol: str) -> list[dict[str, Any]]:
    records = []
    for row in payload:
        t = int(row["fundingTime"])
        records.append(
            {
                "id": f"binance_futures:{symbol}:funding:{t}",
                "venue": "binance_futures",
                "symbol": symbol,
                "funding_time_ms": t,
                "funding_rate": str(row["fundingRate"]),
                "mark_price": str(row.get("markPrice", "")),
                "event_time": iso_utc(t),
            }
        )
    return records


def latest_event_ms(dataset: str, id_prefix: str, *, data_lake_uri: str) -> Optional[int]:
    """Return the newest event time already in Bronze for one series."""
    safe_prefix = id_prefix.replace("'", "''")
    try:
        rows = _lake.read_bronze_rows(
            dataset,
            data_lake_uri=data_lake_uri,
            sql=(
                "SELECT max(event_time) AS latest FROM lake_table "
                f"WHERE source_record_id LIKE '{safe_prefix}%'"
            ),
        )
    except Exception:  # noqa: BLE001 - empty/missing dataset means full backfill
        return None
    latest = rows[0].get("latest") if rows else None
    return parse_iso_ms(str(latest)) if latest else None


def _ingest(raw: bytes, records: list[dict[str, Any]], dataset: str, uri: str, meta: dict) -> dict:
    return _lake.ingest_to_lake(
        raw=raw,
        records=records,
        dataset=dataset,
        data_lake_uri=uri,
        metadata=meta,
        provider=PROVIDER,
    )


def capture_klines(
    symbol: str,
    *,
    interval: str,
    market: str,
    start_ms: int,
    end_ms: int,
    data_lake_uri: str,
    fetch: Fetcher = http_fetch,
    now_ms: Optional[int] = None,
) -> dict[str, Any]:
    spec = MARKETS[market]
    step = INTERVAL_MS[interval]
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    cursor, pages, written, runs = start_ms, 0, 0, []
    while cursor < end_ms and pages < MAX_PAGES_PER_SERIES:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": spec["page_limit"],
        }
        raw = fetch(spec["klines_url"], params)
        pages += 1
        payload = json.loads(raw)
        if not payload:
            break
        records = kline_records(
            payload, venue=spec["venue"], symbol=symbol, interval=interval, now_ms=now_ms
        )
        if records:
            result = _ingest(
                raw,
                records,
                config.LAKE_DATASET_OHLCV,
                data_lake_uri,
                {"symbol": symbol, "interval": interval, "market": market},
            )
            written += len(records)
            runs.append(result.get("ingest_run_id") or result.get("batch_id") or "")
        last_open = int(payload[-1][0])
        if len(payload) < spec["page_limit"] or not records:
            break
        cursor = last_open + step
    return {"symbol": symbol, "interval": interval, "market": market, "pages": pages, "bars_written": written}


def capture_funding(
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
    data_lake_uri: str,
    fetch: Fetcher = http_fetch,
) -> dict[str, Any]:
    cursor, pages, written = start_ms, 0, 0
    while cursor < end_ms and pages < MAX_PAGES_PER_SERIES:
        params = {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": FUNDING_PAGE_LIMIT}
        raw = fetch(FUNDING_URL, params)
        pages += 1
        payload = json.loads(raw)
        if not payload:
            break
        records = funding_records(payload, symbol=symbol)
        _ingest(raw, records, config.LAKE_DATASET_FUNDING, data_lake_uri, {"symbol": symbol})
        written += len(records)
        if len(payload) < FUNDING_PAGE_LIMIT:
            break
        cursor = int(payload[-1]["fundingTime"]) + 1
    return {"symbol": symbol, "pages": pages, "funding_written": written}


def run_capture(
    symbols: list[str],
    *,
    interval: str = "1h",
    market: str = "futures",
    days: int = 730,
    funding: bool = True,
    data_lake_uri: str = "",
    fetch: Fetcher = http_fetch,
    now_ms: Optional[int] = None,
) -> dict[str, Any]:
    if interval not in INTERVAL_MS:
        raise ValueError(f"unsupported interval {interval!r}")
    if market not in MARKETS:
        raise ValueError(f"unsupported market {market!r}")
    require_provider(PROVIDER)
    uri = data_lake_uri or _lake.default_data_lake_uri()
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    backfill_start = now_ms - days * 86_400_000
    venue = MARKETS[market]["venue"]
    series = []
    for symbol in symbols:
        symbol = symbol.strip().upper()
        if not symbol:
            continue
        latest = latest_event_ms(config.LAKE_DATASET_OHLCV, f"{venue}:{symbol}:{interval}:", data_lake_uri=uri)
        start = latest + INTERVAL_MS[interval] if latest is not None else backfill_start
        item = capture_klines(
            symbol, interval=interval, market=market, start_ms=start, end_ms=now_ms,
            data_lake_uri=uri, fetch=fetch, now_ms=now_ms,
        )
        item["resumed_from"] = iso_utc(start)
        if funding and market == "futures":
            latest_f = latest_event_ms(config.LAKE_DATASET_FUNDING, f"binance_futures:{symbol}:funding:", data_lake_uri=uri)
            f_start = latest_f + 1 if latest_f is not None else backfill_start
            item["funding"] = capture_funding(symbol, start_ms=f_start, end_ms=now_ms, data_lake_uri=uri, fetch=fetch)
        series.append(item)
    return {
        "status": "success",
        "provider": PROVIDER,
        "data_lake_uri": uri,
        "datasets": [config.LAKE_DATASET_OHLCV, config.LAKE_DATASET_FUNDING],
        "series": series,
    }


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    ap.add_argument("--interval", default="1h", choices=sorted(INTERVAL_MS))
    ap.add_argument("--market", default="futures", choices=sorted(MARKETS))
    ap.add_argument("--days", type=int, default=730, help="backfill depth when a series is empty")
    ap.add_argument("--no-funding", action="store_true")
    ap.add_argument("--data-lake-uri", default="")
    ap.add_argument("--json", action="store_true", dest="as_json")
    args = ap.parse_args(argv)
    try:
        result = run_capture(
            args.symbols.split(","),
            interval=args.interval,
            market=args.market,
            days=args.days,
            funding=not args.no_funding,
            data_lake_uri=args.data_lake_uri,
        )
    except Exception as exc:  # noqa: BLE001 - fail closed with a bounded message
        payload = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        print(json.dumps(payload) if args.as_json else f"OHLCV capture FAILED: {payload['error']}", file=sys.stderr)
        return 2
    if args.as_json:
        print(json.dumps(result, sort_keys=True))
    else:
        for item in result["series"]:
            funding_note = item.get("funding", {}).get("funding_written", 0)
            print(
                f"{item['symbol']} {item['interval']} {item['market']}: +{item['bars_written']} bars "
                f"(+{funding_note} funding) from {item['resumed_from']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
