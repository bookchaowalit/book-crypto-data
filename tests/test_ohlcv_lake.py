"""No-network tests for the Binance public OHLCV/funding lake capture."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from book_crypto import config, lake, ohlcv, policy

HOUR = 3_600_000
T0 = 1_700_000_000_000 - (1_700_000_000_000 % HOUR)


def kline(open_ms: int, close: float) -> list:
    return [open_ms, str(close - 1), str(close + 2), str(close - 3), str(close), "10.5",
            open_ms + HOUR - 1, "1000.0", 42, "5.0", "500.0", "0"]


class FakeBinance:
    """Serves `n_bars` hourly bars and 8h funding from T0, honoring pagination."""

    def __init__(self, n_bars: int, page_limit: int = 3):
        self.bars = [kline(T0 + i * HOUR, 100.0 + i) for i in range(n_bars)]
        self.funding = [
            {"symbol": "BTCUSDT", "fundingTime": T0 + i * 8 * HOUR, "fundingRate": "0.0001", "markPrice": "100"}
            for i in range(n_bars // 8 + 1)
        ]
        self.page_limit = page_limit
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, url: str, params: dict) -> bytes:
        self.calls.append((url, dict(params)))
        start, end = params["startTime"], params["endTime"]
        if "fundingRate" in url:
            rows = [r for r in self.funding if start <= r["fundingTime"] <= end]
        else:
            rows = [b for b in self.bars if start <= b[0] <= end]
        return json.dumps(rows[: min(self.page_limit, params["limit"])]).encode()


@unittest.skipUnless(lake.find_solo_empire_root() is not None, "shared data_lake adapter not found")
class OhlcvLakeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="ohlcv-lake-")
        self.uri = self.tmp.name

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, fake, now_ms, **kw):
        patches = [
            mock.patch.dict(ohlcv.MARKETS["futures"], {"page_limit": fake.page_limit}),
            mock.patch.object(ohlcv, "FUNDING_PAGE_LIMIT", fake.page_limit),
        ]
        for p in patches:
            p.start()
        try:
            return ohlcv.run_capture(
                ["BTCUSDT"], interval="1h", market="futures", days=1,
                data_lake_uri=self.uri, fetch=fake, now_ms=now_ms, **kw,
            )
        finally:
            for p in patches:
                p.stop()

    def test_provider_is_free_and_trading_api_blocked(self):
        self.assertTrue(policy.evaluate_provider(ohlcv.PROVIDER).allowed)
        self.assertFalse(policy.evaluate_provider("binance_trading_api").allowed)

    def test_drops_forming_bar(self):
        records = ohlcv.kline_records(
            [kline(T0, 100.0), kline(T0 + HOUR, 101.0)],
            venue="binance_futures", symbol="BTCUSDT", interval="1h", now_ms=T0 + HOUR + 10,
        )
        self.assertEqual([r["open_time_ms"] for r in records], [T0])
        self.assertEqual(records[0]["id"], f"binance_futures:BTCUSDT:1h:{T0}")
        self.assertEqual(records[0]["event_time"], ohlcv.iso_utc(T0))

    def test_backfill_paginates_then_resumes_without_duplicates(self):
        fake = FakeBinance(n_bars=10)
        now = T0 + 10 * HOUR  # all 10 bars closed
        first = self._run(fake, now_ms=now)
        self.assertEqual(first["series"][0]["bars_written"], 10)
        self.assertGreaterEqual(first["series"][0]["pages"], 4)  # 3 per page

        rows = lake.read_bronze_rows(config.LAKE_DATASET_OHLCV, data_lake_uri=self.uri)
        ids = {r["source_record_id"] for r in rows}
        self.assertEqual(len(ids), 10)
        self.assertTrue(all(r["privacy_class"] == "public" for r in rows))
        payload = json.loads(rows[0]["payload_json"])
        self.assertEqual(payload["venue"], "binance_futures")

        # New bars appear; a second run starts after the latest stored bar.
        fake.bars.extend(kline(T0 + i * HOUR, 100.0 + i) for i in range(10, 13))
        fake.calls.clear()
        second = self._run(fake, now_ms=T0 + 13 * HOUR, funding=False)
        self.assertEqual(second["series"][0]["bars_written"], 3)
        first_call = fake.calls[0][1]
        self.assertEqual(first_call["startTime"], T0 + 10 * HOUR)
        rows = lake.read_bronze_rows(config.LAKE_DATASET_OHLCV, data_lake_uri=self.uri)
        self.assertEqual(len({r["source_record_id"] for r in rows}), 13)

    def test_funding_is_captured(self):
        fake = FakeBinance(n_bars=24)
        result = self._run(fake, now_ms=T0 + 24 * HOUR)
        self.assertGreaterEqual(result["series"][0]["funding"]["funding_written"], 3)
        rows = lake.read_bronze_rows(config.LAKE_DATASET_FUNDING, data_lake_uri=self.uri)
        self.assertTrue(all(r["source_record_id"].startswith("binance_futures:BTCUSDT:funding:") for r in rows))

    def test_no_signed_or_order_endpoints(self):
        source = Path(ohlcv.__file__).read_text(encoding="utf-8")
        for forbidden in ("X-MBX-APIKEY", "/order", "signature", "/account"):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
