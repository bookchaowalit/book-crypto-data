"""Data-quality guards: NaN/inf/negatives/duplicates never reach Bronze or CSV."""
from __future__ import annotations

import contextlib
import csv
import io
import json
import math
import tempfile
import unittest
from pathlib import Path

from book_crypto import ingest, lake, ohlcv, quality
from book_crypto.fsutil import atomic_append_csv, atomic_write_csv

EVENT_TIME = "2026-08-01T12:00:00Z"
HOUR = 3_600_000
T0 = 1_700_000_000_000 - (1_700_000_000_000 % HOUR)

# json.loads accepts NaN/Infinity literals, so upstream can hand us these.
DIRTY_API = json.loads(
    """{
  "bitcoin": {"usd": 50000.0, "usd_24h_change": NaN, "usd_24h_vol": -5,
              "usd_market_cap": Infinity, "thb": NaN, "last_updated_at": 1720000000},
  "ethereum": {"usd": -1, "thb": "108000"},
  "dogecoin": {"usd": "not-a-number"},
  "solana": {"thb": 5000},
  "broken": [1, 2, 3]
}"""
)


def _kline(open_ms: int, o="100", h="103", lo="97", c="101", vol="10.5", trades=42) -> list:
    return [open_ms, o, h, lo, c, vol, open_ms + HOUR - 1, "1000.0", trades, "5.0", "500.0", "0"]


class QualityHelperTests(unittest.TestCase):
    def test_finite_number(self):
        self.assertEqual(quality.finite_number("1.5"), 1.5)
        self.assertEqual(quality.finite_number(0), 0.0)
        for bad in (None, "", "  ", "abc", True, float("nan"), float("inf"), "-inf", "NaN", [1]):
            self.assertIsNone(quality.finite_number(bad), bad)

    def test_non_negative_number(self):
        self.assertEqual(quality.non_negative_number("0"), 0.0)
        self.assertIsNone(quality.non_negative_number(-0.01))

    def test_dedupe_keeps_first(self):
        kept, dropped = quality.dedupe_by_key([{"id": "a", "v": 1}, {"id": "a", "v": 2}, {"id": "b"}])
        self.assertEqual([r.get("v") for r in kept], [1, None])
        self.assertEqual(dropped, [{"id": "a", "v": 2}])


class PriceRecordQualityTests(unittest.TestCase):
    def test_invalid_prices_rejected_and_optional_fields_blanked(self):
        records, rejected = lake.price_records_with_report(
            DIRTY_API, ["usd", "thb"], event_time=EVENT_TIME
        )
        by_id = {r["id"]: r for r in records}
        self.assertEqual(sorted(by_id), ["bitcoin:usd", "ethereum:thb", "solana:thb"])
        btc = by_id["bitcoin:usd"]
        self.assertEqual(btc["change_24h_pct"], 0.0)
        self.assertEqual(btc["volume_24h"], "")
        self.assertEqual(btc["market_cap"], "")
        for record in records:
            for value in record.values():
                if isinstance(value, float):
                    self.assertTrue(math.isfinite(value), record)
        self.assertEqual(
            quality.summarize_rejections(rejected),
            {"invalid_price": 5, "not_an_object": 1},
        )

    def test_duplicate_currency_is_rejected_once(self):
        api = {"bitcoin": {"usd": 1.0}}
        records, rejected = lake.price_records_with_report(api, ["usd", "usd"], event_time=EVENT_TIME)
        self.assertEqual([r["id"] for r in records], ["bitcoin:usd"])
        self.assertEqual(rejected, [{"id": "bitcoin:usd", "reason": "duplicate_id"}])

    def test_csv_projection_matches_bronze_filter(self):
        rows = ingest.price_rows_for_csv(DIRTY_API, ["usd", "thb", "usd"])
        self.assertEqual(
            sorted((r["coin_id"], r["currency"]) for r in rows),
            [("bitcoin", "usd"), ("ethereum", "thb"), ("solana", "thb")],
        )
        history = ingest.history_rows_for_csv(DIRTY_API, ["usd", "thb"])
        self.assertEqual(len(history), 3)

    def test_alerts_ignore_non_finite_changes(self):
        with contextlib.redirect_stdout(io.StringIO()):
            alerts = ingest.print_alerts(
                {"x": {"usd": 1, "usd_24h_change": float("nan")}, "y": {"usd": 1, "usd_24h_change": "9"}},
                5.0,
                ["usd"],
            )
        self.assertEqual([a["coin"] for a in alerts], ["y"])

    def test_zero_valid_records_fails_before_lake_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(RuntimeError) as ctx, contextlib.redirect_stdout(io.StringIO()):
                ingest._lake_ingest_prices(
                    b"{}", {"bitcoin": {"usd": float("nan")}}, ["usd"],
                    data_lake_uri=tmp, output_dir=Path(tmp),
                )
            self.assertIn("zero valid records", str(ctx.exception))
            self.assertEqual(list(Path(tmp).iterdir()), [])


class OhlcvQualityTests(unittest.TestCase):
    def test_malformed_and_duplicate_bars_dropped(self):
        payload = [
            _kline(T0),
            _kline(T0),  # duplicate open time
            _kline(T0 + HOUR, h="NaN"),
            _kline(T0 + 2 * HOUR, lo="-1"),
            _kline(T0 + 3 * HOUR, h="99"),  # high below close
            _kline(T0 + 4 * HOUR, trades="x"),
            [T0 + 5 * HOUR, "1"],  # truncated row
            _kline(T0 + 6 * HOUR),
        ]
        records = ohlcv.kline_records(
            payload, venue="binance_futures", symbol="BTCUSDT", interval="1h", now_ms=T0 + 10 * HOUR
        )
        self.assertEqual([r["open_time_ms"] for r in records], [T0, T0 + 6 * HOUR])

    def test_funding_drops_non_finite_and_duplicates(self):
        payload = [
            {"fundingTime": T0, "fundingRate": "-0.0001"},
            {"fundingTime": T0, "fundingRate": "0.0002"},
            {"fundingTime": T0 + 1, "fundingRate": "NaN"},
            {"fundingRate": "0.1"},
        ]
        records = ohlcv.funding_records(payload, symbol="BTCUSDT")
        self.assertEqual([(r["funding_time_ms"], r["funding_rate"]) for r in records], [(T0, "-0.0001")])

    def test_days_must_be_positive(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as ctx:
            ohlcv.main(["--days", "0"])
        self.assertEqual(ctx.exception.code, 2)


class CliValidationTests(unittest.TestCase):
    def _exit_code(self, argv: list[str]) -> int:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as ctx:
            ingest.main(argv)
        return ctx.exception.code

    def test_rejects_empty_lists_and_bad_threshold(self):
        self.assertEqual(self._exit_code(["--coins", " , "]), 2)
        self.assertEqual(self._exit_code(["--vs-currencies", ","]), 2)
        self.assertEqual(self._exit_code(["--alert-threshold", "nan"]), 2)
        self.assertEqual(self._exit_code(["--alert-threshold", "-1"]), 2)

    def test_csv_arg_normalizes(self):
        self.assertEqual(ingest._csv_arg(" Bitcoin,bitcoin,,ETHEREUM "), ["bitcoin", "ethereum"])


class AtomicWriteTests(unittest.TestCase):
    def test_write_and_append_leave_no_temp_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            atomic_write_csv(path, ["a", "b"], [{"a": 1, "b": 2}])
            atomic_append_csv(path, ["a", "b"], [{"a": 3, "b": 4}])
            with path.open(newline="", encoding="utf-8") as f:
                self.assertEqual(list(csv.reader(f)), [["a", "b"], ["1", "2"], ["3", "4"]])
            self.assertEqual([p.name for p in Path(tmp).iterdir()], ["out.csv"])

    def test_failed_write_keeps_previous_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            atomic_write_csv(path, ["a"], [{"a": 1}])
            with self.assertRaises(ValueError):
                atomic_write_csv(path, ["a"], [{"a": 2, "unexpected": 3}])
            self.assertEqual(path.read_bytes(), b"a\r\n1\r\n")
            self.assertEqual([p.name for p in Path(tmp).iterdir()], ["out.csv"])


if __name__ == "__main__":
    unittest.main()
