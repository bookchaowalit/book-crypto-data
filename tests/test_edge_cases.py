"""Edge-case regressions: overflowing numbers, bad epoch values, CSV text, ids."""
from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from book_crypto import ohlcv, quality, store

HOUR = 3_600_000
T0 = 1_700_000_000_000 - (1_700_000_000_000 % HOUR)
NOW = T0 + 10 * HOUR


def _bar(open_ms, close_ms=None, trades=10):
    close_ms = open_ms + HOUR - 1 if close_ms is None else close_ms
    return [open_ms, "1", "2", "0.5", "1.5", "10", close_ms, "15", trades]


class NumberEdgeCases(unittest.TestCase):
    def test_huge_json_int_is_rejected_not_raised(self) -> None:
        huge = json.loads("1" + "0" * 400)
        self.assertIsNone(quality.finite_number(huge))
        self.assertIsNone(quality.non_negative_number(huge))

    def test_epoch_ms(self) -> None:
        self.assertEqual(quality.epoch_ms(T0), T0)
        self.assertEqual(quality.epoch_ms(float(T0)), T0)
        self.assertEqual(quality.epoch_ms(str(T0)), T0)
        for bad in (float("inf"), float("nan"), 1.5, True, -1, 10**20, "x", None):
            self.assertIsNone(quality.epoch_ms(bad), bad)


class KlineEdgeCases(unittest.TestCase):
    def test_infinite_or_out_of_range_times_are_dropped(self) -> None:
        payload = json.loads(json.dumps([_bar(T0)]))
        payload.append(_bar(float("inf"), T0 + HOUR))
        payload.append(_bar(-(10**15), T0))
        payload.append(_bar(T0 + HOUR, trades=2.5))
        payload.append(_bar(T0 + 2 * HOUR, trades=True))
        records = ohlcv.kline_records(payload, venue="v", symbol="S", interval="1h", now_ms=NOW)
        self.assertEqual([r["open_time_ms"] for r in records], [T0])

    def test_float_encoded_times_are_accepted(self) -> None:
        records = ohlcv.kline_records(
            [_bar(str(float(T0)), str(float(T0 + HOUR - 1)))],
            venue="v", symbol="S", interval="1h", now_ms=NOW,
        )
        self.assertEqual(records[0]["open_time_ms"], T0)

    def test_funding_rows_with_bad_times_are_dropped(self) -> None:
        payload = [
            {"fundingTime": float("inf"), "fundingRate": "0.0001"},
            {"fundingTime": 10**20, "fundingRate": "0.0001"},
            {"fundingTime": T0, "fundingRate": "0.0001"},
            "not-a-dict",
        ]
        records = ohlcv.funding_records(payload, symbol="S")
        self.assertEqual([r["funding_time_ms"] for r in records], [T0])


class CsvAndIdEdgeCases(unittest.TestCase):
    def test_unquoted_line_separator_stays_in_one_row(self) -> None:
        buf = io.StringIO()
        csv.writer(buf).writerows([["coin", "note"], ["btc", "a b"], ["eth", "c"]])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "p.csv"
            path.write_text("﻿" + buf.getvalue(), encoding="utf-8", newline="")
            rows, error = store._read_csv(path)
        self.assertIsNone(error)
        self.assertEqual(rows, [{"coin": "btc", "note": "a b"}, {"coin": "eth", "note": "c"}])

    def test_decoded_id_with_percent_is_not_decoded_again(self) -> None:
        payload = {"items": [{"record_id": "x%41"}, {"record_id": "xA"}]}
        with mock.patch.object(store, "load_records", return_value=payload):
            self.assertEqual(store.get_record("x%41"), {"record_id": "x%41"})
            self.assertEqual(store.get_record("x%3A"), None)
            self.assertEqual(store.get_record("xA"), {"record_id": "xA"})


if __name__ == "__main__":
    unittest.main()
