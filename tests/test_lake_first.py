"""Lake-first pilot tests for book-crypto-data."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from book_crypto import config, lake
from book_crypto.ingest import (
    project_prices_csv,
    run_live_ingest,
    price_rows_for_csv,
)


SAMPLE_API = {
    "bitcoin": {
        "usd": 50000.0,
        "usd_24h_change": 2.5,
        "usd_24h_vol": 1e9,
        "usd_market_cap": 1e12,
        "thb": 1800000.0,
        "thb_24h_change": 1.1,
        "thb_24h_vol": 1e8,
        "thb_market_cap": 3e13,
        "last_updated_at": 1720000000,
    },
    "ethereum": {
        "usd": 3000.0,
        "usd_24h_change": -1.2,
        "usd_24h_vol": 5e8,
        "usd_market_cap": 3e11,
        "thb": 108000.0,
        "thb_24h_change": -0.5,
        "thb_24h_vol": 5e7,
        "thb_market_cap": 1e13,
        "last_updated_at": 1720000000,
    },
}


class NormalizeTests(unittest.TestCase):
    def test_price_records_include_id_and_event_time(self):
        records = lake.price_records_from_api(SAMPLE_API, ["usd", "thb"])
        self.assertEqual(len(records), 4)
        ids = {r["id"] for r in records}
        self.assertIn("bitcoin:usd", ids)
        self.assertIn("ethereum:thb", ids)
        for row in records:
            self.assertTrue(row["event_time"].endswith("Z") or "T" in row["event_time"])
            self.assertIn("price", row)

    def test_csv_projection_shape(self):
        rows = price_rows_for_csv(SAMPLE_API, ["usd"])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["coin_id"], "bitcoin")
        self.assertIn("updated_at", rows[0])


class LakeFirstOrderingTests(unittest.TestCase):
    def test_csv_not_written_when_lake_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            raw = json.dumps(SAMPLE_API).encode("utf-8")

            def boom(*_a, **_k):
                raise lake.LakeIngestError("simulated lake failure")

            with mock.patch.object(lake, "ingest_to_lake", side_effect=boom):
                with mock.patch(
                    "book_crypto.ingest.fetch_prices",
                    return_value=(raw, SAMPLE_API),
                ):
                    with self.assertRaises(lake.LakeIngestError):
                        run_live_ingest(
                            coins=["bitcoin", "ethereum"],
                            currencies=["usd", "thb"],
                            output_dir=out,
                            fetch_trending_flag=False,
                            project_csv=True,
                        )
            self.assertFalse((out / "crypto_prices.csv").exists())
            self.assertFalse((out / "crypto_history.csv").exists())

    def test_csv_written_only_after_successful_lake(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            raw = json.dumps(SAMPLE_API).encode("utf-8")
            order: list[str] = []

            def fake_ingest(**kwargs):
                order.append("lake")
                self.assertIn("raw", kwargs)
                self.assertTrue(kwargs["records"])
                return {
                    "status": "success",
                    "run_id": "test-run",
                    "record_count": len(kwargs["records"]),
                    "raw_key": "landing/test",
                    "bronze_key": "bronze/test",
                    "manifest_key": "control/test",
                    "data_lake": {"uri": "file:///tmp/lake"},
                }

            def fake_project_prices(*_a, **_k):
                order.append("csv_prices")
                return out / "crypto_prices.csv"

            def fake_project_history(*_a, **_k):
                order.append("csv_history")
                return out / "crypto_history.csv"

            with mock.patch.object(lake, "ingest_to_lake", side_effect=fake_ingest):
                with mock.patch.object(lake, "write_lineage", return_value=out / "lake_lineage.json"):
                    with mock.patch(
                        "book_crypto.ingest.fetch_prices",
                        return_value=(raw, SAMPLE_API),
                    ):
                        with mock.patch(
                            "book_crypto.ingest.project_prices_csv",
                            side_effect=fake_project_prices,
                        ):
                            with mock.patch(
                                "book_crypto.ingest.project_history_csv",
                                side_effect=fake_project_history,
                            ):
                                run_live_ingest(
                                    coins=["bitcoin", "ethereum"],
                                    currencies=["usd", "thb"],
                                    output_dir=out,
                                    fetch_trending_flag=False,
                                    project_csv=True,
                                )
            self.assertEqual(order, ["lake", "csv_prices", "csv_history"])


@unittest.skipUnless(
    lake.find_solo_empire_root() is not None,
    "Solo Empire monorepo with data_lake not found",
)
class RealLakeIngestTests(unittest.TestCase):
    def test_ingest_payload_writes_landing_bronze_manifest(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")

        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            out = Path(tmp) / "projection"
            out.mkdir()
            raw = json.dumps(SAMPLE_API, separators=(",", ":")).encode("utf-8")
            records = lake.price_records_from_api(SAMPLE_API, ["usd", "thb"])
            result = lake.ingest_to_lake(
                raw=raw,
                records=records,
                dataset=config.LAKE_DATASET_PRICES,
                data_lake_uri=lake_uri,
                metadata={"test": True},
            )
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["record_count"], 4)
            self.assertTrue(result["raw_key"].startswith("landing/"))
            self.assertTrue(result["bronze_key"].startswith("bronze/"))
            self.assertTrue(result["manifest_key"].startswith("control/"))

            lake_root = Path(lake_uri)
            self.assertTrue((lake_root / result["raw_key"]).is_file())
            self.assertTrue((lake_root / result["bronze_key"]).is_file())
            self.assertTrue((lake_root / result["manifest_key"]).is_file())
            self.assertEqual((lake_root / result["raw_key"]).read_bytes(), raw)

            # Projection remains optional and separate.
            project_prices_csv(SAMPLE_API, ["usd", "thb"], out)
            self.assertTrue((out / "crypto_prices.csv").is_file())
            lineage_path = lake.write_lineage(
                result, dataset=config.LAKE_DATASET_PRICES, data_dir=out
            )
            lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
            self.assertEqual(lineage["source"], config.LAKE_SOURCE)
            self.assertIn(config.LAKE_DATASET_PRICES, lineage["datasets"])


class PathDiscoveryTests(unittest.TestCase):
    def test_find_solo_empire_from_package(self):
        root = lake.find_solo_empire_root(config.PROJECT_ROOT)
        if root is None:
            self.skipTest("not nested under solo-empire")
        self.assertTrue((root / "infra" / "scripts" / "data_lake" / "ingest.py").is_file())


if __name__ == "__main__":
    # Ensure package import works when tests are run from repo root.
    src = Path(__file__).resolve().parents[1] / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    unittest.main()
