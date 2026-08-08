"""Offline tests for book-crypto-data free-only API + policy."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from book_crypto import config, lake, policy
from book_crypto.api import DataProductHandler
from book_crypto.http_client import UpstreamError, request_json
from book_crypto.store import (
    get_record,
    load_csv_projection,
    load_history,
    load_records,
    seed_fixtures,
    seed_lake_from_sample,
)


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures"

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


def _set_data_dir(path: Path):
    return mock.patch.object(config, "DATA_DIR", path)


def _set_lake_uri(uri: str):
    return mock.patch.object(config, "DATA_LAKE_URI", uri)


def _set_read_mode(mode: str, fallback: str = "error"):
    return mock.patch.multiple(
        config,
        LAKE_READ_MODE=mode,
        LAKE_READ_FALLBACK=fallback,
    )


def _set_silver_mode(mode: str):
    return mock.patch.object(config, "SILVER_READ_MODE", mode)


def _write_crypto_silver(lake_uri: str):
    root = lake.find_solo_empire_root()
    if root is None:
        raise RuntimeError("Solo Empire root is required for the Silver pilot")
    scripts = root / "infra" / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    from data_lake.silver import SilverProductContract, transform_bronze_to_silver

    contract = SilverProductContract(
        source=config.LAKE_SOURCE,
        domain=config.LAKE_DOMAIN,
        bronze_dataset=config.LAKE_DATASET_PRICES,
        silver_dataset=config.LAKE_DATASET_PRICES,
        product_schema_version="crypto.silver.v1",
        bronze_product_schema_version=config.SCHEMA_VERSION,
        bronze_schema_version=config.LAKE_BRONZE_SCHEMA_VERSION,
        normalizer="crypto_prices",
        required_fields=("source_record_id", "coin_id", "currency", "price"),
        privacy_class=config.LAKE_PRIVACY_CLASS,
        retention_class=config.LAKE_RETENTION_CLASS,
    )
    return transform_bronze_to_silver(contract, data_lake_uri=lake_uri)


def _lake_deps_available() -> bool:
    try:
        import duckdb  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError:
        return False
    return lake.find_solo_empire_root() is not None


@unittest.skipUnless(_lake_deps_available(), "pyarrow/duckdb + monorepo data_lake required")
class StoreLakeTests(unittest.TestCase):
    def test_bronze_records_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lake_uri = str(root / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd", "thb"], data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri):
                payload = load_records()
            self.assertEqual(payload["source_kind"], "bronze_parquet")
            self.assertEqual(payload["data_status"], "stale")  # fixed past last_updated_at
            self.assertGreaterEqual(len(payload["items"]), 1)
            self.assertIn("record_id", payload["items"][0])
            self.assertNotIn("crypto_prices.csv", payload["path"])

    def test_empty_bronze(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "empty-lake")
            Path(lake_uri).mkdir()
            with _set_lake_uri(lake_uri):
                payload = load_records()
            self.assertEqual(payload["data_status"], "empty")
            self.assertEqual(payload["items"], [])
            self.assertEqual(payload["source_kind"], "bronze_parquet")

    def test_csv_does_not_feed_load_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = root / "data"
            lake_uri = str(root / "empty-lake")
            Path(lake_uri).mkdir()
            with mock.patch.object(config, "FIXTURES_DIR", FIXTURE_ROOT):
                seed_fixtures(data_dir)
            # CSV projection exists...
            csv_payload = load_csv_projection(kind="records", data_dir=data_dir)
            self.assertGreaterEqual(len(csv_payload["items"]), 1)
            # ...but API loaders ignore it when Bronze is empty.
            with _set_lake_uri(lake_uri), _set_data_dir(data_dir):
                payload = load_records(data_dir)
            self.assertEqual(payload["data_status"], "empty")
            self.assertEqual(payload["items"], [])
            self.assertEqual(payload["source_kind"], "bronze_parquet")

    def test_invalid_record_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd"], data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri):
                self.assertIsNone(get_record("this-id-does-not-exist"))

    def test_history_load_from_bronze(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd", "thb"], data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri):
                hist = load_history()
            self.assertEqual(hist["source_kind"], "bronze_parquet")
            self.assertGreaterEqual(len(hist["items"]), 1)
            self.assertTrue(str(hist["items"][0]["record_id"]).endswith("#h0") or "#h" in hist["items"][0]["record_id"])

    def test_iceberg_mode_keeps_item_parity_with_bronze(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd", "thb"], data_lake_uri=lake_uri)

            def read_catalog_rows(dataset, *, data_lake_uri=None, sql=None):
                return lake.read_bronze_rows(
                    dataset, data_lake_uri=data_lake_uri, sql=sql
                )

            with _set_lake_uri(lake_uri), _set_read_mode("parquet"):
                expected = load_records()
            with _set_lake_uri(lake_uri), _set_read_mode("iceberg"), mock.patch.object(
                lake, "read_iceberg_rows", side_effect=read_catalog_rows
            ):
                actual = load_records()

            self.assertEqual(actual["items"], expected["items"])
            self.assertEqual(actual["source_kind"], "iceberg_rest_duckdb")
            self.assertEqual(actual["read_mode"], "iceberg")
            self.assertIsNone(actual["fallback_reason"])

    def test_iceberg_mode_can_fallback_to_bronze_explicitly(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd"], data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri), _set_read_mode("iceberg", "parquet"), mock.patch.object(
                lake,
                "read_iceberg_rows",
                side_effect=lake.LakeUnavailable("catalog temporarily unavailable"),
            ):
                payload = load_records()

            self.assertGreaterEqual(len(payload["items"]), 1)
            self.assertEqual(payload["source_kind"], "bronze_parquet_fallback")
            self.assertEqual(payload["read_mode"], "iceberg")
            self.assertIn("catalog temporarily unavailable", payload["fallback_reason"])

    def test_iceberg_mode_is_fail_closed_without_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd"], data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri), _set_read_mode("iceberg", "error"), mock.patch.object(
                lake,
                "read_iceberg_rows",
                side_effect=lake.LakeUnavailable("catalog credentials unavailable"),
            ):
                payload = load_records()

            self.assertEqual(payload["data_status"], "error")
            self.assertEqual(payload["items"], [])
            self.assertEqual(payload["source_kind"], "iceberg_rest_duckdb")
            self.assertIn("catalog credentials unavailable", payload["error"])

    def test_compare_mode_reports_silver_parity_but_serves_bronze(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd", "thb"], data_lake_uri=lake_uri)
            _write_crypto_silver(lake_uri)
            with _set_lake_uri(lake_uri), _set_silver_mode("compare"):
                payload = load_records()
            self.assertEqual(payload["source_kind"], "bronze_parquet")
            self.assertEqual(payload["silver_read_mode"], "compare")
            self.assertEqual(payload["silver_parity"]["status"], "passed")
            self.assertGreaterEqual(len(payload["items"]), 1)

    def test_silver_mode_serves_silver_without_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lake_uri = str(root / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd"], data_lake_uri=lake_uri)
            _write_crypto_silver(lake_uri)
            (root / "data").mkdir()
            (root / "data" / config.RECORDS_FILE).write_text(
                "coin_id,currency,price\npoison,usd,1\n", encoding="utf-8"
            )
            with _set_lake_uri(lake_uri), _set_data_dir(root / "data"), _set_silver_mode("silver"):
                payload = load_records(root / "data")
            self.assertEqual(payload["source_kind"], "silver_parquet")
            self.assertEqual(payload["silver_read_mode"], "silver")
            self.assertEqual(payload["silver_parity"]["status"], "passed")
            self.assertNotIn("poison", {item["coin_id"] for item in payload["items"]})

    def test_silver_mode_fails_closed_when_parity_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd"], data_lake_uri=lake_uri)
            with _set_lake_uri(lake_uri), _set_silver_mode("silver"):
                payload = load_records()
            self.assertEqual(payload["data_status"], "malformed")
            self.assertEqual(payload["items"], [])
            self.assertEqual(payload["source_kind"], "silver_parquet")
            self.assertIn("Silver serving is fail-closed", payload["error"])

    def test_silver_mode_keeps_history_on_bronze_snapshot_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            seed_lake_from_sample(SAMPLE_API, ["usd", "thb"], data_lake_uri=lake_uri)
            _write_crypto_silver(lake_uri)
            with _set_lake_uri(lake_uri), _set_silver_mode("silver"):
                payload = load_history()
            self.assertEqual(payload["source_kind"], "bronze_parquet")
            self.assertEqual(payload["silver_parity"]["status"], "not_applicable")
            self.assertGreaterEqual(len(payload["items"]), 1)


class CsvProjectionOnlyTests(unittest.TestCase):
    def test_csv_projection_helper_still_works_for_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            with mock.patch.object(config, "FIXTURES_DIR", FIXTURE_ROOT):
                seed_fixtures(data_dir)
            payload = load_csv_projection(kind="records", data_dir=data_dir)
            self.assertEqual(payload["source_kind"], "csv_projection")
            self.assertGreaterEqual(len(payload["items"]), 1)

    def test_malformed_csv_projection(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            bad = data_dir / config.RECORDS_FILE
            bad.write_bytes(b"\xff\xfe\x00not-csv")
            payload = load_csv_projection(kind="records", data_dir=data_dir)
            self.assertEqual(payload["data_status"], "malformed")


class PolicyTests(unittest.TestCase):
    def test_free_only_blocks_paid(self):
        with mock.patch.object(config, "FREE_ONLY", True), mock.patch.object(config, "ALLOW_PAID_PROVIDERS", False):
            decision = policy.evaluate_provider("coingecko_pro")
            self.assertFalse(decision.allowed)
            self.assertIn(decision.status, {"blocked", "unknown"})

    def test_free_provider_allowed(self):
        with mock.patch.object(config, "FREE_ONLY", True), mock.patch.object(config, "ALLOW_PAID_PROVIDERS", False):
            decision = policy.evaluate_provider("coingecko_public")
            self.assertTrue(decision.allowed)
            self.assertEqual(decision.status, "free")

    def test_missing_credentials(self):
        with mock.patch.dict(os.environ, {"FIRECRAWL_API_KEY": "", "TEQUILA_API_KEY": ""}, clear=False):
            status = policy.missing_credential_status("FIRECRAWL_API_KEY")
            self.assertEqual(status["status"], "missing")
            self.assertFalse(status["present"])

    def test_external_writes_blocked(self):
        with mock.patch.object(config, "FREE_ONLY", True), mock.patch.object(config, "ALLOW_EXTERNAL_WRITES", False):
            self.assertFalse(policy.external_writes_allowed())
            with self.assertRaises(PermissionError):
                policy.require_external_writes("telegram")


class HttpClientTests(unittest.TestCase):
    def test_blocks_paid_provider_before_request(self):
        with mock.patch.object(config, "FREE_ONLY", True), mock.patch.object(config, "ALLOW_PAID_PROVIDERS", False):
            with self.assertRaises(PermissionError):
                request_json("GET", "https://example.invalid/paid", provider="coingecko_pro")

    def test_timeout(self):
        if policy.evaluate_provider("coingecko_public").allowed is False:
            self.skipTest("no free provider for timeout simulation")

        class Boom:
            def request(self, *args, **kwargs):
                raise TimeoutError("timed out")

        fake_httpx = mock.Mock()
        fake_httpx.request = Boom().request
        with mock.patch("book_crypto.http_client.httpx", fake_httpx), mock.patch.object(config, "MAX_RETRIES", 0):
            with self.assertRaises(UpstreamError) as ctx:
                request_json(
                    "GET",
                    "https://example.invalid/timeout",
                    provider="coingecko_public",
                    sleep=lambda _: None,
                )
            self.assertEqual(ctx.exception.kind, "timeout")

    def test_http_429(self):
        if policy.evaluate_provider("coingecko_public").allowed is False:
            self.skipTest("no free provider for 429 simulation")

        resp = mock.Mock()
        resp.status_code = 429
        resp.content = b"rate limited"
        fake_httpx = mock.Mock()
        fake_httpx.request.return_value = resp
        with mock.patch("book_crypto.http_client.httpx", fake_httpx), mock.patch.object(config, "MAX_RETRIES", 0):
            with self.assertRaises(UpstreamError) as ctx:
                request_json(
                    "GET",
                    "https://example.invalid/429",
                    provider="coingecko_public",
                    sleep=lambda _: None,
                )
            self.assertEqual(ctx.exception.status_code, 429)
            self.assertEqual(ctx.exception.kind, "rate_limit")

    def test_malformed_upstream_json(self):
        if policy.evaluate_provider("coingecko_public").allowed is False:
            self.skipTest("no free provider")
        resp = mock.Mock()
        resp.status_code = 200
        resp.content = b"{not-json"
        resp.json.side_effect = ValueError("bad json")
        fake_httpx = mock.Mock()
        fake_httpx.request.return_value = resp
        with mock.patch("book_crypto.http_client.httpx", fake_httpx):
            with self.assertRaises(UpstreamError):
                request_json("GET", "https://example.invalid/bad", provider="coingecko_public")


@unittest.skipUnless(_lake_deps_available(), "pyarrow/duckdb + monorepo data_lake required")
class ApiContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.data_dir = root / "data"
        self.data_dir.mkdir()
        self.lake_uri = str(root / "lake")
        # Bronze only — no CSV files created for the API surface.
        seed_lake_from_sample(
            SAMPLE_API,
            ["usd", "thb"],
            data_lake_uri=self.lake_uri,
            data_dir=self.data_dir,
        )
        self.assertFalse((self.data_dir / config.RECORDS_FILE).exists())
        self.data_patch = _set_data_dir(self.data_dir)
        self.lake_patch = _set_lake_uri(self.lake_uri)
        self.read_patch = _set_read_mode("parquet", "error")
        self.data_patch.start()
        self.lake_patch.start()
        self.read_patch.start()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), DataProductHandler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.lake_patch.stop()
        self.data_patch.stop()
        self.read_patch.stop()
        self.tmp.cleanup()

    def _get(self, path: str):
        try:
            with urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def _post(self, path: str, headers=None):
        req = Request(f"http://127.0.0.1:{self.port}{path}", data=b"{}", method="POST")
        req.add_header("Content-Type", "application/json")
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        try:
            with urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def test_healthz(self):
        status, body = self._get("/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["repository"], config.REPO_NAME)
        self.assertIn(body["data_status"], {"ok", "empty", "stale", "malformed", "error"})

    def test_metadata_envelope(self):
        status, body = self._get("/v1/metadata")
        self.assertEqual(status, 200)
        for key in ("schema_version", "source", "retrieved_at", "data_status", "items", "next_cursor"):
            self.assertIn(key, body)
        self.assertEqual(body["schema_version"], config.SCHEMA_VERSION)
        self.assertEqual(body["source"], config.REPO_NAME)
        self.assertTrue(body["items"])
        meta = body["items"][0]
        self.assertIn("providers", meta)
        self.assertEqual(meta["storage_model"], "lake_first_bronze_duckdb")
        self.assertEqual(meta["records_source_kind"], "bronze_parquet")
        self.assertEqual(meta["history_source_kind"], "bronze_parquet")
        self.assertEqual(meta["read_mode"], "parquet")
        self.assertEqual(meta["read_fallback"], "error")

    def test_metadata_reports_feature_flagged_iceberg_mode(self):
        def read_catalog_rows(dataset, *, data_lake_uri=None, sql=None):
            return lake.read_bronze_rows(dataset, data_lake_uri=data_lake_uri, sql=sql)

        with _set_read_mode("iceberg"), mock.patch.object(
            lake, "read_iceberg_rows", side_effect=read_catalog_rows
        ):
            status, body = self._get("/v1/metadata")

        self.assertEqual(status, 200)
        meta = body["items"][0]
        self.assertEqual(meta["storage_model"], "lake_first_iceberg_rest_duckdb")
        self.assertEqual(meta["records_source_kind"], "iceberg_rest_duckdb")
        self.assertEqual(meta["history_source_kind"], "iceberg_rest_duckdb")
        self.assertEqual(meta["read_mode"], "iceberg")
        self.assertEqual(meta["read_fallback"], "error")

    def test_records_and_history(self):
        status, body = self._get("/v1/records?limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["items"]), 1)
        self.assertTrue(body["items"][0]["record_id"])
        status, hist = self._get("/v1/history?limit=1")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(hist["items"]), 1)

    def test_record_by_id(self):
        _, body = self._get("/v1/records?limit=1")
        rid = body["items"][0]["record_id"]
        from urllib.parse import quote

        status, one = self._get(f"/v1/records/{quote(rid, safe='')}")
        self.assertEqual(status, 200)
        self.assertEqual(one["items"][0]["record_id"], rid)

    def test_invalid_record_id_404(self):
        status, body = self._get("/v1/records/not-a-real-record-id")
        self.assertEqual(status, 404)
        self.assertEqual(body["data_status"], "not_found")

    def test_refresh_forbidden_by_default(self):
        with mock.patch.object(config, "ALLOW_REFRESH", False):
            status, body = self._post("/v1/refresh")
            self.assertEqual(status, 403)
            self.assertEqual(body["data_status"], "forbidden")

    def test_get_never_needs_network(self):
        status, body = self._get("/v1/records")
        self.assertEqual(status, 200)
        self.assertIn("items", body)
        self.assertGreaterEqual(len(body["items"]), 1)

    def test_api_serves_without_csv_files(self):
        """Hard guarantee: API works with Bronze only and zero CSV projections."""
        self.assertFalse(any(self.data_dir.glob("*.csv")))
        status, body = self._get("/v1/records")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(body["items"]), 1)
        status, meta = self._get("/v1/metadata")
        self.assertEqual(status, 200)
        self.assertEqual(meta["items"][0]["storage_model"], "lake_first_bronze_duckdb")
        # Even if CSV appears later, loaders still report bronze_parquet.
        (self.data_dir / config.RECORDS_FILE).write_text(
            "coin_id,currency,price,change_24h_pct,volume_24h,market_cap,updated_at\n"
            "fake,usd,1,0,0,0,2099-01-01 00:00:00\n",
            encoding="utf-8",
        )
        status, body = self._get("/v1/records")
        self.assertEqual(status, 200)
        coins = {item["coin_id"] for item in body["items"]}
        self.assertIn("bitcoin", coins)
        self.assertNotIn("fake", coins)


class SmokeCompileTests(unittest.TestCase):
    def test_modules_import(self):
        from book_crypto import api, config as cfg, http_client, policy as pol, store

        self.assertTrue(cfg.FREE_ONLY)
        self.assertFalse(cfg.ALLOW_PAID_PROVIDERS)
        self.assertFalse(cfg.ALLOW_EXTERNAL_WRITES)
        self.assertEqual(cfg.LAKE_READ_MODE, "parquet")
        self.assertEqual(cfg.LAKE_READ_FALLBACK, "error")
        self.assertTrue(hasattr(api, "main"))
        self.assertTrue(hasattr(store, "load_records"))
        self.assertTrue(hasattr(store, "load_csv_projection"))
        self.assertTrue(hasattr(pol, "provider_matrix"))
        self.assertTrue(hasattr(http_client, "request_json"))


if __name__ == "__main__":
    unittest.main()
