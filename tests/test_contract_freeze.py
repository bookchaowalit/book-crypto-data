"""Compatibility freeze tests for data-product v1 contract.

Frozen map is owned by Solo Empire:
  docs/systems/data-product-contracts-v1.yaml
Breaking changes require a new schema_version (e.g. crypto.v2).
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path

from book_crypto import config
from book_crypto.store import envelope


ENVELOPE_KEYS = ['schema_version', 'source', 'retrieved_at', 'data_status', 'items', 'next_cursor']
FROZEN_PORT = 8101
FROZEN_SCHEMA = 'crypto.v1'


def _shared_runtime_available():
    try:
        envelope(items=[], data_status="empty")
    except (ImportError, ModuleNotFoundError):
        return False
    return True


class ContractFreezeTests(unittest.TestCase):
    def test_schema_version_frozen(self):
        self.assertEqual(config.SCHEMA_VERSION, FROZEN_SCHEMA)

    def test_port_frozen(self):
        self.assertEqual(int(config.API_PORT), FROZEN_PORT)

    def test_free_only_defaults(self):
        self.assertTrue(config.FREE_ONLY)
        self.assertFalse(config.ALLOW_PAID_PROVIDERS)
        self.assertFalse(config.ALLOW_EXTERNAL_WRITES)
        self.assertFalse(config.ALLOW_REFRESH)

    @unittest.skipUnless(_shared_runtime_available(), "shared data_lake adapter not found")
    def test_envelope_required_keys(self):
        body = envelope(items=[], data_status="empty")
        for key in ENVELOPE_KEYS:
            self.assertIn(key, body)
        self.assertEqual(body["schema_version"], FROZEN_SCHEMA)
        self.assertEqual(body["source"], config.REPO_NAME)
        self.assertIsInstance(body["items"], list)
        self.assertIn("next_cursor", body)

    def test_api_get_only_surface_and_refresh_forbidden(self):
        api_src = Path(__file__).resolve().parents[1] / "src" / 'book_crypto' / "api.py"
        text = api_src.read_text(encoding="utf-8")
        for path in ("/healthz", "/v1/metadata", "/v1/records", "/v1/history", "/v1/refresh"):
            self.assertIn(path, text)
        self.assertIn("def do_GET", text)
        self.assertIn("def do_POST", text)
        self.assertIn("403", text)
        # Consumers must not assume refresh works by default
        self.assertFalse(config.ALLOW_REFRESH)

    def test_loopback_default_host(self):
        self.assertIn(config.API_HOST, {"127.0.0.1", "localhost", "::1"})


if __name__ == "__main__":
    unittest.main()
