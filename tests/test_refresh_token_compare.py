"""The /v1/refresh bearer token must be compared in constant time."""
from __future__ import annotations

import unittest
from unittest import mock

from book_crypto import api, config


class RefreshTokenCompareTests(unittest.TestCase):
    def test_uses_hmac_compare_digest(self):
        with mock.patch.object(config, "REFRESH_TOKEN", "s3cret-token"), mock.patch.object(
            api.hmac, "compare_digest", wraps=api.hmac.compare_digest
        ) as spy:
            self.assertTrue(api._refresh_token_ok("s3cret-token"))
            self.assertFalse(api._refresh_token_ok("s3cret-tokeN"))
        self.assertEqual(spy.call_count, 2)

    def test_empty_unset_and_non_ascii_tokens_are_rejected(self):
        with mock.patch.object(config, "REFRESH_TOKEN", "s3cret-token"):
            self.assertFalse(api._refresh_token_ok(""))
            self.assertFalse(api._refresh_token_ok("s3cret-tokén"))
        with mock.patch.object(config, "REFRESH_TOKEN", ""):
            self.assertFalse(api._refresh_token_ok(""))
            self.assertFalse(api._refresh_token_ok("anything"))


if __name__ == "__main__":
    unittest.main()
