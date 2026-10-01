"""CSV projection stamps must be UTC: product_store reads naive stamps as UTC."""
from __future__ import annotations

import os
import time
import unittest
from datetime import datetime, timezone

from book_crypto import ingest


@unittest.skipUnless(hasattr(time, "tzset"), "needs time.tzset")
class ProjectionTimestampUtcTests(unittest.TestCase):
    def test_stamp_is_utc_on_a_non_utc_host(self):
        old = os.environ.get("TZ")
        os.environ["TZ"] = "Asia/Bangkok"  # UTC+7, no DST
        time.tzset()
        try:
            stamp = ingest._projection_timestamp()
        finally:
            if old is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = old
            time.tzset()
        parsed = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        drift = abs((datetime.now(timezone.utc) - parsed).total_seconds())
        self.assertLess(drift, 300, f"projection stamp {stamp!r} is not UTC")


if __name__ == "__main__":
    unittest.main()
