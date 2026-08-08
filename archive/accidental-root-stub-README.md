<!-- Preserved from accidental monorepo root tracking path:
     projects/product/engineering/book-dev/github/bookchaowalit/book-crypto-data/README.md
     Canonical nested repository is this tools/book-crypto-data checkout. -->

# Book Crypto Data

Crypto market data product extracted from the legacy opportunity scraper.
It fetches CoinGecko prices and trending coins, writes normalized CSV
snapshots/history under `data/`, and exposes a local CLI.

Migration status: local source-of-truth staging. The legacy repository remains
available until the domain contract and consumer handoff are complete.

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .
python -m book_crypto.ingest --help
book-crypto-data --coins bitcoin,ethereum --vs-currencies usd,thb
```

The smoke test is offline. Network access is only used by the ingestion CLI.
Never commit `.env` or runtime data.
