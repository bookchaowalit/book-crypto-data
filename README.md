# book-crypto-data

Crypto market data product (CoinGecko free public API).

Migration status: **lake-first pilot (end-to-end)**. Live ingest writes exact
API bytes to Object Storage landing + Bronze Parquet before any local CSV
projection. The read-only HTTP API queries Bronze via DuckDB and does **not**
read CSV. CSV remains an optional CLI projection only.
No paid providers, billing credentials, or public deployment are enabled by default.

Shared procedure (do not invent a competing path; paths from Solo Empire monorepo root):

- `docs/systems/data-lake-architecture.md`
- `learning/platform-engineering/app-cli-data-lake-playbook.md`
- `learning/platform-engineering/multi-engine-lake-query-lab.md` — DuckDB production + Postgres/ClickHouse lab on the same Bronze
- `infra/scripts/data_lake/product_adapter.py` — shared lake-first adapter (also used by `book-stock-data`)

## Data contract

| Field | Value |
|---|---|
| `source` | `book-crypto-data` |
| `domain` | `market` |
| `dataset` (prices) | `crypto_prices` |
| `dataset` (trending) | `crypto_trending` |
| Bronze `schema_version` | `1` |
| Product envelope `schema_version` | `crypto.v1` |
| `privacy_class` | `public` |
| `retention_class` | `operational` |

```text
CoinGecko API
    → landing/source=book-crypto-data/.../payload.json   (exact bytes)
    → bronze/domain=market/dataset=crypto_prices/...     (Parquet)
    → control/manifests/...
    → HTTP API  ← DuckDB query of Bronze Parquet
    → data/*.csv  (optional CLI projection only; not used by API)
```

Local `data/crypto_prices.csv` and `data/crypto_history.csv` are **optional CLI
projections**. The HTTP API does not read them. By default it reads Bronze
Parquet (`storage_model=lake_first_bronze_duckdb`); the guarded Silver parity
pilot is controlled by `SILVER_READ_MODE`:

| Value | Behavior |
|---|---|
| `bronze` | Read and serve Bronze; default and production-safe |
| `compare` | Read Silver, report `silver_parity`, but still serve Bronze |
| `silver` | Serve current-state Silver only when parity passes; history remains Bronze until a separate history contract exists |

All modes use DuckDB over Parquet and never use CSV. Run `compare` first and
inspect `/v1/metadata` for `silver_parity.status=passed` before enabling
`silver`. The shared writer/read helper and the exact commands are documented
in the monorepo [free-cloud runbook](../../../../../../../../../docs/operations/FREE-CLOUD-E2E-RUNBOOK.md).

### Optional Iceberg REST catalog read pilot

The API can compare a catalog-backed read without changing the Bronze contract:

| Variable | Default | Meaning |
|---|---|---|
| `LAKE_READ_MODE` | `parquet` | `parquet` reads Bronze parts directly; `iceberg` resolves the registered table through the Iceberg catalog and queries it with DuckDB |
| `LAKE_READ_FALLBACK` | `error` | `error` is fail-closed; `parquet` explicitly falls back to direct Bronze when the catalog read is unavailable |
| `ICEBERG_CATALOG_URI` | unset | Catalog REST/SQL URI from the runtime secret provider |
| `ICEBERG_WAREHOUSE_URI` | unset | Provider-issued warehouse URI/name |
| `ICEBERG_CATALOG_TOKEN` | unset | Catalog credential; never put it in this repository or API response |

For the hosted R2 lane, inject the catalog/S3 values from Infisical and run the
API only after the matching table (`bronze.market_crypto_prices`) has been
registered:

```bash
npx --yes @infisical/cli@0.43.120 run \
  --projectId=<solo-empire-project-id> --env=dev --path=/cloudflare -- \
  bash -lc 'export SOLO_EMPIRE_DATA_LAKE_URI=s3://<bucket>/<prefix>; \
    export LAKE_READ_MODE=iceberg; export LAKE_READ_FALLBACK=error; \
    PYTHONPATH=src /path/to/solo-empire/.venv/bin/python \
    -m book_crypto.api --host 127.0.0.1 --port 8101'
```

`/v1/metadata` reports `storage_model=lake_first_iceberg_rest_duckdb` and
`records_source_kind=iceberg_rest_duckdb` on a successful catalog read. If the
operator deliberately sets `LAKE_READ_FALLBACK=parquet`, metadata reports
`bronze_parquet_fallback` and includes a non-secret `fallback_reason`. The
default remains direct Bronze Parquet, and CSV never participates in either
read path. See the monorepo [free-cloud runbook](../../../../../../../../../docs/operations/FREE-CLOUD-E2E-RUNBOOK.md)
and [data-lake playbook](../../../../../../../../../learning/platform-engineering/app-cli-data-lake-playbook.md).

#### Live R2 parity evidence

Verified on 2026-08-07 with a bounded public fixture prefix
`portfolio-demo/book-crypto-iceberg-001`:

| Check | Result |
|---|---|
| Iceberg table | `bronze.market_crypto_prices` |
| Bronze/manifest rows | 2 |
| Iceberg vs direct Parquet rows | parity passed |
| Raw bytes and lineage | exact / passed |
| Registration retry | `already_registered` |
| Snapshot retry | idempotent |
| API storage model | `lake_first_iceberg_rest_duckdb` |
| API source kinds | records/history = `iceberg_rest_duckdb` |
| CSV dependency | none |

The fixture is intentionally old, so the API correctly reports
`data_status=stale`; this is a freshness result, not a query failure. The
default `LAKE_READ_MODE=parquet` remains unchanged until a product explicitly
chooses the catalog-backed path.

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .
# Lake writes need the monorepo data-lake runtime (pyarrow + infra/scripts)
pip install -r <solo-empire>/infra/requirements-data-lake.txt
python -m book_crypto.ingest --help
book-crypto-data --help
```

### CLI (ingestion)

```bash
# Fixture / dry-run mode (no upstream network, no lake write)
python -m book_crypto.ingest --fixture
python -m book_crypto.ingest --dry-run

# Live free-only collection: lake first, then CSV projection
python -m book_crypto.ingest --coins bitcoin,ethereum
python -m book_crypto.ingest --coins bitcoin --data-lake-uri /path/to/data/lake
python -m book_crypto.ingest --coins bitcoin --no-project-csv   # lake only
```

Environment:

| Variable | Purpose |
|---|---|
| `SOLO_EMPIRE_DATA_LAKE_URI` / `DATA_LAKE_URI` | Lake root (`file://...` or path; default monorepo `data/lake`) |
| `SOLO_EMPIRE_ROOT` | Monorepo root if not discovered by walking parents |
| `DATA_DIR` | Local CSV projection directory (default `./data`) |

On live ingest, lake write failure exits non-zero and **does not** update CSV.

Runtime projections stay under repo-local `data/` (plus `data/lake_lineage.json`).

### Local read-only API

```bash
# Point at the same lake the CLI wrote (default: monorepo data/lake)
export SOLO_EMPIRE_DATA_LAKE_URI=/path/to/solo-empire/data/lake
python -m book_crypto.api --host 127.0.0.1 --port 8101
```

Default bind: `127.0.0.1:8101`. GET handlers query Bronze Parquet via DuckDB
(no upstream scrape, no CSV dependency). `/v1/metadata` reports
`storage_model=lake_first_bronze_duckdb` and `records_source_kind=bronze_parquet`.

| Method | Path | Notes |
|---|---|---|
| GET | `/healthz` | Liveness + Bronze data status (no upstream calls) |
| GET | `/v1/metadata` | Schema, counts, provider matrix, lake lineage |
| GET | `/v1/records?limit=50&cursor=` | Latest snapshot from Bronze |
| GET | `/v1/records/{record_id}` | Single record |
| GET | `/v1/history?limit=50&cursor=` | Full Bronze history rows |
| POST | `/v1/refresh` | **403 by default** |

Consumer example:

```bash
curl -s http://127.0.0.1:8101/v1/records?limit=10 | python -m json.tool
```

Versioned response envelope:

```json
{
  "schema_version": "crypto.v1",
  "source": "book-crypto-data",
  "retrieved_at": "2026-08-01T12:00:00Z",
  "data_status": "ok",
  "items": [],
  "next_cursor": null
}
```

Domain-specific fields remain domain-specific inside `items`.

## Free-only defaults

```text
FREE_ONLY=true
ALLOW_PAID_PROVIDERS=false
ALLOW_EXTERNAL_WRITES=false
API_HOST=127.0.0.1
API_PORT=8101
ALLOW_REFRESH=false
```

Allowed default: CoinGecko keyless/public low-volume REST with cache and backoff

### Provider matrix

| Provider | Classification | Notes |
|---|---|---|
| `coingecko_public` | `free` | CoinGecko keyless public REST; rate-limited |
| `coingecko_pro` | `blocked` | CoinGecko Pro requires paid plan |
| `coingecko_webhooks` | `blocked` | Webhooks/high-frequency polling not free-default |

Missing paid credentials produce an explicit skipped/blocked status; they never
trigger a paid fallback.

## Tests

```bash
# From this package (with src on PYTHONPATH / editable install)
python -m unittest discover -s tests -v

# Prefer the monorepo venv when verifying real lake writes:
# /path/to/solo-empire/.venv/bin/python -m unittest discover -s tests -v
```

Coverage includes offline fixtures, mocked upstream HTTP, API contract against
Bronze (no CSV), empty lake, CLI CSV projection helper, timeout/429, invalid
record IDs, free-only blocking, lake-first ordering, and real local lake write/
DuckDB read when pyarrow+duckdb are available.

## Safety

- API GET handlers read Bronze Parquet via DuckDB only (not CSV).
- Live CLI writes lake artifacts before optional CSV projection.
- No secrets in source, fixtures, or `.env.example`.
- Do not enable billing, public bind, or `ALLOW_REFRESH` without owner approval.
