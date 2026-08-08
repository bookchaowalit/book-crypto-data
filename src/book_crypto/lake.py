"""Lake-first adapter for book-crypto-data.

Delegates durable write/read to the shared Solo Empire product adapter so
sibling data products (stock, fx, …) reuse one contract.

Shared procedure:
  learning/platform-engineering/app-cli-data-lake-playbook.md
  docs/systems/data-lake-architecture.md
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import config


def _load_shared():
    """Import shared product_adapter from monorepo infra/scripts."""
    # Prefer walking to monorepo root the same way product_adapter does.
    cur = config.PROJECT_ROOT.resolve()
    for parent in [cur, *cur.parents]:
        scripts = parent / "infra" / "scripts"
        if (scripts / "data_lake" / "product_adapter.py").is_file():
            if str(scripts) not in sys.path:
                sys.path.insert(0, str(scripts))
            break
    from data_lake import product_adapter as pa  # type: ignore
    return pa


_pa = None


def _pa_mod():
    global _pa
    if _pa is None:
        _pa = _load_shared()
    return _pa


# Re-export exception types for existing call sites.
class LakeUnavailable(RuntimeError):
    pass


class LakeIngestError(RuntimeError):
    pass


def _sync_exc():
    pa = _pa_mod()
    global LakeUnavailable, LakeIngestError
    LakeUnavailable = pa.LakeUnavailable  # type: ignore
    LakeIngestError = pa.LakeIngestError  # type: ignore


def _contract():
    pa = _pa_mod()
    _sync_exc()
    return pa.LakeProductContract(
        source=config.LAKE_SOURCE,
        domain=config.LAKE_DOMAIN,
        product_schema_version=config.SCHEMA_VERSION,
        privacy_class=config.LAKE_PRIVACY_CLASS,
        retention_class=config.LAKE_RETENTION_CLASS,
        bronze_schema_version=config.LAKE_BRONZE_SCHEMA_VERSION,
        project_root=config.PROJECT_ROOT,
        data_lake_uri=config.DATA_LAKE_URI,
        solo_empire_root=config.SOLO_EMPIRE_ROOT,
        lineage_filename=config.LINEAGE_FILE,
        datasets=(config.LAKE_DATASET_PRICES, config.LAKE_DATASET_TRENDING),
    )


def utc_now_iso() -> str:
    return _pa_mod().utc_now_iso()


def find_solo_empire_root(start: Optional[Path] = None) -> Optional[Path]:
    return _pa_mod().find_solo_empire_root(
        start or config.PROJECT_ROOT,
        solo_empire_root=config.SOLO_EMPIRE_ROOT,
    )


def default_data_lake_uri(solo_root: Optional[Path] = None) -> str:
    del solo_root
    return _pa_mod().default_data_lake_uri(
        data_lake_uri=config.DATA_LAKE_URI,
        project_root=config.PROJECT_ROOT,
        solo_empire_root=config.SOLO_EMPIRE_ROOT,
    )


def is_remote_lake_uri(data_lake_uri: str) -> bool:
    return _pa_mod().is_remote_lake_uri(data_lake_uri)


def price_records_from_api(
    data: dict[str, Any],
    currencies: list[str],
    *,
    event_time: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Normalize CoinGecko simple/price JSON into Bronze-ready records."""
    received = event_time or utc_now_iso()
    records: list[dict[str, Any]] = []
    for coin_id, info in data.items():
        if not isinstance(info, dict):
            continue
        last_updated = info.get("last_updated_at")
        if last_updated is not None:
            try:
                coin_event_time = (
                    datetime.fromtimestamp(int(last_updated), tz=timezone.utc)
                    .replace(microsecond=0)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            except (TypeError, ValueError, OSError, OverflowError):
                coin_event_time = received
        else:
            coin_event_time = received

        for curr in currencies:
            records.append(
                {
                    "id": f"{coin_id}:{curr}",
                    "coin_id": coin_id,
                    "currency": curr,
                    "price": info.get(curr, ""),
                    "change_24h_pct": round(info.get(f"{curr}_24h_change", 0) or 0, 2),
                    "volume_24h": info.get(f"{curr}_24h_vol", ""),
                    "market_cap": info.get(f"{curr}_market_cap", ""),
                    "event_time": coin_event_time,
                    "last_updated_at": last_updated if last_updated is not None else "",
                }
            )
    return records


def trending_records_from_api(
    trending: list[dict[str, Any]],
    *,
    event_time: Optional[str] = None,
) -> list[dict[str, Any]]:
    received = event_time or utc_now_iso()
    records: list[dict[str, Any]] = []
    for coin in trending:
        coin_id = str(coin.get("id", "")).strip()
        if not coin_id:
            continue
        records.append(
            {
                "id": coin_id,
                "coin_id": coin_id,
                "name": coin.get("name", ""),
                "symbol": coin.get("symbol", ""),
                "market_cap_rank": coin.get("market_cap_rank", ""),
                "score": coin.get("score", ""),
                "event_time": received,
            }
        )
    return records


def ingest_to_lake(
    *,
    raw: bytes,
    records: list[dict[str, Any]],
    dataset: str,
    data_lake_uri: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    content_type: str = "application/json",
    input_format: str = "json",
) -> dict[str, Any]:
    _sync_exc()
    return _pa_mod().ingest_to_lake(
        _contract(),
        raw=raw,
        records=records,
        dataset=dataset,
        data_lake_uri=data_lake_uri,
        metadata=metadata,
        content_type=content_type,
        input_format=input_format,
        provider="coingecko_public",
    )


def write_lineage(
    result: dict[str, Any],
    *,
    dataset: str,
    data_dir: Optional[Path] = None,
) -> Path:
    return _pa_mod().write_lineage(
        _contract(),
        result,
        dataset=dataset,
        data_dir=Path(data_dir or config.DATA_DIR),
    )


def load_lineage(data_dir: Optional[Path] = None) -> Optional[dict[str, Any]]:
    return _pa_mod().load_lineage(
        Path(data_dir or config.DATA_DIR),
        filename=config.LINEAGE_FILE,
    )


def resolve_lake_root(data_lake_uri: Optional[str] = None) -> Path:
    uri = data_lake_uri or default_data_lake_uri()
    return _pa_mod().resolve_lake_root(uri)


def bronze_dataset_dir(
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
) -> Path:
    return _pa_mod().bronze_dataset_dir(
        _contract(), dataset, data_lake_uri=data_lake_uri
    )


def list_bronze_parquet_files(
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
) -> list[Path]:
    return _pa_mod().list_bronze_parquet_files(
        _contract(), dataset, data_lake_uri=data_lake_uri
    )


def read_bronze_rows(
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    _sync_exc()
    return _pa_mod().read_bronze_rows(
        _contract(), dataset, data_lake_uri=data_lake_uri, sql=sql
    )


def read_silver_rows(
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    """Read the shared Silver Parquet projection for a product dataset."""
    _sync_exc()
    try:
        from data_lake.silver import read_silver_rows as _read_shared_silver  # type: ignore

        return _read_shared_silver(
            data_lake_uri=data_lake_uri or default_data_lake_uri(),
            domain=config.LAKE_DOMAIN,
            dataset=dataset,
            silver_schema_version="1",
            sql=sql,
        )
    except (LakeUnavailable, LakeIngestError):
        raise
    except Exception as exc:  # noqa: BLE001 - normalize shared runtime errors
        raise LakeIngestError(
            f"Silver DuckDB query failed for {dataset}: {exc}"
        ) from exc


def read_iceberg_rows(
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    _sync_exc()
    return _pa_mod().read_iceberg_rows(
        _contract(), dataset, data_lake_uri=data_lake_uri, sql=sql
    )


def parse_payload_json(row: dict[str, Any]) -> dict[str, Any]:
    return _pa_mod().parse_payload_json(row)


def select_latest_bronze_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _pa_mod().select_latest_bronze_rows(rows)
