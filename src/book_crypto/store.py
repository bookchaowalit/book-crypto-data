"""Read-only data store for the book-crypto-data API.

Primary path: Bronze Parquet via DuckDB (lake-first).
CSV under ``data/`` is an optional CLI projection only and is not used by the
HTTP API loaders.
"""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote

from . import config
from . import lake


TIMESTAMP_KEYS = (
    "updated_at",
    "event_time",
    "scraped_at",
    "date",
    "timestamp",
    "retrieved_at",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_ts(value: str) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def make_record_id(row: dict[str, Any]) -> str:
    parts = []
    for field in config.ID_FIELDS:
        raw = str(row.get(field, "")).strip()
        if not raw and field == "url":
            raw = str(row.get("name", "") or row.get("title", "")).strip()
        parts.append(raw)
    joined = config.ID_SEP.join(parts)
    if not joined or joined == config.ID_SEP * (len(parts) - 1):
        blob = json.dumps(row, sort_keys=True, default=str)
        return "hash:" + hashlib.sha1(blob.encode("utf-8")).hexdigest()[:16]
    # URLs and other path-hostile IDs become stable hashes so /v1/records/{id} works
    if any(ch in joined for ch in ("://", "?", "#", " ")) or joined.count("/") > 0:
        return "hash:" + hashlib.sha1(joined.encode("utf-8")).hexdigest()[:16]
    return joined.replace("/", "_")


def _freshness(rows: list[dict[str, Any]]) -> tuple[str, Optional[str]]:
    if not rows:
        return "empty", None
    latest: Optional[datetime] = None
    latest_raw = None
    for row in rows:
        for key in TIMESTAMP_KEYS:
            if key in row and row[key]:
                parsed = _parse_ts(str(row[key]))
                if parsed and (latest is None or parsed > latest):
                    latest = parsed
                    latest_raw = str(row[key])
                break
    if latest is None:
        return "ok", latest_raw
    age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600.0
    if age_hours > config.STALE_AFTER_HOURS:
        return "stale", latest_raw
    return "ok", latest_raw


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def bronze_price_item(row: dict[str, Any], *, history: bool = False, history_idx: int = 0) -> dict[str, Any]:
    """Project one Bronze envelope row into the product API item shape."""
    payload = lake.parse_payload_json(row)
    event_time = _stringify(row.get("event_time") or payload.get("event_time") or "")
    item: dict[str, Any] = {
        "coin_id": _stringify(payload.get("coin_id", "")),
        "currency": _stringify(payload.get("currency", "")),
        "price": _stringify(payload.get("price", "")),
        "change_24h_pct": _stringify(payload.get("change_24h_pct", "")),
        "volume_24h": _stringify(payload.get("volume_24h", "")),
        "market_cap": _stringify(payload.get("market_cap", "")),
        "updated_at": event_time,
        "event_time": event_time,
        "ingest_run_id": _stringify(row.get("ingest_run_id", "")),
        "source_record_id": _stringify(row.get("source_record_id", "")),
        "raw_object_key": _stringify(row.get("raw_object_key", "")),
    }
    if history:
        item["date"] = event_time
        item["record_id"] = make_record_id(item) + f"#h{history_idx}"
    else:
        item["record_id"] = make_record_id(item)
    return item


def silver_price_item(row: dict[str, Any], *, history: bool = False, history_idx: int = 0) -> dict[str, Any]:
    """Project one shared Silver row into the same API item shape as Bronze."""
    event_time = _stringify(row.get("event_time") or row.get("last_updated_at") or "")
    item: dict[str, Any] = {
        "coin_id": _stringify(row.get("coin_id", "")),
        "currency": _stringify(row.get("currency", "")),
        "price": _stringify(row.get("price", "")),
        "change_24h_pct": _stringify(row.get("change_24h_pct", "")),
        "volume_24h": _stringify(row.get("volume_24h", "")),
        "market_cap": _stringify(row.get("market_cap", "")),
        "updated_at": event_time,
        "event_time": event_time,
        "ingest_run_id": _stringify(row.get("bronze_ingest_run_id", "")),
        "source_record_id": _stringify(row.get("source_record_id", "")),
        "raw_object_key": _stringify(row.get("raw_object_key", "")),
    }
    if history:
        item["date"] = event_time
        item["record_id"] = make_record_id(item) + f"#h{history_idx}"
    else:
        item["record_id"] = make_record_id(item)
    return item


def _silver_read_mode() -> str:
    mode = str(getattr(config, "SILVER_READ_MODE", "bronze")).strip().lower()
    if mode not in {"bronze", "compare", "silver"}:
        raise lake.LakeIngestError(
            "SILVER_READ_MODE must be one of: bronze, compare, silver"
        )
    return mode


def _parity_key(item: dict[str, Any]) -> str:
    return "|".join(
        _stringify(item.get(field))
        for field in ("source_record_id", "event_time", "coin_id", "currency")
    )


def _parity_report(
    bronze_items: list[dict[str, Any]],
    silver_items: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compare API-visible values, excluding physical-layer lineage fields."""
    fields = (
        "coin_id",
        "currency",
        "price",
        "change_24h_pct",
        "volume_24h",
        "market_cap",
        "event_time",
    )
    bronze = {_parity_key(item): item for item in bronze_items}
    silver = {_parity_key(item): item for item in silver_items}
    added = sorted(set(silver) - set(bronze))
    removed = sorted(set(bronze) - set(silver))
    changed: list[dict[str, Any]] = []
    for key in sorted(set(bronze) & set(silver)):
        differences = {
            field: {"bronze": bronze[key].get(field), "silver": silver[key].get(field)}
            for field in fields
            if bronze[key].get(field) != silver[key].get(field)
        }
        if differences:
            changed.append({"key": key, "fields": differences})
    return {
        "status": "passed" if not added and not removed and not changed else "failed",
        "bronze_count": len(bronze_items),
        "silver_count": len(silver_items),
        "added_count": len(added),
        "removed_count": len(removed),
        "changed_count": len(changed),
        "examples": {
            "added": added[:5],
            "removed": removed[:5],
            "changed": changed[:5],
        },
    }


def _lake_uri_for_reads() -> Optional[str]:
    if config.DATA_LAKE_URI.strip():
        return config.DATA_LAKE_URI.strip()
    return lake.default_data_lake_uri()


def _display_dataset_path(uri: str, dataset: str) -> str:
    """Return a safe path label for local and remote lake responses."""
    try:
        return str(lake.bronze_dataset_dir(dataset, data_lake_uri=uri))
    except Exception:  # noqa: BLE001 - remote s3:// has no local Path
        return (
            f"{uri.rstrip('/')}/bronze/domain={config.LAKE_DOMAIN}/"
            f"dataset={dataset}/schema_version={config.LAKE_BRONZE_SCHEMA_VERSION}"
        )


def _display_silver_dataset_path(uri: str, dataset: str) -> str:
    """Return a safe Silver path label for local and remote responses."""
    return (
        f"{uri.rstrip('/')}/silver/domain={config.LAKE_DOMAIN}/"
        f"dataset={dataset}/schema_version=1"
    )


def _read_silver_dataset_rows(
    dataset: str,
    *,
    data_lake_uri: str,
) -> tuple[list[dict[str, Any]], str]:
    rows = lake.read_silver_rows(dataset, data_lake_uri=data_lake_uri)
    return rows, "silver_s3_parquet" if lake.is_remote_lake_uri(data_lake_uri) else "silver_parquet"


def _read_price_projection(
    *,
    data_lake_uri: str,
    history: bool,
) -> tuple[list[dict[str, Any]], str, Optional[str], Optional[dict[str, Any]], str]:
    """Read Bronze and optionally compare/serve the shared Silver projection."""
    mode = _silver_read_mode()
    bronze_rows, bronze_source_kind, fallback_reason = _read_dataset_rows(
        config.LAKE_DATASET_PRICES,
        data_lake_uri=data_lake_uri,
    )
    if history:
        bronze_items = [
            bronze_price_item(row, history=True, history_idx=idx)
            for idx, row in enumerate(bronze_rows)
        ]
    else:
        bronze_items = [
            bronze_price_item(row, history=False)
            for row in lake.select_latest_bronze_rows(bronze_rows)
        ]
    if mode == "bronze":
        return bronze_items, bronze_source_kind, fallback_reason, None, mode
    if history:
        # The current crypto Silver contract is a deduplicated snapshot. It is
        # not a historical table, so keep /v1/history on Bronze until a
        # separately versioned crypto_prices_history Silver contract exists.
        return (
            bronze_items,
            bronze_source_kind,
            fallback_reason,
            {
                "status": "not_applicable",
                "reason": "crypto_prices Silver is a current snapshot; history remains Bronze",
                "bronze_count": len(bronze_items),
                "silver_count": 0,
            },
            mode,
        )

    try:
        silver_rows, silver_source_kind = _read_silver_dataset_rows(
            config.LAKE_DATASET_PRICES,
            data_lake_uri=data_lake_uri,
        )
        silver_items = [
            silver_price_item(row, history=False)
            for row in lake.select_latest_bronze_rows(silver_rows)
        ]
        parity = _parity_report(bronze_items, silver_items)
    except (lake.LakeUnavailable, lake.LakeIngestError) as exc:
        parity = {
            "status": "unavailable",
            "bronze_count": len(bronze_items),
            "silver_count": 0,
            "added_count": 0,
            "removed_count": 0,
            "changed_count": 0,
            "error": f"{type(exc).__name__}: {exc}",
        }
        if mode == "silver":
            raise lake.LakeIngestError(
                f"Silver serving is unavailable: {parity['error']}"
            ) from exc
        return bronze_items, bronze_source_kind, fallback_reason, parity, mode

    if mode == "silver":
        if parity["status"] != "passed":
            raise lake.LakeIngestError(
                "Silver serving is fail-closed until parity passes: "
                + json.dumps(parity, ensure_ascii=False, sort_keys=True)
            )
        return silver_items, silver_source_kind, None, parity, mode
    return bronze_items, bronze_source_kind, fallback_reason, parity, mode


def _read_dataset_rows(
    dataset: str,
    *,
    data_lake_uri: str,
) -> tuple[list[dict[str, Any]], str, Optional[str]]:
    """Read one dataset with an explicit catalog mode and optional fallback."""
    mode = config.LAKE_READ_MODE
    fallback = config.LAKE_READ_FALLBACK
    if mode not in {"parquet", "iceberg"}:
        raise lake.LakeIngestError(
            "LAKE_READ_MODE must be one of: parquet, iceberg"
        )
    if fallback not in {"error", "parquet"}:
        raise lake.LakeIngestError(
            "LAKE_READ_FALLBACK must be one of: error, parquet"
        )

    if mode == "parquet":
        return (
            lake.read_bronze_rows(dataset, data_lake_uri=data_lake_uri),
            "bronze_s3_parquet" if lake.is_remote_lake_uri(data_lake_uri) else "bronze_parquet",
            None,
        )

    try:
        return (
            lake.read_iceberg_rows(dataset, data_lake_uri=data_lake_uri),
            "iceberg_rest_duckdb",
            None,
        )
    except (lake.LakeUnavailable, lake.LakeIngestError) as exc:
        if fallback != "parquet":
            raise
        rows = lake.read_bronze_rows(dataset, data_lake_uri=data_lake_uri)
        return (
            rows,
            "bronze_s3_parquet_fallback" if lake.is_remote_lake_uri(data_lake_uri) else "bronze_parquet_fallback",
            f"{type(exc).__name__}: {exc}",
        )


def load_records(
    data_dir: Optional[Path] = None,
    *,
    data_lake_uri: Optional[str] = None,
) -> dict[str, Any]:
    """Load latest prices from Bronze, or the guarded Silver pilot."""
    del data_dir  # API/store path is lake-only; kept for call-site compatibility.
    uri = data_lake_uri or _lake_uri_for_reads()
    mode = str(getattr(config, "SILVER_READ_MODE", "bronze")).strip().lower()
    path = (
        _display_silver_dataset_path(uri, config.LAKE_DATASET_PRICES)
        if mode == "silver"
        else _display_dataset_path(uri, config.LAKE_DATASET_PRICES)
    )
    try:
        items, source_kind, fallback_reason, parity, mode = _read_price_projection(
            data_lake_uri=uri,
            history=False,
        )
        status, _ = _freshness(items)
        return {
            "items": items,
            "data_status": status if items else "empty",
            "error": None,
            "path": path,
            "source_kind": source_kind,
            "read_mode": config.LAKE_READ_MODE,
            "read_fallback": config.LAKE_READ_FALLBACK,
            "fallback_reason": fallback_reason,
            "silver_read_mode": mode,
            "silver_parity": parity,
            "retrieved_at": utc_now_iso(),
        }
    except lake.LakeUnavailable as exc:
        return {
            "items": [],
            "data_status": "error",
            "error": str(exc),
            "path": path,
            "source_kind": "silver_parquet" if mode == "silver" else "iceberg_rest_duckdb" if config.LAKE_READ_MODE == "iceberg" else "bronze_parquet",
            "read_mode": config.LAKE_READ_MODE,
            "read_fallback": config.LAKE_READ_FALLBACK,
            "fallback_reason": None,
            "silver_read_mode": mode,
            "silver_parity": None,
            "retrieved_at": utc_now_iso(),
        }
    except lake.LakeIngestError as exc:
        return {
            "items": [],
            "data_status": "malformed",
            "error": str(exc),
            "path": path,
            "source_kind": "silver_parquet" if mode == "silver" else "iceberg_rest_duckdb" if config.LAKE_READ_MODE == "iceberg" else "bronze_parquet",
            "read_mode": config.LAKE_READ_MODE,
            "read_fallback": config.LAKE_READ_FALLBACK,
            "fallback_reason": None,
            "silver_read_mode": mode,
            "silver_parity": None,
            "retrieved_at": utc_now_iso(),
        }


def load_history(
    data_dir: Optional[Path] = None,
    *,
    data_lake_uri: Optional[str] = None,
) -> dict[str, Any]:
    """Load full price history from Bronze or the guarded Silver pilot."""
    del data_dir
    uri = data_lake_uri or _lake_uri_for_reads()
    mode = str(getattr(config, "SILVER_READ_MODE", "bronze")).strip().lower()
    path = _display_dataset_path(uri, config.LAKE_DATASET_PRICES)
    try:
        items, source_kind, fallback_reason, parity, mode = _read_price_projection(
            data_lake_uri=uri,
            history=True,
        )
        status, _ = _freshness(items)
        return {
            "items": items,
            "data_status": status if items else "empty",
            "error": None,
            "path": path,
            "source_kind": source_kind,
            "read_mode": config.LAKE_READ_MODE,
            "read_fallback": config.LAKE_READ_FALLBACK,
            "fallback_reason": fallback_reason,
            "silver_read_mode": mode,
            "silver_parity": parity,
            "retrieved_at": utc_now_iso(),
        }
    except lake.LakeUnavailable as exc:
        return {
            "items": [],
            "data_status": "error",
            "error": str(exc),
            "path": path,
            "source_kind": "iceberg_rest_duckdb" if config.LAKE_READ_MODE == "iceberg" else "bronze_parquet",
            "read_mode": config.LAKE_READ_MODE,
            "read_fallback": config.LAKE_READ_FALLBACK,
            "fallback_reason": None,
            "silver_read_mode": mode,
            "silver_parity": None,
            "retrieved_at": utc_now_iso(),
        }
    except lake.LakeIngestError as exc:
        return {
            "items": [],
            "data_status": "malformed",
            "error": str(exc),
            "path": path,
            "source_kind": "iceberg_rest_duckdb" if config.LAKE_READ_MODE == "iceberg" else "bronze_parquet",
            "read_mode": config.LAKE_READ_MODE,
            "read_fallback": config.LAKE_READ_FALLBACK,
            "fallback_reason": None,
            "silver_read_mode": mode,
            "silver_parity": None,
            "retrieved_at": utc_now_iso(),
        }


def get_record(
    record_id: str,
    data_dir: Optional[Path] = None,
    *,
    data_lake_uri: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    record_id = unquote(record_id)
    payload = load_records(data_dir, data_lake_uri=data_lake_uri)
    for item in payload["items"]:
        if item.get("record_id") == record_id:
            return item
    return None


def paginate(
    items: list[dict[str, Any]],
    *,
    limit: int = 50,
    cursor: Optional[str] = None,
) -> tuple[list[dict[str, Any]], Optional[str]]:
    limit = max(1, min(int(limit), 500))
    start = 0
    if cursor:
        try:
            start = max(0, int(cursor))
        except ValueError:
            start = 0
    end = start + limit
    page = items[start:end]
    next_cursor = str(end) if end < len(items) else None
    return page, next_cursor


def envelope(
    *,
    items: list[dict[str, Any]],
    data_status: str,
    next_cursor: Optional[str] = None,
    retrieved_at: Optional[str] = None,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    body = {
        "schema_version": config.SCHEMA_VERSION,
        "source": config.REPO_NAME,
        "retrieved_at": retrieved_at or utc_now_iso(),
        "data_status": data_status,
        "items": items,
        "next_cursor": next_cursor,
    }
    if extra:
        body.update(extra)
    return body


def _read_csv(path: Path) -> tuple[list[dict[str, str]], Optional[str]]:
    """CLI-only helper for optional CSV projections."""
    if not path.exists():
        return [], None
    try:
        text = path.read_text(encoding="utf-8")
        if not text.strip():
            return [], None
        reader = csv.DictReader(text.splitlines())
        rows = [dict(row) for row in reader]
        return rows, None
    except Exception as exc:  # noqa: BLE001
        return [], f"malformed: {exc}"


def load_csv_projection(
    *,
    kind: str = "records",
    data_dir: Optional[Path] = None,
) -> dict[str, Any]:
    """Read optional local CSV projection (CLI UX only; not used by HTTP API)."""
    data_dir = Path(data_dir or config.DATA_DIR)
    if kind == "history":
        path = data_dir / config.HISTORY_FILE
    else:
        path = data_dir / config.RECORDS_FILE
    rows, err = _read_csv(path)
    if err:
        return {
            "items": [],
            "data_status": "malformed",
            "error": err,
            "path": str(path),
            "source_kind": "csv_projection",
            "retrieved_at": utc_now_iso(),
        }
    status, _ = _freshness(rows)
    items = []
    for idx, row in enumerate(rows):
        item = dict(row)
        if kind == "history":
            item["record_id"] = make_record_id(row) + f"#h{idx}"
        else:
            item["record_id"] = make_record_id(row)
        items.append(item)
    return {
        "items": items,
        "data_status": status if rows else "empty",
        "error": None,
        "path": str(path),
        "source_kind": "csv_projection",
        "retrieved_at": utc_now_iso(),
    }


def seed_fixtures(data_dir: Optional[Path] = None) -> Path:
    """Copy package fixtures into the local data directory (CLI dry-run helper).

    Seeds optional CSV projections only. Does not write Bronze and does not
    make the HTTP API serve these files.
    """
    data_dir = Path(data_dir or config.DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    src_records = config.FIXTURES_DIR / config.RECORDS_FILE
    src_history = config.FIXTURES_DIR / config.HISTORY_FILE
    if src_records.exists():
        (data_dir / config.RECORDS_FILE).write_text(src_records.read_text(encoding="utf-8"), encoding="utf-8")
    if src_history.exists():
        (data_dir / config.HISTORY_FILE).write_text(src_history.read_text(encoding="utf-8"), encoding="utf-8")
    return data_dir


def seed_lake_from_sample(
    sample_api: dict[str, Any],
    currencies: list[str],
    *,
    data_lake_uri: str,
    data_dir: Optional[Path] = None,
) -> dict[str, Any]:
    """Test/dev helper: write one price batch into Bronze for API reads."""
    records = lake.price_records_from_api(sample_api, currencies)
    raw = json.dumps(sample_api, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    result = lake.ingest_to_lake(
        raw=raw,
        records=records,
        dataset=config.LAKE_DATASET_PRICES,
        data_lake_uri=data_lake_uri,
        metadata={"seed": "sample"},
    )
    if data_dir is not None:
        lake.write_lineage(result, dataset=config.LAKE_DATASET_PRICES, data_dir=data_dir)
    return result
