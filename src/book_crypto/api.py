"""Local read-only HTTP API for book-crypto-data.

Serves normalized Bronze Parquet data via DuckDB. GET handlers never scrape
upstream providers and do not read local CSV projections. Bind defaults to
127.0.0.1. POST /v1/refresh is disabled by default (403).
"""
from __future__ import annotations

import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

from . import config
from .lake import load_lineage
from .policy import provider_matrix
from .store import envelope, get_record, load_history, load_records, paginate, utc_now_iso


# Browser consumers (insights/portfolio) run on other loopback ports and need CORS.
# Allow only local development origins — never reflect arbitrary Origin values.
_LOCAL_ORIGIN_RE = re.compile(r"^https?://(127\.0\.0\.1|localhost|\[::1\])(:\d+)?$", re.I)


def _allowed_cors_origin(origin: str | None) -> str | None:
    if not origin:
        return None
    origin = origin.strip()
    if _LOCAL_ORIGIN_RE.match(origin):
        return origin
    return None


def _apply_cors_headers(handler: BaseHTTPRequestHandler) -> None:
    origin = _allowed_cors_origin(handler.headers.get("Origin"))
    if not origin:
        return
    handler.send_header("Access-Control-Allow-Origin", origin)
    handler.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Accept, Content-Type")
    handler.send_header("Access-Control-Max-Age", "600")
    handler.send_header("Vary", "Origin")


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: dict[str, Any]) -> None:
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(payload)))
    handler.send_header("Cache-Control", "no-store")
    _apply_cors_headers(handler)
    handler.end_headers()
    handler.wfile.write(payload)


def _query_int(qs: dict[str, list[str]], name: str, default: int) -> int:
    raw = qs.get(name, [str(default)])[0]
    try:
        return int(raw)
    except ValueError:
        return default


class DataProductHandler(BaseHTTPRequestHandler):
    server_version = "book-crypto-data-api/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:  # quieter tests
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def do_OPTIONS(self) -> None:  # noqa: N802
        """CORS preflight for local browser consumers (GET-only)."""
        origin = _allowed_cors_origin(self.headers.get("Origin"))
        if not origin:
            self.send_response(403)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(b'{"error":"origin not allowed"}')
            return
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Accept, Content-Type")
        self.send_header("Access-Control-Max-Age", "600")
        self.send_header("Vary", "Origin")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        qs = parse_qs(parsed.query)

        if path == "/healthz":
            records = load_records()
            body = {
                "status": "ok",
                "repository": config.REPO_NAME,
                "domain": config.DOMAIN,
                "data_status": records["data_status"],
                "retrieved_at": utc_now_iso(),
                "free_only": config.FREE_ONLY,
                "allow_paid_providers": config.ALLOW_PAID_PROVIDERS,
                "allow_external_writes": config.ALLOW_EXTERNAL_WRITES,
                "allow_refresh": config.ALLOW_REFRESH,
            }
            return _json_response(self, 200, body)

        if path == "/v1/metadata":
            records = load_records()
            history = load_history()
            lineage = load_lineage()
            item = {
                "repository": config.REPO_NAME,
                "domain": config.DOMAIN,
                "schema_version": config.SCHEMA_VERSION,
                "record_count": len(records["items"]),
                "history_count": len(history["items"]),
                "records_path": records["path"],
                "history_path": history["path"],
                "records_source_kind": records.get("source_kind"),
                "history_source_kind": history.get("source_kind"),
                "silver_read_mode": records.get(
                    "silver_read_mode", config.SILVER_READ_MODE
                ),
                "silver_parity": records.get("silver_parity"),
                "providers": provider_matrix(),
                "api_bind": f"{config.API_HOST}:{config.API_PORT}",
                "free_only": config.FREE_ONLY,
                "allow_paid_providers": config.ALLOW_PAID_PROVIDERS,
                "allow_external_writes": config.ALLOW_EXTERNAL_WRITES,
                "allow_refresh": config.ALLOW_REFRESH,
                # Bronze remains the durable contract. The read engine is a
                # feature flag so the hosted Iceberg catalog can be compared
                # without changing the product envelope or CSV boundary.
                "storage_model": (
                    "lake_first_silver_s3_duckdb"
                    if str(records.get("source_kind", "")).startswith("silver_s3_")
                    else "lake_first_silver_duckdb"
                    if str(records.get("source_kind", "")).startswith("silver_")
                    else "lake_first_iceberg_rest_duckdb"
                    if str(records.get("source_kind", "")).startswith("iceberg_")
                    else "lake_first_bronze_s3_duckdb"
                    if str(records.get("source_kind", "")).startswith("bronze_s3_parquet")
                    else "lake_first_bronze_duckdb"
                ),
                "read_mode": records.get("read_mode", config.LAKE_READ_MODE),
                "read_fallback": records.get("read_fallback", config.LAKE_READ_FALLBACK),
                "fallback_reason": records.get("fallback_reason"),
                "lake_domain": config.LAKE_DOMAIN,
                "lake_source": config.LAKE_SOURCE,
                "lake_dataset_prices": config.LAKE_DATASET_PRICES,
                "lake_lineage": lineage,
            }
            return _json_response(
                self,
                200,
                envelope(
                    items=[item],
                    data_status=records["data_status"],
                    retrieved_at=records["retrieved_at"],
                ),
            )

        if path == "/v1/records":
            records = load_records()
            limit = _query_int(qs, "limit", 50)
            cursor = qs.get("cursor", [None])[0]
            page, next_cursor = paginate(records["items"], limit=limit, cursor=cursor)
            return _json_response(
                self,
                200,
                envelope(
                    items=page,
                    data_status=records["data_status"],
                    next_cursor=next_cursor,
                    retrieved_at=records["retrieved_at"],
                ),
            )

        if path.startswith("/v1/records/"):
            record_id = unquote(path[len("/v1/records/"):])
            if not record_id or record_id == "/":
                return _json_response(
                    self,
                    400,
                    envelope(items=[], data_status="error", extra={"error": "missing record_id"}),
                )
            item = get_record(record_id)
            if item is None:
                return _json_response(
                    self,
                    404,
                    envelope(
                        items=[],
                        data_status="not_found",
                        extra={"error": "record not found", "record_id": record_id},
                    ),
                )
            records = load_records()
            return _json_response(
                self,
                200,
                envelope(
                    items=[item],
                    data_status=records["data_status"],
                    retrieved_at=records["retrieved_at"],
                ),
            )

        if path == "/v1/history":
            history = load_history()
            limit = _query_int(qs, "limit", 50)
            cursor = qs.get("cursor", [None])[0]
            page, next_cursor = paginate(history["items"], limit=limit, cursor=cursor)
            return _json_response(
                self,
                200,
                envelope(
                    items=page,
                    data_status=history["data_status"],
                    next_cursor=next_cursor,
                    retrieved_at=history["retrieved_at"],
                ),
            )

        return _json_response(
            self,
            404,
            envelope(items=[], data_status="not_found", extra={"error": "unknown endpoint"}),
        )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path != "/v1/refresh":
            return _json_response(
                self,
                404,
                envelope(items=[], data_status="not_found", extra={"error": "unknown endpoint"}),
            )

        # Disabled by default. Requires ALLOW_REFRESH=true and matching token.
        auth = self.headers.get("Authorization", "")
        token = ""
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
        allowed = (
            config.ALLOW_REFRESH
            and bool(config.REFRESH_TOKEN)
            and token
            and token == config.REFRESH_TOKEN
        )
        if not allowed:
            return _json_response(
                self,
                403,
                envelope(
                    items=[],
                    data_status="forbidden",
                    extra={
                        "error": "refresh disabled",
                        "detail": "POST /v1/refresh returns 403 unless ALLOW_REFRESH=true and a local REFRESH_TOKEN matches",
                    },
                ),
            )

        # Even when enabled, this endpoint only acknowledges; it does not auto-scrape
        # in this free-only slice (owner must run CLI separately).
        return _json_response(
            self,
            202,
            envelope(
                items=[{"accepted": True, "action": "refresh_acknowledged"}],
                data_status="accepted",
                extra={
                    "note": (
                        "Run the CLI ingest command to write Bronze Parquet; "
                        "CSV projection is optional for CLI UX only."
                    )
                },
            ),
        )


def create_server(host: Optional[str] = None, port: Optional[int] = None) -> ThreadingHTTPServer:
    host = host or config.API_HOST
    port = port if port is not None else config.API_PORT
    return ThreadingHTTPServer((host, port), DataProductHandler)


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=f"Local read-only API for book-crypto-data")
    parser.add_argument("--host", default=config.API_HOST, help="Bind host (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=config.API_PORT, help="Bind port")
    args = parser.parse_args(argv)

    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        print(
            "WARNING: binding outside loopback requires owner approval and is not the free-only default.",
            file=sys.stderr,
        )

    server = create_server(args.host, args.port)
    print(f"[{config.REPO_NAME}] API listening on http://{args.host}:{args.port}")
    print("  GET /healthz /v1/metadata /v1/records /v1/records/{id} /v1/history")
    print("  POST /v1/refresh (403 by default)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
