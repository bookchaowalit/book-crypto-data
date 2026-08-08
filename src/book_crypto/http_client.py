"""Bounded free-only HTTP helper with timeout/429 handling."""
from __future__ import annotations

import time
from typing import Any, Callable, Optional

from . import config
from .policy import require_provider

try:
    import httpx
except ImportError:  # pragma: no cover - exercised in missing-dep environments
    httpx = None  # type: ignore


class UpstreamError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None, kind: str = "error"):
        super().__init__(message)
        self.status_code = status_code
        self.kind = kind


def ensure_httpx() -> Any:
    if httpx is None:
        raise UpstreamError("httpx is not installed; install with: pip install httpx", kind="dependency")
    return httpx


def request_json(
    method: str,
    url: str,
    *,
    provider: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    json_body: Optional[dict] = None,
    timeout: Optional[float] = None,
    transport: Any = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    """Perform an upstream request under free-only policy.

    Never used by the read-only API GET handlers.
    """
    require_provider(provider)
    client_mod = ensure_httpx()
    timeout = config.REQUEST_TIMEOUT_SECONDS if timeout is None else timeout
    last_error: Exception | None = None

    for attempt in range(config.MAX_RETRIES + 1):
        try:
            kwargs: dict[str, Any] = {
                "method": method,
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
            if json_body is not None:
                kwargs["json"] = json_body
            if transport is not None:
                with client_mod.Client(transport=transport, timeout=timeout) as client:
                    resp = client.request(**{k: v for k, v in kwargs.items() if k != "timeout"})
            else:
                resp = client_mod.request(**kwargs)

            if resp.status_code == 429:
                if attempt >= config.MAX_RETRIES:
                    raise UpstreamError("Upstream rate limited (HTTP 429)", status_code=429, kind="rate_limit")
                sleep(min(2 ** attempt, 8))
                continue

            if resp.status_code >= 400:
                raise UpstreamError(
                    f"Upstream HTTP {resp.status_code}",
                    status_code=resp.status_code,
                    kind="http_error",
                )

            content = resp.content
            if len(content) > config.MAX_RESPONSE_BYTES:
                raise UpstreamError("Upstream response exceeded size limit", kind="size_limit")
            return resp.json()
        except UpstreamError:
            raise
        except Exception as exc:  # timeout / network
            last_error = exc
            name = type(exc).__name__.lower()
            msg = str(exc).lower()
            if "timeout" in name or "timeout" in msg:
                if attempt >= config.MAX_RETRIES:
                    raise UpstreamError("Upstream request timed out", kind="timeout") from exc
                sleep(min(2 ** attempt, 8))
                continue
            raise UpstreamError(f"Upstream request failed: {exc}", kind="network") from exc

    raise UpstreamError(f"Upstream request failed: {last_error}", kind="network")
