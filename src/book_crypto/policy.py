"""Free-only provider policy for book-crypto-data."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import config


PROVIDERS: dict[str, dict[str, str]] = {
    'coingecko_public': {'status': 'free', 'reason': 'CoinGecko keyless public REST; rate-limited'},
    'coingecko_pro': {'status': 'blocked', 'reason': 'CoinGecko Pro requires paid plan'},
    'coingecko_webhooks': {'status': 'blocked', 'reason': 'Webhooks/high-frequency polling not free-default'},
}


@dataclass(frozen=True)
class ProviderDecision:
    name: str
    allowed: bool
    status: str
    reason: str


def provider_status(name: str) -> str:
    meta = PROVIDERS.get(name)
    if not meta:
        return "unknown"
    return meta["status"]


def evaluate_provider(name: str) -> ProviderDecision:
    meta = PROVIDERS.get(name)
    if meta is None:
        return ProviderDecision(
            name=name,
            allowed=False,
            status="unknown",
            reason="Provider is not positively classified as free for the intended use",
        )

    status = meta["status"]
    reason = meta["reason"]

    if status == "blocked":
        return ProviderDecision(name=name, allowed=False, status=status, reason=reason)

    if status == "unknown":
        # Unknown sources stay disabled unless an owner explicitly opts in to paid/unknown.
        if config.FREE_ONLY and not config.ALLOW_PAID_PROVIDERS:
            return ProviderDecision(
                name=name,
                allowed=False,
                status=status,
                reason=reason + " (disabled while FREE_ONLY=true)",
            )
        return ProviderDecision(name=name, allowed=True, status=status, reason=reason)

    # free
    if status != "free":
        return ProviderDecision(
            name=name,
            allowed=False,
            status="unknown",
            reason=f"Unexpected provider status: {status}",
        )

    # Even free providers must not be used if ALLOW_PAID_PROVIDERS is false and
    # FREE_ONLY is true — free remains allowed.
    if not config.ALLOW_PAID_PROVIDERS and status == "blocked":
        return ProviderDecision(name=name, allowed=False, status=status, reason=reason)

    return ProviderDecision(name=name, allowed=True, status=status, reason=reason)


def require_provider(name: str) -> ProviderDecision:
    decision = evaluate_provider(name)
    if not decision.allowed:
        raise PermissionError(
            f"Provider {name!r} blocked (status={decision.status}): {decision.reason}"
        )
    return decision


def external_writes_allowed() -> bool:
    return bool(config.ALLOW_EXTERNAL_WRITES) and not config.FREE_ONLY


def require_external_writes(action: str) -> None:
    if not external_writes_allowed():
        raise PermissionError(
            f"External write {action!r} blocked: ALLOW_EXTERNAL_WRITES=false / FREE_ONLY=true"
        )


def provider_matrix() -> list[dict[str, Any]]:
    rows = []
    for name in sorted(PROVIDERS):
        decision = evaluate_provider(name)
        rows.append(
            {
                "provider": name,
                "classification": PROVIDERS[name]["status"],
                "allowed_now": decision.allowed,
                "reason": decision.reason,
            }
        )
    return rows


def missing_credential_status(env_var: str) -> dict[str, Any]:
    import os

    present = bool(os.environ.get(env_var, "").strip())
    return {
        "credential": env_var,
        "present": present,
        "status": "present" if present else "missing",
        "effect": "ignored_in_free_only" if not present else "optional_only",
    }
