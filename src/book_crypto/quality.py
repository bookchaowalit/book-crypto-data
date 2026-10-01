"""Record-level data quality guards applied before Bronze and CSV writes.

Upstream JSON can carry ``NaN``/``Infinity`` literals (Python's ``json`` parses
them), non-numeric strings, negative values where only non-negative ones make
sense, and duplicate identifiers. These helpers let the normalizers drop or
blank such values explicitly instead of persisting them.
"""
from __future__ import annotations

import math
from collections import Counter
from typing import Any, Iterable, Optional


def finite_number(value: Any) -> Optional[float]:
    """Return ``value`` as a finite float, or ``None`` when it is not one.

    Booleans, blanks, non-numeric strings, NaN and +/-inf are rejected.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            number = float(value)
        except OverflowError:  # an int beyond float range (JSON allows 1e400 digits)
            return None
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            return None
    else:
        return None
    return number if math.isfinite(number) else None


# 9999-12-31T23:59:59Z; datetime cannot represent anything later.
MAX_EPOCH_MS = 253_402_300_799_999


def epoch_ms(value: Any) -> Optional[int]:
    """Return an integral epoch-millisecond timestamp in ``[0, MAX_EPOCH_MS]``.

    ``int()`` alone raised ``OverflowError`` on ``Infinity`` and truncated
    fractional or boolean values; out-of-range values crashed ``datetime``.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        number = value
    else:
        as_float = finite_number(value)
        if as_float is None or not as_float.is_integer():
            return None
        number = int(as_float)
    return number if 0 <= number <= MAX_EPOCH_MS else None


def non_negative_number(value: Any) -> Optional[float]:
    """Return a finite, non-negative float, or ``None``."""
    number = finite_number(value)
    if number is None or number < 0:
        return None
    return number


def dedupe_by_key(
    records: Iterable[dict[str, Any]],
    key: str = "id",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep the first record per ``key``; return ``(kept, duplicates)``."""
    seen: set[Any] = set()
    kept: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    for record in records:
        value = record.get(key)
        if value in seen:
            duplicates.append(record)
            continue
        seen.add(value)
        kept.append(record)
    return kept, duplicates


def summarize_rejections(rejected: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Count rejected rows by their ``reason`` field."""
    return dict(sorted(Counter(str(r.get("reason", "unknown")) for r in rejected).items()))
