"""Atomic local-state writes for CSV projections and other small files.

A crash or ``Ctrl-C`` mid-write must never leave a truncated projection behind:
content goes to a temporary file in the same directory, is flushed and
fsynced, and then replaces the target with ``os.replace`` (atomic on POSIX
and Windows for same-filesystem paths).
"""
from __future__ import annotations

import csv
import io
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable, Sequence


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
    return path


def csv_text(
    fieldnames: Sequence[str],
    rows: Iterable[dict[str, Any]],
    *,
    header: bool = True,
) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(fieldnames))
    if header:
        writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def atomic_write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[dict[str, Any]],
) -> Path:
    """Replace ``path`` with a CSV of ``rows`` atomically."""
    return atomic_write_text(path, csv_text(fieldnames, rows))


def atomic_append_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[dict[str, Any]],
) -> Path:
    """Append ``rows`` by rewriting the file atomically (header once).

    Projections are small operational views, so copying the existing content is
    cheap and guarantees the file is either the old or the new version.
    """
    path = Path(path)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if existing and not existing.endswith("\n"):
        existing += "\r\n"
    body = csv_text(fieldnames, rows, header=not existing.strip())
    return atomic_write_text(path, existing + body)
