"""
core/utils.py
-------------
Pure utility functions with no side effects.
Kept separate so they can be tested independently of
any I/O or external service.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Union


# ──────────────────────────────────────────────
# Time helpers
# ──────────────────────────────────────────────
def ms_to_datetime(ms: int | float) -> datetime:
    """
    Convert epoch milliseconds → naive UTC datetime
    (tzinfo stripped so SQL Server datetime2 is happy).
    """
    dt_utc = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)
    return dt_utc.replace(tzinfo=None)


def normalize_quote_time(
    raw: Union[int, float, str, None],
) -> datetime | None:
    """
    Coerce a Schwab quote-time value to a naive UTC datetime.

    Handles
    -------
    - epoch milliseconds (int/float > 10^10)
    - epoch seconds      (int/float ≤ 10^10)
    - ISO 8601 string    (e.g. '2024-12-06T15:30:00Z')
    - None / unknown     → returns None
    """
    if isinstance(raw, (int, float)):
        ts = float(raw)
        if ts > 1e10:
            return ms_to_datetime(ts)
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt_utc.replace(tzinfo=None)

    if isinstance(raw, str):
        try:
            normalized = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            return dt.replace(tzinfo=None)
        except ValueError:
            return None

    return None
