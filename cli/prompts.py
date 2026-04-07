"""
cli/prompts.py
--------------
Interactive prompt helpers used when CLI args are omitted.

Isolated here so main() stays clean and these can be
replaced (or suppressed) in automated / scheduled runs.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def prompt_portfolio_id(default: int = 1) -> int:
    """
    Prompt the user for a PortfolioID.
    Returns *default* on empty input or invalid integer.
    """
    raw = input(f"\nPortfolioID to load (default {default}): ").strip()
    if not raw:
        log.debug("PortfolioID: user accepted default %d", default)
        return default
    try:
        pid = int(raw)
        log.debug("PortfolioID from prompt: %d", pid)
        return pid
    except ValueError:
        log.warning("Invalid PortfolioID '%s' — using default %d", raw, default)
        print(f"  ⚠ Invalid value '{raw}' — using default {default}")
        return default


def prompt_date(label: str) -> str:
    """
    Prompt for a YYYY-MM-DD date string.  No validation — passed through as-is
    to the API, which will reject bad formats with a clear error message.
    """
    value = input(f"{label} (YYYY-MM-DD): ").strip()
    log.debug("%s = %s", label, value)
    return value
