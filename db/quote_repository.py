"""
db/quote_repository.py
----------------------
Data access layer for persisting Schwab quote data.

Design patterns used:
  - Repository    : QuoteRepository owns all SQL write logic for quotes.
  - Unit of Work  : save() runs truncate + batch insert + stored-proc call
                    as a single transaction; either all succeed or all roll back.
  - Dependency Injection : factory injected; easy to swap for a test double.
"""

from __future__ import annotations

import json
import logging

from core.logging_setup import TRACE_LEVEL, traced
from core.utils import normalize_quote_time
from db.connection import SqlConnectionFactory

log = logging.getLogger(__name__)

_INSERT_SQL = """
    INSERT INTO dbo.SchwabQuotes_Raw
        (Symbol, LastPrice, BidPrice, AskPrice, QuoteTime, RawJson)
    VALUES
        (?, ?, ?, ?, ?, ?)
"""

_TRUNCATE_RAW   = "TRUNCATE TABLE dbo.SchwabQuotes_Raw;"
_TRUNCATE_STAGE = "TRUNCATE TABLE dbo.SchwabQuotes_Stage;"
_LOAD_PROC      = "EXEC usp_Load_SchwabQuotes;"


class QuoteRepository:
    """
    Writes Schwab quote payloads to SQL Server staging tables.

    Parameters
    ----------
    factory : SqlConnectionFactory
        Provides managed database connections.
    """

    def __init__(self, factory: SqlConnectionFactory) -> None:
        self._factory = factory

    @traced
    def save(self, quotes: dict) -> int:
        """
        Persist a batch of quotes as a single Unit of Work:

        1. Truncate ``dbo.SchwabQuotes_Raw`` and ``dbo.SchwabQuotes_Stage``.
        2. Insert one row per symbol into ``dbo.SchwabQuotes_Raw``.
        3. Execute ``usp_Load_SchwabQuotes`` to transform into the stage table.

        Parameters
        ----------
        quotes : dict
            Raw Schwab quotes response
            ``{symbol: {quote: {...}, fundamental: {...}}, ...}``

        Returns
        -------
        int  Number of rows inserted into ``dbo.SchwabQuotes_Raw``.

        Raises
        ------
        ValueError  If *quotes* is empty.
        """
        if not quotes:
            log.warning("save() called with empty quotes dict — nothing to persist")
            raise ValueError("quotes dict must not be empty")

        log.info("Persisting %d quote(s) to SQL Server", len(quotes))

        with self._factory.connect() as conn:
            cursor = conn.cursor()

            # ── Step 1: truncate staging tables ───────────────────
            log.debug("Truncating raw/stage tables")
            cursor.execute(_TRUNCATE_RAW)
            cursor.execute(_TRUNCATE_STAGE)
            conn.commit()
            log.info("Staging tables cleared")

            # ── Step 2: batch insert ──────────────────────────────
            rows_inserted = 0
            for symbol, payload in quotes.items():
                row = self._build_row(symbol, payload)
                log.log(
                    TRACE_LEVEL,
                    "INSERT symbol=%s  last=%s  bid=%s  ask=%s  time=%s",
                    row[0], row[1], row[2], row[3], row[4],
                )
                cursor.execute(_INSERT_SQL, row)
                rows_inserted += 1

            conn.commit()
            log.info("Inserted %d row(s) into dbo.SchwabQuotes_Raw", rows_inserted)

            # ── Step 3: stored procedure ──────────────────────────
            log.debug("Executing usp_Load_SchwabQuotes")
            cursor.execute(_LOAD_PROC)
            conn.commit()
            log.info("usp_Load_SchwabQuotes completed")

        return rows_inserted

    # ── Private helpers ────────────────────────
    @staticmethod
    def _build_row(symbol: str, payload: dict) -> tuple:
        """Extract flat fields from a single symbol's quote payload."""
        quote = payload.get("quote", {}) if isinstance(payload, dict) else {}

        last_price = quote.get("lastPrice")
        bid_price  = quote.get("bidPrice")
        ask_price  = quote.get("askPrice")

        raw_time   = quote.get("quoteTime") or quote.get("tradeTime")
        quote_time = normalize_quote_time(raw_time)

        return (
            symbol,
            last_price,
            bid_price,
            ask_price,
            quote_time,
            json.dumps(payload),
        )
