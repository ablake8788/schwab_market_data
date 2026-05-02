"""
db/price_history_repository.py
-------------------------------
Data access layer for persisting Schwab price history (OHLCV bars).

Design patterns used:
  - Repository    : PriceHistoryRepository owns all SQL write logic for bars.
  - Unit of Work  : save() runs the full insert batch as one transaction.
  - Dependency Injection : factory injected; easy to swap for a test double.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from core.logging_setup import TRACE_LEVEL, traced
from db.connection import SqlConnectionFactory

log = logging.getLogger(__name__)

_INSERT_SQL = """
    INSERT INTO dbo.SchwabQuotesHistory_Raw
        (Symbol, BarDateTime, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, RawJson)
    VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
"""


class PriceHistoryRepository:
    """
    Writes Schwab price history (OHLCV) bars to SchwabQuotesHistory_Raw.

    Parameters
    ----------
    factory : SqlConnectionFactory
        Provides managed database connections.
    """

    def __init__(self, factory: SqlConnectionFactory) -> None:
        self._factory = factory

    @traced
    def save(self, symbol: str, history: dict) -> int:
        """
        Persist OHLCV bars for a single symbol.

        Parameters
        ----------
        symbol  : str   Ticker symbol (e.g. 'AAPL')
        history : dict  Raw Schwab price history response:
                        { "candles": [{ "open", "high", "low", "close",
                                        "volume", "datetime" }, ...],
                          "symbol": "AAPL", "empty": false }

        Returns
        -------
        int  Number of bars inserted.

        Raises
        ------
        ValueError  If history payload is empty or has no candles.
        """
        if not history:
            log.warning("save() called with empty history for %s", symbol)
            raise ValueError(f"Empty history payload for {symbol}")

        candles = history.get("candles", [])
        if not candles:
            log.info("No candles returned for %s — skipping insert", symbol)
            return 0

        log.info("Persisting %d bar(s) for %s", len(candles), symbol)

        with self._factory.connect() as conn:
            cursor = conn.cursor()
            rows_inserted = 0

            for candle in candles:
                row = self._build_row(symbol, candle)
                log.log(
                    TRACE_LEVEL,
                    "INSERT bar: symbol=%s  dt=%s  open=%s  close=%s",
                    row[0], row[1], row[2], row[5],
                )
                cursor.execute(_INSERT_SQL, row)
                rows_inserted += 1

            conn.commit()
            log.info("Inserted %d bar(s) for %s", rows_inserted, symbol)

        return rows_inserted

    # ── Private helpers ────────────────────────
    @staticmethod
    def _build_row(symbol: str, candle: dict) -> tuple:
        """
        Extract a flat insert row from a single candle dict.

        Schwab returns datetime as epoch milliseconds.
        Converts to naive UTC datetime for SQL Server datetime2.
        """
        raw_dt = candle.get("datetime")
        if isinstance(raw_dt, (int, float)):
            bar_dt = datetime.fromtimestamp(
                float(raw_dt) / 1000.0, tz=timezone.utc
            ).replace(tzinfo=None)
        else:
            bar_dt = None

        return (
            symbol,
            bar_dt,
            candle.get("open"),
            candle.get("high"),
            candle.get("low"),
            candle.get("close"),
            candle.get("volume"),
            json.dumps(candle),
        )
