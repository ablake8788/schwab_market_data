"""
db/portfolio_repository.py
--------------------------
Data access layer for portfolio symbols.

Design patterns used:
  - Repository : PortfolioRepository isolates all SQL queries about portfolios
                 behind a domain-language interface (get_symbols).
                 Callers work with plain Python lists, never with cursors or SQL.
  - Dependency Injection : receives SqlConnectionFactory; swap for a fake in tests.
"""

from __future__ import annotations

import logging

from core.logging_setup import traced
from db.connection import SqlConnectionFactory

log = logging.getLogger(__name__)

_SYMBOL_QUERY = """
    SELECT DISTINCT Symbol
    FROM   dbo.SchwabMarketDataPortfolioSymbol
    WHERE  PortfolioID = ?
      AND  IsActive    = 1
"""


class PortfolioRepository:
    """
    Read-only repository for portfolio / symbol data.

    Parameters
    ----------
    factory : SqlConnectionFactory
        Provides managed database connections.
    """

    def __init__(self, factory: SqlConnectionFactory) -> None:
        self._factory = factory

    @traced
    def get_symbols(self, portfolio_id: int) -> list[str]:
        """
        Return active, uppercased ticker symbols for *portfolio_id*.

        Raises
        ------
        ValueError
            If no active symbols are found for the given portfolio.
        """
        log.info("Fetching active symbols for PortfolioID=%d", portfolio_id)

        with self._factory.connect() as conn:
            cursor = conn.cursor()
            log.debug("Executing symbol query for PortfolioID=%d", portfolio_id)
            cursor.execute(_SYMBOL_QUERY, portfolio_id)
            rows = cursor.fetchall()

        symbols = [row[0].strip().upper() for row in rows if row[0]]
        log.info(
            "Loaded %d symbol(s) for PortfolioID=%d: %s",
            len(symbols), portfolio_id, symbols,
        )

        if not symbols:
            log.error("No active symbols for PortfolioID=%d", portfolio_id)
            raise ValueError(f"No active symbols found for PortfolioID={portfolio_id}")

        return symbols
