"""
schwab/market_data.py
---------------------
Schwab Market Data API client.

Design patterns used:
  - Facade      : SchwabMarketDataClient exposes simple get_quotes() /
                  get_price_history() methods, hiding HTTP, auth headers,
                  error handling, and JSON parsing from callers.
  - Dependency Injection : receives SchwabConfig; auth token is passed per-call
                           so the client is stateless and easy to test.
"""

from __future__ import annotations

import json
import logging
from typing import Sequence

import requests

from core.config import SchwabConfig
from core.logging_setup import TRACE_LEVEL, traced

log = logging.getLogger(__name__)


class SchwabMarketDataClient:
    """
    Thin, stateless wrapper around the Schwab Market Data REST API.

    All methods raise RuntimeError on non-200 responses so callers
    don't need to inspect HTTP status codes.

    Parameters
    ----------
    config : SchwabConfig
        Immutable Schwab API URLs / credentials.
    """

    def __init__(self, config: SchwabConfig) -> None:
        self._cfg = config

    # ── Public API ─────────────────────────────
    @traced
    def get_quotes(
        self,
        symbols: str | Sequence[str],
        access_token: str,
    ) -> dict:
        """
        Fetch real-time quotes for one or more symbols.

        Parameters
        ----------
        symbols : str or list[str]
            Ticker symbols to quote.
        access_token : str
            Valid Schwab OAuth access token.

        Returns
        -------
        dict
            Raw Schwab response: {symbol: {quote: {...}, fundamental: {...}}, ...}
        """
        if isinstance(symbols, str):
            symbols = [symbols]

        log.info("Requesting quotes for %d symbol(s): %s", len(symbols), symbols)
        params = {
            "symbols": ",".join(symbols),
            "fields": "quote,fundamental",
        }
        log.debug("GET %s  params=%s", self._cfg.quotes_url, params)

        resp = requests.get(
            self._cfg.quotes_url,
            headers=self._auth_header(access_token),
            params=params,
            timeout=30,
        )
        log.debug(
            "Quotes response: status=%s  bytes=%d",
            resp.status_code,
            len(resp.content),
        )

        if resp.status_code != 200:
            log.error("Quotes API error: %s %s", resp.status_code, resp.text)
            raise RuntimeError(f"Quotes API error {resp.status_code}: {resp.text}")

        data = resp.json()
        log.info("Quotes received for %d symbol(s)", len(data))
        log.log(TRACE_LEVEL, "Raw quotes JSON (first 500): %s", json.dumps(data)[:500])
        return data

    @traced
    def get_price_history(
        self,
        symbol: str,
        access_token: str,
        start_date: str,
        end_date: str,
        frequency: str = "daily",
    ) -> dict:
        """
        Fetch OHLCV price history for a single symbol.

        Parameters
        ----------
        symbol : str
        access_token : str
        start_date : str   YYYY-MM-DD
        end_date : str     YYYY-MM-DD
        frequency : str    'daily' | 'weekly' | 'monthly' (passed through)

        Returns
        -------
        dict  Raw Schwab price history response.
        """
        log.info(
            "Price history: symbol=%s  %s → %s  freq=%s",
            symbol, start_date, end_date, frequency,
        )
        params = {"symbol": symbol, "startDate": start_date, "endDate": end_date}
        log.debug("GET %s  params=%s", self._cfg.price_history_url, params)

        resp = requests.get(
            self._cfg.price_history_url,
            headers=self._auth_header(access_token),
            params=params,
            timeout=30,
        )
        log.debug("Price history response: status=%s", resp.status_code)

        if resp.status_code != 200:
            log.error("Price history API error: %s %s", resp.status_code, resp.text)
            raise RuntimeError(
                f"Price history API error {resp.status_code}: {resp.text}"
            )

        data = resp.json()
        log.info("Price history received for %s", symbol)
        return data

    # ── Private helpers ────────────────────────
    @staticmethod
    def _auth_header(access_token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {access_token}"}
