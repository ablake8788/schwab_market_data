"""
main.py
-------
Application entry point — Composition Root.

All dependencies are wired here and nowhere else.
No business logic lives in this file; it only:
  1. Parses CLI args.
  2. Sets up logging.
  3. Loads config (Singleton).
  4. Constructs and injects all collaborators.
  5. Calls the orchestration sequence.

Design patterns in use across the project
------------------------------------------
| Pattern            | Where                                          |
|--------------------|------------------------------------------------|
| Singleton          | AppConfig.load()                               |
| Strategy           | TokenStore / FileTokenStore                    |
| Template Method    | SchwabAuth.get_valid_tokens()                  |
| Repository         | PortfolioRepository, QuoteRepository           |
| Factory Method     | SqlConnectionFactory.create() / .connect()     |
| Context Manager    | SqlConnectionFactory.connect()                 |
| Facade             | SchwabMarketDataClient, this main()            |
| Dependency Inject. | Every class receives its deps via __init__     |
| Decorator          | @traced on all key functions                   |
| Value Object       | SchwabConfig, SqlConfig, CliArgs               |
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback

from cli import parse_args, prompt_date, prompt_portfolio_id
from core import AppConfig, setup_logging
from db import PortfolioRepository, QuoteRepository, PriceHistoryRepository, SqlConnectionFactory
from schwab import FileTokenStore, SchwabAuth, SchwabMarketDataClient

log = logging.getLogger(__name__)


def main() -> None:
    # ── 1. CLI args ────────────────────────────
    args = parse_args()

    # ── 2. Logging ─────────────────────────────
    setup_logging(args.log_dir)
    log.info("=" * 60)
    log.info("Schwab Market Data Loader starting")
    log.info("Python %s | PID %d", sys.version.split()[0], os.getpid())
    log.info("=" * 60)

    try:
        # ── 3. Config (Singleton) ───────────────
        cfg = AppConfig.load()
        log.info("Config loaded from schwab_market_data.ini")

        # ── 4. Wire dependencies ────────────────
        token_store       = FileTokenStore(cfg.schwab.token_file)
        auth              = SchwabAuth(cfg.schwab, token_store)
        market_data       = SchwabMarketDataClient(cfg.schwab)
        db_factory        = SqlConnectionFactory(cfg.sql)
        portfolio_repo    = PortfolioRepository(db_factory)
        quote_repo        = QuoteRepository(db_factory)
        price_history_repo = PriceHistoryRepository(db_factory)

        # ── 5. Resolve inputs ───────────────────
        tokens = auth.get_valid_tokens()
        access_token = tokens["access_token"]
        log.debug("Access token obtained (first 20): %s...", access_token[:20])

        portfolio_id = args.portfolio if args.portfolio is not None else prompt_portfolio_id()
        start_date   = args.start     or prompt_date("Start date")
        end_date     = args.end       or prompt_date("End date  ")
        log.info("PortfolioID=%d  date range: %s → %s", portfolio_id, start_date, end_date)

        # ── 6. Load symbols ─────────────────────
        symbols = portfolio_repo.get_symbols(portfolio_id)
        print(f"\nLoaded {len(symbols)} symbol(s): {', '.join(symbols)}")

        # ── 7. Fetch & persist real-time quotes ─
        print(f"\nFetching quotes for: {', '.join(symbols)}")
        quotes = market_data.get_quotes(symbols, access_token)

        print("\n=== RAW QUOTE DATA ===")
        print(json.dumps(quotes, indent=2))

        print("\nSaving quotes to SQL Server …")
        rows = quote_repo.save(quotes)
        print(f"Quotes done — {rows} row(s) inserted.")
        log.info("Quotes saved. Rows inserted: %d", rows)

        # ── 8. Fetch & persist price history ────
        print(f"\nFetching price history ({start_date} → {end_date}) …")
        history_rows_total = 0

        for symbol in symbols:
            try:
                log.info("Fetching price history for %s", symbol)
                history = market_data.get_price_history(
                    symbol=symbol,
                    access_token=access_token,
                    start_date=start_date,
                    end_date=end_date,
                )
                rows_saved = price_history_repo.save(symbol, history)
                history_rows_total += rows_saved
                print(f"  {symbol}: {rows_saved} bar(s) saved")
                log.info("Price history saved for %s — %d bar(s)", symbol, rows_saved)

            except Exception as exc:
                # One symbol failing should not abort the whole run
                log.error("Price history failed for %s: %s", symbol, exc)
                print(f"  {symbol}: SKIPPED — {exc}")

        print(f"\nPrice history done — {history_rows_total} total bar(s) inserted.")
        log.info("Price history complete. Total bars: %d", history_rows_total)
        log.info("Run complete.")

    except KeyboardInterrupt:
        log.warning("Interrupted by user")
        print("\nInterrupted.")
        sys.exit(0)

    except Exception as exc:
        log.critical("Unhandled exception: %s\n%s", exc, traceback.format_exc())
        print(f"\nFATAL: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()






# """
# main.py
# -------
# Application entry point — Composition Root.
#
# All dependencies are wired here and nowhere else.
# No business logic lives in this file; it only:
#   1. Parses CLI args.
#   2. Sets up logging.
#   3. Loads config (Singleton).
#   4. Constructs and injects all collaborators.
#   5. Calls the orchestration sequence.
#
# Design patterns in use across the project
# ------------------------------------------
# | Pattern            | Where                                          |
# |--------------------|------------------------------------------------|
# | Singleton          | AppConfig.load()                               |
# | Strategy           | TokenStore / FileTokenStore                    |
# | Template Method    | SchwabAuth.get_valid_tokens()                  |
# | Repository         | PortfolioRepository, QuoteRepository           |
# | Factory Method     | SqlConnectionFactory.create() / .connect()     |
# | Context Manager    | SqlConnectionFactory.connect()                 |
# | Facade             | SchwabMarketDataClient, this main()            |
# | Dependency Inject. | Every class receives its deps via __init__     |
# | Decorator          | @traced on all key functions                   |
# | Value Object       | SchwabConfig, SqlConfig, CliArgs               |
# """
#
# from __future__ import annotations
#
# import json
# import logging
# import os
# import sys
# import traceback
#
# from cli import parse_args, prompt_date, prompt_portfolio_id
# from core import AppConfig, setup_logging
# from db import PortfolioRepository, QuoteRepository, SqlConnectionFactory
# from schwab import FileTokenStore, SchwabAuth, SchwabMarketDataClient
#
# log = logging.getLogger(__name__)
#
#
# def main() -> None:
#     # ── 1. CLI args ────────────────────────────
#     args = parse_args()
#
#     # ── 2. Logging ─────────────────────────────
#     setup_logging(args.log_dir)
#     log.info("=" * 60)
#     log.info("Schwab Market Data Loader starting")
#     log.info("Python %s | PID %d", sys.version.split()[0], os.getpid())
#     log.info("=" * 60)
#
#     try:
#         # ── 3. Config (Singleton) ───────────────
#         cfg = AppConfig.load()
#         log.info("Config loaded from schwab_market_data.ini")
#
#         # ── 4. Wire dependencies ────────────────
#         token_store  = FileTokenStore(cfg.schwab.token_file)
#         auth         = SchwabAuth(cfg.schwab, token_store)
#         market_data  = SchwabMarketDataClient(cfg.schwab)
#         db_factory   = SqlConnectionFactory(cfg.sql)
#         portfolio_repo = PortfolioRepository(db_factory)
#         quote_repo     = QuoteRepository(db_factory)
#
#         # ── 5. Resolve inputs ───────────────────
#         tokens = auth.get_valid_tokens()
#         access_token = tokens["access_token"]
#         log.debug("Access token obtained (first 20): %s...", access_token[:20])
#
#         portfolio_id = args.portfolio if args.portfolio is not None else prompt_portfolio_id()
#         start_date   = args.start     or prompt_date("Start date")
#         end_date     = args.end       or prompt_date("End date  ")
#         log.info("PortfolioID=%d  date range: %s → %s", portfolio_id, start_date, end_date)
#
#         # ── 6. Load symbols ─────────────────────
#         symbols = portfolio_repo.get_symbols(portfolio_id)
#         print(f"\nLoaded {len(symbols)} symbol(s): {', '.join(symbols)}")
#
#         # ── 7. Fetch quotes ─────────────────────
#         print(f"\nFetching quotes for: {', '.join(symbols)}")
#         quotes = market_data.get_quotes(symbols, access_token)
#
#         print("\n=== RAW QUOTE DATA ===")
#         print(json.dumps(quotes, indent=2))
#
#         # ── 8. Persist ──────────────────────────
#         print("\nSaving to SQL Server …")
#         rows = quote_repo.save(quotes)
#         print(f"Done — {rows} row(s) inserted.")
#         log.info("Run complete. Rows inserted: %d", rows)
#
#     except KeyboardInterrupt:
#         log.warning("Interrupted by user")
#         print("\nInterrupted.")
#         sys.exit(0)
#
#     except Exception as exc:
#         log.critical("Unhandled exception: %s\n%s", exc, traceback.format_exc())
#         print(f"\nFATAL: {exc}")
#         sys.exit(1)
#
#
# if __name__ == "__main__":
#     main()
