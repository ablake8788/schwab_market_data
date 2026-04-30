################################
#
# Looking at your script, the main I/O-bound operations that benefit from async + callbacks are:
#
# Token refresh (requests.post)
# Quotes API call (requests.get)
# Price history API call (requests.get)
# SQL Server operations (pyodbc)
#
# Here's the modernized version using asyncio, aiohttp, and aioodbc with callbacks:Created a file, read a fileCreated a file, read a fileHere's a summary of every change made:
# New dependencies — replace requests + pyodbc with:
# pip install aiohttp aioodbc
# Callback contract — every async function now accepts an optional callback(result, error)
#   parameter that fires in the finally block, whether the call succeeds or fails:
# pythondef on_quotes(data, err):
#     if err:  print(f"Error: {err}")
#     else:    print(f"Got quotes for: {', '.join(data.keys())}")
#
# await get_quotes(symbols, token, callback=on_quotes)
# Key structural changes:
# OriginalAsync versionrequests.get/postaiohttp.ClientSession + async withpyodbc.connectawait aioodbc.connectSequential per-symbolasyncio.create_task + asyncio.gather for concurrent quote + history fetchesNo callbackscallback(result, error) on every I/O function
# Concurrency win — quotes for all symbols and price history for each symbol now fire simultaneously via asyncio.gather, rather than one at a time. For a 20-symbol portfolio this cuts wall-clock API time from ~20s to ~1–2s.

## compile
# python.exe -m PyInstaller --onefile Schwab_market_data_async.py





## compile
## python.exe -m PyInstaller --onefile --add-data "schwab_market_data.ini;." Schwab_market_data_async.py
## run
## dist\Schwab_market_data_async.exe --portfolio 1 --start 2024-01-01 --end 2024-12-31

import argparse
import asyncio
import base64
import configparser
import json
import os
import sys
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, unquote, urlparse

import aiohttp    # pip install aiohttp
import aioodbc    # pip install aioodbc

"""
Schwab Market Data Loader — Async + Callback Edition

Architecture:
  - One shared aiohttp.ClientSession for ALL HTTP calls (fixes CancelledError
    when firing 35 concurrent per-symbol sessions).
  - Session is created in main() and passed down to every API function.
  - Token OAuth calls (pre-session) use their own short-lived sessions since
    they run before the shared session opens.
  - Callback chain driven by add_done_callback — the event loop wires each
    step to the next; main() only awaits the tail of pending tasks.

Callback chain:
  get_valid_tokens
      → on_tokens_done → get_symbols_for_portfolio
          → on_symbols_done → get_quotes  (shared session)
                            → get_price_history × N  (shared session, concurrent)
              → on_quotes_done → save_quotes_to_db
                  → on_save_done
              → on_history_done × N
"""


# ── Path helper ────────────────────────────────────────────────────────────────

def resource_path(relative_path: str) -> Path:
    """Works for both .py (dev) and PyInstaller .exe (prod)."""
    base = (
        Path(sys.executable).resolve().parent
        if getattr(sys, "frozen", False)
        else Path(__file__).resolve().parent
    )
    return base / relative_path


# ── Config ─────────────────────────────────────────────────────────────────────

CONFIG_FILENAME = "schwab_market_data.ini"
config_path = resource_path(CONFIG_FILENAME)

if not config_path.exists():
    raise FileNotFoundError(f"{CONFIG_FILENAME} not found at {config_path}")

config = configparser.ConfigParser()
if not config.read(config_path):
    raise FileNotFoundError(f"{CONFIG_FILENAME} could not be read at {config_path}")

schwab_cfg = config["schwab"]
sql_cfg    = config["sqlserver"]

APP_KEY       = schwab_cfg["app_key"]
APP_SECRET    = schwab_cfg["app_secret"]
REDIRECT_URI  = schwab_cfg["redirect_uri"]
TOKEN_FILE    = str(resource_path(schwab_cfg.get("token_file", "schwab_tokens.json")))

AUTH_URL          = schwab_cfg.get("auth_url",          "https://api.schwabapi.com/v1/oauth/authorize")
TOKEN_URL         = schwab_cfg.get("token_url",         "https://api.schwabapi.com/v1/oauth/token")
QUOTES_URL        = schwab_cfg.get("quotes_url",        "https://api.schwabapi.com/marketdata/v1/quotes")
PRICE_HISTORY_URL = schwab_cfg.get("price_history_url", "https://api.schwabapi.com/marketdata/v1/pricehistory")

SQL_DRIVER     = sql_cfg["driver"]
SQL_SERVER     = sql_cfg["server"]
SQL_DATABASE   = sql_cfg["database"]
SQL_USERNAME   = sql_cfg["username"]
SQL_PASSWORD   = sql_cfg["password"]
SQL_TRUST_CERT = sql_cfg.get("trust_cert", "yes")


# ── Time helpers ───────────────────────────────────────────────────────────────

def ms_to_datetime(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)


def normalize_quote_time(raw) -> Optional[datetime]:
    if isinstance(raw, (int, float)):
        ts = float(raw)
        return ms_to_datetime(ts) if ts > 1e10 else \
               datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None
    return None


# ── Token file helpers (sync — local JSON, no async needed) ───────────────────

def load_tokens() -> Optional[dict]:
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_tokens(tokens: dict) -> None:
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2)
    print(f"Tokens saved to {TOKEN_FILE}")


# ── OAuth helpers ──────────────────────────────────────────────────────────────

def _basic_auth_header() -> str:
    return "Basic " + base64.b64encode(f"{APP_KEY}:{APP_SECRET}".encode()).decode()


def build_authorize_url() -> str:
    return f"{AUTH_URL}?client_id={APP_KEY}&redirect_uri={REDIRECT_URI}"


def extract_code_from_redirect_url(redirect_url: str) -> str:
    redirect_url = redirect_url.strip()
    if not redirect_url:
        raise ValueError("No redirect URL was pasted.")
    qs = parse_qs(urlparse(redirect_url).query)
    codes = qs.get("code")
    if not codes or not codes[0].strip():
        raise ValueError(f"No 'code' parameter found in: {redirect_url}")
    return unquote(codes[0].strip())


async def _post_token(session: aiohttp.ClientSession, data: dict) -> dict:
    """POST to Schwab token endpoint with Basic auth."""
    headers = {
        "Authorization": _basic_auth_header(),
        "Content-Type":  "application/x-www-form-urlencoded",
    }
    async with session.post(TOKEN_URL, headers=headers, data=data) as resp:
        text = await resp.text()
        if resp.status != 200:
            raise RuntimeError(f"Token endpoint error {resp.status}: {text}")
        return json.loads(text)


async def get_tokens_from_auth_code(auth_code: str) -> dict:
    """Exchange auth code for access + refresh tokens."""
    # Short-lived session — runs before the shared session is open
    async with aiohttp.ClientSession() as session:
        result = await _post_token(session, {
            "grant_type":   "authorization_code",
            "code":         auth_code,
            "redirect_uri": REDIRECT_URI,
        })
    save_tokens(result)
    print("Initial tokens obtained successfully.")
    return result


async def refresh_access_token(tokens: dict) -> dict:
    """Use refresh_token to get a new access_token."""
    rt = tokens.get("refresh_token")
    if not rt:
        raise RuntimeError("No refresh_token available.")
    async with aiohttp.ClientSession() as session:
        result = await _post_token(session, {
            "grant_type":    "refresh_token",
            "refresh_token": rt,
        })
    save_tokens(result)
    print("Access token refreshed.")
    return result


async def initial_login_flow() -> dict:
    """Full interactive browser OAuth flow."""
    url = build_authorize_url()
    print("\n=== Authorize the app in your browser ===")
    print(url)
    try:
        webbrowser.open(url)
    except Exception:
        pass
    print("\nPaste the FULL redirect URL from the browser address bar:")
    redirect_url = input("> ").strip()
    auth_code = extract_code_from_redirect_url(redirect_url)
    print(f"Auth code (truncated): {auth_code[:20]}…")
    return await get_tokens_from_auth_code(auth_code)


async def get_valid_tokens() -> dict:
    """
    Guarantee a valid access token.
    Path A — no file on disk  → full browser login
    Path B — file exists      → refresh (falls back to login if refresh fails)
    """
    tokens = load_tokens()
    if tokens is None:
        print("No tokens on disk — running full login flow…")
        return await initial_login_flow()
    try:
        print("Attempting token refresh…")
        return await refresh_access_token(tokens)
    except Exception as exc:
        print(f"Refresh failed ({exc}) — falling back to full login…")
        return await initial_login_flow()


# ── SQL Server helpers ─────────────────────────────────────────────────────────

def _dsn() -> str:
    return (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate={SQL_TRUST_CERT};"
    )


async def get_symbols_for_portfolio(portfolio_id: int) -> list[str]:
    """Load active symbols for a portfolio from SQL Server."""
    async with await aioodbc.connect(dsn=_dsn()) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT DISTINCT Symbol
                FROM dbo.SchwabMarketDataPortfolioSymbol
                WHERE PortfolioID = ?
                  AND IsActive = 1
                """,
                portfolio_id,
            )
            rows = await cursor.fetchall()

    result = [r[0].strip().upper() for r in rows if r[0]]
    if not result:
        raise ValueError(f"No symbols found for PortfolioID={portfolio_id}")
    return result


# ── Market-data API calls (shared session) ────────────────────────────────────

async def get_quotes(
    symbols: list[str],
    access_token: str,
    session: aiohttp.ClientSession,
) -> dict:
    """
    Fetch quotes for all symbols in one API call.
    Uses the shared ClientSession passed from main().
    """
    if isinstance(symbols, str):
        symbols = [symbols]
    params  = {"symbols": ",".join(symbols), "fields": "quote,fundamental"}
    headers = {"Authorization": f"Bearer {access_token}"}
    async with session.get(QUOTES_URL, headers=headers, params=params) as resp:
        text = await resp.text()
        if resp.status != 200:
            raise RuntimeError(f"Quotes error {resp.status}: {text}")
        return json.loads(text)


# async def get_price_history(
#     symbol: str,
#     access_token: str,
#     start_date: str,
#     end_date: str,
#     session: aiohttp.ClientSession,
# ) -> dict:
#     """
#     Fetch price history for a single symbol.
#     Uses the shared ClientSession passed from main().
#     """
#     params  = {"symbol": symbol, "startDate": start_date, "endDate": end_date}
#     headers = {"Authorization": f"Bearer {access_token}"}
#     async with session.get(PRICE_HISTORY_URL, headers=headers, params=params) as resp:
#         text = await resp.text()
#         if resp.status != 200:
#             raise RuntimeError(f"Price history error {resp.status}: {text}")
#         return json.loads(text)


async def get_price_history(
    symbol: str,
    access_token: str,
    start_date: str,
    end_date: str,
    session: aiohttp.ClientSession,
) -> dict:
    params = {
        "symbol":    symbol,
        "startDate": date_to_epoch_ms(start_date),   # ← convert here
        "endDate":   date_to_epoch_ms(end_date),     # ← convert here
    }
    headers = {"Authorization": f"Bearer {access_token}"}
    async with session.get(PRICE_HISTORY_URL, headers=headers, params=params) as resp:
        text = await resp.text()
        if resp.status != 200:
            raise RuntimeError(f"Price history error {resp.status}: {text}")
        return json.loads(text)



# ── DB save ────────────────────────────────────────────────────────────────────

async def save_quotes_to_db(quotes_json: dict) -> int:
    """
    Truncate raw/stage tables, insert rows, run ETL stored proc.
    Returns the number of rows inserted.
    """
    if not quotes_json:
        print("No quote data to save.")
        return 0

    async with await aioodbc.connect(dsn=_dsn()) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("TRUNCATE TABLE dbo.SchwabQuotes_Raw;")
            await cursor.execute("TRUNCATE TABLE dbo.SchwabQuotes_Stage;")
            await conn.commit()
            print("Cleared staging tables.")

            insert_sql = """
                INSERT INTO dbo.SchwabQuotes_Raw
                    (Symbol, LastPrice, BidPrice, AskPrice, QuoteTime, RawJson)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            rows = 0
            for symbol, payload in quotes_json.items():
                quote = payload.get("quote", {}) if isinstance(payload, dict) else {}
                qt    = normalize_quote_time(
                            quote.get("quoteTime") or quote.get("tradeTime"))
                await cursor.execute(insert_sql, (
                    symbol,
                    quote.get("lastPrice"),
                    quote.get("bidPrice"),
                    quote.get("askPrice"),
                    qt,
                    json.dumps(payload),
                ))
                rows += 1

            await conn.commit()
            print(f"Inserted {rows} row(s) into dbo.SchwabQuotes_Raw.")

            await cursor.execute("EXEC usp_Load_SchwabQuotes;")
            await conn.commit()
            print("Ran usp_Load_SchwabQuotes.")

    return rows


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Schwab Market Data Loader (async)")
    p.add_argument("--portfolio", type=int)
    p.add_argument("--start")
    p.add_argument("--end")
    return p.parse_args()


# add this helper near your other time helpers
def date_to_epoch_ms(date_str: str) -> int:
    """Convert YYYY-MM-DD to epoch milliseconds for Schwab price history API."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)



# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    args = parse_args()

    if args.portfolio is not None:
        portfolio_id = args.portfolio
    else:
        raw = input("\nPortfolioID (default 1): ").strip()
        portfolio_id = int(raw) if raw.isdigit() else 1

    start_str = args.start or input("Start date (YYYY-MM-DD): ").strip()
    end_str   = args.end   or input("End date   (YYYY-MM-DD): ").strip()
    print(f"Date range: {start_str} → {end_str}")

    tokens = await get_valid_tokens()
    access_token = tokens["access_token"]
    print(f"[CB] Token obtained. Expires in: {tokens.get('expires_in')}s")

    symbols = await get_symbols_for_portfolio(portfolio_id)
    print(f"[CB] {len(symbols)} symbol(s) loaded: {', '.join(symbols)}")

    async with aiohttp.ClientSession() as session:

        # ── Callbacks ─────────────────────────────────────────────────────────

        def on_save_done(fut):
            try:
                if fut.exception():
                    print(f"[CB] DB save failed: {fut.exception()}")
                else:
                    print(f"[CB] {fut.result()} row(s) saved to SQL Server.")
            except asyncio.CancelledError:
                print("[CB] DB save was cancelled.")
            print("Done.")

        def on_history_done(sym: str, fut: asyncio.Future):
            try:
                if fut.exception():
                    print(f"[CB] History for {sym} failed: {fut.exception()}")
                else:
                    candles = fut.result().get("candles", [])
                    print(f"[CB] History for {sym}: {len(candles)} candle(s) received.")
            except asyncio.CancelledError:
                print(f"[CB] History for {sym} was cancelled.")

        def on_quotes_done(fut: asyncio.Future):
            try:
                if fut.exception():
                    print(f"[CB] Quote fetch failed: {fut.exception()}")
                    return
            except asyncio.CancelledError:
                print("[CB] Quote fetch was cancelled.")
                return
            data = fut.result()
            print(f"[CB] Quotes received for: {', '.join(data.keys())}")
            print("\n=== RAW QUOTE DATA ===")
            print(json.dumps(data, indent=2))
            # Chain: quotes done → kick off DB save
            save_task = asyncio.create_task(save_quotes_to_db(data))
            save_task.add_done_callback(on_save_done)

        # ── Fire all tasks ─────────────────────────────────────────────────────
        quote_task = asyncio.create_task(
            get_quotes(symbols, access_token, session)
        )
        quote_task.add_done_callback(on_quotes_done)

        for sym in symbols:
            hist_task = asyncio.create_task(
                get_price_history(sym, access_token, start_str, end_str, session)
            )
            hist_task.add_done_callback(
                lambda fut, s=sym: on_history_done(s, fut)
            )

        # ── Wait for ALL tasks including chained save ──────────────────────────
        # Loop until no pending tasks remain — this handles the chained
        # save_task that gets created inside on_quotes_done AFTER the first
        # gather would otherwise return.
        while True:
            pending = [
                t for t in asyncio.all_tasks()
                if t is not asyncio.current_task()
            ]
            if not pending:
                break
            await asyncio.gather(*pending, return_exceptions=True)
            # Loop again — on_quotes_done may have spawned save_task
            # after the gather started, so we check once more.



# async def main():
#     args = parse_args()
#
#     # Resolve inputs up front (blocking input is fine before the event loop heats up)
#     if args.portfolio is not None:
#         portfolio_id = args.portfolio
#     else:
#         raw = input("\nPortfolioID (default 1): ").strip()
#         portfolio_id = int(raw) if raw.isdigit() else 1
#
#     start_str = args.start or input("Start date (YYYY-MM-DD): ").strip()
#     end_str   = args.end   or input("End date   (YYYY-MM-DD): ").strip()
#     print(f"Date range: {start_str} → {end_str}")
#
#     # ── Get tokens (uses its own short-lived sessions internally) ─────────────
#     tokens = await get_valid_tokens()
#     access_token = tokens["access_token"]
#     print(f"[CB] Token obtained. Expires in: {tokens.get('expires_in')}s")
#
#     # ── Load symbols ───────────────────────────────────────────────────────────
#     symbols = await get_symbols_for_portfolio(portfolio_id)
#     print(f"[CB] {len(symbols)} symbol(s) loaded: {', '.join(symbols)}")
#
#     # ── Open ONE shared session for all remaining HTTP calls ───────────────────
#     # This is the fix for CancelledError: 35 symbols × 1 session each
#     # overwhelms the DNS resolver before the loop can finish.
#     # One shared session = one connector pool, one DNS resolver, no cancellations.
#     async with aiohttp.ClientSession() as session:
#
#         print(f"\nFetching quotes and price history concurrently for {len(symbols)} symbol(s)…")
#
#         # ── Callbacks ─────────────────────────────────────────────────────────
#
#         def on_save_done(fut):
#             if fut.exception():
#                 print(f"[CB] DB save failed: {fut.exception()}")
#             else:
#                 print(f"[CB] {fut.result()} row(s) saved to SQL Server.")
#             print("Done.")
#
#         def on_history_done(sym: str, fut: asyncio.Future):
#             if fut.exception():
#                 print(f"[CB] History for {sym} failed: {fut.exception()}")
#             else:
#                 candles = fut.result().get("candles", [])
#                 print(f"[CB] History for {sym}: {len(candles)} candle(s) received.")
#
#         def on_quotes_done(fut: asyncio.Future):
#             if fut.exception():
#                 print(f"[CB] Quote fetch failed: {fut.exception()}")
#                 return
#             data = fut.result()
#             print(f"[CB] Quotes received for: {', '.join(data.keys())}")
#             print("\n=== RAW QUOTE DATA ===")
#             print(json.dumps(data, indent=2))
#             # Chain: quotes done → kick off DB save
#             save_task = asyncio.create_task(save_quotes_to_db(data))
#             save_task.add_done_callback(on_save_done)
#
#         # ── Fire quote task ────────────────────────────────────────────────────
#         quote_task = asyncio.create_task(
#             get_quotes(symbols, access_token, session)
#         )
#         quote_task.add_done_callback(on_quotes_done)
#
#         # ── Fire one history task per symbol (all concurrent) ─────────────────
#         for sym in symbols:
#             hist_task = asyncio.create_task(
#                 get_price_history(sym, access_token, start_str, end_str, session)
#             )
#             hist_task.add_done_callback(
#                 lambda fut, s=sym: on_history_done(s, fut)
#             )
#
#         # ── Wait for quote + all history tasks, then any chained saves ─────────
#         await asyncio.sleep(0)  # yield so tasks can start
#         pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
#         if pending:
#             await asyncio.gather(*pending, return_exceptions=True)
#
#     # Session closes cleanly here — all tasks finished inside the async with block
#
#
# if __name__ == "__main__":
#     asyncio.run(main())














#################################
# import argparse
# import asyncio
# import base64
# import configparser
# import json
# import os
# import sys
# import webbrowser
# from datetime import datetime, timezone
# from pathlib import Path
# from typing import Callable, Optional
# from urllib.parse import parse_qs, unquote, urlparse
#
# import aiohttp       # pip install aiohttp
# import aioodbc       # pip install aioodbc
#
# """
# Schwab Market Data Loader — Async + Callback Edition
#
# Key changes from the sync version:
#   - requests  → aiohttp  (async HTTP)
#   - pyodbc    → aioodbc  (async SQL Server via ODBC)
#   - All I/O functions are now `async def`
#   - Each major operation accepts an optional `callback` parameter
#     that fires when the operation completes (or fails).
#
# Callback contract:
#     callback(result, error)
#       result  → the return value on success (or None on failure)
#       error   → an Exception on failure (or None on success)
# """
#
#
# # ── Path helper ────────────────────────────────────────────────────────────────
#
# def resource_path(relative_path: str) -> Path:
#     base = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) \
#         else Path(__file__).resolve().parent
#     return base / relative_path
#
#
# # ── Config ─────────────────────────────────────────────────────────────────────
#
# CONFIG_FILENAME = "schwab_market_data.ini"
# config_path = resource_path(CONFIG_FILENAME)
#
# if not config_path.exists():
#     raise FileNotFoundError(f"{CONFIG_FILENAME} not found at {config_path}")
#
# config = configparser.ConfigParser()
# if not config.read(config_path):
#     raise FileNotFoundError(f"{CONFIG_FILENAME} could not be read at {config_path}")
#
# schwab_cfg = config["schwab"]
# sql_cfg    = config["sqlserver"]
#
# APP_KEY       = schwab_cfg["app_key"]
# APP_SECRET    = schwab_cfg["app_secret"]
# REDIRECT_URI  = schwab_cfg["redirect_uri"]
# TOKEN_FILE    = str(resource_path(schwab_cfg.get("token_file", "schwab_tokens.json")))
#
# AUTH_URL          = schwab_cfg.get("auth_url",          "https://api.schwabapi.com/v1/oauth/authorize")
# TOKEN_URL         = schwab_cfg.get("token_url",         "https://api.schwabapi.com/v1/oauth/token")
# QUOTES_URL        = schwab_cfg.get("quotes_url",        "https://api.schwabapi.com/marketdata/v1/quotes")
# PRICE_HISTORY_URL = schwab_cfg.get("price_history_url", "https://api.schwabapi.com/marketdata/v1/pricehistory")
#
# SQL_DRIVER     = sql_cfg["driver"]
# SQL_SERVER     = sql_cfg["server"]
# SQL_DATABASE   = sql_cfg["database"]
# SQL_USERNAME   = sql_cfg["username"]
# SQL_PASSWORD   = sql_cfg["password"]
# SQL_TRUST_CERT = sql_cfg.get("trust_cert", "yes")
#
#
# # ── Time helpers ───────────────────────────────────────────────────────────────
#
# def ms_to_datetime(ms: int) -> datetime:
#     return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)
#
# def normalize_quote_time(raw):
#     if isinstance(raw, (int, float)):
#         ts = float(raw)
#         return ms_to_datetime(ts) if ts > 1e10 else \
#                datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
#     if isinstance(raw, str):
#         try:
#             return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
#         except Exception:
#             return None
#     return None
#
#
# # ── Token file (sync — just reading/writing local JSON, no need to async) ─────
#
# def load_tokens() -> Optional[dict]:
#     if not os.path.exists(TOKEN_FILE):
#         return None
#     with open(TOKEN_FILE, "r", encoding="utf-8") as f:
#         return json.load(f)
#
# def save_tokens(tokens: dict):
#     with open(TOKEN_FILE, "w", encoding="utf-8") as f:
#         json.dump(tokens, f, indent=2)
#     print(f"Tokens saved to {TOKEN_FILE}")
#
#
# # ── OAuth helpers (async) ─────────────────────────────────────────────────────
#
# def _basic_auth_header() -> str:
#     return "Basic " + base64.b64encode(f"{APP_KEY}:{APP_SECRET}".encode()).decode()
#
# def build_authorize_url() -> str:
#     return f"{AUTH_URL}?client_id={APP_KEY}&redirect_uri={REDIRECT_URI}"
#
# def extract_code_from_redirect_url(redirect_url: str) -> str:
#     redirect_url = redirect_url.strip()
#     if not redirect_url:
#         raise ValueError("No redirect URL was pasted.")
#     qs = parse_qs(urlparse(redirect_url).query)
#     codes = qs.get("code")
#     if not codes or not codes[0].strip():
#         raise ValueError(f"No 'code' parameter found in: {redirect_url}")
#     return unquote(codes[0].strip())
#
#
# async def _post_token(session: aiohttp.ClientSession, data: dict) -> dict:
#     """Shared helper: POST to token endpoint with Basic auth."""
#     headers = {
#         "Authorization": _basic_auth_header(),
#         "Content-Type":  "application/x-www-form-urlencoded",
#     }
#     async with session.post(TOKEN_URL, headers=headers, data=data) as resp:
#         text = await resp.text()
#         if resp.status != 200:
#             raise RuntimeError(f"Token endpoint error {resp.status}: {text}")
#         return json.loads(text)
#
#
# async def get_tokens_from_auth_code(
#     auth_code: str,
#     callback: Optional[Callable] = None,
# ) -> dict:
#     """
#     Exchange an authorization code for access + refresh tokens.
#     callback(tokens, error)
#     """
#     result = error = None
#     try:
#         async with aiohttp.ClientSession() as session:
#             result = await _post_token(session, {
#                 "grant_type":   "authorization_code",
#                 "code":         auth_code,
#                 "redirect_uri": REDIRECT_URI,
#             })
#         save_tokens(result)
#         print("Initial tokens obtained successfully.")
#     except Exception as exc:
#         error = exc
#     finally:
#         if callback:
#             callback(result, error)
#     return result
#
#
# async def refresh_access_token(
#     tokens: dict,
#     callback: Optional[Callable] = None,
# ) -> dict:
#     """
#     Refresh the access token using the stored refresh_token.
#     callback(new_tokens, error)
#     """
#     result = error = None
#     try:
#         rt = tokens.get("refresh_token")
#         if not rt:
#             raise RuntimeError("No refresh_token available.")
#         async with aiohttp.ClientSession() as session:
#             result = await _post_token(session, {
#                 "grant_type":    "refresh_token",
#                 "refresh_token": rt,
#             })
#         save_tokens(result)
#         print("Access token refreshed.")
#     except Exception as exc:
#         error = exc
#     finally:
#         if callback:
#             callback(result, error)
#     return result
#
#
# async def initial_login_flow(callback: Optional[Callable] = None) -> dict:
#     """
#     Full interactive OAuth flow (browser-based).
#     callback(tokens, error)
#     """
#     result = error = None
#     try:
#         url = build_authorize_url()
#         print("\n=== Authorize the app in your browser ===")
#         print(url)
#         try:
#             webbrowser.open(url)
#         except Exception:
#             pass
#
#         print("\nPaste the FULL redirect URL from the browser address bar:")
#         redirect_url = input("> ").strip()
#         auth_code = extract_code_from_redirect_url(redirect_url)
#         print(f"Auth code (truncated): {auth_code[:20]}…")
#
#         result = await get_tokens_from_auth_code(auth_code)
#     except Exception as exc:
#         error = exc
#     finally:
#         if callback:
#             callback(result, error)
#     return result
#
#
# async def get_valid_tokens(callback: Optional[Callable] = None) -> dict:
#     """
#     Return a valid access token, refreshing or re-logging-in as needed.
#     callback(tokens, error)
#     """
#     result = error = None
#     try:
#         tokens = load_tokens()
#         if tokens is None:
#             print("No tokens on disk — running full login flow…")
#             result = await initial_login_flow()
#         else:
#             try:
#                 print("Attempting token refresh…")
#                 result = await refresh_access_token(tokens)
#             except Exception as exc:
#                 print(f"Refresh failed ({exc}) — falling back to full login…")
#                 result = await initial_login_flow()
#     except Exception as exc:
#         error = exc
#     finally:
#         if callback:
#             callback(result, error)
#     return result
#
#
# # ── SQL Server helpers (async via aioodbc) ─────────────────────────────────────
#
# def _dsn() -> str:
#     return (
#         f"DRIVER={{{SQL_DRIVER}}};"
#         f"SERVER={SQL_SERVER};"
#         f"DATABASE={SQL_DATABASE};"
#         f"UID={SQL_USERNAME};"
#         f"PWD={SQL_PASSWORD};"
#         f"TrustServerCertificate={SQL_TRUST_CERT};"
#     )
#
# async def get_symbols_for_portfolio(
#     portfolio_id: int,
#     callback: Optional[Callable] = None,
# ) -> list[str]:
#     """
#     Load active symbols for a portfolio from SQL Server.
#     callback(symbols, error)
#     """
#     result = error = None
#     try:
#         async with await aioodbc.connect(dsn=_dsn()) as conn:
#             async with conn.cursor() as cursor:
#                 await cursor.execute(
#                     """
#                     SELECT DISTINCT Symbol
#                     FROM dbo.SchwabMarketDataPortfolioSymbol
#                     WHERE PortfolioID = ?
#                       AND IsActive = 1
#                     """,
#                     portfolio_id,
#                 )
#                 rows = await cursor.fetchall()
#
#         result = [r[0].strip().upper() for r in rows if r[0]]
#         if not result:
#             raise ValueError(f"No symbols found for PortfolioID={portfolio_id}")
#     except Exception as exc:
#         error = exc
#     finally:
#         if callback:
#             callback(result, error)
#     return result
#
#
# # ── Market-data API calls (async) ─────────────────────────────────────────────
#
# async def get_quotes(
#     symbols: list[str],
#     access_token: str,
#     callback: Optional[Callable] = None,
# ) -> dict:
#     """
#     Fetch quotes from Schwab for a list of symbols.
#     callback(data, error)
#     """
#     result = error = None
#     try:
#         if isinstance(symbols, str):
#             symbols = [symbols]
#         params  = {"symbols": ",".join(symbols), "fields": "quote,fundamental"}
#         headers = {"Authorization": f"Bearer {access_token}"}
#
#         async with aiohttp.ClientSession() as session:
#             async with session.get(QUOTES_URL, headers=headers, params=params) as resp:
#                 text = await resp.text()
#                 if resp.status != 200:
#                     raise RuntimeError(f"Quotes error {resp.status}: {text}")
#                 result = json.loads(text)
#     except Exception as exc:
#         error = exc
#     finally:
#         if callback:
#             callback(result, error)
#     return result
#
#
# async def get_price_history(
#     symbol: str,
#     access_token: str,
#     start_date: str,
#     end_date: str,
#     callback: Optional[Callable] = None,
# ) -> dict:
#     """
#     Fetch price history for a single symbol.
#     callback(data, error)
#     """
#     result = error = None
#     try:
#         params  = {"symbol": symbol, "startDate": start_date, "endDate": end_date}
#         headers = {"Authorization": f"Bearer {access_token}"}
#
#         async with aiohttp.ClientSession() as session:
#             async with session.get(PRICE_HISTORY_URL, headers=headers, params=params) as resp:
#                 text = await resp.text()
#                 if resp.status != 200:
#                     raise RuntimeError(f"Price history error {resp.status}: {text}")
#                 result = json.loads(text)
#     except Exception as exc:
#         error = exc
#     finally:
#         if callback:
#             callback(result, error)
#     return result
#
#
# # ── DB save (async) ───────────────────────────────────────────────────────────
#
# async def save_quotes_to_db(
#     quotes_json: dict,
#     callback: Optional[Callable] = None,
# ):
#     """
#     Truncate raw/stage tables, insert quote rows, run ETL stored proc.
#     callback(rows_inserted, error)
#     """
#     result = error = None
#     try:
#         if not quotes_json:
#             print("No quote data to save.")
#             result = 0
#             return
#
#         async with await aioodbc.connect(dsn=_dsn()) as conn:
#             async with conn.cursor() as cursor:
#                 await cursor.execute("TRUNCATE TABLE dbo.SchwabQuotes_Raw;")
#                 await cursor.execute("TRUNCATE TABLE dbo.SchwabQuotes_Stage;")
#                 await conn.commit()
#                 print("Cleared staging tables.")
#
#                 insert_sql = """
#                     INSERT INTO dbo.SchwabQuotes_Raw
#                         (Symbol, LastPrice, BidPrice, AskPrice, QuoteTime, RawJson)
#                     VALUES (?, ?, ?, ?, ?, ?)
#                 """
#                 rows = 0
#                 for symbol, payload in quotes_json.items():
#                     quote = payload.get("quote", {}) if isinstance(payload, dict) else {}
#                     qt    = normalize_quote_time(
#                                 quote.get("quoteTime") or quote.get("tradeTime"))
#                     await cursor.execute(insert_sql, (
#                         symbol,
#                         quote.get("lastPrice"),
#                         quote.get("bidPrice"),
#                         quote.get("askPrice"),
#                         qt,
#                         json.dumps(payload),
#                     ))
#                     rows += 1
#
#                 await conn.commit()
#                 print(f"Inserted {rows} row(s) into dbo.SchwabQuotes_Raw.")
#                 result = rows
#
#                 await cursor.execute("EXEC usp_Load_SchwabQuotes;")
#                 await conn.commit()
#                 print("Ran usp_Load_SchwabQuotes.")
#     except Exception as exc:
#         error = exc
#     finally:
#         if callback:
#             callback(result, error)
#
#
# # ── CLI ───────────────────────────────────────────────────────────────────────
#
# def parse_args():
#     p = argparse.ArgumentParser(description="Schwab Market Data Loader (async)")
#     p.add_argument("--portfolio", type=int)
#     p.add_argument("--start")
#     p.add_argument("--end")
#     return p.parse_args()
#
#
# async def main():
#     args = parse_args()
#     loop = asyncio.get_event_loop()
#
#     # ── Resolve CLI / user inputs up front (blocking input is fine here) ──────
#     if args.portfolio is not None:
#         portfolio_id = args.portfolio
#     else:
#         raw = input("\nPortfolioID (default 1): ").strip()
#         portfolio_id = int(raw) if raw.isdigit() else 1
#
#     start_str = args.start or input("Start date (YYYY-MM-DD): ").strip()
#     end_str   = args.end   or input("End date   (YYYY-MM-DD): ").strip()
#     print(f"Date range: {start_str} → {end_str}")
#
#     # ── Step 3 callback: save quotes to DB, chain on_save ────────────────────
#     def on_save_done(fut):
#         if fut.exception():
#             print(f"[CB] DB save failed: {fut.exception()}")
#         else:
#             print(f"[CB] {fut.result()} row(s) saved to SQL Server.")
#         print("Done.")
#
#     # ── Step 2b callback: each price history result ───────────────────────────
#     def on_history_done(sym, fut):
#         if fut.exception():
#             print(f"[CB] History for {sym} failed: {fut.exception()}")
#         else:
#             candles = fut.result().get("candles", [])
#             print(f"[CB] History for {sym}: {len(candles)} candle(s) received.")
#
#     # ── Step 2a callback: quotes received → print + kick off DB save ─────────
#     def on_quotes_done(fut):
#         if fut.exception():
#             print(f"[CB] Quote fetch failed: {fut.exception()}")
#             return
#         data = fut.result()
#         print(f"[CB] Quotes received for: {', '.join(data.keys())}")
#         print("\n=== RAW QUOTE DATA ===")
#         print(json.dumps(data, indent=2))
#
#         # Chain: quotes done → start DB save
#         save_task = asyncio.create_task(save_quotes_to_db(data))
#         save_task.add_done_callback(on_save_done)
#
#     # ── Step 1b callback: symbols loaded → fire quote + history tasks ─────────
#     def on_symbols_done(fut):
#         if fut.exception():
#             print(f"[CB] Symbol lookup failed: {fut.exception()}")
#             return
#         symbols = fut.result()
#         print(f"[CB] {len(symbols)} symbol(s) loaded: {', '.join(symbols)}")
#         print(f"\nFetching quotes and price history concurrently for {len(symbols)} symbol(s)…")
#
#         # Chain: symbols done → fire quote task
#         quote_task = asyncio.create_task(get_quotes(symbols, _access_token))
#         quote_task.add_done_callback(on_quotes_done)
#
#         # Chain: symbols done → fire one history task per symbol
#         for sym in symbols:
#             hist_task = asyncio.create_task(
#                 get_price_history(sym, _access_token, start_str, end_str)
#             )
#             hist_task.add_done_callback(lambda fut, s=sym: on_history_done(s, fut))
#
#     # ── Step 1a callback: tokens ready → load symbols ─────────────────────────
#     def on_tokens_done(fut):
#         nonlocal _access_token
#         if fut.exception():
#             print(f"[CB] Token error: {fut.exception()}")
#             return
#         tokens = fut.result()
#         _access_token = tokens["access_token"]
#         print(f"[CB] Token obtained. Expires in: {tokens.get('expires_in')}s")
#
#         # Chain: token done → start symbol lookup
#         sym_task = asyncio.create_task(get_symbols_for_portfolio(portfolio_id))
#         sym_task.add_done_callback(on_symbols_done)
#
#     # ── Kick off the chain ────────────────────────────────────────────────────
#     # Only this one line starts everything; every subsequent step fires
#     # automatically from its predecessor's add_done_callback.
#     _access_token = None  # shared across callbacks via nonlocal
#
#     token_task = asyncio.create_task(get_valid_tokens())
#     token_task.add_done_callback(on_tokens_done)
#
#     # Wait for the token task; the rest of the chain runs on its own
#     await token_task
#
#     # Give chained tasks (symbols → quotes → save, history) time to complete
#     await asyncio.sleep(0)          # yield control so tasks can start
#     pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
#     if pending:
#         await asyncio.gather(*pending, return_exceptions=True)
#
#
# if __name__ == "__main__":
#     asyncio.run(main())



# ── Main (async orchestration with inline callbacks) ──────────────────────────

# async def main():
#     args = parse_args()
#
#     # ── Callback definitions ──────────────────────────────────────────────────
#     # Each callback follows the (result, error) contract.
#
#     def on_tokens(tokens, err):
#         if err:
#             print(f"[CB] Token error: {err}")
#         else:
#             print(f"[CB] Token obtained. Expires in: {tokens.get('expires_in')}s")
#
#     def on_symbols(symbols, err):
#         if err:
#             print(f"[CB] Symbol lookup error: {err}")
#         else:
#             print(f"[CB] {len(symbols)} symbol(s) loaded: {', '.join(symbols)}")
#
#     def on_quotes(data, err):
#         if err:
#             print(f"[CB] Quote fetch error: {err}")
#         else:
#             print(f"[CB] Quotes received for: {', '.join(data.keys())}")
#
#     def on_save(rows, err):
#         if err:
#             print(f"[CB] DB save error: {err}")
#         else:
#             print(f"[CB] {rows} quote row(s) saved to SQL Server.")
#
#     # ── Orchestration ─────────────────────────────────────────────────────────
#
#     tokens = await get_valid_tokens(callback=on_tokens)
#     if not tokens:
#         print("Could not obtain tokens. Exiting.")
#         return
#     access_token = tokens["access_token"]
#
#     # Resolve portfolio ID
#     if args.portfolio is not None:
#         portfolio_id = args.portfolio
#     else:
#         raw = input("\nPortfolioID (default 1): ").strip()
#         portfolio_id = int(raw) if raw.isdigit() else 1
#
#     # Resolve date range
#     start_str = args.start or input("Start date (YYYY-MM-DD): ").strip()
#     end_str   = args.end   or input("End date   (YYYY-MM-DD): ").strip()
#     print(f"Date range: {start_str} → {end_str}")
#
#     # Load symbols (async + callback)
#     symbols = await get_symbols_for_portfolio(portfolio_id, callback=on_symbols)
#     if not symbols:
#         return
#
#     # ── Concurrent quote + history fetches ───────────────────────────────────
#     # Fire quotes for all symbols AND price history for each symbol concurrently.
#
#     print(f"\nFetching quotes and price history concurrently for {len(symbols)} symbol(s)…")
#
#     quote_task   = asyncio.create_task(get_quotes(symbols, access_token, callback=on_quotes))
#     history_tasks = [
#         asyncio.create_task(
#             get_price_history(sym, access_token, start_str, end_str,
#                               callback=lambda d, e, s=sym: print(
#                                   f"[CB] History for {s}: "
#                                   + (f"error {e}" if e else f"{len(d.get('candles', []))} candles")))
#         )
#         for sym in symbols
#     ]
#
#     # Wait for everything; gather returns results in order
#     results = await asyncio.gather(quote_task, *history_tasks, return_exceptions=True)
#
#     quotes_data = results[0]
#     if isinstance(quotes_data, Exception):
#         print(f"Quote fetch failed: {quotes_data}")
#         return
#
#     print("\n=== RAW QUOTE DATA ===")
#     print(json.dumps(quotes_data, indent=2))
#
#     # Save quotes to SQL Server (async + callback)
#     print("\nSaving quotes to SQL Server…")
#     await save_quotes_to_db(quotes_data, callback=on_save)
#     print("Done.")


# if __name__ == "__main__":
#     asyncio.run(main())