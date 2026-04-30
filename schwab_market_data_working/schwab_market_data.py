import argparse
import base64
import configparser
import json
import os
import sys
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import pyodbc
import requests


"""
Schwab Market Data Loader

High-level flow:
1. Read Schwab + SQL Server configuration from schwab_market_data.ini.
2. Ensure we have a valid Schwab OAuth access token (use refresh_token if possible).
3. Get PortfolioID (CLI arg or prompt).
4. Look up the list of symbols for that PortfolioID from SQL Server table
   dbo.SchwabMarketDataPortfolioSymbol.
5. Call Schwab Market Data Quotes API for those symbols.
6. Print the raw JSON response.
7. Save quote data into SQL Server.
"""


# ---------- Helper to resolve paths (script vs PyInstaller EXE) ----------

def resource_path(relative_path: str) -> Path:
    """
    Get path to resource (works for .py and PyInstaller .exe).

    - When frozen (PyInstaller), use the folder where the .exe lives.
    - When running as a standard Python script, use the folder where this .py file lives.
    """
    if getattr(sys, "frozen", False):
        base_path = Path(sys.executable).resolve().parent
    else:
        base_path = Path(__file__).resolve().parent
    return base_path / relative_path


# ---------- Config loading ----------

CONFIG_FILENAME = "../schwab_market_data.ini"
config_path = resource_path(CONFIG_FILENAME)

if not config_path.exists():
    raise FileNotFoundError(f"{CONFIG_FILENAME} not found at {config_path}")

config = configparser.ConfigParser()
read_files = config.read(config_path)

if not read_files:
    raise FileNotFoundError(f"{CONFIG_FILENAME} not found at {config_path}")

schwab_cfg = config["schwab"]
sql_cfg = config["sqlserver"]


# ---------- Schwab app/API configuration ----------

APP_KEY = schwab_cfg["app_key"]
APP_SECRET = schwab_cfg["app_secret"]
REDIRECT_URI = schwab_cfg["redirect_uri"]

# Store token file next to the script/EXE
TOKEN_FILE = str(resource_path(schwab_cfg.get("token_file", "schwab_tokens.json")))

AUTH_URL = schwab_cfg.get("auth_url", "https://api.schwabapi.com/v1/oauth/authorize")
TOKEN_URL = schwab_cfg.get("token_url", "https://api.schwabapi.com/v1/oauth/token")
QUOTES_URL = schwab_cfg.get("quotes_url", "https://api.schwabapi.com/marketdata/v1/quotes")
PRICE_HISTORY_URL = schwab_cfg.get(
    "price_history_url",
    "https://api.schwabapi.com/marketdata/v1/pricehistory"
)


# ---------- SQL Server configuration ----------

SQL_DRIVER = sql_cfg["driver"]
SQL_SERVER = sql_cfg["server"]
SQL_DATABASE = sql_cfg["database"]
SQL_USERNAME = sql_cfg["username"]
SQL_PASSWORD = sql_cfg["password"]
SQL_TRUST_CERT = sql_cfg.get("trust_cert", "yes")


# ---------- Time conversion helpers ----------

def ms_to_datetime(ms: int) -> datetime:
    """
    Convert milliseconds since Unix epoch to naive UTC datetime
    suitable for SQL Server datetime2.
    """
    dt_utc = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt_utc.replace(tzinfo=None)


def normalize_quote_time(quote_time_raw):
    """
    Normalize quote time into naive UTC datetime if possible.
    Handles:
      - epoch milliseconds
      - epoch seconds
      - ISO 8601 strings
    """
    if isinstance(quote_time_raw, (int, float)):
        ts = float(quote_time_raw)
        if ts > 10**10:
            return ms_to_datetime(ts)
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt_utc.replace(tzinfo=None)

    if isinstance(quote_time_raw, str):
        try:
            s = quote_time_raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=None)
        except Exception:
            return None

    return None


# ---------- Token helpers ----------

def load_tokens():
    """
    Load saved OAuth tokens if present.
    """
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_tokens(tokens: dict):
    """
    Save OAuth tokens to disk.
    """
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2)
    print(f"Tokens saved to {TOKEN_FILE}")


# ---------- OAuth helpers ----------

def build_authorize_url() -> str:
    """
    Build the Schwab OAuth authorize URL.
    """
    return f"{AUTH_URL}?client_id={APP_KEY}&redirect_uri={REDIRECT_URI}"


def extract_code_from_redirect_url(redirect_url: str) -> str:
    """
    Extract and decode the OAuth authorization code from the full redirect URL.
    """
    redirect_url = redirect_url.strip()

    if not redirect_url:
        raise ValueError(
            "No redirect URL was pasted.\n"
            "You must paste the FULL callback URL from the browser address bar.\n"
            "Example:\n"
            "https://carfourless.com/schwab/callback?code=...&session=..."
        )

    parsed = urlparse(redirect_url)
    qs = parse_qs(parsed.query)

    code_list = qs.get("code")

    if not code_list or not code_list[0].strip():
        raise ValueError(
            "No 'code' parameter found in the pasted URL.\n"
            f"URL received:\n{redirect_url}\n\n"
            "Make sure you copied the FULL URL AFTER login redirect."
        )

    code = code_list[0].strip()
    code = unquote(code)

    return code


def get_tokens_from_auth_code(auth_code: str) -> dict:
    """
    Exchange authorization code for access_token + refresh_token.
    """
    creds = f"{APP_KEY}:{APP_SECRET}"
    encoded_creds = base64.b64encode(creds.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {encoded_creds}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": REDIRECT_URI,
    }

    resp = requests.post(TOKEN_URL, headers=headers, data=data)
    if resp.status_code != 200:
        raise RuntimeError(f"Error getting tokens: {resp.status_code} {resp.text}")

    return resp.json()


def initial_login_flow() -> dict:
    """
    Run full interactive OAuth login flow.
    """
    url = build_authorize_url()
    print("\n=== STEP 1: Authorize the app in your browser ===")
    print("Opening browser with Schwab login page...")
    print("If it doesn't open automatically, copy/paste this URL into your browser:")
    print(url)

    try:
        webbrowser.open(url)
    except Exception:
        pass

    print("\nAfter you log in and approve, Schwab will redirect you to your callback URL.")
    print("The page may show an error or be blank. That's OK.")
    print(">>> Copy the FULL URL from the browser's address bar and paste it below.")
    redirect_url = input("\nPaste full redirect URL here: ").strip()

    auth_code = extract_code_from_redirect_url(redirect_url)
    print(f"\nGot authorization code (truncated): {auth_code[:20]}...")

    tokens = get_tokens_from_auth_code(auth_code)
    save_tokens(tokens)
    print("Initial tokens obtained successfully.")
    return tokens


def refresh_access_token(tokens: dict) -> dict:
    """
    Use refresh_token to obtain a new access_token.
    """
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise RuntimeError("No refresh_token available. Run initial login again.")

    creds = f"{APP_KEY}:{APP_SECRET}"
    encoded_creds = base64.b64encode(creds.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {encoded_creds}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    resp = requests.post(TOKEN_URL, headers=headers, data=data)
    if resp.status_code != 200:
        raise RuntimeError(f"Error refreshing token: {resp.status_code} {resp.text}")

    new_tokens = resp.json()
    save_tokens(new_tokens)
    print("Access token refreshed.")
    return new_tokens


def get_valid_tokens() -> dict:
    """
    Ensure we have a valid access token.
    """
    tokens = load_tokens()

    if tokens is None:
        print("No tokens found on disk. Running full login flow...")
        tokens = initial_login_flow()
        save_tokens(tokens)
        return tokens

    try:
        print("Attempting to refresh access token...")
        new_tokens = refresh_access_token(tokens)
        save_tokens(new_tokens)
        return new_tokens
    except Exception as e:
        print(f"Refreshing token failed: {e}")
        print("Falling back to full login flow...")
        tokens = initial_login_flow()
        save_tokens(tokens)
        return tokens


# ---------- SQL Server helpers ----------

def get_sql_connection():
    """
    Create and return a SQL Server connection.
    """
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate={SQL_TRUST_CERT};"
    )
    return pyodbc.connect(conn_str)


def get_symbols_for_portfolio(portfolio_id: int) -> list[str]:
    """
    Load active symbols for a portfolio from SQL Server.
    """
    conn = get_sql_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT DISTINCT Symbol
        FROM dbo.SchwabMarketDataPortfolioSymbol
        WHERE PortfolioID = ?
          AND IsActive = 1
        """,
        portfolio_id,
    )

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    symbols = [row[0].strip().upper() for row in rows if row[0]]

    if not symbols:
        raise ValueError(f"No symbols found for PortfolioID = {portfolio_id}")

    return symbols


# ---------- Market data ----------

def get_quotes(symbols, access_token: str) -> dict:
    """
    Get quotes from Schwab Market Data API.
    """
    if isinstance(symbols, str):
        symbols = [symbols]

    params = {
        "symbols": ",".join(symbols),
        "fields": "quote,fundamental",
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    resp = requests.get(QUOTES_URL, headers=headers, params=params)
    if resp.status_code != 200:
        raise RuntimeError(f"Error fetching quotes: {resp.status_code} {resp.text}")

    return resp.json()


def get_price_history(symbol, access_token, start_date, end_date, frequency="daily"):
    """
    Get price history from Schwab API.
    """
    params = {
        "symbol": symbol,
        "startDate": start_date,
        "endDate": end_date,
    }
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(PRICE_HISTORY_URL, headers=headers, params=params)
    if resp.status_code != 200:
        raise RuntimeError(f"Error fetching history: {resp.status_code} {resp.text}")
    return resp.json()


def save_quotes_to_db(quotes_json: dict):
    """
    Save quote payload into SQL Server raw/stage tables.
    """
    if not quotes_json:
        print("No quote data to save.")
        return

    conn = get_sql_connection()
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE dbo.SchwabQuotes_Raw;")
    cursor.execute("TRUNCATE TABLE dbo.SchwabQuotes_Stage;")
    conn.commit()
    print("Cleared dbo.SchwabQuotes_Raw and dbo.SchwabQuotes_Stage before insert.")

    insert_sql = """
        INSERT INTO dbo.SchwabQuotes_Raw
            (Symbol, LastPrice, BidPrice, AskPrice, QuoteTime, RawJson)
        VALUES
            (?, ?, ?, ?, ?, ?)
    """

    rows_inserted = 0

    for symbol, payload in quotes_json.items():
        quote = payload.get("quote", {}) if isinstance(payload, dict) else {}

        last_price = quote.get("lastPrice")
        bid_price = quote.get("bidPrice")
        ask_price = quote.get("askPrice")

        quote_time_raw = quote.get("quoteTime") or quote.get("tradeTime") or None
        quote_time = normalize_quote_time(quote_time_raw)

        raw_json_str = json.dumps(payload)

        params = (
            symbol,
            last_price,
            bid_price,
            ask_price,
            quote_time,
            raw_json_str,
        )

        cursor.execute(insert_sql, params)
        rows_inserted += 1

    conn.commit()
    print(f"Inserted {rows_inserted} row(s) into dbo.SchwabQuotes_Raw.")

    cursor.execute("EXEC usp_Load_SchwabQuotes;")
    conn.commit()
    print("Ran usp_Load_SchwabQuotes to load into dbo.SchwabQuotes_Stage.")

    cursor.close()
    conn.close()


# ---------- CLI ----------

def parse_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Schwab Market Data Loader")
    parser.add_argument("--portfolio", type=int, help="PortfolioID to load")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    return parser.parse_args()


# ---------- Main ----------

def main():
    args = parse_args()

    tokens = get_valid_tokens()
    access_token = tokens["access_token"]

    if args.portfolio is not None:
        portfolio_id = args.portfolio
    else:
        print("\nEnter PortfolioID to load symbols from (default: 1):")
        portfolio_input = input("> ").strip()
        if portfolio_input:
            try:
                portfolio_id = int(portfolio_input)
            except ValueError:
                print(f"Invalid PortfolioID '{portfolio_input}'. Using default 1.")
                portfolio_id = 1
        else:
            portfolio_id = 1

    start_str = args.start if args.start else input("Start date (YYYY-MM-DD): ").strip()
    end_str = args.end if args.end else input("End date   (YYYY-MM-DD): ").strip()

    print(f"Date range: {start_str} to {end_str}")

    symbols = get_symbols_for_portfolio(portfolio_id)

    print(f"\nLoaded {len(symbols)} symbol(s) from PortfolioID {portfolio_id}:")
    print(", ".join(symbols))

    print(f"\nRequesting quotes for: {', '.join(symbols)}")
    data = get_quotes(symbols, access_token)

    print("\n=== RAW QUOTE DATA ===")
    print(json.dumps(data, indent=2))

    print("\nSaving quotes to SQL Server...")
    save_quotes_to_db(data)
    print("Done.")


if __name__ == "__main__":
    main()
###########################################################
###########################################################
###########################################################

# import base64
# import webbrowser
# import traceback
# import requests
# import configparser
# import urllib.request
# import urllib.parse
# import json
# import sys
# import os
# import pyodbc
# #
# from datetime import datetime, timedelta, timezone
# from urllib.parse import urlparse, parse_qs
# from pathlib import Path
#
# """
# Schwab Market Data Loader
#
# High-level flow:
# 1. Read Schwab + SQL Server configuration from schwab_market_data.ini.
# 2. Ensure we have a valid Schwab OAuth access token (use refresh_token if possible).
# 3. Ask user for a PortfolioID.
# 4. Look up the list of symbols for that PortfolioID from SQL Server table
#    dbo.SchwabMarketDataPortfolioSymbol.
# 5. Call Schwab Market Data Quotes API for those symbols.
# 6. Print the raw JSON response (for debugging/verification).
# 7. Save quote data into SQL Server:
#    - Truncate raw/stage tables.
#    - Insert into dbo.SchwabQuotes_Raw.
#    - Call usp_Load_SchwabQuotes to transform/load into dbo.SchwabQuotes_Stage.
# """
#
# # ====== CONFIG LOADING FROM INI FILE ======
# # First attempt: try to read schwab_market_data.ini from the same folder as this script.
# config = configparser.ConfigParser()
# config_path = os.path.join(os.path.dirname(__file__), "schwab_market_data.ini")
# read_files = config.read(config_path)
#
# if not read_files:
#     raise FileNotFoundError(f"config.ini not found at {config_path}")
#
# schwab_cfg = config["schwab"]  # not strictly needed here, reloaded below
#
# # ---------- Helper to resolve paths (script vs PyInstaller EXE) ----------
#
# def resource_path(relative_path: str) -> Path:
#     """
#     Get path to resource (works for .py and PyInstaller .exe).
#
#     - When frozen (PyInstaller), use the folder where the .exe lives.
#     - When running as a standard Python script, use the folder where this .py file lives.
#     """
#     if getattr(sys, "frozen", False):
#         # Running as a bundled EXE
#         base_path = Path(sys.executable).resolve().parent
#     else:
#         # Running as a normal script
#         base_path = Path(__file__).resolve().parent
#
#     return base_path / relative_path
#
# # Second attempt: use resource_path to handle both script + packaged EXE.
# CONFIG_FILENAME = "schwab_market_data.ini"
# config_path = resource_path(CONFIG_FILENAME)
#
# if not config_path.exists():
#     # If the INI can't be found next to the script/EXE, we fail early.
#     raise FileNotFoundError(f"{CONFIG_FILENAME} not found at {config_path}")
#
# config = configparser.ConfigParser()
# read_files = config.read(config_path)
#
# if not read_files:
#     raise FileNotFoundError(f"{CONFIG_FILENAME} not found at {config_path}")
#
# # --- Schwab + SQL configuration sections ---
# schwab_cfg = config["schwab"]
# sql_cfg = config["sqlserver"]
#
# # --- Schwab app/API configuration ---
# APP_KEY      = schwab_cfg["app_key"]       # Schwab Developer App Key (client_id)
# APP_SECRET   = schwab_cfg["app_secret"]    # Schwab Developer App Secret (client_secret)
# REDIRECT_URI = schwab_cfg["redirect_uri"]  # Must match callback URL in Schwab app config
#
# TOKEN_FILE = schwab_cfg.get("token_file", "schwab_tokens.json")
#
# AUTH_URL   = schwab_cfg.get("auth_url",   "https://api.schwabapi.com/v1/oauth/authorize")
# TOKEN_URL  = schwab_cfg.get("token_url",  "https://api.schwabapi.com/v1/oauth/token")
# QUOTES_URL = schwab_cfg.get("quotes_url", "https://api.schwabapi.com/marketdata/v1/quotes")
# #
# ## history
# PRICE_HISTORY_URL = schwab_cfg.get(
#     "price_history_url",
#     "https://api.schwabapi.com/marketdata/v1/pricehistory"
# )
#
#
# # --- SQL Server configuration ---
# SQL_DRIVER     = sql_cfg["driver"]            # e.g. "ODBC Driver 17 for SQL Server"
# SQL_SERVER     = sql_cfg["server"]
# SQL_DATABASE   = sql_cfg["database"]
# SQL_USERNAME   = sql_cfg["username"]
# SQL_PASSWORD   = sql_cfg["password"]
# SQL_TRUST_CERT = sql_cfg.get("trust_cert", "yes")
#
# config_path = resource_path("schwab_market_data.ini")
#
# if not config_path.exists():
#     raise FileNotFoundError(f"config.ini not found at {config_path}")
#
# # ---------- Time conversion helpers ----------
#
# def ms_to_datetime(ms: int) -> datetime:
#     """
#     Convert milliseconds since Unix epoch to a naive datetime
#     (no timezone) suitable for a SQL Server datetime2 column.
#
#     Schwab quoteTime/tradeTime may come back as epoch milliseconds;
#     this helper normalizes that to a Python datetime in UTC and then
#     strips tzinfo (so you can store in a naive datetime2 column).
#     """
#     dt_utc = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
#     # Drop tzinfo if your SQL column is naive (datetime2 without offset)
#     return dt_utc.replace(tzinfo=None)
#
#
# # ---------- Helpers for token file ----------
#
# def load_tokens():
#     """
#     Load previously saved OAuth tokens from disk, if present.
#
#     Returns:
#         dict or None: Parsed JSON with at least access_token/refresh_token,
#         or None if no token file exists.
#     """
#     if not os.path.exists(TOKEN_FILE):
#         return None
#     with open(TOKEN_FILE, "r") as f:
#         return json.load(f)
#
#
# def save_tokens(tokens: dict):
#     """
#     Persist OAuth tokens to disk, so we can reuse refresh_token
#     instead of doing full browser login every time.
#     """
#     with open(TOKEN_FILE, "w") as f:
#         json.dump(tokens, f, indent=2)
#     print(f"Tokens saved to {TOKEN_FILE}")
#
#
# # ---------- OAuth step 1: get authorization code ----------
#
# def build_authorize_url() -> str:
#     """
#     Build the Schwab authorization URL to initiate the OAuth flow.
#
#     User flow:
#     1. Open this URL in a browser.
#     2. Log in and approve.
#     3. Schwab redirects to REDIRECT_URI with ?code=... in the query string.
#     """
#     # NOTE: Schwab expects exactly client_id and redirect_uri here
#     # Format: https://api.schwabapi.com/v1/oauth/authorize?client_id=...&redirect_uri=...
#     return f"{AUTH_URL}?client_id={APP_KEY}&redirect_uri={REDIRECT_URI}"
#
#
# def extract_code_from_redirect_url(redirect_url: str) -> str:
#     """
#     Parse the full redirect URL from the browser address bar
#     and extract the OAuth authorization `code` parameter.
#     """
#     parsed = urlparse(redirect_url)
#     qs = parse_qs(parsed.query)
#     code_list = qs.get("code")
#     if not code_list:
#         raise ValueError("No 'code' parameter found in the URL. Did you paste the full redirect URL?")
#     code = code_list[0]
#     return code
#
#
# def get_tokens_from_auth_code(auth_code: str) -> dict:
#     """
#     Exchange authorization code for access_token + refresh_token.
#
#     POST https://api.schwabapi.com/v1/oauth/token
#
#     This is called once during the initial login flow.
#     """
#     # Basic auth: base64(APP_KEY:APP_SECRET)
#     creds = f"{APP_KEY}:{APP_SECRET}"
#     encoded_creds = base64.b64encode(creds.encode("utf-8")).decode("utf-8")
#
#     headers = {
#         "Authorization": f"Basic {encoded_creds}",
#         "Content-Type": "application/x-www-form-urlencoded",
#     }
#
#     data = {
#         "grant_type": "authorization_code",
#         "code": auth_code,
#         "redirect_uri": REDIRECT_URI,
#     }
#
#     resp = requests.post(TOKEN_URL, headers=headers, data=data)
#     if resp.status_code != 200:
#         # If this fails, check app_key/app_secret/redirect_uri and whether the code is still valid.
#         raise RuntimeError(f"Error getting tokens: {resp.status_code} {resp.text}")
#
#     tokens = resp.json()
#     return tokens
#
#
# def initial_login_flow() -> dict:
#     """
#     Full first-time login flow:
#
#       1. Build Schwab authorization URL.
#       2. Open in user's web browser.
#       3. Prompt user to paste the FULL redirect URL (after login).
#       4. Extract authorization code from that URL.
#       5. Exchange code for access_token + refresh_token.
#       6. Save tokens to disk.
#
#     This should only be needed the first time (or if refresh_token stops working).
#     """
#     url = build_authorize_url()
#     print("\n=== STEP 1: Authorize the app in your browser ===")
#     print("Opening browser with Schwab login page...")
#     print("If it doesn't open automatically, copy/paste this URL into your browser:")
#     print(url)
#
#     # Try to open browser automatically
#     try:
#         webbrowser.open(url)
#     except Exception:
#         # If browser can't open automatically, user still sees the URL to copy manually.
#         pass
#
#     print("\nAfter you log in and approve, Schwab will redirect you to your callback URL.")
#     print("The page may show an error or be blank. That's OK.")
#     print(">>> Copy the FULL URL from the browser's address bar and paste it below.")
#     redirect_url = input("\nPaste full redirect URL here: ").strip()
#
#     auth_code = extract_code_from_redirect_url(redirect_url)
#     print(f"\nGot authorization code (truncated): {auth_code[:20]}...")
#
#     tokens = get_tokens_from_auth_code(auth_code)
#     save_tokens(tokens)
#     print("Initial tokens obtained successfully.")
#     return tokens
#
#
# # ---------- OAuth step 2: refresh access token using refresh_token ----------
#
# def refresh_access_token(tokens: dict) -> dict:
#     """
#     Use an existing refresh_token to fetch a new access_token.
#
#     This is the normal path once a token file already exists.
#     """
#     refresh_token = tokens.get("refresh_token")
#     if not refresh_token:
#         raise RuntimeError("No refresh_token available. Run initial login again.")
#
#     creds = f"{APP_KEY}:{APP_SECRET}"
#     encoded_creds = base64.b64encode(creds.encode("utf-8")).decode("utf-8")
#
#     headers = {
#         "Authorization": f"Basic {encoded_creds}",
#         "Content-Type": "application/x-www-form-urlencoded",
#     }
#
#     data = {
#         "grant_type": "refresh_token",
#         "refresh_token": refresh_token,
#     }
#
#     resp = requests.post(TOKEN_URL, headers=headers, data=data)
#     if resp.status_code != 200:
#         # Common causes: refresh_token expired, app credentials changed, etc.
#         raise RuntimeError(f"Error refreshing token: {resp.status_code} {resp.text}")
#
#     new_tokens = resp.json()
#     save_tokens(new_tokens)
#     print("Access token refreshed.")
#     return new_tokens
#
#
# def get_valid_tokens() -> dict:
#     """
#     Ensure we have a valid access token.
#
#     Flow:
#     1. Try to load tokens from disk.
#     2. If none found -> run full browser login.
#     3. If tokens found -> try to refresh using refresh_token.
#     4. If refresh fails for any reason -> fall back to full login again.
#     """
#     # 1) Load whatever we already have
#     tokens = load_tokens()  # should return dict or None
#
#     if tokens is None:
#         print("No tokens found on disk. Running full login flow...")
#         tokens = initial_login_flow()   # does browser OAuth, returns dict
#         save_tokens(tokens)             # ensure we persist them
#         return tokens
#
#     # 2) Try to refresh using refresh_token
#     try:
#         print("Attempting to refresh access token...")
#         new_tokens = refresh_access_token(tokens)  # should return updated dict
#         save_tokens(new_tokens)
#         return new_tokens
#
#     except Exception as e:
#         # Any error (expired/invalid refresh token, bad request, etc.)
#         print(f"Refreshing token failed: {e}")
#         print("Falling back to full login flow...")
#         tokens = initial_login_flow()
#         save_tokens(tokens)
#         return tokens
#
#
# # ---------- Market data: get symbols from portfolio table ----------
#
# def get_symbols_for_portfolio(portfolio_id: int) -> list[str]:
#     """
#     Load distinct symbols for a given PortfolioID from SQL Server.
#
#     Expects SQL table:
#         dbo.SchwabMarketDataPortfolioSymbol
#             - PortfolioID INT
#             - Symbol VARCHAR(...)
#             - IsActive BIT
#             - (other optional columns)
#
#     Only rows with IsActive = 1 are used.
#     """
#     conn = get_sql_connection()
#     cursor = conn.cursor()
#
#     cursor.execute(
#         """
#         SELECT DISTINCT Symbol
#         FROM dbo.SchwabMarketDataPortfolioSymbol
#         WHERE PortfolioID = ?
#           AND IsActive = 1
#         """,
#         portfolio_id,
#     )
#
#     rows = cursor.fetchall()
#     cursor.close()
#     conn.close()
#
#     # Normalize symbols to uppercase, strip any whitespace, and ignore NULLs.
#     symbols = [row[0].strip().upper() for row in rows if row[0]]
#
#     if not symbols:
#         # Fail fast if the chosen PortfolioID has no symbols.
#         raise ValueError(f"No symbols found for PortfolioID = {portfolio_id}")
#
#     return symbols
#
#
# # ---------- Market data: call Schwab quotes API ----------
#
# def get_quotes(symbols, access_token: str) -> dict:
#     """
#     Call Schwab Market Data API to get quotes for a list of symbols.
#
#     Endpoint (per Schwab docs):
#         GET /marketdata/v1/quotes?symbols=...&fields=quote,fundamental
#
#     Args:
#         symbols: list[str] or single str symbol.
#         access_token: OAuth access token with Market Data scope.
#
#     Returns:
#         dict: JSON response from Schwab (symbol -> { quote, fundamental, ... })
#     """
#     if isinstance(symbols, str):
#         symbols = [symbols]
#
#     params = {
#         "symbols": ",".join(symbols),
#         "fields": "quote,fundamental",  # Adjustable if you want more/less data
#     }
#
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#     }
#
#     resp = requests.get(QUOTES_URL, headers=headers, params=params)
#     if resp.status_code != 200:
#         raise RuntimeError(f"Error fetching quotes: {resp.status_code} {resp.text}")
#
#     return resp.json()
#
#
# def normalize_quote_time(quote_time_raw):
#     """
#     Normalize Schwab's quote time value into a Python datetime, if possible.
#
#     Handles:
#       - Epoch milliseconds (int/float, very large numbers like 1700000000000)
#       - Epoch seconds (int/float, smaller numbers like 1700000000)
#       - ISO 8601 strings like '2024-12-06T15:30:00Z'
#
#     Returns:
#       datetime (naive, UTC-based) or None if the value can't be parsed.
#     """
#     # Numeric timestamps
#     if isinstance(quote_time_raw, (int, float)):
#         ts = float(quote_time_raw)
#         # Heuristic: if it's bigger than 10^10, it's probably milliseconds
#         if ts > 10**10:     # 10000000000
#             return ms_to_datetime(ts)  # treat as milliseconds
#         else:
#             # treat as seconds
#             dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
#             return dt_utc.replace(tzinfo=None)
#
#     # String timestamps (e.g., ISO 8601 like 2024-12-06T15:30:00Z)
#     if isinstance(quote_time_raw, str):
#         try:
#             s = quote_time_raw.replace("Z", "+00:00")  # support trailing Z
#             dt = datetime.fromisoformat(s)
#             return dt.replace(tzinfo=None)
#         except Exception:
#             return None
#
#     # Anything else: we don't know how to interpret it
#     return None
#
#
# ####### SQL Server helpers ########
#
# def get_sql_connection():
#     """
#     Create and return a new pyodbc connection to SQL Server
#     using values from the [sqlserver] section of the INI file.
#     """
#     conn_str = (
#         f"DRIVER={{{SQL_DRIVER}}};"
#         f"SERVER={SQL_SERVER};"
#         f"DATABASE={SQL_DATABASE};"
#         f"UID={SQL_USERNAME};"
#         f"PWD={SQL_PASSWORD};"
#         f"TrustServerCertificate={SQL_TRUST_CERT};"
#     )
#     return pyodbc.connect(conn_str)
#
#
# def save_quotes_to_db(quotes_json: dict):
#     """
#     Save Schwab quote response into SQL Server staging.
#
#     Expected quotes_json structure (simplified):
#         {
#           "AAPL": { "quote": {...}, "fundamental": {...} },
#           "MSFT": { "quote": {...}, "fundamental": {...} },
#           ...
#         }
#
#     Processing steps:
#       1. TRUNCATE dbo.SchwabQuotes_Raw and dbo.SchwabQuotes_Stage.
#       2. Insert each symbol's quote data into dbo.SchwabQuotes_Raw.
#       3. Execute usp_Load_SchwabQuotes to transform/load into dbo.SchwabQuotes_Stage.
#     """
#
#     if not quotes_json:
#         print("No quote data to save.")
#         return
#
#     conn = get_sql_connection()
#     cursor = conn.cursor()
#
#     # Clear staging/raw tables first to avoid mixing runs.
#     cursor.execute("TRUNCATE TABLE dbo.SchwabQuotes_Raw;")
#     cursor.execute("TRUNCATE TABLE dbo.SchwabQuotes_Stage;")
#     conn.commit()
#     print("Cleared dbo.SchwabQuotes_Raw and dbo.SchwabQuotes_Stage before insert.")
#
#     insert_sql = """
#         INSERT INTO dbo.SchwabQuotes_Raw
#             (Symbol, LastPrice, BidPrice, AskPrice, QuoteTime, RawJson)
#         VALUES
#             (?, ?, ?, ?, ?, ?)
#     """
#
#     rows_inserted = 0
#
#     for symbol, payload in quotes_json.items():
#         # Each payload is expected to be a dict that includes a "quote" child object.
#         quote = payload.get("quote", {}) if isinstance(payload, dict) else {}
#
#         # Pull some key fields from the quote section; defaults to None if not present.
#         last_price = quote.get("lastPrice")
#         bid_price  = quote.get("bidPrice")
#         ask_price  = quote.get("askPrice")
#
#         # Schwab may give quoteTime or tradeTime; try quoteTime first, then tradeTime.
#         quote_time_raw = quote.get("quoteTime") or quote.get("tradeTime") or None
#         quote_time = normalize_quote_time(quote_time_raw)
#
#         # Store the entire payload as JSON text for debugging/auditing.
#         raw_json_str = json.dumps(payload)
#
#         params = (
#             symbol,
#             last_price,
#             bid_price,
#             ask_price,
#             quote_time,
#             raw_json_str,
#         )
#
#         cursor.execute(insert_sql, params)
#         rows_inserted += 1
#
#     # Commit all inserts at once for better performance.
#     conn.commit()
#     print(f"Inserted {rows_inserted} row(s) into dbo.SchwabQuotes_Raw.")
#
#     # Now run your ETL/transform stored procedure once per batch.
#     cursor.execute("EXEC usp_Load_SchwabQuotes;")
#     conn.commit()
#     print("Ran usp_Load_SchwabQuotes to load into dbo.SchwabQuotes_Stage.")
#
#     cursor.close()
#     conn.close()
#
# def get_price_history(symbol, access_token, start_date, end_date, frequency="daily"):
#     params = {
#         "symbol": symbol,
#         "startDate": start_date,  # you adapt to API’s exact format
#         "endDate": end_date,
#         # plus any other required params – frequency, periodType etc.
#     }
#     headers = {"Authorization": f"Bearer {access_token}"}
#     resp = requests.get(PRICE_HISTORY_URL, headers=headers, params=params)
#     if resp.status_code != 200:
#         raise RuntimeError(f"Error fetching history: {resp.status_code} {resp.text}")
#     return resp.json()
#
#
#
# ##################### Main #################################
# # ---------- Main entry point ----------
#
# def main():
#     """
#     Main orchestration function:
#       1. Ensure we have a valid Schwab access_token.
#       2. Prompt user for a PortfolioID (default 1).
#       3. Load symbols for that PortfolioID from SQL Server.
#       4. Call Schwab quotes API for those symbols.
#       5. Print raw JSON (for inspection).
#       6. Save quotes into SQL Server staging tables.
#     """
#     # 1. Get a valid access_token (refresh if possible; otherwise full login).
#     tokens = get_valid_tokens()
#     access_token = tokens["access_token"]
#
#     # 2. Ask which portfolio to load, then get symbols from DB.
#     print("\nEnter PortfolioID to load symbols from (default: 1):")
#     portfolio_input = input("> ").strip()
#     # 2.a get time frame
#     start_str = input("Start date (YYYY-MM-DD): ").strip()
#     end_str = input("End date   (YYYY-MM-DD): ").strip()
#
#
#     if portfolio_input:
#         try:
#             portfolio_id = int(portfolio_input)
#         except ValueError:
#             print(f"Invalid PortfolioID '{portfolio_input}'. Using default 1.")
#             portfolio_id = 1
#     else:
#         portfolio_id = 1
#
#     # Get symbols from SQL Server for this portfolio.
#     symbols = get_symbols_for_portfolio(portfolio_id)
#
#
#
#     print(f"\nLoaded {len(symbols)} symbol(s) from PortfolioID {portfolio_id}:")
#     print(", ".join(symbols))
#
#     # 3. Call market data API & load symbols.
#     print(f"\nRequesting quotes for: {', '.join(symbols)}")
#     data = get_quotes(symbols, access_token)
#
#     # 4. Pretty-print raw JSON (you can remove this in production if too noisy).
#     print("\n=== RAW QUOTE DATA ===")
#     print(json.dumps(data, indent=2))
#
#     # 5. Save to SQL Server staging / final tables.
#     print("\nSaving quotes to SQL Server...")
#     save_quotes_to_db(data)
#     print("Done.")
#
#
# if __name__ == "__main__":
#     main()
