# Schwab Market Data Loader

Fetches real-time quotes (and price history) from the **Schwab Market Data API**
and persists them to **SQL Server** staging tables.

---

## Project structure

```
schwab_loader/
│
├── main.py                        # Entry point — Composition Root
│
├── core/                          # Shared infrastructure
│   ├── config.py                  # AppConfig Singleton + value objects
│   ├── logging_setup.py           # TRACE level, setup_logging(), @traced
│   └── utils.py                   # Pure time-conversion helpers
│
├── schwab/                        # Schwab API layer
│   ├── auth.py                    # OAuth 2.0 (Template Method)
│   ├── market_data.py             # REST client Facade
│   └── token_store.py             # Token persistence Strategy
│
├── db/                            # SQL Server data access
│   ├── connection.py              # Factory + Context Manager
│   ├── portfolio_repository.py    # Portfolio/symbol reads
│   └── quote_repository.py        # Quote writes (Unit of Work)
│
├── cli/                           # Command-line interface
│   ├── args.py                    # argparse wiring → CliArgs dataclass
│   └── prompts.py                 # Interactive input helpers
│
└── logs/                          # Created at runtime
    └── schwab_market_data_YYYYMMDD.log
```

---

## Design patterns applied

| Pattern | Module(s) | Purpose |
|---|---|---|
| **Singleton** | `core/config.py` — `AppConfig.load()` | Config loaded once; every module gets the same instance |
| **Value Object** | `SchwabConfig`, `SqlConfig`, `CliArgs` | Immutable, frozen dataclasses; no accidental mutation |
| **Strategy** | `schwab/token_store.py` — `TokenStore` ABC + `FileTokenStore` | Swap file ↔ Redis ↔ DB token storage without touching auth code |
| **Template Method** | `schwab/auth.py` — `get_valid_tokens()` | Algorithm skeleton (load → refresh → full-login) with each step as a focused private method |
| **Facade** | `schwab/market_data.py` — `SchwabMarketDataClient` | Hides HTTP, headers, error handling behind `get_quotes()` / `get_price_history()` |
| **Repository** | `db/portfolio_repository.py`, `db/quote_repository.py` | Domain-language interface over SQL; callers never touch cursors |
| **Factory Method** | `db/connection.py` — `SqlConnectionFactory` | Centralises ODBC string assembly; one place to change connection params |
| **Context Manager** | `db/connection.py` — `.connect()` | Guarantees connection is closed even on exception; no leaks |
| **Unit of Work** | `db/quote_repository.py` — `save()` | Truncate + batch insert + stored proc run as one transaction |
| **Decorator** | `core/logging_setup.py` — `@traced` | Entry/exit/exception TRACE logs on every decorated function |
| **Dependency Injection** | Every class `__init__` | Dependencies injected, never imported globally; easy to unit-test |
| **Composition Root** | `main.py` | Only place where all objects are wired together |

---

## Configuration

Copy and fill in `schwab_market_data.ini` next to `main.py`:

```ini
[schwab]
app_key       = YOUR_APP_KEY
app_secret    = YOUR_APP_SECRET
redirect_uri  = https://your-callback-url/callback
token_file    = schwab_tokens.json

; optional overrides — defaults shown
auth_url          = https://api.schwabapi.com/v1/oauth/authorize
token_url         = https://api.schwabapi.com/v1/oauth/token
quotes_url        = https://api.schwabapi.com/marketdata/v1/quotes
price_history_url = https://api.schwabapi.com/marketdata/v1/pricehistory

[sqlserver]
driver   = ODBC Driver 17 for SQL Server
server   = YOUR_SERVER
database = YOUR_DATABASE
username = YOUR_USERNAME
password = YOUR_PASSWORD
trust_cert = yes
```

---

## Usage

```bash
# Interactive (prompts for portfolio ID and date range)
python main.py

# Fully non-interactive
python main.py --portfolio 2 --start 2024-01-01 --end 2024-12-31

# Custom log directory
python main.py --portfolio 1 --log-dir /var/log/schwab
```

---

## Logging

| Handler | Level | Location |
|---|---|---|
| Console (stdout) | DEBUG and above | Terminal |
| Rotating file | TRACE and above | `logs/schwab_market_data_YYYYMMDD.log` |

Log format:
```
2024-12-06 15:30:00.123 | TRACE    | auth.get_valid_tokens:42 | → ENTER get_valid_tokens()
2024-12-06 15:30:00.124 | DEBUG    | auth._refresh:88         | POST https://api.schwabapi.com/... grant_type=refresh_token
2024-12-06 15:30:00.890 | INFO     | auth._refresh:97         | Token refresh successful
```

The `@traced` decorator automatically emits `→ ENTER` / `← EXIT` lines at TRACE
level for every decorated function, and `✗ EXCEPTION` at ERROR level with full
traceback on any failure.

---

## SQL Server objects expected

| Object | Purpose |
|---|---|
| `dbo.SchwabMarketDataPortfolioSymbol` | Source of symbols per PortfolioID |
| `dbo.SchwabQuotes_Raw` | Raw quote staging (truncated each run) |
| `dbo.SchwabQuotes_Stage` | Transformed stage (loaded by stored proc) |
| `usp_Load_SchwabQuotes` | ETL stored procedure |

---

## Adding a new token store (Strategy example)

```python
# schwab/token_store.py  — add alongside FileTokenStore
class RedisTokenStore(TokenStore):
    def __init__(self, redis_client, key: str = "schwab:tokens"):
        self._r = redis_client
        self._key = key

    def load(self):
        data = self._r.get(self._key)
        return json.loads(data) if data else None

    def save(self, tokens):
        self._r.set(self._key, json.dumps(tokens))

    def clear(self):
        self._r.delete(self._key)

# main.py — swap one line, nothing else changes
token_store = RedisTokenStore(redis.Redis())
```

## Dependencies

```
pip install requests pyodbc
```
