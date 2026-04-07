"""
core/config.py
--------------
Centralised, immutable application configuration.

Design patterns used:
  - Singleton  : AppConfig is instantiated once via AppConfig.load(); subsequent
                 calls return the cached instance.
  - Value Object / dataclass : All fields are read-only after construction.
  - Factory method : AppConfig.load(path) is the only entry point.
"""

from __future__ import annotations

import configparser
import sys
from dataclasses import dataclass, field
from pathlib import Path


# ──────────────────────────────────────────────
# Path helper (works for .py and PyInstaller EXE)
# ──────────────────────────────────────────────
def _base_dir() -> Path:
    if getattr(sys, "frozen", False):           # PyInstaller bundle
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def resource_path(relative: str) -> Path:
    """Resolve a path relative to the project root (script or EXE)."""
    return _base_dir() / relative


# ──────────────────────────────────────────────
# Sub-config value objects
# ──────────────────────────────────────────────
@dataclass(frozen=True)
class SchwabConfig:
    app_key: str
    app_secret: str
    redirect_uri: str
    token_file: Path
    auth_url: str
    token_url: str
    quotes_url: str
    price_history_url: str


@dataclass(frozen=True)
class SqlConfig:
    driver: str
    server: str
    database: str
    username: str
    password: str
    trust_cert: str


# ──────────────────────────────────────────────
# Root config (Singleton)
# ──────────────────────────────────────────────
@dataclass(frozen=True)
class AppConfig:
    schwab: SchwabConfig
    sql: SqlConfig

    # ── Singleton bookkeeping ──────────────────
    _instance: "AppConfig | None" = field(default=None, init=False, repr=False, compare=False)

    # class-level cache (not stored on instance to keep frozen=True happy)
    _cache: "AppConfig | None" = None

    @classmethod
    def load(cls, config_file: str = "schwab_market_data.ini") -> "AppConfig":
        """
        Factory / Singleton.
        Reads the INI file once and caches the result.  Subsequent calls
        with the same path return the cached instance immediately.
        """
        if cls._cache is not None:
            return cls._cache

        path = resource_path(config_file)
        if not path.exists():
            raise FileNotFoundError(
                f"Config file not found: {path}\n"
                "Create schwab_market_data.ini next to the script / EXE."
            )

        parser = configparser.ConfigParser()
        if not parser.read(path):
            raise FileNotFoundError(f"Could not read config file: {path}")

        s = parser["schwab"]
        q = parser["sqlserver"]

        schwab = SchwabConfig(
            app_key=s["app_key"],
            app_secret=s["app_secret"],
            redirect_uri=s["redirect_uri"],
            token_file=resource_path(s.get("token_file", "schwab_tokens.json")),
            auth_url=s.get("auth_url", "https://api.schwabapi.com/v1/oauth/authorize"),
            token_url=s.get("token_url", "https://api.schwabapi.com/v1/oauth/token"),
            quotes_url=s.get("quotes_url", "https://api.schwabapi.com/marketdata/v1/quotes"),
            price_history_url=s.get(
                "price_history_url",
                "https://api.schwabapi.com/marketdata/v1/pricehistory",
            ),
        )

        sql = SqlConfig(
            driver=q["driver"],
            server=q["server"],
            database=q["database"],
            username=q["username"],
            password=q["password"],
            trust_cert=q.get("trust_cert", "yes"),
        )

        instance = cls(schwab=schwab, sql=sql)
        cls._cache = instance      # cache it
        return instance

    @classmethod
    def reset(cls) -> None:
        """Clear cached singleton (useful in unit tests)."""
        cls._cache = None
