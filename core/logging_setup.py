"""
core/logging_setup.py
---------------------
Custom TRACE logging level, rotating file + console setup,
and the @traced decorator used across all modules.

Design patterns used:
  - Decorator pattern  : @traced wraps any function with entry/exit/error logging
  - Singleton-ish      : setup_logging configures the root logger once;
                         all modules call logging.getLogger(__name__) after that
  - Open/Closed        : new handlers can be added without touching caller code
"""

from __future__ import annotations

import logging
import os
import sys
import traceback
from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Callable


# ──────────────────────────────────────────────
# Custom TRACE level (below DEBUG = 10)
# ──────────────────────────────────────────────
TRACE_LEVEL: int = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")


def _trace(self: logging.Logger, message: str, *args, **kwargs) -> None:
    """Bound method injected into logging.Logger as .trace()."""
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, message, args, **kwargs)


logging.Logger.trace = _trace  # type: ignore[attr-defined]


# ──────────────────────────────────────────────
# Logging initialiser
# ──────────────────────────────────────────────
_LOG_FMT = (
    "%(asctime)s.%(msecs)03d | %(levelname)-8s | "
    "%(module)s.%(funcName)s:%(lineno)d | %(message)s"
)
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(log_dir: Path | None = None) -> logging.Logger:
    """
    Configure the root logger once for the whole application.

    Handlers
    --------
    Console  (stdout)   DEBUG  and above
    File     (rotating) TRACE  and above  →  logs/schwab_market_data_YYYYMMDD.log
                        5 MB per file, 5 backups kept

    Returns the module-level logger for the caller's convenience.
    """
    if log_dir is None:
        log_dir = Path(__file__).resolve().parent.parent / "logs"

    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"schwab_market_data_{datetime.now():%Y%m%d}.log"
    formatter = logging.Formatter(_LOG_FMT, datefmt=_DATE_FMT)

    root = logging.getLogger()
    # Only configure once even if main() is called multiple times in tests
    if root.handlers:
        return logging.getLogger(__name__)

    root.setLevel(TRACE_LEVEL)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Rotating file handler
    try:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=5 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(TRACE_LEVEL)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
        print(f"[logging] Log file → {log_file}")
    except OSError as exc:
        print(f"[logging] WARNING: Cannot open log file {log_file}: {exc}")

    return logging.getLogger(__name__)


# ──────────────────────────────────────────────
# @traced decorator
# ──────────────────────────────────────────────
def traced(fn: Callable) -> Callable:
    """
    Decorator — emits TRACE-level entry / exit lines with
    truncated args and return value; logs exceptions at ERROR
    with full traceback and re-raises.

    Usage::

        @traced
        def my_func(x, y):
            ...
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        log = logging.getLogger(fn.__module__)

        def _clip(v: object) -> str:
            s = repr(v)
            return s if len(s) <= 200 else s[:197] + "..."

        arg_str = ", ".join(
            [_clip(a) for a in args] +
            [f"{k}={_clip(v)}" for k, v in kwargs.items()]
        )
        log.log(TRACE_LEVEL, "→ ENTER %s(%s)", fn.__qualname__, arg_str)
        try:
            result = fn(*args, **kwargs)
            log.log(TRACE_LEVEL, "← EXIT  %s → %s", fn.__qualname__, _clip(result))
            return result
        except Exception as exc:
            log.error(
                "✗ EXCEPTION in %s: %s\n%s",
                fn.__qualname__,
                exc,
                traceback.format_exc(),
            )
            raise

    return wrapper
