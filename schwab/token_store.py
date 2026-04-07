"""
schwab/token_store.py
---------------------
Token persistence abstraction.

Design patterns used:
  - Strategy  : TokenStore is an abstract base; FileTokenStore is the concrete
                implementation.  A future RedisTokenStore / DBTokenStore just
                implements the same interface — callers never change.
  - Interface / ABC : enforces load / save / clear contract on every concrete store.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from core.logging_setup import traced

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Abstract strategy
# ──────────────────────────────────────────────
class TokenStore(ABC):
    """Abstract token persistence strategy."""

    @abstractmethod
    def load(self) -> Optional[dict]:
        """Return stored tokens dict or None if nothing is persisted."""

    @abstractmethod
    def save(self, tokens: dict) -> None:
        """Persist tokens."""

    @abstractmethod
    def clear(self) -> None:
        """Delete persisted tokens."""


# ──────────────────────────────────────────────
# Concrete strategy — JSON file
# ──────────────────────────────────────────────
class FileTokenStore(TokenStore):
    """Persist OAuth tokens as a JSON file on disk."""

    def __init__(self, path: Path) -> None:
        self._path = path

    # ── Strategy interface ─────────────────────
    @traced
    def load(self) -> Optional[dict]:
        if not self._path.exists():
            log.warning("Token file not found: %s", self._path)
            return None
        log.debug("Loading tokens from %s", self._path)
        with self._path.open("r", encoding="utf-8") as fh:
            tokens = json.load(fh)
        log.debug("Tokens loaded. Keys: %s", list(tokens.keys()))
        return tokens

    @traced
    def save(self, tokens: dict) -> None:
        log.debug("Saving tokens to %s", self._path)
        with self._path.open("w", encoding="utf-8") as fh:
            json.dump(tokens, fh, indent=2)
        log.info("Tokens saved to %s", self._path)

    @traced
    def clear(self) -> None:
        if self._path.exists():
            self._path.unlink()
            log.info("Token file deleted: %s", self._path)
