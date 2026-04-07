"""
schwab/auth.py
--------------
Schwab OAuth 2.0 authentication service.

Design patterns used:
  - Template Method : get_valid_tokens() defines the algorithm skeleton
                      (load → try refresh → fallback to full login).
                      Each step is a focused private method.
  - Dependency Injection : SchwabAuth receives a TokenStore and SchwabConfig,
                           making it easy to swap in fakes during testing.
  - Single Responsibility : only handles OAuth; knows nothing about SQL or market data.
"""

from __future__ import annotations

import base64
import logging
import webbrowser
from urllib.parse import parse_qs, unquote, urlparse

import requests

from core.config import SchwabConfig
from core.logging_setup import traced
from schwab.token_store import TokenStore

log = logging.getLogger(__name__)


class SchwabAuth:
    """
    Manages the full Schwab OAuth 2.0 lifecycle:
      1. Initial browser-based authorisation flow
      2. Token refresh using refresh_token
      3. Token persistence via an injected TokenStore

    Parameters
    ----------
    config : SchwabConfig
        Immutable Schwab API credentials / URLs.
    token_store : TokenStore
        Any concrete strategy for loading / saving tokens.
    """

    def __init__(self, config: SchwabConfig, token_store: TokenStore) -> None:
        self._cfg = config
        self._store = token_store

    # ── Public API ─────────────────────────────
    @traced
    def get_valid_tokens(self) -> dict:
        """
        Template Method — ensure a valid access_token is available.

        Algorithm
        ---------
        1. Load tokens from store.
        2. If none → run full interactive login.
        3. If tokens exist → try token refresh.
        4. If refresh fails → fall back to full login.
        """
        tokens = self._store.load()

        if tokens is None:
            log.info("No persisted tokens — starting full login flow")
            return self._full_login()

        try:
            log.info("Attempting token refresh")
            return self._refresh(tokens)
        except Exception as exc:
            log.warning("Token refresh failed (%s) — falling back to full login", exc)
            return self._full_login()

    # ── Template Method steps ──────────────────
    @traced
    def _full_login(self) -> dict:
        """Interactive browser OAuth flow → exchange code → persist tokens."""
        url = self._build_auth_url()
        log.info("Opening browser for Schwab authorisation")
        print("\n=== Schwab OAuth Login ===")
        print(f"Opening: {url}")
        try:
            webbrowser.open(url)
        except Exception as exc:
            log.warning("Could not open browser automatically: %s", exc)

        print("\nAfter approving, copy the FULL redirect URL from your browser's address bar.")
        redirect_url = input("Paste redirect URL here: ").strip()
        log.debug("User pasted redirect URL (length=%d)", len(redirect_url))

        auth_code = self._extract_auth_code(redirect_url)
        tokens = self._exchange_code(auth_code)
        self._store.save(tokens)
        log.info("Full login complete")
        return tokens

    @traced
    def _refresh(self, tokens: dict) -> dict:
        """Use refresh_token to obtain a new access_token."""
        refresh_token = tokens.get("refresh_token")
        if not refresh_token:
            raise ValueError("No refresh_token in stored tokens")

        headers = {
            "Authorization": f"Basic {self._basic_auth()}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

        log.debug("POST %s  grant_type=refresh_token", self._cfg.token_url)
        resp = requests.post(self._cfg.token_url, headers=headers, data=data, timeout=30)
        log.debug("Refresh response: %s", resp.status_code)

        if resp.status_code != 200:
            log.error("Token refresh HTTP error: %s %s", resp.status_code, resp.text)
            raise RuntimeError(f"Token refresh failed: {resp.status_code} {resp.text}")

        new_tokens = resp.json()
        self._store.save(new_tokens)
        log.info("Token refresh successful")
        return new_tokens

    @traced
    def _exchange_code(self, auth_code: str) -> dict:
        """Exchange authorisation code for access + refresh tokens."""
        headers = {
            "Authorization": f"Basic {self._basic_auth()}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self._cfg.redirect_uri,
        }

        log.debug("POST %s  grant_type=authorization_code", self._cfg.token_url)
        resp = requests.post(self._cfg.token_url, headers=headers, data=data, timeout=30)
        log.debug("Token exchange response: %s", resp.status_code)

        if resp.status_code != 200:
            log.error("Token exchange failed: %s %s", resp.status_code, resp.text)
            raise RuntimeError(f"Token exchange failed: {resp.status_code} {resp.text}")

        tokens = resp.json()
        log.info("Token exchange OK. Keys: %s", list(tokens.keys()))
        return tokens

    # ── Private helpers ────────────────────────
    def _build_auth_url(self) -> str:
        url = (
            f"{self._cfg.auth_url}"
            f"?client_id={self._cfg.app_key}"
            f"&redirect_uri={self._cfg.redirect_uri}"
        )
        log.debug("Auth URL: %s", url)
        return url

    def _extract_auth_code(self, redirect_url: str) -> str:
        if not redirect_url:
            raise ValueError(
                "No redirect URL provided.\n"
                "Paste the FULL callback URL from your browser's address bar."
            )
        parsed = urlparse(redirect_url)
        qs = parse_qs(parsed.query)
        code_list = qs.get("code")
        if not code_list or not code_list[0].strip():
            raise ValueError(
                f"No 'code' param found in URL:\n{redirect_url}\n"
                "Make sure you copied the FULL redirect URL after login."
            )
        code = unquote(code_list[0].strip())
        log.debug("Auth code extracted (first 20): %s...", code[:20])
        return code

    def _basic_auth(self) -> str:
        raw = f"{self._cfg.app_key}:{self._cfg.app_secret}"
        return base64.b64encode(raw.encode()).decode()
