"""
db/connection.py
----------------
SQL Server connection factory and context manager.

Design patterns used:
  - Factory Method  : SqlConnectionFactory.create() centralises connection-string
                      assembly; callers never build raw ODBC strings.
  - Context Manager : ManagedConnection wraps a pyodbc connection in
                      __enter__ / __exit__ so connections are always closed,
                      even on exceptions — eliminating connection leaks.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import pyodbc

from core.config import SqlConfig
from core.logging_setup import traced

log = logging.getLogger(__name__)


class SqlConnectionFactory:
    """
    Assembles the ODBC connection string from SqlConfig and opens connections.

    Usage
    -----
    factory = SqlConnectionFactory(config.sql)
    with factory.connect() as conn:
        cursor = conn.cursor()
        ...
    """

    def __init__(self, config: SqlConfig) -> None:
        self._cfg = config

    @property
    def _connection_string(self) -> str:
        c = self._cfg
        return (
            f"DRIVER={{{c.driver}}};"
            f"SERVER={c.server};"
            f"DATABASE={c.database};"
            f"UID={c.username};"
            f"PWD={c.password};"
            f"TrustServerCertificate={c.trust_cert};"
        )

    @traced
    def create(self) -> pyodbc.Connection:
        """Open and return a raw pyodbc connection (caller must close it)."""
        log.debug(
            "Opening SQL connection: server=%s  db=%s  user=%s",
            self._cfg.server,
            self._cfg.database,
            self._cfg.username,
        )
        conn = pyodbc.connect(self._connection_string)
        log.debug("SQL connection established")
        return conn

    @contextmanager
    def connect(self) -> Generator[pyodbc.Connection, None, None]:
        """
        Context manager — always closes the connection on exit.

        Usage::

            with factory.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
        """
        conn = self.create()
        try:
            yield conn
        finally:
            conn.close()
            log.debug("SQL connection closed")
