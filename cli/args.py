"""
cli/args.py
-----------
Command-line argument definition and parsing.

Kept separate from main() so it can be imported and tested in isolation,
and so the argument schema lives in one obvious place.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class CliArgs:
    """Typed result of CLI argument parsing."""
    portfolio: Optional[int]
    start: Optional[str]
    end: Optional[str]
    log_dir: Optional[Path]


def parse_args(argv=None) -> CliArgs:
    """
    Parse and return typed CLI arguments.

    Parameters
    ----------
    argv : list[str] | None
        Argument list (defaults to sys.argv when None).
    """
    parser = argparse.ArgumentParser(
        description="Schwab Market Data Loader — fetch quotes and persist to SQL Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  # Interactive prompts for all inputs:
  python main.py

  # Fully non-interactive:
  python main.py --portfolio 2 --start 2024-01-01 --end 2024-12-31

  # Custom log directory:
  python main.py --portfolio 1 --log-dir /var/log/schwab
""",
    )

    parser.add_argument(
        "--portfolio", "-p",
        type=int,
        metavar="ID",
        help="PortfolioID to load symbols from (default: prompt)",
    )
    parser.add_argument(
        "--start", "-s",
        metavar="YYYY-MM-DD",
        help="Start date for price history",
    )
    parser.add_argument(
        "--end", "-e",
        metavar="YYYY-MM-DD",
        help="End date for price history",
    )
    parser.add_argument(
        "--log-dir",
        metavar="DIR",
        type=Path,
        help="Directory for rotating log files (default: ./logs)",
    )

    ns = parser.parse_args(argv)
    return CliArgs(
        portfolio=ns.portfolio,
        start=ns.start,
        end=ns.end,
        log_dir=ns.log_dir,
    )
