#!/usr/bin/env python3
"""
CLI helper functions for validation and display.

Keeps `main.py` focused on orchestration.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional

from sportsbook_odds_service.sportsbook_weighted_odds_interface import MoneylineOdds
from polymarket_odds_service.polymarket_market_analyzer import MarketOdds


@dataclass(frozen=True)
class CLIArgs:
    team_a: str
    team_b: str
    play_date: Optional[date]


def _parse_date(date_str: str) -> Optional[date]:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def validate_input(argv: List[str]) -> Optional[CLIArgs]:
    """
    Validate environment + CLI arguments.

    This is the single public validation entrypoint. It prints user-friendly
    errors itself and returns None when invalid.

    Args:
        argv: argv list, e.g. sys.argv. Expected:
              <team_a> <team_b> [YYYY-MM-DD]

    Returns:
        CLIArgs when valid, otherwise None.
    """
    env_err = _validate_environment()
    if env_err:
        _print_error_summary("Environment Configuration Error", env_err)
        return None

    try:
        return _parse_and_validate_cli_args(argv)
    except ValueError as e:
        _print_error_summary("Invalid Arguments", str(e))
        return None


def _validate_environment() -> Optional[str]:
    """
    Returns an error message string if invalid; otherwise None.
    """
    # API_KEY is needed for The Odds API calls
    import os

    api_key = os.getenv("API_KEY")
    if not api_key:
        return (
            "API_KEY environment variable is not set.\n"
            "Please set it using: export API_KEY='your_api_key_here'\n"
            "Or create a .env file in the nba/sportsbook_odds_service directory with: API_KEY=your_api_key_here"
        )
    if len(api_key.strip()) == 0:
        return "API_KEY environment variable is empty."
    return None


def _validate_teams(team_a: str, team_b: str) -> Optional[str]:
    """
    Returns an error message string if invalid; otherwise None.
    """
    if not team_a or not team_b:
        return "Team names cannot be empty."

    ta = team_a.strip()
    tb = team_b.strip()
    if len(ta) == 0 or len(tb) == 0:
        return "Team names cannot be empty after stripping whitespace."
    if ta.lower() == tb.lower():
        return "Team names must be different."
    return None


def _parse_and_validate_cli_args(argv: List[str]) -> CLIArgs:
    """
    Parses argv for: <team_a> <team_b> [date]

    Raises:
        ValueError: for any validation failure (with a user-friendly message).
    """
    if len(argv) < 3:
        raise ValueError(
            "Insufficient arguments provided.\n\n"
            "Usage:\n"
            "  python3 main.py <team_a> <team_b> [date]\n\n"
            "Example:\n"
            "  python3 main.py 'Chicago Bulls' 'Detroit Pistons' 2026-01-07\n"
            "  python3 main.py 'Chicago Bulls' 'Detroit Pistons'  # Uses today's date"
        )

    team_a = argv[1].strip()
    team_b = argv[2].strip()

    play_date: Optional[date] = None
    if len(argv) >= 4:
        play_date = _parse_date(argv[3])
        if play_date is None:
            raise ValueError(
                f"Date '{argv[3]}' is not valid. Use YYYY-MM-DD format (e.g., 2026-01-07)."
            )

    team_err = _validate_teams(team_a, team_b)
    if team_err:
        raise ValueError(team_err)

    return CLIArgs(team_a=team_a, team_b=team_b, play_date=play_date)


def _print_error_summary(error_type: str, details: str):
    print("\n" + "=" * 80)
    print(f"ERROR: {error_type}")
    print("=" * 80)
    print(details)
    print("=" * 80 + "\n")


def print_sportsbook_odds(sportsbook_result: Optional[MoneylineOdds]):
    print("\n" + "=" * 80)
    print("SPORTSBOOK ODDS")
    print("=" * 80)
    if sportsbook_result:
        print(sportsbook_result.to_string())
    else:
        print("Failed to fetch sportsbook odds")
    print("=" * 80)


def print_polymarket_moneyline(polymarket_results: Optional[List[MarketOdds]]):
    if polymarket_results:
        print("\n" + "=" * 80)
        print(f"POLYMARKET MONEYLINE ({len(polymarket_results)} markets)")
        print("=" * 80)
        print(f"Total markets: {len(polymarket_results)}")
        for i, market in enumerate(polymarket_results, 1):
            print(f"  {i}. {market.market}")
            bid_s = f"{market.best_bid:.4f}" if market.best_bid is not None else "N/A"
            ask_s = f"{market.best_ask:.4f}" if market.best_ask is not None else "N/A"
            spread_s = f"{market.spread:.4f}" if market.spread is not None else "N/A"

            print(
                f"     Bid: {bid_s} (vol: {market.bid_volume:.2f}) | "
                f"Ask: {ask_s} (vol: {market.ask_volume:.2f}) | "
                f"Spread: {spread_s}"
            )
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("POLYMARKET MONEYLINE")
        print("=" * 80)
        print("No Polymarket data available (event not found or error occurred)")
        print("=" * 80)


