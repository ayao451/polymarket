#!/usr/bin/env python3
"""
CLI helper functions for validation and display.

Keeps `value_bets.py` focused on orchestration.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional

from pinnacle_scraper.pinnacle_sportsbook_odds_interface import MoneylineOdds, SpreadOdds, TotalsOdds
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
    # No sportsbook API keys required anymore (Pinnacle Arcadia is fetched directly).
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
            "  python3 value_bets.py <team_a> <team_b> [date]\n\n"
            "Example:\n"
            "  python3 value_bets.py 'Chicago Bulls' 'Detroit Pistons' 2026-01-07\n"
            "  python3 value_bets.py 'Chicago Bulls' 'Detroit Pistons'  # Uses today's date"
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


def print_sportsbook_spread_odds(spreads: Optional[List[SpreadOdds]]):
    """
    Print sportsbook spread odds in the same banner style as moneyline.
    """
    print("\n" + "=" * 80)
    print("SPORTSBOOK SPREAD ODDS")
    print("=" * 80)
    
    if not spreads:
        print("No sportsbook spread odds available")
        print("=" * 80)
        return
    
    def _sort_key(s: SpreadOdds):
        try:
            return abs(float(s.away_point))
        except Exception:
            return 0.0
    
    for s in sorted(spreads, key=_sort_key):
        print(s.to_string())
    
    print("=" * 80)


def print_sportsbook_totals_odds(totals: Optional[List[TotalsOdds]]):
    """
    Print sportsbook totals odds in the same banner style as moneyline.
    """
    print("\n" + "=" * 80)
    print("SPORTSBOOK TOTALS ODDS")
    print("=" * 80)

    if not totals:
        print("No sportsbook totals odds available")
        print("=" * 80)
        return

    def _sort_key(t: TotalsOdds):
        try:
            return float(t.total_point)
        except Exception:
            return 0.0

    for t in sorted(totals, key=_sort_key):
        print(t.to_string())

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


def print_polymarket_spreads(polymarket_spreads: Optional[List[MarketOdds]]):
    """Print Polymarket spread odds."""
    if polymarket_spreads:
        print("\n" + "=" * 80)
        print(f"POLYMARKET SPREADS ({len(polymarket_spreads)} markets)")
        print("=" * 80)
        
        # Group by base question
        def _base_question(label: str) -> str:
            s = label or ""
            i = s.rfind("(")
            return s[:i].strip() if i != -1 else s.strip()

        def _outcome_label(label: str) -> str:
            s = label or ""
            i = s.rfind("(")
            j = s.rfind(")")
            if i == -1 or j == -1 or j <= i:
                return ""
            return (s[i + 1 : j] or "").strip()

        groups: dict[str, list] = {}
        for m in polymarket_spreads:
            groups.setdefault(_base_question(m.market), []).append(m)

        for q in sorted(groups.keys()):
            print(f"\n- {q}")
            for m in sorted(groups[q], key=lambda mm: _outcome_label(mm.market)):
                bid_s = f"{m.best_bid:.4f}" if m.best_bid is not None else "N/A"
                ask_s = f"{m.best_ask:.4f}" if m.best_ask is not None else "N/A"
                spr_s = f"{m.spread:.4f}" if m.spread is not None else "N/A"
                outcome = _outcome_label(m.market)
                print(f"  * {outcome}")
                print(
                    f"    Bid: {bid_s} (vol: {m.bid_volume:.2f}) | "
                    f"Ask: {ask_s} (vol: {m.ask_volume:.2f}) | "
                    f"Spread: {spr_s}"
                )
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("POLYMARKET SPREADS")
        print("=" * 80)
        print("No Polymarket spread data available")
        print("=" * 80)


def print_polymarket_totals(polymarket_totals: Optional[List[MarketOdds]]):
    """Print Polymarket totals odds."""
    if polymarket_totals:
        print("\n" + "=" * 80)
        print(f"POLYMARKET TOTALS ({len(polymarket_totals)} markets)")
        print("=" * 80)
        
        # Group by base question
        def _base_q(label: str) -> str:
            s = label or ""
            i = s.rfind("(")
            return s[:i].strip() if i != -1 else s.strip()

        def _outcome(label: str) -> str:
            s = label or ""
            i = s.rfind("(")
            j = s.rfind(")")
            if i == -1 or j == -1 or j <= i:
                return ""
            return (s[i + 1 : j] or "").strip()

        groups: dict[str, list] = {}
        for m in polymarket_totals:
            groups.setdefault(_base_q(m.market), []).append(m)

        for q in sorted(groups.keys()):
            print(f"\n- {q}")
            for m in sorted(groups[q], key=lambda mm: _outcome(mm.market)):
                bid_s = f"{m.best_bid:.4f}" if m.best_bid is not None else "N/A"
                ask_s = f"{m.best_ask:.4f}" if m.best_ask is not None else "N/A"
                spr_s = f"{m.spread:.4f}" if m.spread is not None else "N/A"
                print(f"  * {_outcome(m.market)}")
                print(
                    f"    Bid: {bid_s} (vol: {m.bid_volume:.2f}) | "
                    f"Ask: {ask_s} (vol: {m.ask_volume:.2f}) | "
                    f"Spread: {spr_s}"
                )
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("POLYMARKET TOTALS")
        print("=" * 80)
        print("No Polymarket totals data available")
        print("=" * 80)


