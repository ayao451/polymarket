#!/usr/bin/env python3
"""
Main entry point for value betting bot (Tennis).

Refactored to use shared helper module.
"""

import sys
import os
import argparse
import traceback

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pinnacle_scraper.pinnacle_odds_service import PinnacleTennisOddsService
from sports_value_bets_helper import (
    SportConfig,
    SportsValueBetsRunner,
)


def _tennis_sort_key(event_tuple):
    """Sort key: ATP prefixed first, then WTA, then everything else."""
    event_slug = event_tuple[0]
    if event_slug.startswith("atp"):
        return (0, event_slug)
    elif event_slug.startswith("wta"):
        return (1, event_slug)
    else:
        return (2, event_slug)


def main() -> int:
    config = SportConfig(
        sport_name="tennis",
        display_name="Tennis",
        pinnacle_service_class=PinnacleTennisOddsService,
        whitelisted_prefixes=["atp", "wta"],
        sort_key_func=_tennis_sort_key,
        default_markets={
            'moneyline': True,
            'spreads': True,
            'totals': False,  # Tennis uses totals_games/totals_sets only
            'totals_games': True,
            'totals_sets': True,
        },
        supports_test_date=True,
        description="Polymarket Sports Betting Bot (Tennis)",
    )
    
    parser = argparse.ArgumentParser(description=config.description)
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--moneyline", action="store_true", help="Run moneyline markets")
    parser.add_argument("--spreads", action="store_true", help="Run spread markets")
    parser.add_argument("--totals-games", action="store_true", help="Run total games (O/U) markets")
    parser.add_argument("--totals-sets", action="store_true", help="Run total sets (O/U) markets")
    parser.add_argument("--test-date", type=str, help="Test on a specific date (YYYY-MM-DD format). If not provided, uses today and tomorrow.")
    args = parser.parse_args()
    
    # Parse tennis-specific markets
    has_moneyline_flag = args.moneyline
    has_spreads_flag = args.spreads
    has_totals_games_flag = args.totals_games
    has_totals_sets_flag = args.totals_sets
    any_flags_provided = has_moneyline_flag or has_spreads_flag or has_totals_games_flag or has_totals_sets_flag
    
    if not any_flags_provided:
        markets_to_run = config.default_markets.copy()
    else:
        markets_to_run = config.default_markets.copy()
        markets_to_run['moneyline'] = has_moneyline_flag
        markets_to_run['spreads'] = has_spreads_flag
        markets_to_run['totals_games'] = has_totals_games_flag
        markets_to_run['totals_sets'] = has_totals_sets_flag
    
    runner = SportsValueBetsRunner(config, verbose=args.verbose)
    test_date = getattr(args, 'test_date', None)
    return runner.run(markets_to_run=markets_to_run, test_date=test_date)


if __name__ == "__main__":
    try:
        exit_code = main()
        raise SystemExit(exit_code)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Script interrupted by user. Exiting gracefully...")
        raise SystemExit(0)
    except Exception as e:
        print(f"\n{'!'*60}")
        print(f"!!! UNEXPECTED ERROR - LOGGING AND CONTINUING !!!")
        print(f"{'!'*60}")
        print(f"  Error: {e}")
        traceback.print_exc()
        print(f"\n  Continuing to allow logging...")
        # Don't exit - let the script finish naturally
        raise SystemExit(0)
