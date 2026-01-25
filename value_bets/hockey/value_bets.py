#!/usr/bin/env python3
"""
Main entry point for value betting bot (Hockey).

Refactored to use shared helper module.
"""

import sys
import os
import traceback

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pinnacle_scraper.pinnacle_odds_service import PinnacleHockeyOddsService
from sports_value_bets_helper import (
    SportConfig,
    SportsValueBetsRunner,
    create_arg_parser,
    parse_markets_args,
)


def _hockey_sort_key(event_tuple):
    """Sort key: NHL prefixed first, everything else second."""
    event_slug = event_tuple[0]
    if event_slug.startswith("nhl"):
        return (0, event_slug)
    else:
        return (1, event_slug)


def main() -> int:
    config = SportConfig(
        sport_name="hockey",
        display_name="Hockey",
        pinnacle_service_class=PinnacleHockeyOddsService,
        whitelisted_prefixes=["nhl", "shl", "ahl", "khl", "dehl", "cehl", "snhl"],
        sort_key_func=_hockey_sort_key,
        default_markets={
            'moneyline': True,
            'spreads': True,
            'totals': True,
        },
        supports_test_date=False,
        description="Polymarket Sports Betting Bot (Hockey)",
    )
    
    parser = create_arg_parser(config.description, config.supports_test_date)
    args = parser.parse_args()
    
    markets_to_run = parse_markets_args(args, config.default_markets)
    
    runner = SportsValueBetsRunner(config, verbose=args.verbose)
    return runner.run(markets_to_run=markets_to_run, test_date=None)


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
        raise SystemExit(0)
