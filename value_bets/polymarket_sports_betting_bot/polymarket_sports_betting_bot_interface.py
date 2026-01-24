#!/usr/bin/env python3
"""
Polymarket Sports Betting Bot interface.

Provides a class-based API for running moneyline/spread/totals value betting:
- Fetches sportsbook (Pinnacle) odds
- Fetches Polymarket odds
- Evaluates value bets using Kelly Criterion
- Executes trades on Polymarket
"""

from __future__ import annotations

from datetime import date
from typing import Dict, List, Set, Optional

from markets.moneyline import Moneyline
from markets.spreads import Spreads
from markets.totals import Totals


class PolymarketSportsBettingBotInterface:
    """
    Bot interface wrapper.

    Orchestrates value betting across moneyline, spread, and totals markets.
    """

    def __init__(self, sport: str = "basketball", verbose: bool = False) -> None:
        self.sport = sport
        self.verbose = verbose
        self.moneyline = Moneyline(sport=sport, verbose=verbose)
        self.spreads = Spreads(sport=sport, verbose=verbose)
        self.totals = Totals(sport=sport, verbose=verbose)

    def run_all_markets(
        self,
        away_team: str,
        home_team: str,
        play_date: date,
        event_slug: str,
        market_slugs_by_event: Dict[str, Dict[str, List[str]]],
        traded_markets: Optional[Set[str]] = None,
        markets_to_run: Optional[Dict[str, bool]] = None,
    ) -> int:
        """
        Run value bet evaluation and execution for specified markets.

        Args:
            away_team: Away team name
            home_team: Home team name
            play_date: Date of the game
            event_slug: Polymarket event slug
            market_slugs_by_event: Dict mapping event_slug -> {
                'moneyline': [market_slug],
                'spreads': [...], 'totals': [...],
                'totals_games': [...], 'totals_sets': [...]  # tennis
            }
            traded_markets: Set of market slugs that have already been traded.
                            Will be updated in-place when trades are executed.
                            Prevents trading the same market slug multiple times.
            markets_to_run: Dict specifying which markets to run:
                {
                    'moneyline': bool, 'spreads': bool, 'totals': bool,
                    'totals_games': bool, 'totals_sets': bool  # tennis
                }
                If None, runs all markets.

        Returns:
            Process-style exit code (0 success, non-zero failure).
        """
        if traded_markets is None:
            traded_markets = set()
        
        # Default to all markets if not specified
        if markets_to_run is None:
            markets_to_run = {
                "moneyline": True,
                "spreads": True,
                "totals": True,
                "totals_games": True,
                "totals_sets": True,
            }

        if self.verbose:
            print(f"\n{'#'*80}")
            print(f"# PROCESSING GAME: {away_team} @ {home_team}")
            print(f"# Date: {play_date}")
            print(f"# Event Slug: {event_slug}")
            print(
                f"# Markets: moneyline={markets_to_run.get('moneyline', False)}, "
                f"spreads={markets_to_run.get('spreads', False)}, "
                f"totals={markets_to_run.get('totals', False)}, "
                f"totals_games={markets_to_run.get('totals_games', False)}, "
                f"totals_sets={markets_to_run.get('totals_sets', False)}"
            )
            print(f"{'#'*80}")
        
        market_slugs = market_slugs_by_event.get(event_slug, {})
        
        if not market_slugs:
            if self.verbose:
                print(f"\n[WARNING] No market slugs found for event: {event_slug}")
            return 1

        # Run moneyline
        if markets_to_run.get('moneyline', False):
            moneyline_slugs = market_slugs.get('moneyline', [])
            if self.verbose:
                print(f"\n[MONEYLINE] Found {len(moneyline_slugs)} moneyline market(s)")
            for i, slug in enumerate(moneyline_slugs, 1):
                if self.verbose:
                    print(f"\n[MONEYLINE {i}/{len(moneyline_slugs)}] Processing market slug: {slug}")
                result = self.moneyline.run(away_team, home_team, play_date, event_slug, slug, traded_markets)

        # Run spreads
        if markets_to_run.get('spreads', False):
            spread_slugs = market_slugs.get('spreads', [])
            if self.verbose:
                print(f"\n[SPREADS] Found {len(spread_slugs)} spread market(s)")
            if spread_slugs:
                if self.verbose:
                    print(f"  Spread slugs: {spread_slugs}")
                for slug in spread_slugs:
                    result = self.spreads.run(away_team, home_team, play_date, event_slug, slug, traded_markets)

        # Run totals (generic O/U, e.g. NBA/hockey)
        if markets_to_run.get("totals", False):
            totals_slugs = market_slugs.get("totals", [])
            if self.verbose:
                print(f"\n[TOTALS] Found {len(totals_slugs)} totals market(s)")
            if totals_slugs:
                if self.verbose:
                    print(f"  Totals slugs: {totals_slugs}")
                for slug in totals_slugs:
                    result = self.totals.run(away_team, home_team, play_date, event_slug, slug, traded_markets)

        # Run totals games (tennis O/U games)
        if markets_to_run.get("totals_games", False):
            tg_slugs = market_slugs.get("totals_games", [])
            if self.verbose:
                print(f"\n[TOTALS GAMES] Found {len(tg_slugs)} total-games market(s)")
            if tg_slugs:
                if self.verbose:
                    print(f"  Totals games slugs: {tg_slugs}")
                for slug in tg_slugs:
                    result = self.totals.run(away_team, home_team, play_date, event_slug, slug, traded_markets)

        # Run totals sets (tennis O/U sets)
        if markets_to_run.get("totals_sets", False):
            ts_slugs = market_slugs.get("totals_sets", [])
            if self.verbose:
                print(f"\n[TOTALS SETS] Found {len(ts_slugs)} total-sets market(s)")
            if ts_slugs:
                if self.verbose:
                    print(f"  Totals sets slugs: {ts_slugs}")
                for slug in ts_slugs:
                    result = self.totals.run(away_team, home_team, play_date, event_slug, slug, traded_markets)

        if self.verbose:
            print(f"\n{'#'*80}")
            print(f"# FINISHED PROCESSING: {away_team} @ {home_team}")
            print(f"{'#'*80}")

        return 0
