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
from typing import Dict, List, Set, Tuple, Optional

from markets.moneyline import Moneyline
from markets.spreads import Spreads
from markets.totals import Totals
from markets.player_props import PlayerProps


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
        self.player_props = PlayerProps(sport=sport, verbose=verbose)

    def run_all_markets(
        self,
        away_team: str,
        home_team: str,
        play_date: date,
        event_slug: str,
        market_slugs_by_event: Dict[str, Dict[str, List[str]]],
        traded_markets: Optional[Set[Tuple[str, str]]] = None,
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
                'spreads': [market_slug1, market_slug2, ...],
                'totals': [market_slug1, market_slug2, ...],
                'player_props': [market_slug1, market_slug2, ...]
            }
            traded_markets: Set of (event_slug, market_slug) tuples that have already been traded.
                            Will be updated in-place when trades are executed.
            markets_to_run: Dict specifying which markets to run:
                {
                    'moneyline': bool,
                    'spreads': bool,
                    'totals': bool,
                    'player_props': bool
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
                'moneyline': True,
                'spreads': True,
                'totals': True,
                'player_props': True,
            }
            
        if self.verbose:
            print(f"\n{'#'*80}")
            print(f"# PROCESSING GAME: {away_team} @ {home_team}")
            print(f"# Date: {play_date}")
            print(f"# Event Slug: {event_slug}")
            print(f"# Markets: moneyline={markets_to_run.get('moneyline', False)}, "
                  f"spreads={markets_to_run.get('spreads', False)}, "
                  f"totals={markets_to_run.get('totals', False)}, "
                  f"player_props={markets_to_run.get('player_props', False)}")
            print(f"{'#'*80}")
        
        market_slugs = market_slugs_by_event.get(event_slug, {})
        
        if not market_slugs:
            if self.verbose:
                print(f"\n[WARNING] No market slugs found for event: {event_slug}")
            return 1

        # Run moneyline
        if markets_to_run.get('moneyline', False):
            moneyline_slugs = market_slugs.get('moneyline', [])
            # Filter out already traded markets
            moneyline_slugs = [s for s in moneyline_slugs if (event_slug, s) not in traded_markets]
            if self.verbose:
                print(f"\n[MONEYLINE] Found {len(moneyline_slugs)} moneyline market(s) (excluding already traded)")
            for i, slug in enumerate(moneyline_slugs, 1):
                if self.verbose:
                    print(f"\n[MONEYLINE {i}/{len(moneyline_slugs)}] Processing market slug: {slug}")
                result = self.moneyline.run(away_team, home_team, play_date, event_slug, slug)
                if result is not None:
                    # A value bet was found and potentially traded - mark as traded
                    traded_markets.add((event_slug, slug))
                    print(f"  [TRACKED] Marked ({event_slug}, {slug}) as traded")

        # Run spreads
        if markets_to_run.get('spreads', False):
            spread_slugs = market_slugs.get('spreads', [])
            # Filter out already traded markets
            spread_slugs = [s for s in spread_slugs if (event_slug, s) not in traded_markets]
            if self.verbose:
                print(f"\n[SPREADS] Found {len(spread_slugs)} spread market(s) (excluding already traded)")
            if spread_slugs:
                if self.verbose:
                    print(f"  Spread slugs: {spread_slugs}")
                for slug in spread_slugs:
                    result = self.spreads.run(away_team, home_team, play_date, event_slug, slug)
                    if result is not None:
                        traded_markets.add((event_slug, slug))
                        print(f"  [TRACKED] Marked ({event_slug}, {slug}) as traded")

        # Run totals
        if markets_to_run.get('totals', False):
            totals_slugs = market_slugs.get('totals', [])
            # Filter out already traded markets
            totals_slugs = [s for s in totals_slugs if (event_slug, s) not in traded_markets]
            if self.verbose:
                print(f"\n[TOTALS] Found {len(totals_slugs)} totals market(s) (excluding already traded)")
            if totals_slugs:
                if self.verbose:
                    print(f"  Totals slugs: {totals_slugs}")
                for slug in totals_slugs:
                    result = self.totals.run(away_team, home_team, play_date, event_slug, slug)
                    if result is not None:
                        traded_markets.add((event_slug, slug))
                        print(f"  [TRACKED] Marked ({event_slug}, {slug}) as traded")

        # Run player props
        if markets_to_run.get('player_props', False):
            player_prop_slugs = market_slugs.get('player_props', [])
            # Filter out already traded markets
            player_prop_slugs = [s for s in player_prop_slugs if (event_slug, s) not in traded_markets]
            if self.verbose:
                print(f"\n[PLAYER PROPS] Found {len(player_prop_slugs)} player prop market(s) (excluding already traded)")
            if player_prop_slugs:
                if self.verbose:
                    print(f"  Player prop slugs: {player_prop_slugs[:5]}..." if len(player_prop_slugs) > 5 else f"  Player prop slugs: {player_prop_slugs}")
                for slug in player_prop_slugs:
                    result = self.player_props.run(away_team, home_team, play_date, event_slug, slug)
                    if result is not None:
                        traded_markets.add((event_slug, slug))
                        print(f"  [TRACKED] Marked ({event_slug}, {slug}) as traded")

        if self.verbose:
            print(f"\n{'#'*80}")
            print(f"# FINISHED PROCESSING: {away_team} @ {home_team}")
            print(f"{'#'*80}")

        return 0
