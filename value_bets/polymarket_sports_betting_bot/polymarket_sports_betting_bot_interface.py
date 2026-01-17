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
from typing import Dict, List

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
    ) -> int:
        """
        Run value bet evaluation and execution for all markets (moneyline, spreads, totals).

        Args:
            away_team: Away team name
            home_team: Home team name
            play_date: Date of the game
            event_slug: Polymarket event slug
            market_slugs_by_event: Dict mapping event_slug -> {
                'moneyline': [market_slug],
                'spreads': [market_slug1, market_slug2, ...],
                'totals': [market_slug1, market_slug2, ...]
            }

        Returns:
            Process-style exit code (0 success, non-zero failure).
        """
        if self.verbose:
            print(f"\n{'#'*80}")
            print(f"# PROCESSING GAME: {away_team} @ {home_team}")
            print(f"# Date: {play_date}")
            print(f"# Event Slug: {event_slug}")
            print(f"{'#'*80}")
        
        market_slugs = market_slugs_by_event.get(event_slug, {})
        
        if not market_slugs:
            if self.verbose:
                print(f"\n[WARNING] No market slugs found for event: {event_slug}")
            return 1

        # Run moneyline
        moneyline_slugs = market_slugs.get('moneyline', [])
        if self.verbose:
            print(f"\n[MONEYLINE] Found {len(moneyline_slugs)} moneyline market(s)")
        for i, slug in enumerate(moneyline_slugs, 1):
            if self.verbose:
                print(f"\n[MONEYLINE {i}/{len(moneyline_slugs)}] Processing market slug: {slug}")
            self.moneyline.run(away_team, home_team, play_date, event_slug, slug)

        # Run spreads
        spread_slugs = market_slugs.get('spreads', [])
        if self.verbose:
            print(f"\n[SPREADS] Found {len(spread_slugs)} spread market(s)")
        if spread_slugs:
            if self.verbose:
                print(f"  Spread slugs: {spread_slugs}")
            self.spreads.run_multiple(away_team, home_team, play_date, event_slug, spread_slugs)

        # Run totals
        totals_slugs = market_slugs.get('totals', [])
        if self.verbose:
            print(f"\n[TOTALS] Found {len(totals_slugs)} totals market(s)")
        if totals_slugs:
            if self.verbose:
                print(f"  Totals slugs: {totals_slugs}")
            self.totals.run_multiple(away_team, home_team, play_date, event_slug, totals_slugs)

        if self.verbose:
            print(f"\n{'#'*80}")
            print(f"# FINISHED PROCESSING: {away_team} @ {home_team}")
            print(f"{'#'*80}")

        return 0
