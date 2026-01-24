#!/usr/bin/env python3
"""
Spreads (handicap) market handler.

Orchestrates the full flow:
1. Fetch sportsbook spread odds
2. Fetch Polymarket spread odds
3. Evaluate for value bets
4. Calculate Kelly bet size
5. Execute trades if value found
"""

from __future__ import annotations

import re
from datetime import date
from typing import List, Optional, Set

from polymarket_odds_service.polymarket_odds import PolymarketOdds

MarketOdds = PolymarketOdds.MarketOdds
from polymarket_sports_betting_bot.value_bet_service import SpreadValueBetService, SpreadValueBet
from value_bet_helpers import log_attempted_spread_bet, log_value_bet

from .market import Market


class Spreads(Market):
    """Handler for spread (handicap) markets."""

    @staticmethod
    def _parse_spread_info_from_slug(slug: str) -> Optional[Tuple[str, float]]:
        """
        Parse spread info from market slug like:
        - 'nba-lal-por-2026-01-17-spread-away-2pt5' -> ('away', 2.5)
        - 'nba-lal-por-2026-01-17-spread-home-3pt5' -> ('home', 3.5)
        
        Returns (side, line) or None if not parseable.
        The line is always positive; the side tells you who is favored.
        """
        m = re.search(r"spread-(away|home)-(\d+)pt(\d+)", slug, flags=re.IGNORECASE)
        if m:
            side = m.group(1).lower()  # 'away' or 'home'
            whole = int(m.group(2))
            decimal = int(m.group(3))
            line = float(f"{whole}.{decimal}")
            return (side, line)
        return None

    def run(
        self,
        away_team: str,
        home_team: str,
        play_date: date,
        event_slug: str,
        market_slug: str,
        traded_markets: Optional[Set[str]] = None,
    ) -> Optional[SpreadValueBet]:
        """
        Run the full spreads flow for a single market slug.
        """
        # Step 1: Get sportsbook spread odds
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"SPREAD: {away_team} @ {home_team}")
            print(f"Date: {play_date}")
            print(f"Event: {event_slug}")
            print(f"Market: {market_slug}")
            print(f"{'='*60}")
            print(f"\n[STEP 1/4] Fetching sportsbook spread odds from Pinnacle...")
        
        sportsbook_spreads = self.sportsbook.get_spread_odds(away_team, home_team, play_date)
        if not sportsbook_spreads:
            if self.verbose:
                print(f"  -> [FAILED] Could not fetch sportsbook spread odds")
            return None
        
        if self.verbose:
            print(f"  -> [SUCCESS] Got {len(sportsbook_spreads)} spread line(s)")
            for i, s in enumerate(sportsbook_spreads, 1):
                print(f"  -> Line {i}: {s.to_string()}")
        
        # Step 2: Get Polymarket odds for this spread market
        if self.verbose:
            print(f"\n[STEP 2/4] Fetching Polymarket spread odds...")
            print(f"  -> Event slug: {event_slug}")
            print(f"  -> Market slug: {market_slug}")
        try:
            polymarket_odds_list = self.polymarket.retrieve_polymarket_odds(event_slug, market_slug)
        except Exception as e:
            if self.verbose:
                print(f"  -> [FAILED] Error fetching Polymarket odds: {e}")
            return None
        
        if not polymarket_odds_list:
            if self.verbose:
                print(f"  -> [FAILED] No Polymarket odds available")
            return None
        
        if self.verbose:
            print(f"  -> [SUCCESS] Got {len(polymarket_odds_list)} Polymarket outcome(s)")
            for odds in polymarket_odds_list:
                print(f"  -> {odds.market}: bid={odds.best_bid}, ask={odds.best_ask}, token={odds.token_id}")
        
        # Parse spread info from slug (e.g., "spread-away-2pt5" -> ('away', 2.5))
        spread_info = self._parse_spread_info_from_slug(market_slug)
        if spread_info:
            spread_side, spread_line = spread_info
            if self.verbose:
                print(f"  -> Parsed from slug: {spread_side} team gets +{spread_line}")
        else:
            spread_side, spread_line = None, None
            if self.verbose:
                print(f"  -> Could not parse spread info from slug")
        
        # Step 3: Evaluate for value bet
        if self.verbose:
            print(f"\n[STEP 3/4] Evaluating for spread value bet...")
            print(f"  -> Matching Polymarket spread line with sportsbook lines")
        
        value_bet_service = SpreadValueBetService(
            sportsbook_spreads=sportsbook_spreads,
            polymarket_spread_results=polymarket_odds_list,
            polymarket_spread_side=spread_side,
            polymarket_spread_line=spread_line,
            away_team=away_team,
            home_team=home_team,
            verbose=self.verbose,
        )
        
        value_bets = value_bet_service.discover_value_bets()
        
        if not value_bets:
            if self.verbose:
                print(f"  -> [NO VALUE] No spread value bet found")
                print(f"  -> Either no matching line or price is too high")
            return None
        
        # Take the best value bet (highest expected payout)
        value_bet = value_bets[0]
        
        # Always print value bet found
        edge_pct = (value_bet.true_prob - value_bet.polymarket_best_ask) * 100
        print(f"\n{'*'*60}")
        print(f"*** VALUE BET FOUND (SPREAD) ***")
        print(f"{'*'*60}")
        print(f"  Game: {away_team} @ {home_team}")
        print(f"  Bet on: {value_bet.team} {value_bet.point:+g}")
        print(f"  Sportsbook (Pinnacle) true prob: {value_bet.true_prob*100:.2f}%")
        print(f"  Polymarket ask price: ${value_bet.polymarket_best_ask:.4f} ({value_bet.polymarket_best_ask*100:.2f}%)")
        print(f"  Edge: {edge_pct:+.2f}%")
        print(f"  Expected payout per $1: ${value_bet.expected_payout_per_1:.4f}")
        print(f"  Token ID: {value_bet.token_id}")
        
        # Log to value_bets.csv
        try:
            log_value_bet(
                value_bet=value_bet,
                away_team=away_team,
                home_team=home_team,
                play_date=play_date,
                event_slug=event_slug,
                market_slug=market_slug,
            )
        except Exception as e:
            print(f"[WARNING] Failed to log value bet: {e}")
        
        # Step 4: Calculate Kelly bet size and execute trade
        if self.verbose:
            print(f"\n[STEP 4/4] Executing trade with Kelly Criterion sizing...")
        trade_result = self.execute_value_bet(value_bet, away_team, home_team, event_slug, market_slug, traded_markets)
        
        # Log attempted value bet (regardless of execution result)
        try:
            executed = trade_result is not None and trade_result.ok
            error = None if executed else (trade_result.error if trade_result else "Trade skipped")
            log_attempted_spread_bet(
                value_bet=value_bet,
                away_team=away_team,
                home_team=home_team,
                event_slug=event_slug,
                market_slug=market_slug,
                executed=executed,
                error=error,
            )
        except Exception as e:
            print(f"[WARNING] Failed to log attempted value bet: {e}")
        
        # Only return value bet if trade succeeded
        if trade_result is not None and trade_result.ok:
            return value_bet
        return None

    def run_multiple(
        self,
        away_team: str,
        home_team: str,
        play_date: date,
        event_slug: str,
        market_slugs: List[str],
    ) -> List[SpreadValueBet]:
        """
        Run the spreads flow for multiple market slugs.
        """
        if self.verbose:
            print(f"\n[SPREADS BATCH] Processing {len(market_slugs)} spread market(s)")
        results: List[SpreadValueBet] = []
        for i, slug in enumerate(market_slugs, 1):
            if self.verbose:
                print(f"\n[SPREAD {i}/{len(market_slugs)}] Processing: {slug}")
            value_bet = self.run(away_team, home_team, play_date, event_slug, slug)
            if value_bet is not None:
                results.append(value_bet)
                if self.verbose:
                    print(f"  -> Added to results (total value bets found: {len(results)})")
        
        if self.verbose:
            print(f"\n[SPREADS BATCH COMPLETE] Found {len(results)} value bet(s) out of {len(market_slugs)} market(s)")
        return results
