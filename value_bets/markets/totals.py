#!/usr/bin/env python3
"""
Totals (over/under) market handler.

Orchestrates the full flow:
1. Fetch sportsbook totals odds
2. Fetch Polymarket totals odds
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
from polymarket_sports_betting_bot.value_bet_service import TotalsValueBetService, TotalsValueBet
from value_bet_helpers import log_attempted_totals_bet, log_value_bet

from .market import Market


class Totals(Market):
    """Handler for totals (over/under) markets."""

    @staticmethod
    def _parse_total_line_from_slug(slug: str) -> Optional[float]:
        """
        Parse total line from market slug.
        Handles: total-228pt5, total-games-24pt5, total-sets-5pt5, match-total-36pt5,
        set-totals-2pt5, 1h-total-115pt5.
        """
        patterns = [
            r"(?:total(?:-games|-sets)?-)(\d+)pt(\d+)",
            r"match-total-(\d+)pt(\d+)",
            r"set-totals-(\d+)pt(\d+)",
            r"total-(\d+)pt(\d+)",
        ]
        for pat in patterns:
            m = re.search(pat, slug, flags=re.IGNORECASE)
            if m:
                whole = int(m.group(1))
                dec = int(m.group(2))
                return float(f"{whole}.{dec}")
        return None

    @staticmethod
    def _totals_type_from_slug(slug: str) -> Optional[str]:
        """
        Infer totals sub-type from market slug for tennis.
        Returns "games" | "sets" | None (generic totals).
        """
        s = (slug or "").lower()
        if "total" not in s:
            return None
        if "match-total" in s or "games" in s:
            return "games"
        if "set-totals" in s or "sets" in s:
            return "sets"
        return None

    def _fetch_sportsbook_totals(
        self,
        away_team: str,
        home_team: str,
        play_date: date,
        totals_type: Optional[str],
    ):
        """Fetch totals odds from sportsbook; totals_type is 'games' | 'sets' | None (generic)."""
        if totals_type == "games":
            return self.sportsbook.get_totals_games_odds(away_team, home_team, play_date)
        if totals_type == "sets":
            return self.sportsbook.get_totals_sets_odds(away_team, home_team, play_date)
        return self.sportsbook.get_totals_odds(away_team, home_team, play_date)

    def run(
        self,
        away_team: str,
        home_team: str,
        play_date: date,
        event_slug: str,
        market_slug: str,
        traded_markets: Optional[Set[str]] = None,
    ) -> Optional[TotalsValueBet]:
        """
        Run the full totals flow for a single market slug.
        Infers totals_type (games/sets) from slug for tennis total-games / total-sets.
        """
        totals_type = self._totals_type_from_slug(market_slug)
        label = "TOTALS (O/U)"
        if totals_type == "games":
            label = "TOTALS GAMES (O/U)"
        elif totals_type == "sets":
            label = "TOTALS SETS (O/U)"

        # Step 1: Get sportsbook totals odds
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"{label}: {away_team} @ {home_team}")
            print(f"Date: {play_date}")
            print(f"Event: {event_slug}")
            print(f"Market: {market_slug}")
            print(f"{'='*60}")
            print(f"\n[STEP 1/4] Fetching sportsbook totals odds from Pinnacle...")
        
        sportsbook_totals = self._fetch_sportsbook_totals(
            away_team, home_team, play_date, totals_type
        )
        if not sportsbook_totals:
            if self.verbose:
                print(f"  -> [FAILED] Could not fetch sportsbook totals odds")
            return None
        
        if self.verbose:
            print(f"  -> [SUCCESS] Got {len(sportsbook_totals)} total line(s)")
            for i, t in enumerate(sportsbook_totals, 1):
                print(f"  -> Line {i}: {t.to_string()}")
        
        # Step 2: Get Polymarket odds for this totals market
        if self.verbose:
            print(f"\n[STEP 2/4] Fetching Polymarket totals odds...")
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
        
        # Parse total line from slug (e.g., "total-228pt5" -> 228.5)
        polymarket_line = self._parse_total_line_from_slug(market_slug)
        if self.verbose:
            print(f"  -> Parsed total line from slug: {polymarket_line}")
        
        # Step 3: Evaluate for value bet
        if self.verbose:
            print(f"\n[STEP 3/4] Evaluating for totals value bet...")
            print(f"  -> Matching Polymarket O/U line with sportsbook lines")
        
        value_bet_service = TotalsValueBetService(
            sportsbook_totals=sportsbook_totals,
            polymarket_totals_results=polymarket_odds_list,
            polymarket_line=polymarket_line,
            verbose=self.verbose,
        )
        
        value_bets = value_bet_service.discover_value_bets()
        
        if not value_bets:
            if self.verbose:
                print(f"  -> [NO VALUE] No totals value bet found")
                print(f"  -> Either no matching line or price is too high")
            return None
        
        # Take the best value bet (highest expected payout)
        value_bet = value_bets[0]
        
        # Always print value bet found
        edge_pct = (value_bet.true_prob - value_bet.polymarket_best_ask) * 100
        vb_label = "TOTALS"
        if totals_type == "games":
            vb_label = "TOTALS GAMES"
        elif totals_type == "sets":
            vb_label = "TOTALS SETS"
        print(f"\n{'*'*60}")
        print(f"*** VALUE BET FOUND ({vb_label}) ***")
        print(f"{'*'*60}")
        print(f"  Game: {away_team} @ {home_team}")
        print(f"  Bet on: {value_bet.side} {value_bet.total_point}")
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
            log_attempted_totals_bet(
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
    ) -> List[TotalsValueBet]:
        """
        Run the totals flow for multiple market slugs.
        """
        if self.verbose:
            print(f"\n[TOTALS BATCH] Processing {len(market_slugs)} totals market(s)")
        results: List[TotalsValueBet] = []
        for i, slug in enumerate(market_slugs, 1):
            if self.verbose:
                print(f"\n[TOTALS {i}/{len(market_slugs)}] Processing: {slug}")
            value_bet = self.run(away_team, home_team, play_date, event_slug, slug)
            if value_bet is not None:
                results.append(value_bet)
                if self.verbose:
                    print(f"  -> Added to results (total value bets found: {len(results)})")
        
        if self.verbose:
            print(f"\n[TOTALS BATCH COMPLETE] Found {len(results)} value bet(s) out of {len(market_slugs)} market(s)")
        return results
