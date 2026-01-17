#!/usr/bin/env python3
"""
Moneyline market handler.

Orchestrates the full flow:
1. Fetch sportsbook moneyline odds
2. Fetch Polymarket odds
3. Evaluate for value bets
4. Calculate Kelly bet size
5. Execute trades if value found
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from polymarket_sports_betting_bot.value_bet_service import ValueBetService, ValueBet

from .market import Market


class Moneyline(Market):
    """Handler for moneyline markets."""

    def run(
        self,
        away_team: str,
        home_team: str,
        play_date: date,
        event_slug: str,
        market_slug: str,
    ) -> Optional[ValueBet]:
        """
        Run the full moneyline flow.
        
        Uses Kelly Criterion to calculate optimal bet size based on:
        - True probability (devigged from sportsbook odds)
        - Polymarket best ask price
        - Current bankroll (USDC balance)
        
        Args:
            away_team: Away team name
            home_team: Home team name
            play_date: Date of the game
            event_slug: Polymarket event slug
            market_slug: Polymarket market slug
            
        Returns:
            ValueBet if a value bet was found, None otherwise
        """
        # Step 1: Get sportsbook moneyline odds
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"MONEYLINE: {away_team} @ {home_team}")
            print(f"Date: {play_date}")
            print(f"Event: {event_slug}")
            print(f"Market: {market_slug}")
            print(f"{'='*60}")
            print(f"\n[STEP 1/4] Fetching sportsbook moneyline odds from Pinnacle...")
        
        sportsbook_odds = self.sportsbook.get_moneyline_odds(away_team, home_team, play_date)
        if sportsbook_odds is None:
            if self.verbose:
                print(f"  -> [FAILED] Could not fetch sportsbook moneyline odds")
                print(f"  -> Possible reasons: game not found, API error, or no odds available")
            return None
        
        if self.verbose:
            print(f"  -> [SUCCESS] Got sportsbook odds")
            print(f"  -> {sportsbook_odds.to_string()}")
        
        # Step 2: Get Polymarket odds
        if self.verbose:
            print(f"\n[STEP 2/4] Fetching Polymarket odds...")
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
        
        # Step 3: Evaluate for value bet
        if self.verbose:
            print(f"\n[STEP 3/4] Evaluating for value bet...")
            print(f"  -> Comparing sportsbook odds (devigged) vs Polymarket ask price")
        
        value_bet_service = ValueBetService(
            away_team=away_team,
            home_team=home_team,
            sportsbook_result=sportsbook_odds,
            verbose=self.verbose,
        )
        
        # Evaluate each outcome and find the best value bet
        value_bet = None
        for odds in polymarket_odds_list:
            result = value_bet_service.evaluate_single(odds)
            if result is not None:
                if value_bet is None or result.expected_payout_per_1 > value_bet.expected_payout_per_1:
                    value_bet = result
        
        if value_bet is None:
            if self.verbose:
                print(f"  -> [NO VALUE] No value bet found in any outcome")
                print(f"  -> Polymarket prices are too high relative to true probabilities")
            return None
        
        # Always print value bet found
        edge_pct = (value_bet.true_prob - value_bet.polymarket_best_ask) * 100
        print(f"\n{'*'*60}")
        print(f"*** VALUE BET FOUND (MONEYLINE) ***")
        print(f"{'*'*60}")
        print(f"  Game: {away_team} @ {home_team}")
        print(f"  Bet on: {value_bet.team}")
        print(f"  Sportsbook (Pinnacle) true prob: {value_bet.true_prob*100:.2f}%")
        print(f"  Polymarket ask price: ${value_bet.polymarket_best_ask:.4f} ({value_bet.polymarket_best_ask*100:.2f}%)")
        print(f"  Edge: {edge_pct:+.2f}%")
        print(f"  Expected payout per $1: ${value_bet.expected_payout_per_1:.4f}")
        print(f"  Token ID: {value_bet.token_id}")
        
        # Step 4: Calculate Kelly bet size and execute trade
        if self.verbose:
            print(f"\n[STEP 4/4] Executing trade with Kelly Criterion sizing...")
        self.execute_value_bet(value_bet, away_team, home_team)
        
        return value_bet
