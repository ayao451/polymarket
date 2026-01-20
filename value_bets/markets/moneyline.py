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

import math
from datetime import date
from typing import Optional

from polymarket_sports_betting_bot.value_bet_service import ValueBetService, ValueBet
from value_bet_helpers import log_attempted_moneyline_bet, log_value_bet
from pinnacle_scraper.sportsbook_odds import ThreeWayMoneylineOdds

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
            print(f"Sport: {self.sport}")
            print(f"{'='*60}")
            print(f"\n[STEP 1/4] Fetching sportsbook moneyline odds from Pinnacle...")
        
        # For soccer, use 3-way moneyline (away, draw, home)
        # For other sports, use 2-way moneyline (away, home)
        if self.sport.lower() == "soccer":
            if self.verbose:
                print(f"  -> [SOCCER] Fetching 3-way moneyline odds (away, draw, home)...")
            sportsbook_odds = self.sportsbook.get_three_way_moneyline_odds(away_team, home_team, play_date)
            is_three_way = True
        else:
            if self.verbose:
                print(f"  -> [2-WAY] Fetching 2-way moneyline odds (away, home)...")
            sportsbook_odds = self.sportsbook.get_moneyline_odds(away_team, home_team, play_date)
            is_three_way = False
        
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
            if is_three_way:
                print(f"  -> [3-WAY] Evaluating all 3 outcomes: away, draw, home")
        
        # For 3-way moneyline (soccer), we need to handle draw outcome
        if is_three_way and isinstance(sportsbook_odds, ThreeWayMoneylineOdds):
            # Create a value bet service that can handle 3-way
            value_bet_service = ValueBetService(
                away_team=away_team,
                home_team=home_team,
                sportsbook_result=None,  # Will handle 3-way separately
                verbose=self.verbose,
            )
            
            # Devig the 3-way odds
            q_away = sportsbook_odds.outcome_1_cost_to_win_1
            q_draw = sportsbook_odds.outcome_2_cost_to_win_1
            q_home = sportsbook_odds.outcome_3_cost_to_win_1
            total = q_away + q_draw + q_home
            if total > 0:
                p_away = q_away / total
                p_draw = q_draw / total
                p_home = q_home / total
                
                if self.verbose:
                    print(f"  -> [3-WAY DEVIG] True probabilities:")
                    print(f"      Away ({away_team}): {p_away*100:.2f}%")
                    print(f"      Draw: {p_draw*100:.2f}%")
                    print(f"      Home ({home_team}): {p_home*100:.2f}%")
            else:
                if self.verbose:
                    print(f"  -> [ERROR] Invalid 3-way odds, cannot devig")
                return None
            
            # Evaluate each Polymarket outcome against the 3-way probabilities
            value_bet = None
            for odds in polymarket_odds_list:
                if self.verbose:
                    print(f"\n    [EVAL] Evaluating Polymarket outcome: {odds.market}")
                
                # Extract outcome from market label
                outcome_team = value_bet_service._extract_outcome_team(odds.market)
                if not outcome_team:
                    if self.verbose:
                        print(f"    [SKIP] Could not extract outcome from: {odds.market}")
                    continue
                
                # Determine which probability to use
                outcome_norm = value_bet_service._normalize_team_name(outcome_team)
                away_norm = value_bet_service._normalize_team_name(away_team)
                home_norm = value_bet_service._normalize_team_name(home_team)
                draw_norm = value_bet_service._normalize_team_name("draw")
                
                if outcome_norm == draw_norm or "draw" in outcome_norm:
                    p_true = p_draw
                    team_label = "Draw"
                elif value_bet_service._team_matches_outcome(away_team, outcome_team):
                    p_true = p_away
                    team_label = away_team
                elif value_bet_service._team_matches_outcome(home_team, outcome_team):
                    p_true = p_home
                    team_label = home_team
                else:
                    if self.verbose:
                        print(f"    [SKIP] Could not match '{outcome_team}' to any outcome")
                    continue
                
                if self.verbose:
                    print(f"    [MATCH] Matched '{outcome_team}' to '{team_label}' (true prob: {p_true*100:.2f}%)")
                
                if odds.best_ask is None:
                    if self.verbose:
                        print(f"    [SKIP] No best_ask available")
                    continue
                
                if p_true < value_bet_service.MIN_TRUE_PROB:
                    if self.verbose:
                        print(f"    [SKIP] True prob {p_true*100:.2f}% below minimum {value_bet_service.MIN_TRUE_PROB*100:.2f}%")
                    continue
                
                polymarket_ask = float(odds.best_ask)
                if polymarket_ask <= 0:
                    if self.verbose:
                        print(f"    [SKIP] Invalid polymarket ask: {polymarket_ask}")
                    continue
                
                payout_per_1 = 1.0 / polymarket_ask
                expected_payout = float(p_true) * float(payout_per_1)
                
                if self.verbose:
                    print(f"    [CALC] Polymarket ask: ${polymarket_ask:.4f} ({polymarket_ask*100:.2f}%)")
                    print(f"    [CALC] Payout per $1 if win: ${payout_per_1:.4f}")
                    print(f"    [CALC] Expected payout = {p_true:.4f} * {payout_per_1:.4f} = ${expected_payout:.4f}")
                    print(f"    [CALC] Threshold: ${value_bet_service.MIN_EXPECTED_PAYOUT_PER_1:.4f}")
                    print(f"    [CALC] Edge: {(expected_payout - 1.0) * 100:.2f}%")
                
                if expected_payout > value_bet_service.MIN_EXPECTED_PAYOUT_PER_1:
                    if self.verbose:
                        print(f"    [VALUE BET!] Expected payout ${expected_payout:.4f} > threshold ${value_bet_service.MIN_EXPECTED_PAYOUT_PER_1:.4f}")
                    candidate = ValueBet(
                        team=team_label,
                        token_id=odds.token_id,
                        true_prob=p_true,
                        polymarket_best_ask=polymarket_ask,
                        expected_payout_per_1=expected_payout,
                    )
                    if value_bet is None or candidate.expected_payout_per_1 > value_bet.expected_payout_per_1:
                        value_bet = candidate
                else:
                    if self.verbose:
                        print(f"    [NO VALUE] Expected payout ${expected_payout:.4f} <= threshold ${value_bet_service.MIN_EXPECTED_PAYOUT_PER_1:.4f}")
        else:
            # 2-way moneyline (basketball, hockey)
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
        # For soccer, we're just printing - not executing trades yet
        if self.verbose:
            print(f"\n[STEP 4/4] Calculating Kelly bet size...")
            if self.sport.lower() == "soccer":
                print(f"  -> [SOCCER] Trade execution disabled - printing only")
        
        # For soccer, skip trade execution (just print)
        if self.sport.lower() == "soccer":
            trade_result = None
            if self.verbose:
                print(f"  -> [SKIP] Trade execution skipped for soccer (printing only mode)")
        else:
            trade_result = self.execute_value_bet(value_bet, away_team, home_team, event_slug)
        
        # Log attempted value bet (regardless of execution result)
        try:
            executed = trade_result is not None and trade_result.ok
            error = None if executed else (trade_result.error if trade_result else "Trade skipped")
            log_attempted_moneyline_bet(
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
