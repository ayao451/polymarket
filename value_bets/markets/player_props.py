#!/usr/bin/env python3
"""
Player props market handler.

Orchestrates the full flow:
1. Parse player prop market slug from Polymarket
2. Fetch sportsbook player prop odds from Pinnacle
3. Fetch Polymarket player prop odds
4. Evaluate for value bets using sportsbook odds
5. Calculate Kelly bet size
6. Execute trades if value found

Note: Only trades when sportsbook odds are available.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Optional

from polymarket_odds_service.polymarket_odds import PolymarketOdds
from polymarket_sports_betting_bot.value_bet_service import PlayerPropValueBetService, PlayerPropValueBet
from value_bet_helpers import log_attempted_player_prop_bet, log_value_bet

from .market import Market


class PlayerProps(Market):
    """Handler for player prop markets."""

    @staticmethod
    def parse_player_prop_slug(market_slug: str) -> Optional[dict]:
        """
        Parse a Polymarket player prop slug to extract player, prop type, and line.
        
        Examples:
            "nba-lal-por-2026-01-17-points-lebron-james-26pt5"
            -> {"player": "lebron-james", "prop_type": "points", "line": 26.5}
            
            "nba-lal-por-2026-01-17-rebounds-deandre-ayton-9pt5"
            -> {"player": "deandre-ayton", "prop_type": "rebounds", "line": 9.5}
        
        Returns:
            Dict with "player", "prop_type", "line" keys, or None if parsing fails
        """
        slug_lower = market_slug.lower()
        
        # Common prop types
        prop_types = ["points", "rebounds", "assists", "threes", "steals", "blocks"]
        
        # Find prop type in slug
        prop_type = None
        prop_type_pos = -1
        for pt in prop_types:
            pos = slug_lower.find(f"-{pt}-")
            if pos > 0:
                prop_type = pt
                prop_type_pos = pos
                break
        
        if prop_type is None:
            return None
        
        # Extract player name (between prop_type and the line)
        # Format: ...-{prop_type}-{player-name}-{line}
        start_pos = prop_type_pos + len(prop_type) + 1  # +1 for the dash
        end_pos = slug_lower.rfind("-")
        
        if end_pos <= start_pos:
            return None
        
        player_slug = slug_lower[start_pos:end_pos]
        
        # Extract line (last part after final dash)
        line_str = slug_lower[end_pos + 1:]
        
        # Parse line: "26pt5" -> 26.5, "9pt5" -> 9.5, "11pt5" -> 11.5
        line_match = re.match(r"^(\d+)pt(\d+)$", line_str)
        if line_match:
            whole = float(line_match.group(1))
            decimal = float(line_match.group(2)) / 10.0
            line = whole + decimal
        else:
            # Try direct float parsing
            try:
                line = float(line_str)
            except ValueError:
                return None
        
        return {
            "player": player_slug.replace("-", " ").strip(),  # Convert "lebron-james" -> "lebron james"
            "prop_type": prop_type,
            "line": line,
        }

    def run(
        self,
        away_team: str,
        home_team: str,
        play_date: date,
        event_slug: str,
        market_slug: str,
    ) -> Optional[PlayerPropValueBet]:
        """
        Run the full player prop flow.
        
        Args:
            away_team: Away team name
            home_team: Home team name
            play_date: Date of the game
            event_slug: Polymarket event slug
            market_slug: Polymarket market slug (e.g., "points-lebron-james-26pt5")
            
        Returns:
            PlayerPropValueBet if a value bet was found, None otherwise
        """
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"PLAYER PROPS: {away_team} @ {home_team}")
            print(f"Date: {play_date}")
            print(f"Event: {event_slug}")
            print(f"Market: {market_slug}")
            print(f"{'='*60}")
            print(f"\n[STEP 1/4] Parsing player prop slug...")
        
        # Parse the market slug
        parsed = self.parse_player_prop_slug(market_slug)
        if not parsed:
            if self.verbose:
                print(f"  -> [FAILED] Could not parse player prop slug: {market_slug}")
            return None
        
        player_name = parsed["player"]
        prop_type = parsed["prop_type"]
        line = parsed["line"]
        
        if self.verbose:
            print(f"  -> [SUCCESS] Parsed player prop")
            print(f"  -> Player: {player_name}")
            print(f"  -> Prop type: {prop_type}")
            print(f"  -> Line: {line}")
        
        # Step 2: Get sportsbook player prop odds
        if self.verbose:
            print(f"\n[STEP 2/4] Fetching sportsbook player prop odds from Pinnacle...")
        
        sportsbook_player_props = self.sportsbook.get_player_props_odds(away_team, home_team, play_date)
        if not sportsbook_player_props:
            if self.verbose:
                print(f"  -> [FAILED] Could not fetch sportsbook player prop odds")
                print(f"  -> Skipping trade (only trade when sportsbook odds are available)")
            return None
        
        if self.verbose:
            print(f"  -> [SUCCESS] Got {len(sportsbook_player_props)} sportsbook player prop(s)")
            for i, prop in enumerate(sportsbook_player_props[:3], 1):
                print(f"  -> Prop {i}: {prop.player_name} {prop.prop_type} {prop.line}")
        
        # Step 3: Get Polymarket odds
        if self.verbose:
            print(f"\n[STEP 3/4] Fetching Polymarket player prop odds...")
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
        
        # Step 4: Evaluate for value bet using sportsbook odds
        if self.verbose:
            print(f"\n[STEP 4/4] Evaluating for value bet...")
            print(f"  -> Matching Polymarket player prop with sportsbook odds")
        
        value_bet_service = PlayerPropValueBetService(
            sportsbook_player_props=sportsbook_player_props,
            polymarket_player_prop_results=polymarket_odds_list,
            polymarket_player_name=player_name,
            polymarket_prop_type=prop_type,
            polymarket_line=line,
            verbose=self.verbose,
        )
        
        value_bets = value_bet_service.discover_value_bets()
        
        if not value_bets:
            if self.verbose:
                print(f"  -> [NO VALUE] No player prop value bet found")
                print(f"  -> Either no matching line or price is too high")
            return None
        
        # Take the best value bet (highest expected payout)
        value_bet = value_bets[0]
        
        # Always print value bet found
        edge_pct = (value_bet.true_prob - value_bet.polymarket_best_ask) * 100
        print(f"\n{'*'*60}")
        print(f"*** PLAYER PROP VALUE BET FOUND ***")
        print(f"{'*'*60}")
        print(f"  Game: {away_team} @ {home_team}")
        print(f"  Player: {value_bet.player_name}")
        print(f"  Prop: {value_bet.prop_type} {value_bet.line} {value_bet.side}")
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
        
        # Step 5: Calculate Kelly bet size and execute trade
        if self.verbose:
            print(f"\n[STEP 5/5] Executing trade with Kelly Criterion sizing...")
        
        # Use the base class execute_value_bet method
        # We need to create a compatible value bet object
        # For now, we'll create a simple wrapper that works with execute_value_bet
        class PlayerPropValueBetWrapper:
            """Wrapper to make PlayerPropValueBet compatible with execute_value_bet."""
            def __init__(self, pp_value_bet: PlayerPropValueBet):
                self.token_id = pp_value_bet.token_id
                self.polymarket_best_ask = pp_value_bet.polymarket_best_ask
                self.true_prob = pp_value_bet.true_prob
                self.expected_payout_per_1 = pp_value_bet.expected_payout_per_1
                # Create a descriptive team/outcome label
                self.team = f"{pp_value_bet.player_name} {pp_value_bet.prop_type} {pp_value_bet.line} {pp_value_bet.side}"
        
        wrapper = PlayerPropValueBetWrapper(value_bet)
        trade_result = self.execute_value_bet(wrapper, away_team, home_team, event_slug)
        
        # Log attempted value bet (regardless of execution result)
        try:
            executed = trade_result is not None and trade_result.ok
            error = None if executed else (trade_result.error if trade_result else "Trade skipped")
            log_attempted_player_prop_bet(
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
