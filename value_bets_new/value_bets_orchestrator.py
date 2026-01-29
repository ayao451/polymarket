#!/usr/bin/env python3
"""
Value bets orchestrator - handles all sports in a unified way.

This orchestrator:
1. Loops through all configured sports
2. For each sport, continuously:
   - Fetches Pinnacle games
   - Fetches Polymarket games
   - Fetches market slugs
   - Matches games between the two platforms
   - Evaluates value bets for each matched game and market
   - Executes trades when value bets are found
"""

from __future__ import annotations

import csv
import os
import sys
import asyncio
from datetime import datetime, timezone
from typing import Optional


# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from value_bets_new.constants import MarketType, Sport, SportsbookOdds, ValueBet, HandicapOdds, TotalOdds
from value_bets_new.polymarket import PolymarketInterface, PolymarketEvent
from value_bets_new.pinnacle_odds_service import PinnacleInterface
from value_bets_new.event_processor import EventProcessor
from value_bets_new.pinnacle_odds_interface import PinnacleSportsbookOddsInterface
from value_bets_new.trade_executor.trade_executor_service import TradeExecutorService, TradeExecutionResult
from value_bets_new.redeem_positions import redeem_position, Position

_SUCCESSFUL_TRADES_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "successful_trades.csv")


def _extract_line_from_market_slug(market_slug: str) -> Optional[float]:
    """
    Extract the line/point value from a market slug.
    
    Examples:
        "nba-bkn-lac-2026-01-25-total-212pt5" -> 212.5
        "nba-bkn-lac-2026-01-25-spread-home-4pt5" -> 4.5
        "nba-bkn-lac-2026-01-25-spread-away-2pt5" -> 2.5
        "match-total-36pt5" -> 36.5
        "set-totals-2pt5" -> 2.5
    
    Returns:
        The line value as a float, or None if not found
    """
    import re
    
    # Pattern for totals: matches "total-212pt5", "match-total-36pt5", "set-totals-2pt5", etc.
    # Format is: whole_number + "pt" + decimal_digit
    totals_patterns = [
        r"(?:total(?:-games|-sets)?-)(\d+)pt(\d+)",
        r"match-total-(\d+)pt(\d+)",
        r"set-totals-(\d+)pt(\d+)",
        r"total-(\d+)pt(\d+)",
    ]
    
    for pattern in totals_patterns:
        match = re.search(pattern, market_slug, flags=re.IGNORECASE)
        if match:
            try:
                whole = int(match.group(1))
                decimal = int(match.group(2))
                return float(f"{whole}.{decimal}")
            except (ValueError, TypeError):
                continue
    
    # Pattern for spreads: matches "spread-away-2pt5" or "spread-home-4pt5"
    # Format is: "spread-(away|home)-" + whole_number + "pt" + decimal_digit
    spread_pattern = r"spread-(?:away|home)-(\d+)pt(\d+)"
    match = re.search(spread_pattern, market_slug, flags=re.IGNORECASE)
    if match:
        try:
            whole = int(match.group(1))
            decimal = int(match.group(2))
            return float(f"{whole}.{decimal}")
        except (ValueError, TypeError):
            pass
    
    return None


def _successful_trades_headers() -> list[str]:
    return [
        "bet_time",
        "sport",
        "market",
        "market_slug",
        "team",
        "game",
        "bet_amount",
        "potential_win",
        "tokens",
        "price",
        "ev",
        "polymarket_best_ask",
        "sportsbook_devigged_odds",
        "token_id",
        "condition_id",
    ]


def _successful_trades_row(
    sport: Sport,
    market: MarketType,
    market_slug: str,
    value_bet: ValueBet,
    game_str: str,
    trade_result: TradeExecutionResult,
) -> list[str]:
    bet_amount = trade_result.size * trade_result.price
    # Potential win: if we win, we get $1 per token, so total payout is the number of tokens
    potential_win = trade_result.size
    bet_time = datetime.now(timezone.utc).isoformat()
    # EV is the expected value (expected_payout_per_1)
    ev = value_bet.expected_payout_per_1
    # Sportsbook devigged odds as probability (true_prob)
    sportsbook_devigged_odds = value_bet.true_prob
    return [
        bet_time,
        sport.value,
        market.value,
        market_slug,
        value_bet.team,
        game_str,
        f"{bet_amount:.2f}",
        f"{potential_win:.2f}",
        f"{trade_result.size:.2f}",
        f"{trade_result.price:.4f}",
        f"{ev:.4f}",
        f"{value_bet.polymarket_best_ask:.4f}",
        f"{sportsbook_devigged_odds:.4f}",
        trade_result.token_id,
        value_bet.condition_id or "",
    ]


class ValueBetsOrchestrator:
    def __init__(self):
        self.polymarket_interface = PolymarketInterface()
        self.pinnacle_interface = PinnacleInterface()
        self.event_processor = EventProcessor()
        self.trade_executor = TradeExecutorService()
        # Create a map of PinnacleSportsbookOddsInterface instances for each sport we support
        supported_sports = [Sport.BASKETBALL, Sport.HOCKEY, Sport.UFC, Sport.TENNIS, Sport.SOCCER]
        self.pinnacle_odds_interfaces = {
            sport: PinnacleSportsbookOddsInterface(sport=sport)
            for sport in supported_sports
        }
        # Thread-safe tracking of traded (market_slug, team) tuples
        self._traded_combinations: set[tuple[str, str]] = set()
        self._traded_lock: Optional[asyncio.Lock] = None
        self._log_lock: Optional[asyncio.Lock] = None
    
    def _get_traded_lock(self) -> asyncio.Lock:
        """Get or create the traded lock in the current event loop."""
        if self._traded_lock is None:
            # Create lock in the current running event loop context
            # This ensures the lock is associated with the correct event loop
            self._traded_lock = asyncio.Lock()
        return self._traded_lock
    
    def _get_log_lock(self) -> asyncio.Lock:
        """Get or create the log lock in the current event loop."""
        if self._log_lock is None:
            # Create lock in the current running event loop context
            # This ensures the lock is associated with the correct event loop
            self._log_lock = asyncio.Lock()
        return self._log_lock
    
    sports_to_markets = {
        Sport.BASKETBALL: [MarketType.MONEYLINE, MarketType.SPREADS, MarketType.TOTALS],
        Sport.HOCKEY: [MarketType.MONEYLINE, MarketType.SPREADS, MarketType.TOTALS],
        Sport.TENNIS: [MarketType.MONEYLINE, MarketType.SPREADS],
        Sport.UFC: [MarketType.MONEYLINE],
        Sport.SOCCER: [MarketType.SPREADS, MarketType.TOTALS],
    }

    sports_to_whitelisted_prefixes = {
        Sport.BASKETBALL: ["nba", "cbb", "bkcl", "bkligend", "bkseriea", "bknbl", "bkcba", "bkfr1", "bkarg", "bkkbl", "euroleague"],
        Sport.HOCKEY: ["nhl", "shl", "ahl", "khl", "dehl", "cehl", "snhl"],
        Sport.TENNIS: ["atp", "wta"],
        Sport.UFC: ["ufc"],
        Sport.SOCCER: ["lal", "bra", "bun", "uel", "epl", "sea", "tur", "spl", "por", "col1", "mex", "ere", "fl1", "aus", "den", "rou1", "cdr", "mar1", "mls", "itc", "dfb", "per1", "chi1", "egy1", "cde", "lib", "cze1"],
    }

    async def run(self) -> None:
        print("[DEBUG] Starting orchestrator...")
        tasks = []
        for sport, markets in self.sports_to_markets.items():
            print(f"[DEBUG] Creating task for sport: {sport.value} with markets: {[m.value for m in markets]}")
            task = asyncio.create_task(self._process_sport(sport, markets))
            tasks.append(task)
        
        await asyncio.gather(*tasks)
    
    async def _process_sport(self, sport: Sport, markets: list[MarketType]) -> None:
        print(f"[DEBUG] Starting to process sport: {sport.value}")
        iteration = 0
        while True:
            try:
                iteration += 1
                print(f"[DEBUG] [{sport.value}] Iteration {iteration}: Fetching polymarket events...")
                polymarket_events = self.polymarket_interface.fetch_polymarket_events(
                    whitelisted_prefixes=self.sports_to_whitelisted_prefixes[sport],
                    markets=markets,
                )
                print(f"[DEBUG] [{sport.value}] Found {len(polymarket_events)} polymarket events")

                if len(polymarket_events) == 0:
                    print(f"[DEBUG] [{sport.value}] No events found, continuing...")
                    await asyncio.sleep(5 * 60)
                    continue
                
                await asyncio.gather(*[
                    self._process_game(sport, polymarket_event)
                    for polymarket_event in polymarket_events
                ])
                
                # Small delay between iterations to prevent tight looping
                await asyncio.sleep(10)
            except Exception as e:
                print(f"[ERROR] [{sport.value}] Exception in _process_sport iteration {iteration}: {e}")
                import traceback
                traceback.print_exc()
                # Wait before retrying to avoid rapid error loops
                await asyncio.sleep(60)
                    
    async def _process_game(self, sport: Sport, polymarket_event: PolymarketEvent) -> None:
        game_str = f"{polymarket_event.away_team} @ {polymarket_event.home_team}"
        print(f"[DEBUG] [{sport.value}] Processing game: {game_str} (event_slug: {polymarket_event.event_slug})")
        print(f"[DEBUG] [{sport.value}] Game has {len(polymarket_event.market_slugs_by_event)} markets")
        await asyncio.gather(*[
            self._process_market(sport, polymarket_event, market, event_slugs)
            for market, event_slugs in polymarket_event.market_slugs_by_event.items()
        ])
    
    async def _process_market(self, sport: Sport, polymarket_event: PolymarketEvent, market: MarketType, event_slugs: list[str]) -> None:
        game_str = f"{polymarket_event.away_team} @ {polymarket_event.home_team}"
        print(f"[DEBUG] [{sport.value}] Processing market: {market.value} for {game_str} with {len(event_slugs)} market slugs")
        for market_slug in event_slugs:
            print(f"[DEBUG] [{sport.value}] Processing market_slug: {market_slug}")
            try:
                polymarket_odds_list = self.polymarket_interface.retrieve_polymarket_odds(polymarket_event.event_slug, market_slug)
                print(f"[DEBUG] [{sport.value}] Retrieved {len(polymarket_odds_list)} polymarket odds for {market_slug}")
            except Exception as e:
                print(f"[DEBUG] [{sport.value}] Error retrieving polymarket odds for {market_slug}: {e}")
                continue

            print(f"[DEBUG] [{sport.value}] Fetching sportsbook odds for {game_str} on {polymarket_event.play_date}")
            
            # Fetch the appropriate odds based on market type
            if market == MarketType.MONEYLINE:
                sportsbook_odds = self.pinnacle_odds_interfaces[sport].get_moneyline_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
                if sportsbook_odds is None:
                    print(f"[DEBUG] [{sport.value}] No moneyline odds found for {game_str}")
                    continue
                print(f"[DEBUG] [{sport.value}] Moneyline odds found: {sportsbook_odds.to_string()}")
                # Process all market_odds with the single moneyline odds
                for market_odds in polymarket_odds_list:
                    await self._process_single_odds(sport, polymarket_event, market, market_slug, market_odds, sportsbook_odds)
            elif market == MarketType.SPREADS:
                spreads_odds_list = self.pinnacle_odds_interfaces[sport].get_spread_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
                if spreads_odds_list is None or len(spreads_odds_list) == 0:
                    print(f"[DEBUG] [{sport.value}] No spread odds found for {game_str}")
                    continue
                print(f"[DEBUG] [{sport.value}] Found {len(spreads_odds_list)} spread lines")
                # Extract line value from market slug
                polymarket_line = _extract_line_from_market_slug(market_slug)
                if polymarket_line is None:
                    print(f"[DEBUG] [{sport.value}] Could not extract line from market_slug: {market_slug}")
                    continue
                print(f"[DEBUG] [{sport.value}] Extracted line from market_slug: {polymarket_line}")
                # For spreads, match by line value (use absolute value since direction doesn't matter for matching)
                for market_odds in polymarket_odds_list:
                    matching_spread = None
                    for spread_odds in spreads_odds_list:
                        if spread_odds.point is not None:
                            # Match by absolute value (spread can be positive or negative)
                            if abs(abs(spread_odds.point) - abs(polymarket_line)) < 0.1:
                                matching_spread = spread_odds
                                print(f"[DEBUG] [{sport.value}] Matched spread line: Polymarket {polymarket_line} to Pinnacle {spread_odds.point}")
                                break
                    if matching_spread is not None:
                        await self._process_single_odds(sport, polymarket_event, market, market_slug, market_odds, matching_spread)
                    else:
                        print(f"[DEBUG] [{sport.value}] No matching spread found for line {polymarket_line}")
            elif market in (MarketType.TOTALS, MarketType.TOTALS_GAMES, MarketType.TOTALS_SETS):
                # Fetch totals odds (returns list of TotalOdds, one per line)
                if market == MarketType.TOTALS:
                    totals_odds_list = self.pinnacle_odds_interfaces[sport].get_totals_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
                elif market == MarketType.TOTALS_GAMES:
                    totals_odds_list = self.pinnacle_odds_interfaces[sport].get_totals_games_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
                else:  # TOTALS_SETS
                    totals_odds_list = self.pinnacle_odds_interfaces[sport].get_totals_sets_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
                
                if totals_odds_list is None or len(totals_odds_list) == 0:
                    print(f"[DEBUG] [{sport.value}] No totals odds found for {game_str}")
                    continue
                print(f"[DEBUG] [{sport.value}] Found {len(totals_odds_list)} totals lines")
                
                # Extract line value from market slug
                polymarket_line = _extract_line_from_market_slug(market_slug)
                if polymarket_line is None:
                    print(f"[DEBUG] [{sport.value}] Could not extract line from market_slug: {market_slug}, trying all lines")
                    # Fallback: try all lines if we can't extract
                    for market_odds in polymarket_odds_list:
                        for totals_odds in totals_odds_list:
                            await self._process_single_odds(sport, polymarket_event, market, market_slug, market_odds, totals_odds)
                else:
                    print(f"[DEBUG] [{sport.value}] Extracted line from market_slug: {polymarket_line}")
                    # For totals, match by line value
                    matching_totals = None
                    for totals_odds in totals_odds_list:
                        if totals_odds.point is not None and abs(totals_odds.point - polymarket_line) < 0.1:
                            matching_totals = totals_odds
                            print(f"[DEBUG] [{sport.value}] Matched totals line: Polymarket {polymarket_line} to Pinnacle {totals_odds.point}")
                            break
                    
                    if matching_totals is not None:
                        for market_odds in polymarket_odds_list:
                            await self._process_single_odds(sport, polymarket_event, market, market_slug, market_odds, matching_totals)
                    else:
                        print(f"[DEBUG] [{sport.value}] No matching totals found for line {polymarket_line}")
            else:
                print(f"[DEBUG] [{sport.value}] Unknown market type: {market}")
                continue
    
    async def _process_single_odds(
        self,
        sport: Sport,
        polymarket_event: PolymarketEvent,
        market: MarketType,
        market_slug: str,
        market_odds,
        sportsbook_odds: SportsbookOdds,
    ) -> None:
        game_str = f"{polymarket_event.away_team} @ {polymarket_event.home_team}"
        
        print(f"[DEBUG] [{sport.value}] ========== Checking market_odds ==========")
        print(f"[DEBUG] [{sport.value}] Team: {market_odds.team_name}")
        print(f"[DEBUG] [{sport.value}] Polymarket best_bid: {market_odds.best_bid}")
        print(f"[DEBUG] [{sport.value}] Polymarket best_ask: {market_odds.best_ask}")
        print(f"[DEBUG] [{sport.value}] Polymarket token_id: {market_odds.token_id}")
        print(f"[DEBUG] [{sport.value}] Sportsbook outcome_1: {sportsbook_odds.outcome_1} (cost_to_win_1: {sportsbook_odds.outcome_1_cost_to_win_1})")
        print(f"[DEBUG] [{sport.value}] Sportsbook outcome_2: {sportsbook_odds.outcome_2} (cost_to_win_1: {sportsbook_odds.outcome_2_cost_to_win_1})")
        
        # Check if we've already traded on this (market_slug, team) combination
        trade_key = (market_slug, market_odds.team_name)
        async with self._get_traded_lock():
            if trade_key in self._traded_combinations:
                print(f"[DEBUG] [{sport.value}] Already traded on {trade_key}, skipping")
                return
        
        print(f"[DEBUG] [{sport.value}] Processing value bet evaluation for {market_odds.team_name}")
        value_bet = self.event_processor.process_two_outcome_event(market_odds.team_name, market_odds, sportsbook_odds)
        if value_bet is not None:
            print(f"[DEBUG] [{sport.value}] ========== VALUE BET FOUND! ==========")
            print(f"[DEBUG] [{sport.value}] Team: {value_bet.team}")
            print(f"[DEBUG] [{sport.value}] True probability: {value_bet.true_prob:.4f}")
            print(f"[DEBUG] [{sport.value}] Polymarket best_ask: {value_bet.polymarket_best_ask:.4f}")
            print(f"[DEBUG] [{sport.value}] Expected payout per $1: {value_bet.expected_payout_per_1:.4f}")
            print(f"[DEBUG] [{sport.value}] Token ID: {value_bet.token_id}")
        else:
            print(f"[DEBUG] [{sport.value}] ========== No value bet found ==========")
            print(f"[DEBUG] [{sport.value}] Team: {market_odds.team_name}")
            print(f"[DEBUG] [{sport.value}] Polymarket best_bid: {market_odds.best_bid}, best_ask: {market_odds.best_ask}")
            print(f"[DEBUG] [{sport.value}] Sportsbook outcome_1_cost: {sportsbook_odds.outcome_1_cost_to_win_1:.4f}, outcome_2_cost: {sportsbook_odds.outcome_2_cost_to_win_1:.4f}")
        
        if value_bet is not None:
            print(f"[DEBUG] [{sport.value}] Attempting to execute trade for value bet...")
            print(f"[DEBUG] [{sport.value}] Value bet details: team={value_bet.team}, token_id={value_bet.token_id}, expected_payout={value_bet.expected_payout_per_1:.4f}")
            trade_result = self.trade_executor.execute_value_bet(
                value_bet,
                game_str=game_str,
            )
            if trade_result is not None:
                print(f"[DEBUG] [{sport.value}] Trade execution successful!")
                print(f"[DEBUG] [{sport.value}] Trade result: size={trade_result.size:.2f}, price={trade_result.price:.4f}, token_id={trade_result.token_id}")
            else:
                print(f"[DEBUG] [{sport.value}] Trade execution failed - trade_result is None")
            
            if trade_result is not None:
                # Mark this combination as traded
                async with self._get_traded_lock():
                    self._traded_combinations.add(trade_key)
                await self._log_successful_trade(
                    sport=sport,
                    market=market,
                    market_slug=market_slug,
                    value_bet=value_bet,
                    game_str=game_str,
                    trade_result=trade_result,
                )
                print(f"[SUCCESS] Trade executed: {value_bet.team} @ {game_str} - ${trade_result.size * trade_result.price:.2f} ({trade_result.size:.2f} tokens @ ${trade_result.price:.4f}) - Expected payout: {value_bet.expected_payout_per_1:.4f}")
                
                # Redeem the position in the background (don't await - let it run independently)
                position = Position(
                    token_id=trade_result.token_id,
                    number_of_shares=trade_result.size
                )
                asyncio.create_task(redeem_position(position))
                


    async def _log_successful_trade(
        self,
        sport: Sport,
        market: MarketType,
        market_slug: str,
        value_bet: ValueBet,
        game_str: str,
        trade_result: TradeExecutionResult,
    ) -> None:
        row = _successful_trades_row(
            sport=sport,
            market=market,
            market_slug=market_slug,
            value_bet=value_bet,
            game_str=game_str,
            trade_result=trade_result,
        )
        async with self._get_log_lock():
            await asyncio.to_thread(self._write_csv_row, row)
    
    def _write_csv_row(self, row: list[str]) -> None:
        """Thread-safe CSV writing helper function."""
        write_headers = not os.path.exists(_SUCCESSFUL_TRADES_CSV) or (
            os.path.getsize(_SUCCESSFUL_TRADES_CSV) == 0
        )
        with open(_SUCCESSFUL_TRADES_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_headers:
                writer.writerow(_successful_trades_headers())
            writer.writerow(row)

    async def _retrieve_sportsbook_odds(self, sport: Sport, polymarket_event: PolymarketEvent, market_type: MarketType) -> Optional[SportsbookOdds]:
        if market_type == MarketType.MONEYLINE:
            return self.pinnacle_odds_interfaces[sport].get_moneyline_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        elif market_type == MarketType.SPREADS:
            return self.pinnacle_odds_interfaces[sport].get_spread_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        elif market_type == MarketType.TOTALS:
            return self.pinnacle_odds_interfaces[sport].get_totals_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        elif market_type == MarketType.TOTALS_GAMES:
            return self.pinnacle_odds_interfaces[sport].get_totals_games_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        elif market_type == MarketType.TOTALS_SETS:
            return self.pinnacle_odds_interfaces[sport].get_totals_sets_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        else:
            raise ValueError(f"Invalid market type: {market_type}")


def main() -> int:
    """Main entry point."""
    orchestrator = ValueBetsOrchestrator()
    
    try:
        asyncio.run(orchestrator.run())
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
