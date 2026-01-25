#!/usr/bin/env python3
"""
Helper module for sports value betting bots.

Contains common logic shared across all sports (basketball, hockey, ufc, tennis).
"""

import sys
import time
import argparse
import traceback
from datetime import datetime, timedelta, timezone
import os
import csv
from typing import Optional, List, Tuple, Dict, Set, Callable, Any
from dataclasses import dataclass

from polymarket_sports_betting_bot.polymarket_sports_betting_bot_interface import (
    PolymarketSportsBettingBotInterface,
)
from trade_executor.trade_executor_service import TradeExecutorService
from value_bet_helpers import (
    fetch_polymarket_events_for_date,
    fetch_market_slugs_by_event,
    match_games,
)

MIN_BANKROLL = 5.0
INNER_LOOP_DURATION_SECONDS = 30 * 60  # 30 minutes


@dataclass
class SportConfig:
    """Configuration for a specific sport."""
    sport_name: str
    display_name: str
    pinnacle_service_class: Any
    whitelisted_prefixes: List[str]
    sort_key_func: Callable[[Tuple], Tuple[int, str]]
    default_markets: Dict[str, bool]
    supports_test_date: bool = False
    description: str = ""


class TradesCounter:
    """Helper class to track trade counts from CSV files."""
    
    def __init__(self, trades_csv_path: str):
        self.trades_csv_path = trades_csv_path
    
    def get_trade_count(self) -> int:
        """Return the current number of trades in the CSV file."""
        if not os.path.exists(self.trades_csv_path):
            return 0
        try:
            with open(self.trades_csv_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                return len(list(reader))
        except Exception:
            return 0


class SportsValueBetsRunner:
    """Main runner class for sports value betting bots."""
    
    def __init__(self, config: SportConfig, verbose: bool = False):
        self.config = config
        self.verbose = verbose
        self.bot = PolymarketSportsBettingBotInterface(sport=config.sport_name, verbose=verbose)
        self.pinnacle = config.pinnacle_service_class(timeout_ms=45000)
        self.trade_executor = TradeExecutorService()
        self.traded_markets: Set[str] = set()
        
        # Get trades CSV path from sport directory
        # The sport-specific value_bets.py files are in value_bets/{sport}/value_bets.py
        # So we need to go up one level from the helper to get to value_bets/, then into {sport}/
        helper_dir = os.path.dirname(os.path.abspath(__file__))
        sport_specific_dir = os.path.join(helper_dir, config.sport_name)
        trades_csv_path = os.path.join(sport_specific_dir, "trades.csv")
        self.trades_counter = TradesCounter(trades_csv_path)
    
    def _check_bankroll(self) -> bool:
        """Check if bankroll is sufficient. Returns True if OK, False if too low."""
        bankroll = self.trade_executor.get_usdc_balance()
        if bankroll is not None and bankroll < MIN_BANKROLL:
            print(f"\n{'!'*60}")
            print(f"!!! BANKROLL TOO LOW - STOPPING !!!")
            print(f"{'!'*60}")
            print(f"  Current bankroll: ${bankroll:.2f}")
            print(f"  Minimum required: ${MIN_BANKROLL:.2f}")
            print(f"  Exiting to protect remaining funds.")
            return False
        return True
    
    def _fetch_polymarket_events(self, today, tomorrow, verbose: bool) -> List[Tuple[str, str, str]]:
        """Fetch Polymarket events for today and tomorrow."""
        if verbose:
            print("\n[STEP 1] Fetching Polymarket events for today and tomorrow...")
        
        polymarket_events_today = fetch_polymarket_events_for_date(
            today,
            whitelisted_prefixes=self.config.whitelisted_prefixes,
            verbose=verbose,
        )
        polymarket_events_tomorrow = fetch_polymarket_events_for_date(
            tomorrow,
            whitelisted_prefixes=self.config.whitelisted_prefixes,
            verbose=verbose,
        )
        polymarket_events = polymarket_events_today + polymarket_events_tomorrow
        
        if verbose:
            print(f"  -> Found {len(polymarket_events_today)} Polymarket events for today")
            print(f"  -> Found {len(polymarket_events_tomorrow)} Polymarket events for tomorrow")
            print(f"  -> Total: {len(polymarket_events)} Polymarket events")
        
        return polymarket_events
    
    def _fetch_market_slugs(self, polymarket_events: List[Tuple[str, str, str]], verbose: bool) -> Dict[str, Dict[str, List[str]]]:
        """Fetch market slugs for all events."""
        if verbose:
            print("\n[STEP 2] Fetching market slugs for each event...")
        
        event_slugs = [slug for slug, _, _ in polymarket_events]
        market_slugs_by_event = fetch_market_slugs_by_event(event_slugs, verbose=verbose)
        
        if verbose:
            print(f"  -> Fetched market slugs for {len(market_slugs_by_event)} events")
        
        return market_slugs_by_event
    
    def _fetch_pinnacle_games(self, today, tomorrow, verbose: bool) -> List[Any]:
        """Fetch Pinnacle games for today and tomorrow."""
        if verbose:
            print("\n[STEP 3] Fetching Pinnacle games for matching...")
        
        pinnacle_games = []
        pinnacle_games_today = []
        pinnacle_games_tomorrow = []
        try:
            pinnacle_games_today = self.pinnacle.list_games_for_date(today, game_status="all")
            pinnacle_games_tomorrow = self.pinnacle.list_games_for_date(tomorrow, game_status="all")
            pinnacle_games = pinnacle_games_today + pinnacle_games_tomorrow
        except Exception as e:
            print(f"  -> [ERROR] Failed to fetch Pinnacle games: {e}")
            pinnacle_games = []
        
        if verbose:
            print(f"  -> Found {len(pinnacle_games_today)} Pinnacle games for today")
            print(f"  -> Found {len(pinnacle_games_tomorrow)} Pinnacle games for tomorrow")
            print(f"  -> Total: {len(pinnacle_games)} Pinnacle games")
        
        return pinnacle_games
    
    def _match_events(self, polymarket_events: List[Tuple[str, str, str]], pinnacle_games: List[Any], verbose: bool) -> List[Tuple]:
        """Match games between Polymarket and Pinnacle."""
        if verbose:
            print("\n[STEP 4] Matching games between Polymarket and Pinnacle...")
        
        matched_events = match_games(polymarket_events, pinnacle_games)
        
        if verbose:
            print(f"  -> Found {len(matched_events)} matched games (in both Polymarket and Pinnacle)")
        
        # Sort matched events using sport-specific sort key
        matched_events = sorted(matched_events, key=self.config.sort_key_func)
        
        return matched_events
    
    def _should_skip_game(self, pinnacle_game: Any, verbose: bool) -> bool:
        """Check if a game should be skipped (already started or too far away)."""
        if pinnacle_game and pinnacle_game.start_time_utc:
            now_utc = datetime.now(timezone.utc)
            time_until_start = pinnacle_game.start_time_utc - now_utc
            
            if pinnacle_game.start_time_utc < now_utc:
                if verbose:
                    print(f"[SKIP] Game has already started")
                    print(f"  Start time: {pinnacle_game.start_time_utc.isoformat()}")
                    print(f"  Current time: {now_utc.isoformat()}")
                return True
            elif time_until_start.total_seconds() > 12 * 60 * 60:  # More than 12 hours
                if verbose:
                    print(f"[SKIP] Game is more than 12 hours away")
                    print(f"  Start time: {pinnacle_game.start_time_utc.isoformat()}")
                    print(f"  Current time: {now_utc.isoformat()}")
                    print(f"  Time until start: {time_until_start.total_seconds()/3600:.1f} hours")
                return True
            else:
                if verbose:
                    print(f"[OK] Game starts in {time_until_start.total_seconds()/60:.0f} minutes")
        
        return False
    
    def _process_game(
        self,
        event_slug: str,
        away_team: str,
        home_team: str,
        pinnacle_game: Any,
        market_slugs_by_event: Dict[str, Dict[str, List[str]]],
        markets_to_run: Dict[str, bool],
        today,
        verbose: bool,
    ) -> None:
        """Process a single game for value bets."""
        if verbose:
            print(f"\n--- Game: {away_team} @ {home_team} ---")
            print("-" * 60)
        
        if self._should_skip_game(pinnacle_game, verbose):
            return
        
        markets = market_slugs_by_event.get(event_slug, {})
        if not markets:
            if verbose:
                print(f"[SKIP] No markets found for this event")
            return
        
        if verbose:
            market_counts = ", ".join([f"{k}={len(v)}" for k, v in markets.items() if isinstance(v, list)])
            print(f"[OK] Found markets: {market_counts}")
        
        if verbose:
            print(f"\n[PROCESSING] Starting value bet analysis for: {away_team} @ {home_team}")
            print(f"  Event slug: {event_slug}")
            print(f"  Markets available: {list(markets.keys())}")
        
        try:
            self.bot.run_all_markets(
                away_team=away_team,
                home_team=home_team,
                play_date=today,
                event_slug=event_slug,
                market_slugs_by_event=market_slugs_by_event,
                traded_markets=self.traded_markets,
                markets_to_run=markets_to_run,
            )
            if verbose:
                print(f"[DONE] Finished processing: {away_team} @ {home_team}")
        except Exception as e:
            print(f"[ERROR] Exception while processing {away_team} @ {home_team}: {e}")
            traceback.print_exc()
    
    def run(
        self,
        markets_to_run: Dict[str, bool],
        test_date: Optional[str] = None,
    ) -> int:
        """Main run loop."""
        print("\n" + "="*80)
        print(f"POLYMARKET SPORTS BETTING BOT - STARTING UP ({self.config.display_name.upper()})")
        print("="*80)
        print(f"Running forever - refetching events every 30 minutes")
        print(f"Verbose mode: {'ON' if self.verbose else 'OFF'}")
        print(f"Markets to run:")
        for market_name, enabled in markets_to_run.items():
            if market_name:  # Skip empty keys
                print(f"  - {market_name.capitalize()}: {'YES' if enabled else 'NO'}")
        
        if self.verbose:
            print("\n[INIT] Creating bot interface...")
            print("[INIT] Bot interface created")
            print("\n[INIT] Creating Pinnacle odds service...")
            print("[INIT] Pinnacle service created (timeout: 45s)")
            print(f"\n[INIT] Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        parsed_test_date = None
        if test_date:
            try:
                parsed_test_date = datetime.strptime(test_date, "%Y-%m-%d").date()
                print(f"\n[TEST MODE] Testing on date: {parsed_test_date}")
            except ValueError:
                print(f"\n[ERROR] Invalid date format. Use YYYY-MM-DD (e.g., 2024-01-13)")
                return 1
        
        # Outer loop: runs forever, refetches events every 30 minutes
        while True:
            now = datetime.now().astimezone()
            if parsed_test_date:
                today = parsed_test_date
                tomorrow = parsed_test_date + timedelta(days=1)
            else:
                today = now.date()
                tomorrow = today + timedelta(days=1)
            
            date_str = today.strftime("%Y-%m-%d")
            tomorrow_str = tomorrow.strftime("%Y-%m-%d")
            
            trade_count_before = self.trades_counter.get_trade_count()
            if self.verbose:
                print(f"\n[INFO] Trades before this run: {trade_count_before}")
            
            if self.verbose:
                print("")
                print("=" * 80)
                print(f"Fetching events and markets for {date_str} and {tomorrow_str} (local time)")
                print("=" * 80)
            
            # Step 1: Fetch Polymarket events
            polymarket_events = self._fetch_polymarket_events(today, tomorrow, self.verbose)
            if not polymarket_events:
                if parsed_test_date:
                    print("\n[TEST MODE] No Polymarket events found for test date. Exiting.")
                    return 0
                print("\n[WARNING] No Polymarket events found. Waiting 5 minutes before retrying...")
                time.sleep(5 * 60)
                continue
            
            # Step 2: Fetch market slugs
            market_slugs_by_event = self._fetch_market_slugs(polymarket_events, self.verbose)
            
            # Step 3: Fetch Pinnacle games
            pinnacle_games = self._fetch_pinnacle_games(today, tomorrow, self.verbose)
            if not pinnacle_games:
                if parsed_test_date:
                    print("\n[TEST MODE] No Pinnacle games found for test date. Exiting.")
                    return 0
                print("\n[WARNING] No Pinnacle games found. Waiting 5 minutes before retrying...")
                time.sleep(5 * 60)
                continue
            
            # Step 4: Match events
            matched_events = self._match_events(polymarket_events, pinnacle_games, self.verbose)
            if not matched_events:
                if parsed_test_date:
                    print("\n[TEST MODE] No matched games found for test date. Exiting.")
                    return 0
                print("\n[WARNING] No matched games found. Waiting 5 minutes before retrying...")
                time.sleep(5 * 60)
                continue
            
            if self.verbose:
                print("")
                print("=" * 80)
                print("MATCHED EVENTS (will be processed):")
                print("=" * 80)
                for i, match_data in enumerate(matched_events, 1):
                    event_slug, away_team, home_team, pinnacle_game = match_data
                    matchup_id = pinnacle_game.matchup_id
                    league = pinnacle_game.league
                    print(f"  {i}. Polymarket: [{event_slug}]")
                    print(f"      Pinnacle:  [MatchupID: {matchup_id}, League: {league}] {away_team} @ {home_team}")
                print("=" * 80)
            
            # Step 5: Main processing loop
            if self.verbose:
                print("")
                print("=" * 80)
                print("STARTING MAIN PROCESSING LOOP")
                print("Processing matched games for value bets...")
                print("=" * 80)
            
            iteration = 0
            inner_loop_start_time = time.time()
            iteration_count = 0
            
            while True:
                iteration += 1
                iteration_count += 1
                elapsed = time.time() - inner_loop_start_time
                remaining = INNER_LOOP_DURATION_SECONDS - elapsed
                
                # In test mode, exit after first iteration
                if parsed_test_date and iteration_count > 1:
                    if self.verbose:
                        print(f"\n[TEST MODE] Completed one iteration. Exiting.")
                    break
                
                # Check if 30 minutes have elapsed
                if elapsed >= INNER_LOOP_DURATION_SECONDS:
                    if self.verbose:
                        print(f"\n[INNER LOOP COMPLETE] 30 minutes elapsed. Breaking to refetch events...")
                    break
                
                # Check bankroll
                if not self._check_bankroll():
                    return 1
                
                bankroll = self.trade_executor.get_usdc_balance()
                if self.verbose:
                    print("\n" + "="*80)
                    print(f"ITERATION #{iteration}")
                    print(f"Bankroll: ${bankroll:.2f}" if bankroll else "Bankroll: Unknown")
                    print(f"Elapsed time in inner loop: {elapsed/60:.1f} minutes")
                    print(f"Remaining time in inner loop: {remaining/60:.1f} minutes")
                    print(f"Processing {len(matched_events)} matched games...")
                    print("="*80)
                
                for i, [event_slug, away_team, home_team, pinnacle_game] in enumerate(matched_events, 1):
                    # Check if 30 minutes have elapsed during game processing
                    if (time.time() - inner_loop_start_time) >= INNER_LOOP_DURATION_SECONDS:
                        if self.verbose:
                            print(f"\n[INNER LOOP COMPLETE] 30 minutes elapsed during game processing. Breaking to refetch events...")
                        break
                    
                    if self.verbose:
                        print(f"\n[Game {i}/{len(matched_events)}] {away_team} @ {home_team}")
                    
                    self._process_game(
                        event_slug=event_slug,
                        away_team=away_team,
                        home_team=home_team,
                        pinnacle_game=pinnacle_game,
                        market_slugs_by_event=market_slugs_by_event,
                        markets_to_run=markets_to_run,
                        today=today,
                        verbose=self.verbose,
                    )
                
                # Check if we should break from inner loop
                if (time.time() - inner_loop_start_time) >= INNER_LOOP_DURATION_SECONDS:
                    if self.verbose:
                        print(f"\n[INNER LOOP COMPLETE] 30 minutes elapsed. Breaking to refetch events...")
                    break
                
                # In test mode, exit after processing all games once
                if parsed_test_date:
                    if self.verbose:
                        print(f"\n[TEST MODE] Completed processing all games. Exiting.")
                    break
                
                if self.verbose:
                    print(f"\n[ITERATION COMPLETE] Continuing inner loop...")
                    remaining_time = (INNER_LOOP_DURATION_SECONDS - (time.time() - inner_loop_start_time)) / 60
                    print(f"  Remaining time in inner loop: {remaining_time:.1f} minutes")
            
            # After inner loop breaks, exit in test mode, otherwise continue to outer loop
            if parsed_test_date:
                print(f"\n[TEST MODE] Test complete. Exiting.")
                return 0
        
        return 0


def create_arg_parser(description: str, supports_test_date: bool = False) -> argparse.ArgumentParser:
    """Create argument parser with common arguments."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--moneyline", action="store_true", help="Run moneyline markets")
    parser.add_argument("--spreads", action="store_true", help="Run spread markets")
    parser.add_argument("--totals", action="store_true", help="Run totals markets")
    
    if supports_test_date:
        parser.add_argument("--test-date", type=str, help="Test on a specific date (YYYY-MM-DD format). If not provided, uses today and tomorrow.")
    
    return parser


def parse_markets_args(args: argparse.Namespace, default_markets: Dict[str, bool]) -> Dict[str, bool]:
    """Parse market arguments and return markets_to_run dict."""
    # Check if any market flags were provided
    has_moneyline_flag = hasattr(args, 'moneyline') and args.moneyline
    has_spreads_flag = hasattr(args, 'spreads') and args.spreads
    has_totals_flag = hasattr(args, 'totals') and args.totals
    any_flags_provided = has_moneyline_flag or has_spreads_flag or has_totals_flag
    
    if not any_flags_provided:
        # If no flags provided, use defaults
        return default_markets.copy()
    
    # If flags provided, only enable the ones specified
    markets_to_run = default_markets.copy()
    markets_to_run['moneyline'] = has_moneyline_flag
    markets_to_run['spreads'] = has_spreads_flag
    markets_to_run['totals'] = has_totals_flag
    
    return markets_to_run
