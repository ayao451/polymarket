#!/usr/bin/env python3
"""
Shared runner for value betting across all sports.

Handles the common business logic, accepting sport-specific configuration.
"""

import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple, Dict, Set, Callable
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


@dataclass
class ValueBetsRunnerConfig:
    """Configuration for sport-specific value betting parameters."""
    
    sport_name: str
    whitelisted_prefixes: List[str]
    pinnacle_service_class: type
    markets_to_run: Dict[str, bool]
    sort_key_function: Optional[Callable[[Tuple], Tuple]] = None
    min_bankroll: float = 5.0
    inner_loop_duration_seconds: int = 30 * 60
    test_date: Optional[str] = None


def _get_trade_count(trades_csv_path: str) -> int:
    """
    Return the current number of trades in the CSV file.
    """
    import os
    import csv
    
    if not os.path.exists(trades_csv_path):
        return 0
    try:
        with open(trades_csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return len(list(reader))
    except Exception:
        return 0


def run_value_bets(config: ValueBetsRunnerConfig, verbose: bool = False) -> int:
    """
    Main runner function that handles all the common business logic.
    
    Args:
        config: Sport-specific configuration
        verbose: Enable verbose output
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    import os
    
    # Determine trades CSV path (in sport-specific folder)
    # The runner is in value_bets/, and we need to go to value_bets/{sport_name}/trades.csv
    runner_dir = os.path.dirname(os.path.abspath(__file__))
    sport_dir = os.path.join(runner_dir, config.sport_name)
    trades_csv_path = os.path.join(sport_dir, "trades.csv")
    
    print("\n" + "="*80)
    print(f"POLYMARKET SPORTS BETTING BOT - STARTING UP ({config.sport_name.upper()})")
    print("="*80)
    print(f"Running forever - refetching events every {config.inner_loop_duration_seconds // 60} minutes")
    print(f"Verbose mode: {'ON' if verbose else 'OFF'}")
    print(f"Markets to run:")
    for market_name, enabled in config.markets_to_run.items():
        print(f"  - {market_name.capitalize()}: {'YES' if enabled else 'NO'}")
    
    if verbose:
        print("\n[INIT] Creating bot interface...")
    bot = PolymarketSportsBettingBotInterface(sport=config.sport_name, verbose=verbose)
    if verbose:
        print("[INIT] Bot interface created")
    
    if verbose:
        print("\n[INIT] Creating Pinnacle odds service...")
    pinnacle = config.pinnacle_service_class(timeout_ms=45000)
    if verbose:
        print("[INIT] Pinnacle service created (timeout: 45s)")
    
    if verbose:
        print(f"\n[INIT] Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    trade_executor = TradeExecutorService()
    
    # Parse test date if provided
    test_date_obj = None
    if config.test_date:
        try:
            test_date_obj = datetime.strptime(config.test_date, "%Y-%m-%d").date()
            print(f"\n[TEST MODE] Testing on date: {test_date_obj}")
        except ValueError:
            print(f"\n[ERROR] Invalid date format. Use YYYY-MM-DD (e.g., 2024-01-13)")
            return 1
    
    # Track which market slugs have been traded; persist across refetch cycles to prevent duplicate bets
    traded_markets: Set[str] = set()
    
    # Outer loop: runs forever, refetches events every 30 minutes
    while True:
        now = datetime.now().astimezone()
        if test_date_obj:
            today = test_date_obj
            tomorrow = test_date_obj + timedelta(days=1)
        else:
            today = now.date()
            tomorrow = today + timedelta(days=1)
        date_str = today.strftime("%Y-%m-%d")
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        
        # Track trades before this iteration
        trade_count_before = _get_trade_count(trades_csv_path)
        if verbose:
            print(f"\n[INFO] Trades before this run: {trade_count_before}")
        
        if verbose:
            print("")
            print("=" * 80)
            print(f"Fetching events and markets for {date_str} and {tomorrow_str} (local time)")
            print("=" * 80)
        
        # Step 1: Fetch all Polymarket events for today and tomorrow
        if verbose:
            print("\n[STEP 1] Fetching Polymarket events for today and tomorrow...")
        polymarket_events_today = fetch_polymarket_events_for_date(
            today,
            whitelisted_prefixes=config.whitelisted_prefixes,
            verbose=verbose,
        )
        polymarket_events_tomorrow = fetch_polymarket_events_for_date(
            tomorrow,
            whitelisted_prefixes=config.whitelisted_prefixes,
            verbose=verbose,
        )
        polymarket_events = polymarket_events_today + polymarket_events_tomorrow
        if verbose:
            print(f"  -> Found {len(polymarket_events_today)} Polymarket events for today")
            print(f"  -> Found {len(polymarket_events_tomorrow)} Polymarket events for tomorrow")
            print(f"  -> Total: {len(polymarket_events)} Polymarket events")
        
        if not polymarket_events:
            if test_date_obj:
                print("\n[TEST MODE] No Polymarket events found for test date. Exiting.")
                return 0
            print("\n[WARNING] No Polymarket events found. Waiting 5 minutes before retrying...")
            time.sleep(5 * 60)
            continue
        
        # Step 2: Fetch all event data and market slugs upfront (to avoid refetching later)
        if verbose:
            print("\n[STEP 2] Fetching market slugs for each event...")
        event_slugs = [slug for slug, _, _ in polymarket_events]
        market_slugs_by_event = fetch_market_slugs_by_event(event_slugs, verbose=verbose)
        if verbose:
            print(f"  -> Fetched market slugs for {len(market_slugs_by_event)} events")
        
        # Step 3: Fetch all Pinnacle games for today and tomorrow
        if verbose:
            print("\n[STEP 3] Fetching Pinnacle games for matching...")
        pinnacle_games = []
        pinnacle_games_today = []
        pinnacle_games_tomorrow = []
        try:
            pinnacle_games_today = pinnacle.list_games_for_date(today, game_status="all")
            pinnacle_games_tomorrow = pinnacle.list_games_for_date(tomorrow, game_status="all")
            pinnacle_games = pinnacle_games_today + pinnacle_games_tomorrow
        except Exception as e:
            print(f"  -> [ERROR] Failed to fetch Pinnacle games: {e}")
            pinnacle_games = []
        
        if verbose:
            print(f"  -> Found {len(pinnacle_games_today)} Pinnacle games for today")
            print(f"  -> Found {len(pinnacle_games_tomorrow)} Pinnacle games for tomorrow")
            print(f"  -> Total: {len(pinnacle_games)} Pinnacle games")
        
        if not pinnacle_games:
            if test_date_obj:
                print("\n[TEST MODE] No Pinnacle games found for test date. Exiting.")
                return 0
            print("\n[WARNING] No Pinnacle games found. Waiting 5 minutes before retrying...")
            time.sleep(5 * 60)
            continue
        
        # Step 4: Match games that exist in both
        if verbose:
            print("\n[STEP 4] Matching games between Polymarket and Pinnacle...")
        matched_events = match_games(polymarket_events, pinnacle_games)
        if verbose:
            print(f"  -> Found {len(matched_events)} matched games (in both Polymarket and Pinnacle)")
        
        if not matched_events:
            if test_date_obj:
                print("\n[TEST MODE] No matched games found for test date. Exiting.")
                return 0
            print("\n[WARNING] No matched games found. Waiting 5 minutes before retrying...")
            time.sleep(5 * 60)
            continue
        
        # Sort matched events if sort key function provided
        if config.sort_key_function:
            matched_events = sorted(matched_events, key=config.sort_key_function)
        
        print("\n" + "=" * 80)
        print("MATCHED EVENTS SUMMARY")
        print("=" * 80)
        for i, match_data in enumerate(matched_events, 1):
            event_slug, away_team, home_team, pinnacle_game = match_data
            matchup_id = pinnacle_game.matchup_id
            league = pinnacle_game.league
            print(f"  {i}. {away_team} @ {home_team}")
            print(f"     Polymarket: {event_slug}")
            print(f"     Pinnacle: MatchupID={matchup_id}, League={league}")
        print("=" * 80 + "\n")
        
        # Log all matched events with both Polymarket and Pinnacle representations
        if verbose:
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
        
        # Step 5: Main processing loop - iterate through matched games and markets
        if verbose:
            print("")
            print("=" * 80)
            print("STARTING MAIN PROCESSING LOOP")
            print("Processing matched games for value bets...")
            print("=" * 80)
        
        iteration = 0
        
        # Inner loop: runs for configured duration, then breaks to refetch events
        inner_loop_start_time = time.time()
        iteration_count = 0
        while True:
            iteration += 1
            iteration_count += 1
            elapsed = time.time() - inner_loop_start_time
            remaining = config.inner_loop_duration_seconds - elapsed
            
            # In test mode, exit after first iteration
            if test_date_obj and iteration_count > 1:
                if verbose:
                    print(f"\n[TEST MODE] Completed one iteration. Exiting.")
                break
            
            # Check if configured duration has elapsed
            if elapsed >= config.inner_loop_duration_seconds:
                if verbose:
                    print(f"\n[INNER LOOP COMPLETE] {config.inner_loop_duration_seconds // 60} minutes elapsed. Breaking to refetch events...")
                break
            
            # Check bankroll before each iteration
            bankroll = trade_executor.get_usdc_balance()
            if bankroll is not None and bankroll < config.min_bankroll:
                print(f"\n{'!'*60}")
                print(f"!!! BANKROLL TOO LOW - STOPPING !!!")
                print(f"{'!'*60}")
                print(f"  Current bankroll: ${bankroll:.2f}")
                print(f"  Minimum required: ${config.min_bankroll:.2f}")
                print(f"  Exiting to protect remaining funds.")
                return 1
            
            if verbose:
                print("\n" + "="*80)
                print(f"ITERATION #{iteration}")
                print(f"Bankroll: ${bankroll:.2f}" if bankroll else "Bankroll: Unknown")
                print(f"Elapsed time in inner loop: {elapsed/60:.1f} minutes")
                print(f"Remaining time in inner loop: {remaining/60:.1f} minutes")
                print(f"Processing {len(matched_events)} matched games...")
                print("="*80)
            
            for i, [event_slug, away_team, home_team, pinnacle_game] in enumerate(matched_events, 1):
                # Check if configured duration has elapsed during game processing
                if (time.time() - inner_loop_start_time) >= config.inner_loop_duration_seconds:
                    if verbose:
                        print(f"\n[INNER LOOP COMPLETE] {config.inner_loop_duration_seconds // 60} minutes elapsed during game processing. Breaking to refetch events...")
                    break
                
                if verbose:
                    print(f"\n--- Game {i}/{len(matched_events)} ---")
                    print(f"[{i}/{len(matched_events)}] {away_team} @ {home_team}")
                    print("-" * 60)
                
                # Skip games that have already started or are more than 12 hours away
                if pinnacle_game and pinnacle_game.start_time_utc:
                    now_utc = datetime.now(timezone.utc)
                    time_until_start = pinnacle_game.start_time_utc - now_utc
                    
                    if pinnacle_game.start_time_utc < now_utc:
                        if verbose:
                            print(f"[SKIP] Game has already started")
                            print(f"  Start time: {pinnacle_game.start_time_utc.isoformat()}")
                            print(f"  Current time: {now_utc.isoformat()}")
                        continue
                    elif time_until_start.total_seconds() > 12 * 60 * 60:  # More than 12 hours
                        if verbose:
                            print(f"[SKIP] Game is more than 12 hours away")
                            print(f"  Start time: {pinnacle_game.start_time_utc.isoformat()}")
                            print(f"  Current time: {now_utc.isoformat()}")
                            print(f"  Time until start: {time_until_start.total_seconds()/3600:.1f} hours")
                        continue
                    else:
                        if verbose:
                            print(f"[OK] Game starts in {time_until_start.total_seconds()/60:.0f} minutes")
                
                # Get markets for this event (from our pre-fetched hashmap)
                markets = market_slugs_by_event.get(event_slug, {})
                if not markets:
                    if verbose:
                        print(f"[SKIP] No markets found for this event")
                    continue
                
                # Build market summary string
                market_summary_parts = []
                for market_name in ['moneyline', 'spreads', 'totals', 'totals_games', 'totals_sets']:
                    count = len(markets.get(market_name, []))
                    if count > 0:
                        market_summary_parts.append(f"{market_name}={count}")
                
                if verbose:
                    print(f"[OK] Found markets: {', '.join(market_summary_parts)}")
                
                # Process this game using the bot interface
                if verbose:
                    print(f"\n[PROCESSING] Starting value bet analysis for: {away_team} @ {home_team}")
                    print(f"  Event slug: {event_slug}")
                    print(f"  Markets available: {list(markets.keys())}")
                try:
                    bot.run_all_markets(
                        away_team=away_team,
                        home_team=home_team,
                        play_date=today,
                        event_slug=event_slug,
                        market_slugs_by_event=market_slugs_by_event,
                        traded_markets=traded_markets,
                        markets_to_run=config.markets_to_run,
                    )
                    if verbose:
                        print(f"[DONE] Finished processing: {away_team} @ {home_team}")
                
                except Exception as e:
                    print(f"[ERROR] Exception while processing {away_team} @ {home_team}: {e}")
                    traceback.print_exc()
                    continue
            
            # Check if we should break from inner loop
            if (time.time() - inner_loop_start_time) >= config.inner_loop_duration_seconds:
                if verbose:
                    print(f"\n[INNER LOOP COMPLETE] {config.inner_loop_duration_seconds // 60} minutes elapsed. Breaking to refetch events...")
                break
            
            # In test mode, exit after processing all games once
            if test_date_obj:
                if verbose:
                    print(f"\n[TEST MODE] Completed processing all games. Exiting.")
                break
            
            if verbose:
                print(f"\n[ITERATION COMPLETE] Continuing inner loop...")
                print(f"  Remaining time in inner loop: {(config.inner_loop_duration_seconds - (time.time() - inner_loop_start_time))/60:.1f} minutes")
        
        # After inner loop breaks, exit in test mode, otherwise continue to outer loop
        if test_date_obj:
            print(f"\n[TEST MODE] Test complete. Exiting.")
            return 0
    
    return 0
