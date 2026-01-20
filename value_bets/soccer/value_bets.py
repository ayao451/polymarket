#!/usr/bin/env python3
"""
Main entry point for value betting bot (Soccer).

Refactored to fetch all events and markets upfront, then iterate through matched games/markets.
Soccer has 3-way moneyline (away, draw, home).
"""

import sys
import time
import argparse
import traceback
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os
import csv
from typing import Optional, List, Tuple, Dict, Set

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from polymarket_sports_betting_bot.polymarket_sports_betting_bot_interface import (
    PolymarketSportsBettingBotInterface,
)
from pinnacle_scraper.pinnacle_odds_service import PinnacleSoccerOddsService
from trade_executor.trade_executor_service import TradeExecutorService
from value_bet_helpers import (
    normalize_team_name,
    teams_match,
    fetch_polymarket_events_for_date,
    fetch_market_slugs_by_event,
    match_games,
)

# Minimum bankroll to continue trading
MIN_BANKROLL = 5.0


def _successful_trades_path() -> str:
    # Keep in sync with TradeExecutorService._trades_csv_path()
    # Store trades.csv in the sport-specific folder
    value_bets_root = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(value_bets_root, "trades.csv")


def _get_trade_count() -> int:
    """
    Return the current number of trades in the CSV file.
    """
    path = _successful_trades_path()
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return len(list(reader))
    except Exception:
        return 0


# Use shared helper functions
_normalize_team_name = normalize_team_name
_teams_match = teams_match
_match_games = match_games


def main() -> int:
    parser = argparse.ArgumentParser(description="Polymarket Sports Betting Bot (Soccer)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--moneyline", action="store_true", help="Run moneyline markets")
    args = parser.parse_args()
    
    # Always run in verbose mode for soccer
    VERBOSE = True
    
    # Determine which markets to run
    # For soccer, we focus on moneyline (3-way) for now
    run_moneyline = args.moneyline if args.moneyline else True
    
    markets_to_run = {
        'moneyline': run_moneyline,
        'spreads': False,  # Not supported for soccer yet
        'totals': False,  # Not supported for soccer yet
        'player_props': False,  # Not supported for soccer yet
    }
    MAX_RUNTIME_SECONDS = 24 * 60 * 60  # 24 hours
    
    print("\n" + "="*80)
    print("POLYMARKET SPORTS BETTING BOT - STARTING UP (SOCCER)")
    print("="*80)
    print(f"Max runtime: {MAX_RUNTIME_SECONDS/3600:.0f} hours")
    print(f"Verbose mode: ON (always enabled for soccer)")
    print(f"Markets to run:")
    print(f"  - Moneyline (3-way): {'YES' if markets_to_run['moneyline'] else 'NO'}")
    print(f"  - Spreads: {'YES' if markets_to_run['spreads'] else 'NO'}")
    print(f"  - Totals: {'YES' if markets_to_run['totals'] else 'NO'}")
    print(f"  - Player Props: {'YES' if markets_to_run['player_props'] else 'NO'}")
    
    if VERBOSE:
        print("\n[INIT] Creating bot interface...")
    bot = PolymarketSportsBettingBotInterface(sport="soccer", verbose=VERBOSE)
    if VERBOSE:
        print("[INIT] Bot interface created")
    
    if VERBOSE:
        print("\n[INIT] Creating Pinnacle odds service...")
    pinnacle = PinnacleSoccerOddsService(timeout_ms=45000)
    if VERBOSE:
        print("[INIT] Pinnacle service created (timeout: 45s)")
    
    start_time = time.time()
    if VERBOSE:
        print(f"\n[INIT] Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    now = datetime.now().astimezone()
    today = now.date()
    date_str = today.strftime("%Y-%m-%d")
    
    # Track trades before this iteration
    trade_count_before = _get_trade_count()
    if VERBOSE:
        print(f"\n[INFO] Trades before this run: {trade_count_before}")
    
    if VERBOSE:
        print("")
        print("=" * 80)
        print(f"Fetching events and markets for {date_str} (local time)")
        print("=" * 80)
    
    # Step 1 & 3: Fetch both Polymarket events and Pinnacle games, ensuring dates match
    # We need to find a date where both have games
    if VERBOSE:
        print("\n[STEP 1/3] Finding date with both Polymarket and Pinnacle EPL games...")
    
    target_date = today
    polymarket_events = []
    pinnacle_games = []
    
    # Try today and next 7 days to find a match
    for day_offset in range(0, 8):
        test_date = today + timedelta(days=day_offset)
        if VERBOSE:
            print(f"  -> Trying date: {test_date}")
        
        # Fetch Polymarket events for this date
        pm_events = fetch_polymarket_events_for_date(
            test_date,
            whitelisted_prefixes=["epl"],
            verbose=False,  # Less verbose for multiple attempts
        )
        if VERBOSE:
            print(f"    Polymarket: {len(pm_events)} events")
        
        # Fetch Pinnacle games for this date
        try:
            pin_games = pinnacle.list_games_for_date(test_date, game_status="all", league_filter="EPL")
            if VERBOSE:
                print(f"    Pinnacle: {len(pin_games)} EPL games")
        except Exception as e:
            if VERBOSE:
                print(f"    Pinnacle: ERROR - {e}")
            pin_games = []
        
        # If we have both, use this date
        if len(pm_events) > 0 and len(pin_games) > 0:
            target_date = test_date
            polymarket_events = pm_events
            pinnacle_games = pin_games
            if VERBOSE:
                print(f"  -> [SUCCESS] Found matching date: {test_date}")
                print(f"      Polymarket: {len(polymarket_events)} events")
                print(f"      Pinnacle: {len(pinnacle_games)} games")
            break
        elif len(pm_events) > 0 or len(pin_games) > 0:
            if VERBOSE:
                print(f"    -> Partial match (PM: {len(pm_events)}, PIN: {len(pin_games)})")
    
    if not polymarket_events:
        print("\n[ERROR] No Polymarket EPL events found in next 7 days. Exiting.")
        return 2
    
    if not pinnacle_games:
        print("\n[ERROR] No Pinnacle EPL games found in next 7 days. Exiting.")
        return 2
    
    # Update today to the target date
    today = target_date
    date_str = today.strftime("%Y-%m-%d")
    if VERBOSE:
        print(f"\n[INFO] Using date {date_str} for processing")
        print(f"  -> Polymarket events: {len(polymarket_events)}")
        print(f"  -> Pinnacle games: {len(pinnacle_games)}")
    
    # Step 2: Fetch all event data and market slugs upfront (to avoid refetching later)
    if VERBOSE:
        print("\n[STEP 2] Fetching market slugs for each event...")
    event_slugs = [slug for slug, _, _ in polymarket_events]
    market_slugs_by_event = fetch_market_slugs_by_event(event_slugs, verbose=VERBOSE)
    if VERBOSE:
        print(f"  -> Fetched market slugs for {len(market_slugs_by_event)} events")
    
    # Step 4: Match games that exist in both
    if VERBOSE:
        print("\n[STEP 4] Matching games between Polymarket and Pinnacle...")
        print(f"  -> Polymarket events to match: {len(polymarket_events)}")
        for pm_slug, pm_away, pm_home in polymarket_events:
            print(f"      Polymarket: {pm_away} @ {pm_home} (slug: {pm_slug})")
        print(f"  -> Pinnacle games available: {len(pinnacle_games)}")
        for pin_game in pinnacle_games[:5]:
            print(f"      Pinnacle: {pin_game.away_team} @ {pin_game.home_team} (league: {pin_game.league})")
    matched_events = _match_games(polymarket_events, pinnacle_games, verbose=VERBOSE)
    if VERBOSE:
        print(f"  -> Found {len(matched_events)} matched games (in both Polymarket and Pinnacle)")
    
    if not matched_events:
        print("\n[ERROR] No matched games found. Exiting.")
        return 2
    
    # Note: Pinnacle odds will be refetched for each game individually to get the most up-to-date data
    
    # Sort matched events: EPL prefixed first, everything else second
    def _sort_key(event_tuple):
        event_slug = event_tuple[0]  # First element is always event_slug
        if event_slug.startswith("epl"):
            return (0, event_slug)  # EPL first
        else:
            return (1, event_slug)  # Everything else second
    
    matched_events = sorted(matched_events, key=_sort_key)
    
    # Log all matched events with both Polymarket and Pinnacle representations
    if VERBOSE:
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
    if VERBOSE:
        print("")
        print("=" * 80)
        print("STARTING MAIN PROCESSING LOOP")
        print("Processing matched games for value bets...")
        print("=" * 80)
    
    iteration = 0
    trade_executor = TradeExecutorService()
    
    # Track which (event_slug, market_slug) combinations have been traded
    # to avoid trading the same market twice
    traded_markets: Set[Tuple[str, str]] = set()
    
    # Create a shared thread pool executor for async processing
    executor = ThreadPoolExecutor(max_workers=10)
    
    while (time.time() - start_time) < MAX_RUNTIME_SECONDS:
        iteration += 1
        elapsed = time.time() - start_time
        remaining = MAX_RUNTIME_SECONDS - elapsed
        
        # Check bankroll before each iteration
        bankroll = trade_executor.get_usdc_balance()
        if bankroll is not None and bankroll < MIN_BANKROLL:
            print(f"\n{'!'*60}")
            print(f"!!! BANKROLL TOO LOW - STOPPING !!!")
            print(f"{'!'*60}")
            print(f"  Current bankroll: ${bankroll:.2f}")
            print(f"  Minimum required: ${MIN_BANKROLL:.2f}")
            print(f"  Exiting to protect remaining funds.")
            
        
        if VERBOSE:
            print("\n" + "="*80)
            print(f"ITERATION #{iteration}")
            print(f"Bankroll: ${bankroll:.2f}" if bankroll else "Bankroll: Unknown")
            print(f"Elapsed time: {elapsed/60:.1f} minutes")
            print(f"Remaining time: {remaining/60:.1f} minutes")
            print(f"Processing {len(matched_events)} matched games in parallel...")
            print("="*80)
        
        async def process_game_async(
            event_slug: str,
            away_team: str,
            home_team: str,
            pinnacle_game,
            game_index: int,
            total_games: int,
        ) -> None:
            """Process a single game asynchronously."""
            if VERBOSE:
                print(f"\n--- Game {game_index}/{total_games} ---")
                print(f"[{game_index}/{total_games}] {away_team} @ {home_team}")
                print("-" * 60)
            
            # Skip games that have already started
            if pinnacle_game and pinnacle_game.start_time_utc:
                now_utc = datetime.now(timezone.utc)
                if pinnacle_game.start_time_utc < now_utc:
                    if VERBOSE:
                        print(f"[SKIP] Game has already started")
                        print(f"  Start time: {pinnacle_game.start_time_utc.isoformat()}")
                        print(f"  Current time: {now_utc.isoformat()}")
                    return
                else:
                    time_until_start = pinnacle_game.start_time_utc - now_utc
                    if VERBOSE:
                        print(f"[OK] Game starts in {time_until_start.total_seconds()/60:.0f} minutes")
            
            # Get markets for this event (from our pre-fetched hashmap)
            markets = market_slugs_by_event.get(event_slug, {})
            if not markets:
                if VERBOSE:
                    print(f"[SKIP] No markets found for this event")
                return
            
            if VERBOSE:
                print(f"[OK] Found markets: moneyline={len(markets.get('moneyline', []))}, "
                      f"spreads={len(markets.get('spreads', []))}, "
                      f"totals={len(markets.get('totals', []))}, "
                      f"player_props={len(markets.get('player_props', []))}")
            
            # Process this game using the bot interface
            if VERBOSE:
                print(f"\n[PROCESSING] Starting value bet analysis for: {away_team} @ {home_team}")
                print(f"  Event slug: {event_slug}")
                print(f"  Markets available: {list(markets.keys())}")
            
            # Run the synchronous bot.run_all_markets in a thread pool
            try:
                await asyncio.get_event_loop().run_in_executor(
                    executor,
                    lambda: bot.run_all_markets(
                        away_team=away_team,
                        home_team=home_team,
                        play_date=today,
                        event_slug=event_slug,
                        market_slugs_by_event=market_slugs_by_event,
                        traded_markets=traded_markets,
                        markets_to_run=markets_to_run,
                    )
                )
                if VERBOSE:
                    print(f"[DONE] Finished processing: {away_team} @ {home_team}")
            except Exception as e:
                print(f"[ERROR] Exception while processing {away_team} @ {home_team}: {e}")
                traceback.print_exc()
        
        # Process all games in parallel
        async def process_all_games() -> None:
            tasks = []
            for i, [event_slug, away_team, home_team, pinnacle_game] in enumerate(matched_events, 1):
                task = process_game_async(
                    event_slug=event_slug,
                    away_team=away_team,
                    home_team=home_team,
                    pinnacle_game=pinnacle_game,
                    game_index=i,
                    total_games=len(matched_events),
                )
                tasks.append(task)
            
            # Run all games in parallel
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Run the async processing
        asyncio.run(process_all_games())
        
        
        # Check if we should continue
        remaining_runtime = MAX_RUNTIME_SECONDS - (time.time() - start_time)
        if remaining_runtime <= 0:
            break
        
        if VERBOSE:
            print(f"\n[ITERATION COMPLETE] Sleeping before next iteration...")
            print(f"  Remaining runtime: {remaining_runtime/60:.1f} minutes")

    # Shutdown the executor
    executor.shutdown(wait=True)
    
    print(f"\n[EXIT] Reached max runtime ({MAX_RUNTIME_SECONDS/3600:.0f} hours). Exiting.")
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        raise SystemExit(exit_code)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Script interrupted by user. Exiting gracefully...")
        raise SystemExit(0)
    except Exception as e:
        print(f"\n{'!'*60}")
        print(f"!!! UNEXPECTED ERROR - LOGGING AND CONTINUING !!!")
        print(f"{'!'*60}")
        print(f"  Error: {e}")
        traceback.print_exc()
        print(f"\n  Continuing to allow logging...")
        # Don't exit - let the script finish naturally
        raise SystemExit(0)
