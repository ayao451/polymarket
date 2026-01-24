#!/usr/bin/env python3
"""
Main entry point for value betting bot.

Refactored to fetch all events and markets upfront, then iterate through matched games/markets.
"""

import sys
import time
import argparse
import traceback
from datetime import datetime, timedelta, timezone
import os
import csv
from typing import Optional, List, Tuple, Dict, Set

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from polymarket_sports_betting_bot.polymarket_sports_betting_bot_interface import (
    PolymarketSportsBettingBotInterface,
)
from pinnacle_scraper.pinnacle_odds_service import PinnacleBasketballOddsService
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
    parser = argparse.ArgumentParser(description="Polymarket Sports Betting Bot")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--moneyline", action="store_true", help="Run moneyline markets")
    parser.add_argument("--spreads", action="store_true", help="Run spread markets")
    parser.add_argument("--totals", action="store_true", help="Run totals markets")
    args = parser.parse_args()
    
    VERBOSE = args.verbose

    # Determine which markets to run
    # If no flags specified, run all markets
    run_moneyline = args.moneyline
    run_spreads = args.spreads
    run_totals = args.totals
    
    # If no flags provided, run all markets
    if not (run_moneyline or run_spreads or run_totals):
        run_moneyline = True
        run_spreads = True
        run_totals = True
    
    markets_to_run = {
        'moneyline': run_moneyline,
        'spreads': run_spreads,
        'totals': run_totals,
    }
    INNER_LOOP_DURATION_SECONDS = 30 * 60  # 30 minutes
    
    print("\n" + "="*80)
    print("POLYMARKET SPORTS BETTING BOT - STARTING UP")
    print("="*80)
    print(f"Running forever - refetching events every 30 minutes")
    print(f"Verbose mode: {'ON' if VERBOSE else 'OFF'}")
    print(f"Markets to run:")
    print(f"  - Moneyline: {'YES' if markets_to_run['moneyline'] else 'NO'}")
    print(f"  - Spreads: {'YES' if markets_to_run['spreads'] else 'NO'}")
    print(f"  - Totals: {'YES' if markets_to_run['totals'] else 'NO'}")
    
    if VERBOSE:
        print("\n[INIT] Creating bot interface...")
    bot = PolymarketSportsBettingBotInterface(verbose=VERBOSE)
    if VERBOSE:
        print("[INIT] Bot interface created")
    
    if VERBOSE:
        print("\n[INIT] Creating Pinnacle odds service...")
    pinnacle = PinnacleBasketballOddsService(timeout_ms=45000)
    if VERBOSE:
        print("[INIT] Pinnacle service created (timeout: 45s)")
    
    if VERBOSE:
        print(f"\n[INIT] Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    trade_executor = TradeExecutorService()

    # Track which market slugs have been traded; persist across refetch cycles to prevent duplicate bets
    traded_markets: Set[str] = set()

    # Outer loop: runs forever, refetches events every 30 minutes
    while True:
        now = datetime.now().astimezone()
        today = now.date()
        tomorrow = today + timedelta(days=1)
        date_str = today.strftime("%Y-%m-%d")
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        
        # Track trades before this iteration
        trade_count_before = _get_trade_count()
        if VERBOSE:
            print(f"\n[INFO] Trades before this run: {trade_count_before}")
        
        if VERBOSE:
            print("")
            print("=" * 80)
            print(f"Fetching events and markets for {date_str} and {tomorrow_str} (local time)")
            print("=" * 80)
        
        # Step 1: Fetch all Polymarket events for today and tomorrow
        if VERBOSE:
            print("\n[STEP 1] Fetching Polymarket events for today and tomorrow...")
        polymarket_events_today = fetch_polymarket_events_for_date(
            today,
            whitelisted_prefixes=["nba", "cbb", "bkcl" , "bkligend", "bkseriea", "bknbl", "bkcba", "bkfr1", "bkarg", "bkkbl", "euroleague"],
            verbose=VERBOSE,
        )
        polymarket_events_tomorrow = fetch_polymarket_events_for_date(
            tomorrow,
            whitelisted_prefixes=["nba", "cbb", "bkcl" , "bkligend", "bkseriea", "bknbl", "bkcba", "bkfr1", "bkarg", "bkkbl", "euroleague"],
            verbose=VERBOSE,
        )
        polymarket_events = polymarket_events_today + polymarket_events_tomorrow
        if VERBOSE:
            print(f"  -> Found {len(polymarket_events_today)} Polymarket events for today")
            print(f"  -> Found {len(polymarket_events_tomorrow)} Polymarket events for tomorrow")
            print(f"  -> Total: {len(polymarket_events)} Polymarket events (NBA/NCAA only)")
        
        if not polymarket_events:
            print("\n[WARNING] No Polymarket events found. Waiting 5 minutes before retrying...")
            time.sleep(5 * 60)
            continue
        
        # Step 2: Fetch all event data and market slugs upfront (to avoid refetching later)
        if VERBOSE:
            print("\n[STEP 2] Fetching market slugs for each event...")
        event_slugs = [slug for slug, _, _ in polymarket_events]
        market_slugs_by_event = fetch_market_slugs_by_event(event_slugs, verbose=VERBOSE)
        if VERBOSE:
            print(f"  -> Fetched market slugs for {len(market_slugs_by_event)} events")
        
        # Step 3: Fetch all Pinnacle games for today and tomorrow (just for matching purposes - we'll refetch odds per game later)
        if VERBOSE:
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
        
        if VERBOSE:
            print(f"  -> Found {len(pinnacle_games_today)} Pinnacle games for today")
            print(f"  -> Found {len(pinnacle_games_tomorrow)} Pinnacle games for tomorrow")
            print(f"  -> Total: {len(pinnacle_games)} Pinnacle games")
        
        if not pinnacle_games:
            print("\n[WARNING] No Pinnacle games found. Waiting 5 minutes before retrying...")
            time.sleep(5 * 60)
            continue
        
        # Step 4: Match games that exist in both
        if VERBOSE:
            print("\n[STEP 4] Matching games between Polymarket and Pinnacle...")
        matched_events = _match_games(polymarket_events, pinnacle_games)
        if VERBOSE:
            print(f"  -> Found {len(matched_events)} matched games (in both Polymarket and Pinnacle)")
        
        if not matched_events:
            print("\n[WARNING] No matched games found. Waiting 5 minutes before retrying...")
            time.sleep(5 * 60)
            continue
        
        # Note: Pinnacle odds will be refetched for each game individually to get the most up-to-date data
        
        # Sort matched events: NBA prefixed first, everything else second
        def _sort_key(event_tuple):
            event_slug = event_tuple[0]  # First element is always event_slug
            if event_slug.startswith("nba"):
                return (0, event_slug)  # NBA first
            else:
                return (1, event_slug)  # Everything else second
        
        matched_events = sorted(matched_events, key=_sort_key)

        print("matched_events:")
        print(matched_events)
        print("=" * 80)
        print("=" * 80)
        
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

        # Inner loop: runs for 30 minutes, then breaks to refetch events
        inner_loop_start_time = time.time()
        while True:
            iteration += 1
            elapsed = time.time() - inner_loop_start_time
            remaining = INNER_LOOP_DURATION_SECONDS - elapsed
            
            # Check if 30 minutes have elapsed
            if elapsed >= INNER_LOOP_DURATION_SECONDS:
                if VERBOSE:
                    print(f"\n[INNER LOOP COMPLETE] 30 minutes elapsed. Breaking to refetch events...")
                break
            
            # Check bankroll before each iteration
            bankroll = trade_executor.get_usdc_balance()
            if bankroll is not None and bankroll < MIN_BANKROLL:
                print(f"\n{'!'*60}")
                print(f"!!! BANKROLL TOO LOW - STOPPING !!!")
                print(f"{'!'*60}")
                print(f"  Current bankroll: ${bankroll:.2f}")
                print(f"  Minimum required: ${MIN_BANKROLL:.2f}")
                print(f"  Exiting to protect remaining funds.")
                return 1
            
            if VERBOSE:
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
                    if VERBOSE:
                        print(f"\n[INNER LOOP COMPLETE] 30 minutes elapsed during game processing. Breaking to refetch events...")
                    break
                
                if VERBOSE:
                    print(f"\n--- Game {i}/{len(matched_events)} ---")
                    print(f"[{i}/{len(matched_events)}] {away_team} @ {home_team}")
                    print("-" * 60)
                
                # Skip games that have already started or are more than 12 hours away
                if pinnacle_game and pinnacle_game.start_time_utc:
                    now_utc = datetime.now(timezone.utc)
                    time_until_start = pinnacle_game.start_time_utc - now_utc
                    
                    if pinnacle_game.start_time_utc < now_utc:
                        if VERBOSE:
                            print(f"[SKIP] Game has already started")
                            print(f"  Start time: {pinnacle_game.start_time_utc.isoformat()}")
                            print(f"  Current time: {now_utc.isoformat()}")
                        continue
                    elif time_until_start.total_seconds() > 12 * 60 * 60:  # More than 12 hours
                        if VERBOSE:
                            print(f"[SKIP] Game is more than 12 hours away")
                            print(f"  Start time: {pinnacle_game.start_time_utc.isoformat()}")
                            print(f"  Current time: {now_utc.isoformat()}")
                            print(f"  Time until start: {time_until_start.total_seconds()/3600:.1f} hours")
                        continue
                    else:
                        if VERBOSE:
                            print(f"[OK] Game starts in {time_until_start.total_seconds()/60:.0f} minutes")
                
                # Get markets for this event (from our pre-fetched hashmap)
                markets = market_slugs_by_event.get(event_slug, {})
                if not markets:
                    if VERBOSE:
                        print(f"[SKIP] No markets found for this event")
                    continue
                
                if VERBOSE:
                    print(f"[OK] Found markets: moneyline={len(markets.get('moneyline', []))}, "
                          f"spreads={len(markets.get('spreads', []))}, "
                          f"totals={len(markets.get('totals', []))}")
                
                # Process this game using the bot interface
                if VERBOSE:
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
                        markets_to_run=markets_to_run,
                    )
                    if VERBOSE:
                        print(f"[DONE] Finished processing: {away_team} @ {home_team}")

                except Exception as e:
                    print(f"[ERROR] Exception while processing {away_team} @ {home_team}: {e}")
                    traceback.print_exc()
                    continue
            
            # Check if we should break from inner loop (30 minutes elapsed)
            if (time.time() - inner_loop_start_time) >= INNER_LOOP_DURATION_SECONDS:
                if VERBOSE:
                    print(f"\n[INNER LOOP COMPLETE] 30 minutes elapsed. Breaking to refetch events...")
                break
            
            if VERBOSE:
                print(f"\n[ITERATION COMPLETE] Continuing inner loop...")
                print(f"  Remaining time in inner loop: {(INNER_LOOP_DURATION_SECONDS - (time.time() - inner_loop_start_time))/60:.1f} minutes")
        
        # After inner loop breaks, continue to outer loop to refetch events
    
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
