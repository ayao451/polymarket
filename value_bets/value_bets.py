#!/usr/bin/env python3
"""
Main entry point for value betting bot.

Refactored to fetch all events and markets upfront, then iterate through matched games/markets.
"""

import sys
import time
from datetime import datetime, timedelta, timezone
import os
import csv
from typing import Optional, List, Tuple, Dict, Set

from polymarket_sports_betting_bot.polymarket_sports_betting_bot_interface import (
    PolymarketSportsBettingBotInterface,
)
from pinnacle_scraper.pinnacle_odds_service import PinnacleBasketballOddsService
from value_bet_helpers import (
    normalize_team_name,
    teams_match,
    fetch_polymarket_events_for_date,
    fetch_all_events_and_markets,
    match_games,
)


def _successful_trades_path() -> str:
    # Keep in sync with TradeExecutorService._trades_csv_path()
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


def _print_trades_since(start_index: int, verbose: bool = False) -> None:
    """
    Print trades added since start_index (0-based row index in CSV) in a human-readable format.
    Always prints something, even if no trades were made.
    Only prints successful trades.
    
    Args:
        start_index: Index to start reading trades from
        verbose: If False, skip header lines
    """
    path = _successful_trades_path()
    
    if verbose:
        print("\n" + "=" * 80)
        print("Trades made during this run:")
        print("=" * 80)
    
    if not os.path.exists(path):
        print("  No trades made in this run.")
        return

    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"  No trades made in this run. (Error reading trade log: {e})")
        return

    if start_index >= len(rows):
        print("  No trades made in this run.")
        return

    new_rows = rows[start_index:]
    if not new_rows:
        print("  No trades made in this run.")
        return

    # Filter to only successful trades
    new_rows = [row for row in new_rows if (row.get("success") or "").lower() == "true"]
    if not new_rows:
        print("  No successful trades made in this run.")
        return

    for i, row in enumerate(new_rows, start=1):
        game = row.get("game") or ""
        team = row.get("team") or ""
        side = row.get("side") or ""
        price = row.get("price") or ""
        size = row.get("size") or ""
        amount = row.get("amount") or ""
        payout = row.get("payout") or ""
        profit = row.get("profit") or ""
        ev = row.get("expected_payout_per_$1") or ""
        ts = row.get("ts") or ""
        success = row.get("success") or ""
        status = row.get("status") or ""
        order_id = row.get("order_id") or ""
        
        # Format the trade in a human-readable way
        print(f"\n  Trade #{i}:")
        if game:
            print(f"    Game:     {game}")
        if team:
            side_str = f" ({side})" if side else ""
            print(f"    Team:     {team}{side_str}")
        if amount:
            print(f"    Amount:   ${amount}")
        if price:
            print(f"    Price:    ${price} per share")
        if payout:
            print(f"    Payout:   ${payout}")
        if profit:
            try:
                profit_val = float(profit)
                profit_sign = "+" if profit_val >= 0 else ""
                print(f"    Profit:   {profit_sign}${profit_val:.2f}")
            except (ValueError, TypeError):
                pass
        if ev:
            try:
                ev_val = float(ev)
                edge_pct = (ev_val - 1.0) * 100
                print(f"    Edge:     {edge_pct:+.2f}% (EV: ${ev_val:.4f} per $1)")
                
                # Calculate and print probabilities
                if price:
                    try:
                        price_val = float(price)
                        if price_val > 0:
                            # Pinnacle predicted probability: true_prob = expected_payout * price
                            # Formula: expected_payout_per_1 = true_prob / price, so true_prob = expected_payout_per_1 * price
                            pinnacle_prob = ev_val * price_val
                            
                            # Devig Polymarket probability
                            # For a two-outcome market, we need both sides to devig properly
                            # Since we only have one side stored, we approximate the other side as (1 - price)
                            # This assumes the market probabilities sum to approximately 1 (common in prediction markets)
                            polymarket_price_other_side = 1.0 - price_val
                            if polymarket_price_other_side > 0:
                                # Devig using proportional method: p1 = q1 / (q1 + q2)
                                total = price_val + polymarket_price_other_side
                                polymarket_prob = price_val / total
                            else:
                                # Fallback: if other side would be <= 0, just use raw price
                                polymarket_prob = price_val
                            
                            print(f"    Pinnacle Probability:   {pinnacle_prob:.1%}")
                            print(f"    Polymarket Probability: {polymarket_prob:.1%}")
                    except (ValueError, TypeError):
                        pass
            except (ValueError, TypeError):
                pass


# Use shared helper functions
_normalize_team_name = normalize_team_name
_teams_match = teams_match

# Use shared helper functions
_fetch_all_polymarket_events_for_date = lambda target_date, league_filter=None: fetch_polymarket_events_for_date(
    target_date,
    include_nba=True,
    include_ncaa=True,
    international_prefixes=[],  # Exclude all international leagues - only NBA/NCAA
    league_filter=league_filter,
)
_fetch_all_events_and_markets = fetch_all_events_and_markets
_match_games = match_games


def main() -> int:
    # Flags
    run_tomorrow_once = "--tomorrow" in sys.argv[1:]
    verbose = "--verbose" in sys.argv[1:] or "-v" in sys.argv[1:]
    
    # Scheduling
    RUN_INTERVAL_SECONDS = 0    
    MAX_RUNTIME_SECONDS = 6 * 60 * 60  # 6 hours
    
    # Backwards-compatible: if user supplies <away> <home> [YYYY-MM-DD], run once.
    positional = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(positional) >= 2:
        bot = PolymarketSportsBettingBotInterface(enable_trading=True, verbose=verbose)
        argv = [sys.argv[0], *positional]
        return bot.run_moneyline(argv)
    
    # Initialize bot with trading enabled
    bot = PolymarketSportsBettingBotInterface(enable_trading=True, verbose=verbose)
    pinnacle = PinnacleBasketballOddsService(timeout_ms=45000)
    
    start_time = time.time()
    any_events_seen_overall = False
    
    while (time.time() - start_time) < MAX_RUNTIME_SECONDS:
        now = datetime.now().astimezone()
        today = now.date()
        if run_tomorrow_once:
            today = today + timedelta(days=1)
        date_str = today.strftime("%Y-%m-%d")
        
        # Track trades before this iteration
        trade_count_before = _get_trade_count()
        print(f"\nTrades before this run: {trade_count_before}")
        
        print("\n" + "=" * 80)
        print(f"Fetching events and markets for {date_str} (local time)")
        print("=" * 80)
        
        # Step 1: Fetch all Polymarket events 
        if verbose:
            print("\nFetching Polymarket events...")
        polymarket_events = _fetch_all_polymarket_events_for_date(today)
        
        # Filter to only NBA/NCAA games (event slugs must contain "nba" or "ncaa" or "cbb")
        filtered_events = []
        for event_slug, away_team, home_team in polymarket_events:
            slug_lower = event_slug.lower()
            if "nba" in slug_lower or "ncaa" in slug_lower or "cbb" in slug_lower:
                filtered_events.append((event_slug, away_team, home_team))
        
        polymarket_events = filtered_events
        if verbose:
            print(f"Found {len(polymarket_events)} Polymarket events (NBA/NCAA only)")
        
        if not polymarket_events:
            print("No Polymarket events found. Exiting.")
            return 2
        
        # Step 2: Fetch all event data and market slugs upfront (to avoid refetching later)
        print("\nFetching event data and market slugs for each event...")
        event_slugs = [slug for slug, _, _ in polymarket_events]
        events_cache, market_slugs_by_event = _fetch_all_events_and_markets(event_slugs)
        print(f"Fetched event data and market slugs for {len(events_cache)} events")
        
        # Step 3: Fetch all Pinnacle games (just for matching purposes - we'll refetch odds per game later)
        print("\nFetching Pinnacle games for matching...")
        try:
            pinnacle_games = pinnacle.list_games_for_date(today, game_status="all")
        except Exception as e:
            print(f"ERROR: Failed to fetch Pinnacle games: {e}")
            pinnacle_games = []
        
        print(f"Found {len(pinnacle_games)} Pinnacle games")
        
        if not pinnacle_games:
            print("No Pinnacle games found. Exiting.")
            return 2
        
        # Step 4: Match games that exist in both
        print("\nMatching games...")
        matched_events = _match_games(polymarket_events, pinnacle_games)
        print(f"Found {len(matched_events)} matched games (in both Polymarket and Pinnacle)")
        
        if not matched_events:
            print("No matched games found. Exiting.")
            return 2
        
        # Note: Pinnacle odds will be refetched for each game individually to get the most up-to-date data
        
        # Sort matched events: NBA first, then CBB/NCAA, then the rest
        def _sort_key(event_tuple):
            event_slug = event_tuple[0]  # First element is always event_slug
            slug_lower = event_slug.lower()
            if "nba-" in slug_lower or slug_lower.startswith("nba-"):
                return (0, event_slug)  # NBA first
            elif "ncaa" in slug_lower or "cbb-" in slug_lower or "-ncaa-" in slug_lower or "-cbb-" in slug_lower:
                return (1, event_slug)  # CBB/NCAA second
            else:
                return (2, event_slug)  # Everything else
        
        matched_events = sorted(matched_events, key=_sort_key)
        
        # Print all matched events with both Polymarket and Pinnacle representations
        print("\n" + "=" * 80)
        print("Matched Events (will be processed):")
        print("=" * 80)
        for i, match_data in enumerate(matched_events, 1):
            if len(match_data) == 4:
                event_slug, away_team, home_team, pinnacle_game = match_data
                matchup_id = pinnacle_game.matchup_id if hasattr(pinnacle_game, 'matchup_id') else 'N/A'
                league = pinnacle_game.league if hasattr(pinnacle_game, 'league') else 'N/A'
                print(f"  {i}. Polymarket: [{event_slug}]")
                print(f"      Pinnacle:  [MatchupID: {matchup_id}, League: {league}] {away_team} @ {home_team}")
            else:
                # Fallback for old format
                event_slug, away_team, home_team = match_data[:3]
                print(f"  {i}. [{event_slug}] {away_team} @ {home_team}")
        print("=" * 80)
        
        any_events_seen_overall = True
        
        # Step 6: Main processing loop - iterate through matched games and markets
        print("\n" + "=" * 80)
        print("Processing matched games for value bets...")
        print("=" * 80)
        
        iteration = 0
        
        while (time.time() - start_time) < MAX_RUNTIME_SECONDS:
            iteration += 1
            print(f"\n{'='*80}")
            print(f"Iteration #{iteration} - Processing {len(matched_events)} matched games...")
            print(f"{'='*80}")
            
            for i, match_data in enumerate(matched_events, 1):
                if len(match_data) == 4:
                    event_slug, away_team, home_team, pinnacle_game = match_data
                else:
                    # Fallback for old format
                    event_slug, away_team, home_team = match_data[:3]
                    pinnacle_game = None
                
                if verbose:
                    print(f"\n[{i}/{len(matched_events)}] {away_team} @ {home_team}")
                    print("-" * 80)
                
                # Skip games that have already started
                if pinnacle_game and pinnacle_game.start_time_utc:
                    now_utc = datetime.now(timezone.utc)
                    if pinnacle_game.start_time_utc < now_utc:
                        if verbose:
                            print(f"  Skipping: Game has already started (start time: {pinnacle_game.start_time_utc.isoformat()})")
                        continue
                
                # Get markets for this event (from our pre-fetched hashmap)
                markets = market_slugs_by_event.get(event_slug, {})
                if not markets:
                    if verbose:
                        print("  Skipping: No markets found")
                    continue
                
                # Get cached event data (avoid refetching)
                cached_event = events_cache.get(event_slug)
                
                # Process this game using the bot interface
                # Note: The bot will still fetch event data internally, but at least we have
                # the market slugs cached upfront. Future optimization: pass cached_event to bot.
                try:
                    bot.run_moneyline(["prog", away_team, home_team, date_str], sport_key="PINNACLE")
                except RuntimeError as e:
                    if verbose:
                        print(f"  Skipping: {e}")
                    continue
                except Exception as e:
                    if verbose:
                        print(f"  Error: {e}")
                    continue
                
                # Small delay between games
                time.sleep(1)
            
            # Print trades made during this iteration
            trade_count_after = _get_trade_count()
            new_trades = trade_count_after - trade_count_before
            print(f"\nTrades after this iteration: {trade_count_after} (new: {new_trades})")
            _print_trades_since(trade_count_before, verbose=verbose)
            trade_count_before = trade_count_after  # Update for next iteration
            
            # Exit conditions
            if run_tomorrow_once:
                print("\nFinished single tomorrow scan. Exiting.")
                return 0
            
            # Sleep before next iteration
            remaining_runtime = MAX_RUNTIME_SECONDS - (time.time() - start_time)
            if remaining_runtime <= 0:
                break
            
            sleep_for = min(RUN_INTERVAL_SECONDS, remaining_runtime)
            sleep_minutes = int(round(sleep_for / 60))
            print(f"\nSleeping {sleep_minutes} minutes until next iteration...")
            time.sleep(sleep_for)
        
        # If we exit the while loop, break out of outer loop too
        break
    
    print(f"\nReached max runtime ({MAX_RUNTIME_SECONDS/3600:.0f} hours). Exiting.")
    return 0 if any_events_seen_overall else 2


if __name__ == "__main__":
    raise SystemExit(main())