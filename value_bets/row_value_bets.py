#!/usr/bin/env python3
"""
Value betting bot for international basketball leagues (ROW = Rest of World).

Targets international leagues only (not NBA or NCAA):
- Euroleague, Eurocup, FIBA
- bkkbl, bkarg, bkfr1, bkcba, bknbl, bkseriea, bkligend, bkcl

Runs in testing mode by default - prints value bets instead of executing trades.
"""

import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from polymarket_sports_betting_bot.polymarket_sports_betting_bot_interface import (
    PolymarketSportsBettingBotInterface,
)
from pinnacle_scraper.pinnacle_odds_service import PinnacleBasketballOddsService
from value_bet_helpers import (
    fetch_polymarket_events_for_date,
    match_games_and_fetch_markets,
)


# International basketball league prefixes (ROW leagues)
INTERNATIONAL_LEAGUE_PREFIXES = [
    "euroleague",
    "eurocup",
    "fib",
    "bkkbl",
    "bkarg",
    "bkfr1",
    "bkcba",
    "bknbl",
    "bkseriea",
    "bkligend",
    "bkcl",
]


def sort_matched_events_by_league(matched_events: List[Tuple]) -> List[Tuple]:
    """Sort matched events by league priority (alphabetical for international leagues)."""
    def _sort_key(match_data):
        event_slug, _, _, pinnacle_game = match_data if len(match_data) == 4 else (match_data[0], None, None, None)
        if not pinnacle_game:
            return (99, event_slug)  # Put unmatched games last
        
        league = pinnacle_game.league or ""
        slug_lower = event_slug.lower()
        
        # For international leagues, sort alphabetically by league, then by slug
        return (0, league.lower(), slug_lower)
    
    return sorted(matched_events, key=_sort_key)


def print_value_bets_in_testing_mode(
    bot: PolymarketSportsBettingBotInterface,
    event_slug: str,
    away_team: str,
    home_team: str,
    date_str: str,
    markets: dict,
) -> int:
    """
    Process a game and print value bets found (testing mode).
    Returns the number of value bets found.
    """
    value_bets_found = 0
    
    try:
        # Run the bot (it will find value bets and execute trades if enable_trading=True)
        # In testing mode, we catch the trades and just print them
        result = bot.run_moneyline(["prog", away_team, home_team, date_str], sport_key="PINNACLE")
        
        # The bot interface doesn't directly return value bets, so we need to intercept
        # For now, we'll rely on the bot's internal printing which happens in testing mode
        # TODO: Refactor bot to return value bets as data structure for easier testing
        
    except RuntimeError as e:
        print(f"  Skipping: {e}")
        return 0
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        return 0
    
    return value_bets_found


def main() -> int:
    """Main entry point for ROW value betting bot."""
    # Flags
    run_tomorrow_once = "--tomorrow" in sys.argv[1:]
    enable_trading = "--trade" in sys.argv[1:]  # Only trade if explicitly requested
    testing_mode = not enable_trading  # Default to testing mode
    
    if testing_mode:
        print("=" * 80)
        print("TESTING MODE: Will print value bets but NOT execute trades")
        print("Add --trade flag to enable actual trading")
        print("=" * 80)
    else:
        print("=" * 80)
        print("TRADING MODE: Will execute trades on value bets")
        print("=" * 80)
    
    # Scheduling
    RUN_INTERVAL_SECONDS = 0
    MAX_RUNTIME_SECONDS = 6 * 60 * 60  # 6 hours
    
    # Initialize bot: verbose=True in testing mode, verbose=False in trading mode
    bot = PolymarketSportsBettingBotInterface(enable_trading=enable_trading, verbose=testing_mode)
    pinnacle = PinnacleBasketballOddsService(timeout_ms=45000)
    
    start_time = time.time()
    any_events_seen_overall = False
    
    while (time.time() - start_time) < MAX_RUNTIME_SECONDS:
        now = datetime.now().astimezone()
        today = now.date()
        if run_tomorrow_once:
            today = today + timedelta(days=1)
        date_str = today.strftime("%Y-%m-%d")
        
        print("\n" + "=" * 80)
        print(f"Fetching international basketball events for {date_str} (local time)")
        print("=" * 80)
        
        # Step 1: Fetch all Polymarket events (international leagues only)
        print("\nFetching Polymarket events (international leagues only)...")
        polymarket_events = fetch_polymarket_events_for_date(
            today,
            include_nba=False,
            include_ncaa=False,
            international_prefixes=INTERNATIONAL_LEAGUE_PREFIXES,
        )
        print(f"Found {len(polymarket_events)} Polymarket events")
        
        if not polymarket_events:
            print("No Polymarket events found. Exiting.")
            if run_tomorrow_once:
                return 0
            time.sleep(60)  # Wait before retry
            continue
        
        any_events_seen_overall = True
        
        # Step 2: Fetch all Pinnacle games
        print("\nFetching Pinnacle games for matching...")
        try:
            pinnacle_games = pinnacle.list_games_for_date(today, game_status="all")
        except Exception as e:
            print(f"ERROR: Failed to fetch Pinnacle games: {e}")
            pinnacle_games = []
        
        print(f"Found {len(pinnacle_games)} Pinnacle games")
        
        if not pinnacle_games:
            print("No Pinnacle games found. Exiting.")
            if run_tomorrow_once:
                return 0
            time.sleep(60)
            continue
        
        # Step 3: Match games that exist in both and fetch market slugs for matched events
        print("\nMatching games and fetching market slugs...")
        matched_events, events_cache, market_slugs_by_event = match_games_and_fetch_markets(
            polymarket_events, pinnacle_games, verbose=testing_mode
        )
        print(f"Found {len(matched_events)} matched games (in both Polymarket and Pinnacle)")
        
        if not matched_events:
            print("No matched games found. Exiting.")
            if run_tomorrow_once:
                return 0
            time.sleep(60)
            continue
        
        # Sort matched events
        matched_events = sort_matched_events_by_league(matched_events)
        
        # Print matched events
        print("\nMatched Games:")
        for i, match_data in enumerate(matched_events, 1):
            if len(match_data) == 4:
                event_slug, away_team, home_team, pinnacle_game = match_data
            else:
                event_slug, away_team, home_team = match_data[:3]
            print(f"{i}. {away_team} @ {home_team}")
        
        # Step 5: Main processing loop
        iteration = 0
        
        while (time.time() - start_time) < MAX_RUNTIME_SECONDS:
            iteration += 1
            
            for i, match_data in enumerate(matched_events, 1):
                if len(match_data) == 4:
                    event_slug, away_team, home_team, pinnacle_game = match_data
                else:
                    event_slug, away_team, home_team = match_data[:3]
                    pinnacle_game = None
                
                # Print current game
                print(f"\nGame {i}: {away_team} vs {home_team}")
                
                # Skip games that have already started
                if pinnacle_game and pinnacle_game.start_time_utc:
                    now_utc = datetime.now(timezone.utc)
                    if pinnacle_game.start_time_utc < now_utc:
                        continue  # Skip silently
                
                # Get markets for this event
                markets = market_slugs_by_event.get(event_slug, {})
                if not markets:
                    continue
                
                # Process this game using the bot interface
                # Pass cached event data and market slugs to avoid refetching
                try:
                    cached_event = events_cache.get(event_slug)
                    cached_markets = market_slugs_by_event.get(event_slug, {})
                    bot.run_moneyline(
                        ["prog", away_team, home_team, date_str], 
                        sport_key="PINNACLE",
                        cached_event_slug=event_slug,
                        cached_event_data=cached_event,
                        cached_market_slugs=cached_markets,
                    )
                    # In testing mode, bot will print "WOULD BUY" for value bets
                    # In trading mode, bot will print "Bet $X on Y for Z with edge: W" when trades are made
                except RuntimeError as e:
                    continue
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    continue
                
                # Small delay between games
                time.sleep(1)
            
            # Exit conditions
            if run_tomorrow_once:
                print("\nFinished single tomorrow scan. Exiting.")
                return 0
            
            # Sleep before next iteration
            remaining_runtime = MAX_RUNTIME_SECONDS - (time.time() - start_time)
            if remaining_runtime <= 0:
                break
            
            sleep_for = min(RUN_INTERVAL_SECONDS, remaining_runtime)
            if sleep_for > 0:
                sleep_minutes = int(round(sleep_for / 60))
                print(f"\nSleeping {sleep_minutes} minutes until next iteration...")
                time.sleep(sleep_for)
            else:
                # No sleep, continue immediately
                break
        
        # If we exit the while loop, break out of outer loop too
        break
    
    print(f"\nReached max runtime ({MAX_RUNTIME_SECONDS/3600:.0f} hours). Exiting.")
    return 0 if any_events_seen_overall else 2


if __name__ == "__main__":
    raise SystemExit(main())
