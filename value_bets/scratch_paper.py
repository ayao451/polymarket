#!/usr/bin/env python3
"""
Scratch paper script to test fetching games with specific slug prefixes.
"""

import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path so imports work
script_dir = os.path.dirname(os.path.abspath(__file__))
value_bets_dir = os.path.dirname(script_dir)
if value_bets_dir not in sys.path:
    sys.path.insert(0, value_bets_dir)

from polymarket_odds_service.find_game import PolymarketGameFinder


def fetch_games_by_slug_prefixes(target_date, slug_prefixes):
    """
    Fetch all Polymarket events for a given date where slug starts with any of the prefixes.
    
    Args:
        target_date: Date to fetch events for
        slug_prefixes: List of slug prefixes to match (e.g., ["euroleague", "bkcba"])
    
    Returns:
        List of tuples: (event_slug, away_team, home_team, title, start_time)
    """
    finder = PolymarketGameFinder()
    events_list = []
    seen_slugs = set()
    
    # Normalize prefixes to lowercase
    prefixes_lower = [p.lower() for p in slug_prefixes]
    
    # Fetch events pages
    for active_only in (True, False):
        for page in range(100):  # Search up to 100 pages
            events = finder.fetch_events_page(
                limit=100,
                offset=page * 100,
                active=active_only,
                closed=False,
                order="startTime",
                ascending=False,
            )
            if not events:
                break
            
            for event in events:
                # Check if event is on target date
                start = finder._parse_start_time(event)
                if start is None:
                    continue
                
                event_date = start.astimezone().date()
                if event_date != target_date:
                    continue
                
                slug_raw = event.get("slug") or ""
                if not slug_raw:
                    continue
                slug = str(slug_raw).strip()
                slug_lower = slug.lower()
                
                # Deduplicate
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                
                # Check if slug starts with any of the prefixes
                matches_prefix = any(slug_lower.startswith(prefix) for prefix in prefixes_lower)
                if not matches_prefix:
                    continue
                
                # Extract title
                title_raw = event.get("title") or ""
                title = str(title_raw).strip() if title_raw else ""
                
                # Parse team names from title
                clean_title = title
                for prefix in ["NBA:", "NCAA:", "NCAAB:", "NBAGL:", "Basketball:"]:
                    if clean_title.startswith(prefix):
                        clean_title = clean_title[len(prefix):].strip()
                        break
                
                parts = clean_title.replace(" vs ", " @ ").replace(" vs. ", " @ ").split(" @ ", 1)
                if len(parts) != 2:
                    away = ""
                    home = ""
                else:
                    away = parts[0].strip().rstrip(".")
                    home = parts[1].strip().rstrip(".")
                
                events_list.append((slug, away, home, title, start))
    
    return events_list


def main():
    # Slug prefixes to match
    slug_prefixes = [
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
    
    now = datetime.now().astimezone()
    today = now.date()
    tomorrow = today + timedelta(days=1)
    
    print("=" * 80)
    print(f"Fetching games with slug prefixes: {', '.join(slug_prefixes)}")
    print("=" * 80)
    
    # Fetch for today
    print(f"\n📅 TODAY ({today.isoformat()}):")
    print("-" * 80)
    today_games = fetch_games_by_slug_prefixes(today, slug_prefixes)
    print(f"Found {len(today_games)} games")
    
    if today_games:
        for i, (slug, away, home, title, start_time) in enumerate(today_games, 1):
            start_str = start_time.strftime("%Y-%m-%d %H:%M:%S") if start_time else "N/A"
            print(f"  {i}. [{slug}]")
            print(f"      {title}")
            if away and home:
                print(f"      {away} @ {home}")
            print(f"      Start: {start_str}")
            print()
    else:
        print("  (No games found)")
    
    # Fetch for tomorrow
    print(f"\n📅 TOMORROW ({tomorrow.isoformat()}):")
    print("-" * 80)
    tomorrow_games = fetch_games_by_slug_prefixes(tomorrow, slug_prefixes)
    print(f"Found {len(tomorrow_games)} games")
    
    if tomorrow_games:
        for i, (slug, away, home, title, start_time) in enumerate(tomorrow_games, 1):
            start_str = start_time.strftime("%Y-%m-%d %H:%M:%S") if start_time else "N/A"
            print(f"  {i}. [{slug}]")
            print(f"      {title}")
            if away and home:
                print(f"      {away} @ {home}")
            print(f"      Start: {start_str}")
            print()
    else:
        print("  (No games found)")
    
    print("=" * 80)
    print(f"Total: {len(today_games)} today, {len(tomorrow_games)} tomorrow")


if __name__ == "__main__":
    main()
