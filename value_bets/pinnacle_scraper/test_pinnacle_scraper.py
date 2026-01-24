#!/usr/bin/env python3
"""
Manual testing interface for Pinnacle scraper.

Usage:
    # Scrape a single game
    python3 test_pinnacle_scraper.py --url "https://www.pinnacle.com/en/basketball/nba/..."

    # Scrape all games from a matchups page
    python3 test_pinnacle_scraper.py --url "https://www.pinnacle.com/en/basketball/matchups/"

    # Scrape all games from a matchups page with limit
    python3 test_pinnacle_scraper.py --url "https://www.pinnacle.com/en/basketball/matchups/" --limit 5
"""

import sys
import os
import argparse
import json
from typing import List, Dict, Any
from urllib.parse import urlparse

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pinnacle_scraper.pinnacle_odds_scraper import (
    _scrape_arcadia_only,
    _extract_game_links_from_matchups_page,
    _looks_like_matchups_page,
)


def _print_game_odds(data: Dict[str, Any], df) -> None:
    """Print odds for a single game in a readable format."""
    if not data.get("ok"):
        print(f"\n{'='*80}")
        print(f"FAILED to scrape game")
        print(f"{'='*80}")
        print(f"URL: {data.get('url', 'N/A')}")
        print(f"Error: {data.get('error', 'Unknown error')}")
        if data.get("fallback"):
            print(f"Fallback method: {data.get('fallback')}")
        return
    
    away = data.get("away_team", "Unknown")
    home = data.get("home_team", "Unknown")
    
    print(f"\n{'='*80}")
    print(f"{away} @ {home}")
    print(f"{'='*80}")
    print(f"URL: {data.get('url', 'N/A')}")
    if data.get("sources"):
        print(f"Sources: {len(data.get('sources', []))} API endpoint(s)")
    
    if df.empty:
        print("\n(No odds available)")
        return
    
    # Group by market type for better display
    market_types = df["market_type"].unique() if "market_type" in df.columns else []
    
    for market_type in sorted(market_types):
        mt_df = df[df["market_type"] == market_type]
        print(f"\n--- {market_type.upper()} ---")
        
        if market_type == "moneyline":
            for _, row in mt_df.iterrows():
                selection = row.get("selection", "N/A")
                odds = row.get("odds", "N/A")
                period = row.get("period_label", "Game")
                print(f"  {period}: {selection} - {odds}")
        
        elif market_type == "spread":
            # Group by line
            spreads_by_line = {}
            for _, row in mt_df.iterrows():
                line = row.get("line", "N/A")
                selection = row.get("selection", "N/A")
                odds = row.get("odds", "N/A")
                period = row.get("period_label", "Game")
                key = (period, line)
                if key not in spreads_by_line:
                    spreads_by_line[key] = {}
                spreads_by_line[key][selection] = odds
            
            for (period, line), selections in sorted(spreads_by_line.items()):
                away_odds = selections.get(away, "N/A")
                home_odds = selections.get(home, "N/A")
                print(f"  {period}: {away} {line:+.1f} ({away_odds}) | {home} {line:+.1f} ({home_odds})")
        
        elif market_type == "totals":
            # Group by line
            totals_by_line = {}
            for _, row in mt_df.iterrows():
                line = row.get("line", "N/A")
                selection = row.get("selection", "N/A")
                odds = row.get("odds", "N/A")
                period = row.get("period_label", "Game")
                key = (period, line)
                if key not in totals_by_line:
                    totals_by_line[key] = {}
                totals_by_line[key][selection] = odds
            
            for (period, line), selections in sorted(totals_by_line.items()):
                over_odds = selections.get("Over", "N/A")
                under_odds = selections.get("Under", "N/A")
                print(f"  {period}: O {line} ({over_odds}) | U {line} ({under_odds})")
        
        elif market_type == "player_prop":
            for _, row in mt_df.iterrows():
                selection = row.get("selection", "N/A")
                line = row.get("line", "N/A")
                odds = row.get("odds", "N/A")
                period = row.get("period_label", "Game")
                print(f"  {period}: {selection} {line} ({odds})")
        
        else:
            # Generic display
            for _, row in mt_df.iterrows():
                selection = row.get("selection", "N/A")
                line = row.get("line", "")
                odds = row.get("odds", "N/A")
                period = row.get("period_label", "Game")
                line_str = f" {line}" if line else ""
                print(f"  {period}: {selection}{line_str} ({odds})")


def scrape_single_game(url: str, timeout_ms: int = 45000) -> None:
    """Scrape and display odds for a single game."""
    print(f"Scraping single game: {url}")
    data, df = _scrape_arcadia_only(url, timeout_ms=timeout_ms)
    _print_game_odds(data, df)


def scrape_matchups_page(url: str, limit: int = 0, timeout_ms: int = 45000) -> None:
    """Scrape and display odds for all games from a matchups page."""
    print(f"Scraping matchups page: {url}")
    print(f"Extracting game links...")
    
    links = _extract_game_links_from_matchups_page(
        matchups_url=url,
        timeout_ms=timeout_ms,
        headless=True,
    )
    
    if not links:
        print("No game links found on the matchups page.")
        return
    
    if limit > 0:
        links = links[:limit]
        print(f"Limited to first {limit} games")
    
    print(f"Found {len(links)} game(s) to scrape\n")
    
    for i, game_url in enumerate(links, 1):
        print(f"\n[{i}/{len(links)}] Processing: {game_url}")
        data, df = _scrape_arcadia_only(game_url, timeout_ms=timeout_ms)
        _print_game_odds(data, df)
        
        # Small delay between games to avoid hammering
        if i < len(links):
            import time
            time.sleep(0.5)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Manual testing interface for Pinnacle scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape a single game
  python3 test_pinnacle_scraper.py --url "https://www.pinnacle.com/en/basketball/nba/..."

  # Scrape all games from matchups page
  python3 test_pinnacle_scraper.py --url "https://www.pinnacle.com/en/basketball/matchups/"

  # Scrape first 5 games from matchups page
  python3 test_pinnacle_scraper.py --url "https://www.pinnacle.com/en/basketball/matchups/" --limit 5
        """,
    )
    parser.add_argument(
        "--url",
        required=True,
        help="Pinnacle URL (single game page or matchups page)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of games to scrape from matchups page (0 = no limit)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=45000,
        help="Request timeout in milliseconds (default: 45000)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON instead of formatted text",
    )
    
    args = parser.parse_args()
    
    url = args.url.strip()
    if not url:
        print("Error: --url is required")
        return 2
    
    # Determine if it's a matchups page or single game
    is_matchups = _looks_like_matchups_page(url)
    
    try:
        if is_matchups:
            scrape_matchups_page(url, limit=args.limit, timeout_ms=args.timeout_ms)
        else:
            scrape_single_game(url, timeout_ms=args.timeout_ms)
        
        print("\n" + "="*80)
        print("Done!")
        return 0
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        return 1
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
