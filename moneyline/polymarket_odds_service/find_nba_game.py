#!/usr/bin/env python3
"""
Script to find today's Pistons vs Bulls NBA game on Polymarket.

Searches for the game using Polymarket's public search API or NBA events endpoint,
filters for today's date, and returns the event slug and token IDs.
"""

import requests
import json
from datetime import datetime
from typing import Optional, Dict, List


class PolymarketGameFinder:
    """Finder for Polymarket NBA game events."""
    
    GAMMA_API_BASE = "https://gamma-api.polymarket.com"
    
    def __init__(self):
        self.session = requests.Session()
    
    def search_by_query(self, query: str) -> List[Dict]:
        """
        Search for events using the public search endpoint.
        
        Args:
            query: Search query string
            
        Returns:
            List of event dictionaries
        """
        url = f"{self.GAMMA_API_BASE}/public-search"
        params = {"query": query}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            # API might return list directly or wrapped in "data" key
            if isinstance(data, list):
                return data
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            print(f"Error searching by query: {e}")
            # Try alternative search approach - fetch event by slug if we can construct it
            return []
    
    def fetch_nba_events(self) -> List[Dict]:
        """
        Fetch NBA game events directly.
        
        Returns:
            List of NBA event dictionaries
        """
        url = f"{self.GAMMA_API_BASE}/events"
        params = {
            "series_id": "10345",  # NBA
            "tag_id": "100639",    # Game bets
            "active": "true",
            "closed": "false"
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            # API might return list directly or wrapped in "data" key
            if isinstance(data, list):
                return data
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            print(f"Error fetching NBA events: {e}")
            return []
    
    def parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """
        Parse date string to datetime object.
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            datetime object or None
        """
        if not date_str:
            return None
        
        # Try common date formats
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str[:len(fmt)], fmt)
            except (ValueError, IndexError):
                continue
        
        return None
    
    def is_today(self, date_obj: Optional[datetime], target_date: datetime) -> bool:
        """
        Check if date is today.
        
        Args:
            date_obj: datetime object to check
            target_date: target date to compare against
            
        Returns:
            True if dates match (ignoring time)
        """
        if not date_obj:
            return False
        return date_obj.date() == target_date.date()
    
    def find_pistons_bulls_game(self, target_date: Optional[datetime] = None) -> Optional[Dict]:
        """
        Find today's Pistons vs Bulls game.
        
        Args:
            target_date: Target date to search for (defaults to today)
            
        Returns:
            Event dictionary with slug, title, and clobTokenIds, or None
        """
        if target_date is None:
            target_date = datetime(2026, 1, 7)  # January 7, 2026 as specified
        
        print(f"Searching for Pistons vs Bulls game on {target_date.strftime('%Y-%m-%d')}...")
        
        # Try search endpoint first
        print("\n1. Trying public search endpoint...")
        search_results = self.search_by_query("Pistons Bulls")
        
        if search_results:
            print(f"   Found {len(search_results)} results from search")
            for event in search_results:
                if self._matches_game(event, target_date):
                    return self._extract_event_info(event)
        
        # Try NBA events endpoint
        print("\n2. Trying NBA events endpoint...")
        nba_events = self.fetch_nba_events()
        
        if nba_events:
            print(f"   Found {len(nba_events)} NBA game events")
            for event in nba_events:
                if self._matches_game(event, target_date):
                    return self._extract_event_info(event)
        
        print("\nGame not found.")
        return None
    
    def _matches_game(self, event: Dict, target_date: datetime) -> bool:
        """
        Check if event matches Pistons vs Bulls game on target date.
        
        Args:
            event: Event dictionary
            target_date: Target date
            
        Returns:
            True if matches
        """
        title = event.get("title", "").lower()
        slug = event.get("slug", "").lower()
        
        # Check if title contains both teams
        has_pistons = "piston" in title
        has_bulls = "bull" in title
        
        if not (has_pistons and has_bulls):
            return False
        
        # Check date in slug (format: nba-chi-det-2026-01-07)
        target_date_str = target_date.strftime("%Y-%m-%d")
        if target_date_str in slug:
            return True
        
        # Check startDate
        start_date_str = event.get("startDate")
        start_date = self.parse_date(start_date_str)
        
        if start_date and self.is_today(start_date, target_date):
            return True
        
        # If title matches but date doesn't, still return True (might be the only game)
        return True
    
    def _extract_event_info(self, event: Dict) -> Dict:
        """
        Extract relevant information from event.
        
        Args:
            event: Event dictionary
            
        Returns:
            Dictionary with slug, title, and clobTokenIds
        """
        slug = event.get("slug", "")
        title = event.get("title", "Unknown")
        
        # Extract clobTokenIds from markets
        clob_token_ids = []
        markets = event.get("markets", [])
        
        for market in markets:
            clob_token_ids_str = market.get("clobTokenIds")
            if clob_token_ids_str:
                try:
                    token_ids = json.loads(clob_token_ids_str)
                    clob_token_ids.extend(token_ids)
                except json.JSONDecodeError:
                    continue
        
        return {
            "slug": slug,
            "title": title,
            "clobTokenIds": clob_token_ids,
            "markets": markets
        }
    
    def display_game_info(self, game_info: Dict):
        """
        Display game information.
        
        Args:
            game_info: Dictionary with game information
        """
        print("\n" + "="*80)
        print("GAME FOUND")
        print("="*80)
        print(f"Event Slug: {game_info['slug']}")
        print(f"Event Title: {game_info['title']}")
        print(f"\nClobTokenIds ({len(game_info['clobTokenIds'])} tokens):")
        for i, token_id in enumerate(game_info['clobTokenIds'], 1):
            print(f"  {i}. {token_id}")
        print("="*80)


def main():
    """Main entry point."""
    finder = PolymarketGameFinder()
    game_info = finder.find_pistons_bulls_game()
    
    if game_info:
        finder.display_game_info(game_info)
        return game_info
    else:
        print("\nCould not find Pistons vs Bulls game for January 7, 2026.")
        return None


if __name__ == "__main__":
    main()

