#!/usr/bin/env python3
"""
Interface to fetch Polymarket market odds (stateful, by matchup only).

Public methods:
- get_market_odds()
- get_moneyline_odds()

Event slug-based fetching is intentionally not exposed to avoid accidental misuse.
All error handling is contained inside this interface; errors return empty lists / None.
"""

from datetime import date, datetime
from typing import List, Optional

from .polymarket_market_analyzer import PolymarketMarketAnalyzer, MarketOdds
from .find_nba_game import PolymarketGameFinder


class PolymarketOddsInterface:
    """Interface to fetch Polymarket market odds."""
    
    def __init__(self, team_a: str, team_b: str, play_date: Optional[date] = None):
        """
        Initialize the interface for a specific NBA matchup.

        During initialization, we find and store the Polymarket event slug.

        Args:
            team_a: First team name
            team_b: Second team name
            play_date: Optional local date (defaults to today)
        """
        self.analyzer = PolymarketMarketAnalyzer()
        self.game_finder = PolymarketGameFinder()
        self.team_a = team_a
        self.team_b = team_b
        self.play_date = play_date

        # Resolved event slug for this matchup (None if not found)
        try:
            self.event_slug: Optional[str] = self._find_event_slug(team_a, team_b, play_date)
        except Exception as e:
            print(f"Warning: Could not find Polymarket event: {e}")
            self.event_slug = None
    
    def _find_event_slug(self, team_a: str, team_b: str, play_date: Optional[date] = None) -> Optional[str]:
        """
        Find Polymarket event slug for an NBA game.
        
        Args:
            team_a: First team name (e.g., "Chicago Bulls")
            team_b: Second team name (e.g., "Detroit Pistons")
            play_date: Date of the game (defaults to today if None)
            
        Returns:
            event_slug (e.g., "nba-chi-det-2026-01-07") or None if not found
        """
        if play_date is None:
            play_date = datetime.now().date()
        
        # Fetch NBA events
        nba_events = self.game_finder.fetch_nba_events()
        if not nba_events:
            return None
        
        # Normalize team names for matching
        team_a_lower = team_a.lower()
        team_b_lower = team_b.lower()
        team_a_words = set(team_a_lower.split())
        team_b_words = set(team_b_lower.split())
        
        # Find matching event
        target_date_str = play_date.strftime("%Y-%m-%d")
        
        for event in nba_events:
            title = event.get("title", "").lower()
            event_slug = event.get("slug", "").lower()
            
            # Check if title contains both teams (check for last word which is usually the team name)
            # e.g., "Bulls" or "Pistons"
            team_a_last_word = team_a_lower.split()[-1]
            team_b_last_word = team_b_lower.split()[-1]
            
            has_team_a = team_a_last_word in title or any(word in title for word in team_a_words if len(word) > 3)
            has_team_b = team_b_last_word in title or any(word in title for word in team_b_words if len(word) > 3)
            
            if has_team_a and has_team_b:
                # Check date in event_slug (format: nba-chi-det-2026-01-07)
                if target_date_str in event_slug:
                    return event.get("slug")
        
        return None

    def get_market_odds(self) -> List[MarketOdds]:
        """
        Get market odds for a Polymarket event.

        Returns empty list if event not found or error occurs.
        """
        return self.analyzer.analyze_markets(self.event_slug)

    def get_moneyline_odds(self) -> List[MarketOdds]:
        """
        Get moneyline odds for a Polymarket event.

        Fetches the moneyline market by calling the analyzer with:
        - event_slug=self.event_slug
        - market_slug=self.event_slug

        For NBA games, the main moneyline market_slug is the same as the event_slug,
        so passing both lets the analyzer select the correct token_ids without scanning
        every market.
        """
        return self.analyzer.analyze_markets(event_slug=self.event_slug, market_slug=self.event_slug)

