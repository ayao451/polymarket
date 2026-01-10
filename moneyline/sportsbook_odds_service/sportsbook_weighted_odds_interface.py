#!/usr/bin/env python3
"""
Interface to fetch weighted odds for NBA games from The Odds API.

Given two teams and a date, fetches odds from The Odds API,
applies weighted averaging across multiple sportsbooks,
and returns formatted results.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

from .fetch_game_odds import (
    NBA_SPORT_KEY,
    MONEYLINE_MARKET_KEY,
    find_event_for_teams_today,
    extract_moneyline_odds_all_books,
)
from .the_odds_api_connector import theOddsAPIConnector
from .weighted_average import (
    BookLine,
    BookOutcome,
    SPORTSBOOK_WEIGHTS,
    build_weights_for_present_books,
    weighted_average_cost_to_win_1,
)


@dataclass(frozen=True)
class MoneylineOdds:
    away_team: str
    home_team: str
    away_cost_to_win_1: float
    home_cost_to_win_1: float

    def to_string(self, decimals: int = 6) -> str:
        decimals = max(0, int(decimals))
        fmt = f".{decimals}f"
        return (
            f"{self.away_team} @ {self.home_team} | "
            f"{self.away_team}: {format(self.away_cost_to_win_1, fmt)} to win $1 | "
            f"{self.home_team}: {format(self.home_cost_to_win_1, fmt)} to win $1"
        )

    def __str__(self) -> str:
        return self.to_string()


class SportsbookWeightedOddsInterface:
    """Interface to fetch weighted odds for NBA games from The Odds API."""
    
    def __init__(self):
        self.odds_connector = theOddsAPIConnector()
    
    def get_moneyline_odds(
        self, 
        team_a: str, 
        team_b: str, 
        play_date: Optional[date] = None,
        decimals: int = 6
    ) -> Optional[MoneylineOdds]:
        """
        Get moneyline odds for an NBA game.
        
        Args:
            team_a: First team name (e.g., "Chicago Bulls")
            team_b: Second team name (e.g., "Detroit Pistons")
            play_date: Date of the game (defaults to today if None)
            decimals: Number of decimal places for odds (default: 6)
            
        Returns:
            MoneylineOdds (with `.to_string()`) or None if error
        """
        try:
            # Fetch odds from The Odds API
            events = self.odds_connector.get_odds(NBA_SPORT_KEY, MONEYLINE_MARKET_KEY)

            # Find matching event
            event = find_event_for_teams_today(events, team_a, team_b, local_date=play_date)
            if not event:
                date_str = play_date.strftime("%Y-%m-%d") if play_date else "today"
                print(f"No matching event found for {team_a} vs {team_b} on {date_str}")
                return None

            # Extract moneyline odds from all books
            books = extract_moneyline_odds_all_books(event)
            if not books:
                print("No bookmaker markets found.")
                return None

            # Get home and away teams from event
            home_team = str(event.get("home_team", ""))
            away_team = str(event.get("away_team", ""))

            # Build weighted averages
            present_keys = [b.bookmaker_key for b in books]
            weights_by_key = build_weights_for_present_books(
                present_keys, weights_map=SPORTSBOOK_WEIGHTS
            )

            book_lines: list[BookLine] = [
                BookLine(
                    bookmaker_key=b.bookmaker_key,
                    outcomes=[
                        BookOutcome(team=o.team, cost_to_win_1=o.cost_to_win_1)
                        for o in b.outcomes
                    ],
                )
                for b in books
            ]

            # Calculate weighted averages
            home_avg = weighted_average_cost_to_win_1(
                book_lines, home_team, weights_by_key=weights_by_key
            )
            away_avg = weighted_average_cost_to_win_1(
                book_lines, away_team, weights_by_key=weights_by_key
            )

            if home_avg is None or away_avg is None:
                print("Could not compute weighted average for both teams.")
                return None

            # Return typed result
            return MoneylineOdds(
                away_team=away_team,
                home_team=home_team,
                away_cost_to_win_1=float(away_avg),
                home_cost_to_win_1=float(home_avg),
            )
        except Exception as e:
            # This method is intentionally exception-safe so callers don't need try/except.
            print(
                "Error: Failed to fetch sportsbook moneyline odds.\n"
                f"Details: {e} ({type(e).__name__})"
            )
            return None

