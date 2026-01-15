#!/usr/bin/env python3
"""
Interface to fetch weighted odds from The Odds API.

Given two teams and a date, fetches odds from The Odds API,
and returns formatted results.

Note: this project currently uses Pinnacle-only sportsbook odds.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

from .fetch_game_odds import (
    NCAA_SPORT_KEY,
    NBA_SPORT_KEY,
    MONEYLINE_MARKET_KEY,
    SPREAD_MARKET_KEY,
    TOTALS_MARKET_KEY,
    find_event_for_teams_today,
    extract_moneyline_odds_all_books,
    extract_spread_odds_all_books,
    extract_totals_odds_all_books,
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


@dataclass(frozen=True)
class SpreadOdds:
    away_team: str
    home_team: str
    away_point: float
    home_point: float
    away_cost_to_win_1: float
    home_cost_to_win_1: float

    def to_string(self, decimals: int = 6) -> str:
        decimals = max(0, int(decimals))
        fmt = f".{decimals}f"
        return (
            f"{self.away_team} ({self.away_point:+g}): {format(self.away_cost_to_win_1, fmt)} to win $1 | "
            f"{self.home_team} ({self.home_point:+g}): {format(self.home_cost_to_win_1, fmt)} to win $1"
        )


@dataclass(frozen=True)
class TotalsOdds:
    away_team: str
    home_team: str
    total_point: float
    over_cost_to_win_1: float
    under_cost_to_win_1: float

    def to_string(self, decimals: int = 6) -> str:
        decimals = max(0, int(decimals))
        fmt = f".{decimals}f"
        return (
            f"{self.away_team} @ {self.home_team} | "
            f"Total {self.total_point:g} | "
            f"Over: {format(self.over_cost_to_win_1, fmt)} to win $1 | "
            f"Under: {format(self.under_cost_to_win_1, fmt)} to win $1"
        )


class SportsbookWeightedOddsInterface:
    """
    Interface to fetch sportsbook odds from The Odds API.

    Current behavior: Pinnacle-only (bookmaker_key="pinnacle").
    """

    PINNACLE_BOOK_KEY = "pinnacle"
    
    def __init__(self):
        self.odds_connector = theOddsAPIConnector()

    def get_moneyline_spread_totals_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
        decimals: int = 6,
        sport_key: str = NBA_SPORT_KEY,
    ) -> tuple[Optional[MoneylineOdds], list[SpreadOdds], list[TotalsOdds]]:
        """
        Fetch moneyline + spreads + totals in a SINGLE Odds API call.

        The Odds API supports requesting multiple markets at once via comma-separated
        `markets`, e.g. "h2h,spreads,totals".
        """
        try:
            if not sport_key:
                sport_key = NBA_SPORT_KEY

            combined_markets = f"{MONEYLINE_MARKET_KEY},{SPREAD_MARKET_KEY},{TOTALS_MARKET_KEY}"
            events = self.odds_connector.get_odds(sport_key, combined_markets)

            event = find_event_for_teams_today(events, team_a, team_b, local_date=play_date)
            if not event:
                date_str = play_date.strftime("%Y-%m-%d") if play_date else "today"
                sport_label = "NBA" if sport_key == NBA_SPORT_KEY else str(sport_key)
                print(
                    f"No matching event found for {team_a} vs {team_b} on {date_str} "
                    f"(sport={sport_label})"
                )
                return None, [], []

            moneyline = self._moneyline_from_event(event)
            spreads = self._spreads_from_event(event)
            totals = self._totals_from_event(event)
            return moneyline, spreads, totals
        except Exception as e:
            print(
                "Error: Failed to fetch sportsbook odds (moneyline+spreads+totals).\n"
                f"Details: {e} ({type(e).__name__})"
            )
            return None, [], []

    # Backwards-compatible alias
    def get_moneyline_and_spread_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
        decimals: int = 6,
        sport_key: str = NBA_SPORT_KEY,
    ) -> tuple[Optional[MoneylineOdds], list[SpreadOdds]]:
        ml, sp, _tot = self.get_moneyline_spread_totals_odds(
            team_a, team_b, play_date, decimals=decimals, sport_key=sport_key
        )
        return ml, sp

    @staticmethod
    def _moneyline_from_event(event) -> Optional[MoneylineOdds]:
        books = extract_moneyline_odds_all_books(event)
        # Pinnacle-only.
        books = [b for b in (books or []) if str(b.bookmaker_key).strip().lower() == SportsbookWeightedOddsInterface.PINNACLE_BOOK_KEY]
        if not books:
            return None

        home_team = str(event.get("home_team", ""))
        away_team = str(event.get("away_team", ""))

        present_keys = [b.bookmaker_key for b in books]
        weights_by_key = build_weights_for_present_books(
            present_keys, weights_map=SPORTSBOOK_WEIGHTS
        )
        book_lines: list[BookLine] = [
            BookLine(
                bookmaker_key=b.bookmaker_key,
                outcomes=[BookOutcome(team=o.team, cost_to_win_1=o.cost_to_win_1) for o in b.outcomes],
            )
            for b in books
        ]

        home_avg = weighted_average_cost_to_win_1(
            book_lines, home_team, weights_by_key=weights_by_key
        )
        away_avg = weighted_average_cost_to_win_1(
            book_lines, away_team, weights_by_key=weights_by_key
        )
        if home_avg is None or away_avg is None:
            return None

        return MoneylineOdds(
            away_team=away_team,
            home_team=home_team,
            away_cost_to_win_1=float(away_avg),
            home_cost_to_win_1=float(home_avg),
        )

    @staticmethod
    def _spreads_from_event(event) -> list[SpreadOdds]:
        books = extract_spread_odds_all_books(event)
        # Pinnacle-only.
        books = [b for b in (books or []) if str(b.bookmaker_key).strip().lower() == SportsbookWeightedOddsInterface.PINNACLE_BOOK_KEY]
        if not books:
            return []

        home_team = str(event.get("home_team", ""))
        away_team = str(event.get("away_team", ""))

        present_keys = [b.bookmaker_key for b in books]
        weights_by_key = build_weights_for_present_books(
            present_keys, weights_map=SPORTSBOOK_WEIGHTS
        )

        def _norm_team(s: str) -> str:
            return " ".join((s or "").strip().lower().split())

        grouped: dict[tuple[float, float], list[BookLine]] = {}
        for b in books:
            o1, o2 = b.outcomes
            if _norm_team(o1.team) == _norm_team(away_team) and _norm_team(o2.team) == _norm_team(home_team):
                away_o, home_o = o1, o2
            elif _norm_team(o2.team) == _norm_team(away_team) and _norm_team(o1.team) == _norm_team(home_team):
                away_o, home_o = o2, o1
            else:
                continue

            key = (float(away_o.point), float(home_o.point))
            grouped.setdefault(key, []).append(
                BookLine(
                    bookmaker_key=b.bookmaker_key,
                    outcomes=[
                        BookOutcome(team=away_team, cost_to_win_1=away_o.cost_to_win_1),
                        BookOutcome(team=home_team, cost_to_win_1=home_o.cost_to_win_1),
                    ],
                )
            )

        results: list[SpreadOdds] = []
        for (away_pt, home_pt), book_lines in sorted(grouped.items(), key=lambda kv: kv[0]):
            away_avg = weighted_average_cost_to_win_1(
                book_lines, away_team, weights_by_key=weights_by_key
            )
            home_avg = weighted_average_cost_to_win_1(
                book_lines, home_team, weights_by_key=weights_by_key
            )
            if away_avg is None or home_avg is None:
                continue

            results.append(
                SpreadOdds(
                    away_team=away_team,
                    home_team=home_team,
                    away_point=float(away_pt),
                    home_point=float(home_pt),
                    away_cost_to_win_1=float(away_avg),
                    home_cost_to_win_1=float(home_avg),
                )
            )

        return results

    @staticmethod
    def _totals_from_event(event) -> list[TotalsOdds]:
        books = extract_totals_odds_all_books(event)
        # Pinnacle-only.
        books = [b for b in (books or []) if str(b.bookmaker_key).strip().lower() == SportsbookWeightedOddsInterface.PINNACLE_BOOK_KEY]
        if not books:
            return []

        home_team = str(event.get("home_team", ""))
        away_team = str(event.get("away_team", ""))

        present_keys = [b.bookmaker_key for b in books]
        weights_by_key = build_weights_for_present_books(
            present_keys, weights_map=SPORTSBOOK_WEIGHTS
        )

        def _norm_side(s: str) -> str:
            return (s or "").strip().lower()

        grouped: dict[float, list[BookLine]] = {}
        for b in books:
            o1, o2 = b.outcomes
            pt = float(o1.point)
            if float(o2.point) != pt:
                continue

            # Identify Over/Under outcomes
            if _norm_side(o1.side) == "over" and _norm_side(o2.side) == "under":
                over_o, under_o = o1, o2
            elif _norm_side(o2.side) == "over" and _norm_side(o1.side) == "under":
                over_o, under_o = o2, o1
            else:
                continue

            grouped.setdefault(pt, []).append(
                BookLine(
                    bookmaker_key=b.bookmaker_key,
                    outcomes=[
                        BookOutcome(team="Over", cost_to_win_1=over_o.cost_to_win_1),
                        BookOutcome(team="Under", cost_to_win_1=under_o.cost_to_win_1),
                    ],
                )
            )

        results: list[TotalsOdds] = []
        for pt, book_lines in sorted(grouped.items(), key=lambda kv: kv[0]):
            over_avg = weighted_average_cost_to_win_1(
                book_lines, "Over", weights_by_key=weights_by_key
            )
            under_avg = weighted_average_cost_to_win_1(
                book_lines, "Under", weights_by_key=weights_by_key
            )
            if over_avg is None or under_avg is None:
                continue

            results.append(
                TotalsOdds(
                    away_team=away_team,
                    home_team=home_team,
                    total_point=float(pt),
                    over_cost_to_win_1=float(over_avg),
                    under_cost_to_win_1=float(under_avg),
                )
            )

        return results
    
    def get_moneyline_odds(
        self, 
        team_a: str, 
        team_b: str, 
        play_date: Optional[date] = None,
        decimals: int = 6,
        sport_key: str = NBA_SPORT_KEY,
    ) -> Optional[MoneylineOdds]:
        """
        Get moneyline odds for an NBA game.
        
        Args:
            team_a: First team name (e.g., "Chicago Bulls")
            team_b: Second team name (e.g., "Detroit Pistons")
            play_date: Date of the game (defaults to today if None)
            decimals: Number of decimal places for odds (default: 6)
            sport_key: The Odds API sport key (defaults to NBA). Example: NCAA_SPORT_KEY.
            
        Returns:
            MoneylineOdds (with `.to_string()`) or None if error
        """
        moneyline, _, _ = self.get_moneyline_spread_totals_odds(
            team_a, team_b, play_date, decimals=decimals, sport_key=sport_key
        )
        return moneyline

    def get_spread_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
        decimals: int = 6,
        sport_key: str = NBA_SPORT_KEY,
    ) -> list[SpreadOdds]:
        """
        Get spread odds for a game, grouped by the exact spread line.

        Note: sportsbooks may disagree on the line. We keep separate entries per line and
        only compare Polymarket outcomes to sportsbook outcomes where (team, +x) matches.
        """
        _, spreads, _ = self.get_moneyline_spread_totals_odds(
            team_a, team_b, play_date, decimals=decimals, sport_key=sport_key
        )
        return spreads

    def get_totals_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
        decimals: int = 6,
        sport_key: str = NBA_SPORT_KEY,
    ) -> list[TotalsOdds]:
        _, _, totals = self.get_moneyline_spread_totals_odds(
            team_a, team_b, play_date, decimals=decimals, sport_key=sport_key
        )
        return totals

