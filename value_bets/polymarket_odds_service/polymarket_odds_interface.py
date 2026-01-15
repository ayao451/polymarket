#!/usr/bin/env python3
"""
Interface to fetch Polymarket market odds (stateful, by matchup only).

This interface finds the Gamma event slug from:
- away team
- home team
- local date
and then fetches markets via the analyzer.
"""

import json
import re
from datetime import date, datetime
from typing import Dict, List, Optional

from .polymarket_market_analyzer import PolymarketMarketAnalyzer, MarketOdds
from .find_game import PolymarketGameFinder


class PolymarketOddsInterface:
    """Interface to fetch Polymarket market odds."""
    
    def __init__(self, away_team: str, home_team: str, play_date: Optional[date] = None):
        """
        Initialize the interface for a specific matchup.

        Args:
            away_team: Away team name
            home_team: Home team name
            play_date: Optional local date (defaults to today)
        """
        self.analyzer = PolymarketMarketAnalyzer()
        self.game_finder = PolymarketGameFinder()
        self.away_team = away_team
        self.home_team = home_team
        self.play_date = play_date

        # Resolved event slug for this matchup (None if not found)
        try:
            self.event_slug: Optional[str] = self._find_event_slug(
                away_team=away_team, home_team=home_team, play_date=play_date
            )
        except Exception as e:
            print(f"Warning: Could not find Polymarket event: {e}")
            self.event_slug = None
    
    def _find_event_slug(
        self, *, away_team: str, home_team: str, play_date: Optional[date] = None
    ) -> Optional[str]:
        if play_date is None:
            play_date = datetime.now().astimezone().date()
        return self.game_finder.find_event_slug(
            away_team=away_team, home_team=home_team, play_date=play_date
        )

    @staticmethod
    def _normalize(s: str) -> str:
        return " ".join(str(s).strip().lower().split())

    @classmethod
    def _team_matches_outcome(cls, team_name: str, outcome_name: str) -> bool:
        """
        Consider a Polymarket outcome to match a team if:
        - exact normalized match, OR
        - outcome matches last word of team name (nickname), OR
        - outcome appears in full team name (or vice versa).
        """
        full_key = cls._normalize(team_name)
        out_key = cls._normalize(outcome_name)
        if not full_key or not out_key:
            return False
        if full_key == out_key:
            return True
        full_last = full_key.split()[-1]
        if out_key == full_last:
            return True
        if out_key in full_key or full_last in out_key:
            return True
        return False

    @staticmethod
    def _looks_like_spread_or_total(text: str) -> bool:
        """
        Heuristic guard: moneyline markets should not include handicaps/totals.

        Examples to exclude:
        - "Iona Gaels (-11.5)"
        - "Team (+3.0)"
        - "Over 145.5" / "Under 145.5"
        """
        s = str(text or "").strip()
        if not s:
            return False

        # Explicit over/under wording (totals)
        low = s.lower()
        if low.startswith("over ") or low.startswith("under "):
            return True

        # Any parenthesized numeric handicap, e.g. "(+3.5)" or "(-11.5)"
        if re.search(r"\(\s*[+-]?\d+(\.\d+)?\s*\)", s):
            return True

        # Any remaining bare numeric (e.g. totals/spreads often contain numbers)
        # Keep this broad because team names virtually never contain digits.
        if re.search(r"\d", s):
            return True

        return False

    @staticmethod
    def _parse_spread_outcome(text: str) -> Optional[tuple[str, float]]:
        """
        Parse a spread outcome label into (team, point).

        Examples we support:
        - "Bulls (+6.5)" -> ("Bulls", +6.5)
        - "Bulls (-11.0)" -> ("Bulls", -11.0)
        - "Bulls +6.5" -> ("Bulls", +6.5)
        - "Bulls -6.5" -> ("Bulls", -6.5)
        """
        s = str(text or "").strip()
        if not s:
            return None
        low = s.lower()
        if low.startswith("over ") or low.startswith("under "):
            return None

        m = re.match(r"^(?P<team>.+?)\s*\(\s*(?P<pt>[+-]?\d+(?:\.\d+)?)\s*\)\s*$", s)
        if not m:
            m = re.match(r"^(?P<team>.+?)\s+(?P<pt>[+-]\d+(?:\.\d+)?)\s*$", s)
        if not m:
            return None

        team = (m.group("team") or "").strip()
        pt_s = (m.group("pt") or "").strip()
        if not team or not pt_s:
            return None
        try:
            pt = float(pt_s)
        except ValueError:
            return None
        return team, float(pt)

    def _select_spread_market_slugs(self, event: Dict) -> List[str]:
        """
        Return market slugs for 2-outcome spread markets matching this matchup.
        """
        slugs: List[str] = []
        markets = event.get("markets", []) or []
        for m in markets:
            slug = str(m.get("slug") or "").strip()
            if not slug:
                continue

            outcomes_raw = m.get("outcomes", "[]")
            try:
                outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            except Exception:
                continue
            if not isinstance(outcomes, list) or len(outcomes) != 2:
                continue

            p1 = self._parse_spread_outcome(str(outcomes[0]))
            p2 = self._parse_spread_outcome(str(outcomes[1]))
            if not p1 or not p2:
                continue

            team1, pt1 = p1
            team2, pt2 = p2

            # Must match the two teams (ignoring handicap), and must not be totals.
            if not (
                (self._team_matches_outcome(self.away_team, team1) and self._team_matches_outcome(self.home_team, team2))
                or (self._team_matches_outcome(self.home_team, team1) and self._team_matches_outcome(self.away_team, team2))
            ):
                continue

            # A typical spread market has opposite lines.
            # If it doesn't, it's still likely a handicap variant; keep it.
            _ = (pt1, pt2)

            slugs.append(slug)

        return slugs

    def _select_moneyline_market_slug(self, event: Dict) -> Optional[str]:
        """
        Return the first 2-outcome market whose outcomes match (away, home).
        """
        markets = event.get("markets", []) or []
        for m in markets:
            slug = str(m.get("slug") or "").strip()
            if not slug:
                continue

            outcomes_raw = m.get("outcomes", "[]")
            try:
                outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            except Exception:
                continue
            if not isinstance(outcomes, list) or len(outcomes) != 2:
                continue

            o1 = str(outcomes[0])
            o2 = str(outcomes[1])

            # Exclude spreads/totals before matching teams.
            # (Spreads can still "match" team names but aren't moneyline.)
            if self._looks_like_spread_or_total(o1) or self._looks_like_spread_or_total(o2):
                continue

            matches = (
                (self._team_matches_outcome(self.away_team, o1) and self._team_matches_outcome(self.home_team, o2))
                or (self._team_matches_outcome(self.home_team, o1) and self._team_matches_outcome(self.away_team, o2))
            )
            if not matches:
                continue
            return slug

        return None

    def get_market_odds(self) -> List[MarketOdds]:
        """
        Get market odds for a Polymarket event.

        Returns empty list if event not found or error occurs.
        """
        if not self.event_slug:
            return []
        return self.analyzer.analyze_markets(self.event_slug)

    def get_moneyline_odds(self) -> List[MarketOdds]:
        """
        Get moneyline odds for a Polymarket event.
        """
        if not self.event_slug:
            return []
        event = self.analyzer.fetch_event_by_slug(self.event_slug)
        if not event:
            return []

        # Polymarket convention: the main moneyline market slug equals the event slug.
        # This is the most reliable way to avoid selecting spreads/totals.
        results = self.analyzer.analyze_event(event, market_slug=self.event_slug)
        if results:
            return results

        # Fallback: if the event doesn't contain a market with slug==event slug,
        # attempt to identify the moneyline market by outcome matching.
        market_slug = self._select_moneyline_market_slug(event)
        if not market_slug:
            print(
                f"Warning: Could not identify a 2-outcome moneyline market for "
                f"{self.away_team} @ {self.home_team} (event_slug={self.event_slug}). Skipping."
            )
            return []

        return self.analyzer.analyze_event(event, market_slug=market_slug)

    def get_spread_odds(self) -> List[MarketOdds]:
        """
        Get spread market odds for a Polymarket event (all spread lines for this matchup).
        """
        if not self.event_slug:
            return []
        event = self.analyzer.fetch_event_by_slug(self.event_slug)
        if not event:
            return []

        # Use the event payload from:
        #   GET https://gamma-api.polymarket.com/events/slug/{event_slug}
        # and pull spread market slugs directly from its `markets` array.
        slugs = self.analyzer.spread_market_slugs_from_event(event)
        if not slugs:
            # Retry once (Gamma can be eventually consistent for sub-markets).
            try:
                event = self.analyzer.fetch_event_by_slug(self.event_slug) or event
            except Exception:
                pass
            slugs = self.analyzer.spread_market_slugs_from_event(event)

        print(
            f"Found {len(slugs)} spread market slugs for "
            f"{self.away_team} vs {self.home_team}: {slugs}"
        )
        if not slugs:
            return []

        results: List[MarketOdds] = []
        for slug in slugs:
            results.extend(self.analyzer.analyze_event(event, market_slug=slug))
        return results

    def get_totals_odds(self) -> List[MarketOdds]:
        """
        Get totals (O/U) market odds for a Polymarket event (full game only).
        """
        if not self.event_slug:
            return []
        event = self.analyzer.fetch_event_by_slug(self.event_slug)
        if not event:
            return []

        slugs = self.analyzer.totals_market_slugs_from_event(event)
        if not slugs:
            # Retry once (Gamma can be eventually consistent for sub-markets).
            try:
                event = self.analyzer.fetch_event_by_slug(self.event_slug) or event
            except Exception:
                pass
            slugs = self.analyzer.totals_market_slugs_from_event(event)

        print(
            f"Found {len(slugs)} totals market slugs for "
            f"{self.away_team} vs {self.home_team}: {slugs}"
        )
        if not slugs:
            return []

        results: List[MarketOdds] = []
        for slug in slugs:
            results.extend(self.analyzer.analyze_event(event, market_slug=slug))
        return results

