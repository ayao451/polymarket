#!/usr/bin/env python3
"""
Pinnacle Odds Service (programmatic interface).

This module exposes a stable interface for callers to:
  - list basketball games for a given local date
  - list hockey games for a given local date
  - fetch odds for a specific matchup id

It reuses the Arcadia plumbing implemented in `pinnacle_odds_scraper.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Literal

import pandas as pd

from .pinnacle_odds_scraper import (
    OddsRow,
    _format_dt_local,
    _format_dt_utc,
    _league_name_from_matchup_item,
    _list_basketball_matchups_for_local_date,
    _list_hockey_matchups_for_local_date,
    _list_soccer_matchups_for_local_date,
    _norm,
    _parse_iso_dt,
    _scrape_arcadia_matchup_id,
    _teams_from_matchup_item,
    _to_float,
)


@dataclass(frozen=True)
class GameInfo:
    matchup_id: int
    away_team: str
    home_team: str
    league: str
    start_time_utc: datetime
    start_date_local: str
    start_time_local: str


@dataclass(frozen=True)
class GameOddsResult:
    game: GameInfo
    markets: List[OddsRow]

    def to_dict(self) -> Dict[str, object]:
        return {
            "matchup_id": self.game.matchup_id,
            "away_team": self.game.away_team,
            "home_team": self.game.home_team,
            "league": self.game.league,
            "start_time_utc": _format_dt_utc(self.game.start_time_utc),
            "start_date_local": self.game.start_date_local,
            "start_time_local": self.game.start_time_local,
            "markets": [m.to_dict() for m in self.markets],
        }

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([m.to_dict() for m in self.markets])


class PinnacleBasketballOddsService:
    """
    Programmatic interface for callers.

    Typical usage:
      svc = PinnacleBasketballOddsService()
      games = svc.list_games_for_date(date.today())
      result = svc.get_game_odds(games[0].matchup_id, game_info=games[0])
    """

    def __init__(self, *, timeout_ms: int = 45000) -> None:
        self.timeout_ms = int(timeout_ms)

    @staticmethod
    def _league_sort_key(league: str) -> tuple[int, str]:
        """
        Priority: NBA first, NCAA second, then everything else alphabetically.
        """
        l = _norm(str(league or ""))
        u = l.upper()
        if u == "NBA" or u.startswith("NBA "):
            return (0, l.lower())
        if u == "NCAA" or u.startswith("NCAA "):
            return (1, l.lower())
        return (2, l.lower())

    def list_games_for_date(
        self,
        local_date,
        *,
        game_status: Literal["started", "notstarted", "all"] = "all"
    ) -> List[GameInfo]:
        """
        List games for a given local date.

        Args:
            local_date: Local date to fetch games for
            game_status: Filter by game status:
                - "started": Only games that have already started
                - "notstarted": Only games that haven't started yet
                - "all": All games (default)

        Returns:
            List of GameInfo objects, sorted by league (NBA first, then NCAA, then alphabetical),
            then by start time.
        """
        timeout_s = max(1.0, float(self.timeout_ms) / 1000.0)
        items = _list_basketball_matchups_for_local_date(local_date=local_date, timeout_s=timeout_s)
        out: List[GameInfo] = []
        now_utc = datetime.now(timezone.utc)
        
        for m in items:
            try:
                mid = int(m.get("id"))
            except Exception:
                continue
            st = _parse_iso_dt(m.get("startTime"))
            if st is None:
                continue
            st_utc = st.astimezone(timezone.utc)
            
            # Filter by game status
            if game_status == "started":
                if st_utc >= now_utc:
                    continue  # Skip games that haven't started
            elif game_status == "notstarted":
                if st_utc < now_utc:
                    continue  # Skip games that have started
            
            away, home = _teams_from_matchup_item(m)
            league = _league_name_from_matchup_item(m)
            date_local, time_local = _format_dt_local(st)
            out.append(
                GameInfo(
                    matchup_id=mid,
                    away_team=_norm(away),
                    home_team=_norm(home),
                    league=league,
                    start_time_utc=st_utc,
                    start_date_local=date_local,
                    start_time_local=time_local,
                )
            )
        out.sort(
            key=lambda g: (
                self._league_sort_key(g.league),
                g.start_date_local,
                g.start_time_local,
                g.matchup_id,
            )
        )
        return out

    def get_game_odds(self, matchup_id: int, *, game_info: Optional[GameInfo] = None) -> GameOddsResult:
        away = game_info.away_team if game_info else ""
        home = game_info.home_team if game_info else ""
        league = game_info.league if game_info else ""
        st = game_info.start_time_utc if game_info else None

        data, _df = _scrape_arcadia_matchup_id(
            int(matchup_id),
            away_team=away,
            home_team=home,
            league=league,
            start_time_utc=st,
            timeout_ms=self.timeout_ms,
        )
        if not data.get("ok"):
            raise RuntimeError(str(data.get("error") or "Failed to fetch odds"))

        if game_info is None:
            # Synthesize minimal info; start time unknown (set to now).
            away2 = _norm(str(data.get("away_team") or ""))
            home2 = _norm(str(data.get("home_team") or ""))
            now = datetime.now(timezone.utc)
            game_info = GameInfo(
                matchup_id=int(matchup_id),
                away_team=away2,
                home_team=home2,
                league=_norm(str(data.get("league") or "")),
                start_time_utc=now,
                start_date_local="",
                start_time_local="",
            )

        markets: List[OddsRow] = []
        for d in data.get("markets") or []:
            if not isinstance(d, dict):
                continue
            try:
                markets.append(
                    OddsRow(
                        away_team=str(d.get("away_team") or game_info.away_team),
                        home_team=str(d.get("home_team") or game_info.home_team),
                        market_type=str(d.get("market_type") or ""),
                        period=int(d.get("period") or 0),
                        period_label=str(d.get("period_label") or ""),
                        is_alternate=bool(d.get("is_alternate") or False),
                        selection=str(d.get("selection") or ""),
                        line=_to_float(d.get("line")),
                        odds=_to_float(d.get("odds")),
                        american_price=_to_float(d.get("american_price")),
                        raw={},
                    )
                )
            except Exception:
                continue

        return GameOddsResult(game=game_info, markets=markets)


class PinnacleHockeyOddsService:
    """
    Programmatic interface for hockey (NHL) callers.

    Typical usage:
      svc = PinnacleHockeyOddsService()
      games = svc.list_games_for_date(date.today())
      result = svc.get_game_odds(games[0].matchup_id, game_info=games[0])
    """

    def __init__(self, *, timeout_ms: int = 45000) -> None:
        self.timeout_ms = int(timeout_ms)

    @staticmethod
    def _league_sort_key(league: str) -> tuple[int, str]:
        """
        Priority: NHL first, then everything else alphabetically.
        """
        l = _norm(str(league or ""))
        u = l.upper()
        if u == "NHL" or u.startswith("NHL "):
            return (0, l.lower())
        return (1, l.lower())

    def list_games_for_date(
        self,
        local_date,
        *,
        game_status: Literal["started", "notstarted", "all"] = "all"
    ) -> List[GameInfo]:
        """
        List hockey games for a given local date.

        Args:
            local_date: Local date to fetch games for
            game_status: Filter by game status:
                - "started": Only games that have already started
                - "notstarted": Only games that haven't started yet
                - "all": All games (default)

        Returns:
            List of GameInfo objects, sorted by league (NHL first, then alphabetical),
            then by start time.
        """
        timeout_s = max(1.0, float(self.timeout_ms) / 1000.0)
        items = _list_hockey_matchups_for_local_date(local_date=local_date, timeout_s=timeout_s)
        out: List[GameInfo] = []
        now_utc = datetime.now(timezone.utc)
        
        for m in items:
            try:
                mid = int(m.get("id"))
            except Exception:
                continue
            st = _parse_iso_dt(m.get("startTime"))
            if st is None:
                continue
            st_utc = st.astimezone(timezone.utc)
            
            # Filter by game status
            if game_status == "started":
                if st_utc >= now_utc:
                    continue  # Skip games that haven't started
            elif game_status == "notstarted":
                if st_utc < now_utc:
                    continue  # Skip games that have started
            
            away, home = _teams_from_matchup_item(m)
            league = _league_name_from_matchup_item(m)
            date_local, time_local = _format_dt_local(st)
            out.append(
                GameInfo(
                    matchup_id=mid,
                    away_team=_norm(away),
                    home_team=_norm(home),
                    league=league,
                    start_time_utc=st_utc,
                    start_date_local=date_local,
                    start_time_local=time_local,
                )
            )
        out.sort(
            key=lambda g: (
                self._league_sort_key(g.league),
                g.start_date_local,
                g.start_time_local,
                g.matchup_id,
            )
        )
        return out

    def get_game_odds(self, matchup_id: int, *, game_info: Optional[GameInfo] = None) -> GameOddsResult:
        away = game_info.away_team if game_info else ""
        home = game_info.home_team if game_info else ""
        league = game_info.league if game_info else ""
        st = game_info.start_time_utc if game_info else None

        data, _df = _scrape_arcadia_matchup_id(
            int(matchup_id),
            away_team=away,
            home_team=home,
            league=league,
            start_time_utc=st,
            timeout_ms=self.timeout_ms,
        )
        if not data.get("ok"):
            raise RuntimeError(str(data.get("error") or "Failed to fetch odds"))

        if game_info is None:
            # Synthesize minimal info; start time unknown (set to now).
            away2 = _norm(str(data.get("away_team") or ""))
            home2 = _norm(str(data.get("home_team") or ""))
            now = datetime.now(timezone.utc)
            game_info = GameInfo(
                matchup_id=int(matchup_id),
                away_team=away2,
                home_team=home2,
                league=_norm(str(data.get("league") or "")),
                start_time_utc=now,
                start_date_local="",
                start_time_local="",
            )

        markets: List[OddsRow] = []
        for d in data.get("markets") or []:
            if not isinstance(d, dict):
                continue
            try:
                markets.append(
                    OddsRow(
                        away_team=str(d.get("away_team") or game_info.away_team),
                        home_team=str(d.get("home_team") or game_info.home_team),
                        market_type=str(d.get("market_type") or ""),
                        period=int(d.get("period") or 0),
                        period_label=str(d.get("period_label") or ""),
                        is_alternate=bool(d.get("is_alternate") or False),
                        selection=str(d.get("selection") or ""),
                        line=_to_float(d.get("line")),
                        odds=_to_float(d.get("odds")),
                        american_price=_to_float(d.get("american_price")),
                        raw={},
                    )
                )
            except Exception:
                continue

        return GameOddsResult(game=game_info, markets=markets)


class PinnacleSoccerOddsService:
    """
    Programmatic interface for soccer callers.

    Typical usage:
      svc = PinnacleSoccerOddsService()
      games = svc.list_games_for_date(date.today())
      result = svc.get_game_odds(games[0].matchup_id, game_info=games[0])
    """

    def __init__(self, *, timeout_ms: int = 45000) -> None:
        self.timeout_ms = int(timeout_ms)

    @staticmethod
    def _league_sort_key(league: str) -> tuple[int, str]:
        """
        Priority: EPL first, then everything else alphabetically.
        """
        l = _norm(str(league or ""))
        u = l.upper()
        if "PREMIER LEAGUE" in u or "EPL" in u or u == "ENGLAND PREMIER LEAGUE":
            return (0, l.lower())
        return (1, l.lower())

    def list_games_for_date(
        self,
        local_date,
        *,
        game_status: Literal["started", "notstarted", "all"] = "all",
        league_filter: Optional[str] = None,
    ) -> List[GameInfo]:
        """
        List soccer games for a given local date.

        Args:
            local_date: Local date to fetch games for
            game_status: Filter by game status:
                - "started": Only games that have already started
                - "notstarted": Only games that haven't started yet
                - "all": All games (default)
            league_filter: Optional league name to filter by (e.g., "England Premier League", "EPL")

        Returns:
            List of GameInfo objects, sorted by league (EPL first, then alphabetical),
            then by start time.
        """
        timeout_s = max(1.0, float(self.timeout_ms) / 1000.0)
        items = _list_soccer_matchups_for_local_date(local_date=local_date, timeout_s=timeout_s)
        out: List[GameInfo] = []
        now_utc = datetime.now(timezone.utc)
        
        for m in items:
            try:
                mid = int(m.get("id"))
            except Exception:
                continue
            st = _parse_iso_dt(m.get("startTime"))
            if st is None:
                continue
            st_utc = st.astimezone(timezone.utc)
            
            # Filter by game status
            if game_status == "started":
                if st_utc >= now_utc:
                    continue  # Skip games that haven't started
            elif game_status == "notstarted":
                if st_utc < now_utc:
                    continue  # Skip games that have started
            
            away, home = _teams_from_matchup_item(m)
            league = _league_name_from_matchup_item(m)
            
            # Filter by league if specified
            if league_filter:
                league_upper = league.upper()
                filter_upper = league_filter.upper()
                # Check for common EPL variations
                epl_variations = ["ENGLAND - PREMIER LEAGUE", "ENGLAND PREMIER LEAGUE", "PREMIER LEAGUE", "EPL", "ENGLISH PREMIER"]
                is_epl = any(variant in league_upper for variant in epl_variations)
                
                # If filtering for EPL, check if this is an EPL game
                if filter_upper in ["EPL", "ENGLAND PREMIER LEAGUE", "PREMIER LEAGUE", "ENGLAND - PREMIER LEAGUE"]:
                    if not is_epl:
                        continue
                else:
                    # For other filters, do substring matching
                    if filter_upper not in league_upper and league_upper not in filter_upper:
                        continue
            
            date_local, time_local = _format_dt_local(st)
            out.append(
                GameInfo(
                    matchup_id=mid,
                    away_team=_norm(away),
                    home_team=_norm(home),
                    league=league,
                    start_time_utc=st_utc,
                    start_date_local=date_local,
                    start_time_local=time_local,
                )
            )
        out.sort(
            key=lambda g: (
                self._league_sort_key(g.league),
                g.start_date_local,
                g.start_time_local,
                g.matchup_id,
            )
        )
        return out

    def get_game_odds(self, matchup_id: int, *, game_info: Optional[GameInfo] = None) -> GameOddsResult:
        away = game_info.away_team if game_info else ""
        home = game_info.home_team if game_info else ""
        league = game_info.league if game_info else ""
        st = game_info.start_time_utc if game_info else None

        data, _df = _scrape_arcadia_matchup_id(
            int(matchup_id),
            away_team=away,
            home_team=home,
            league=league,
            start_time_utc=st,
            timeout_ms=self.timeout_ms,
        )
        if not data.get("ok"):
            raise RuntimeError(str(data.get("error") or "Failed to fetch odds"))

        if game_info is None:
            # Synthesize minimal info; start time unknown (set to now).
            away2 = _norm(str(data.get("away_team") or ""))
            home2 = _norm(str(data.get("home_team") or ""))
            now = datetime.now(timezone.utc)
            game_info = GameInfo(
                matchup_id=int(matchup_id),
                away_team=away2,
                home_team=home2,
                league=_norm(str(data.get("league") or "")),
                start_time_utc=now,
                start_date_local="",
                start_time_local="",
            )

        markets: List[OddsRow] = []
        for d in data.get("markets") or []:
            if not isinstance(d, dict):
                continue
            try:
                markets.append(
                    OddsRow(
                        away_team=str(d.get("away_team") or game_info.away_team),
                        home_team=str(d.get("home_team") or game_info.home_team),
                        market_type=str(d.get("market_type") or ""),
                        period=int(d.get("period") or 0),
                        period_label=str(d.get("period_label") or ""),
                        is_alternate=bool(d.get("is_alternate") or False),
                        selection=str(d.get("selection") or ""),
                        line=_to_float(d.get("line")),
                        odds=_to_float(d.get("odds")),
                        american_price=_to_float(d.get("american_price")),
                        raw={},
                    )
                )
            except Exception:
                continue

        return GameOddsResult(game=game_info, markets=markets)
