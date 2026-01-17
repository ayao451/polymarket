#!/usr/bin/env python3
"""
Pinnacle sportsbook odds adapter for the Polymarket bot.

This replaces `sportsbook_odds_service/*` (The Odds API) by using Pinnacle's
Arcadia guest endpoints via `pinnacle_odds_service.PinnacleBasketballOddsService`.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from .pinnacle_odds_service import PinnacleBasketballOddsService, PinnacleHockeyOddsService
from .sportsbook_odds import SportsbookOdds, HandicapOdds, TotalOdds

def _cost_to_win_1(decimal_odds: float) -> Optional[float]:
    try:
        d = float(decimal_odds)
    except Exception:
        return None
    if d <= 0:
        return None
    return 1.0 / d

def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())



class PinnacleSportsbookOddsInterface:
    """
    Replacement for SportsbookWeightedOddsInterface, backed by Pinnacle.
    Supports multiple sports via service parameter.
    """

    def __init__(self, sport: str, timeout_ms: int = 45000) -> None:
        """
        Initialize the interface.
        
        Args:
            timeout_ms: Timeout for API requests
            service: Optional pre-instantiated service (basketball or hockey). If None, creates based on sport.
            sport: Sport type ("basketball" or "hockey"). Ignored if service is provided.
        """
        if sport.lower() == "hockey":
            self._svc = PinnacleHockeyOddsService(timeout_ms=timeout_ms)
        if sport.lower() == "basketball":
            self._svc = PinnacleBasketballOddsService(timeout_ms=timeout_ms)

    def _find_game_and_rows(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> Optional[tuple[str, str, list]]:
        """
        Find the matching game and return (away_team, home_team, market_rows).
        Returns None if game not found or odds fetch fails.
        """
        local_date = play_date or date.today()
        games = self._svc.list_games_for_date(local_date)

        ta = _norm(team_a)
        tb = _norm(team_b)

        match = None
        for g in games:
            a = _norm(g.away_team)
            h = _norm(g.home_team)
            if (a == ta and h == tb) or (a == tb and h == ta) or (h == ta and a == tb) or (h == tb and a == ta):
                match = g
                break

        if match is None:
            return None

        try:
            res = self._svc.get_game_odds(match.matchup_id, game_info=match)
        except RuntimeError as e:
            print(
                f"Warning: failed to fetch Pinnacle odds for {match.away_team} @ {match.home_team} "
                f"(matchup_id={match.matchup_id}): {e}"
            )
            return None

        # Only full-game (period=0) markets are used for Polymarket matching.
        rows = [r for r in (res.markets or []) if int(r.period or 0) == 0 and not bool(r.is_alternate or False)]
        return match.away_team, match.home_team, rows

    def get_moneyline_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> Optional[SportsbookOdds]:
        """Fetch moneyline odds for a game."""
        result = self._find_game_and_rows(team_a, team_b, play_date)
        if result is None:
            return None

        away_team, home_team, rows = result

        ml_home = next((r for r in rows if r.market_type == "moneyline" and _norm(r.selection) == _norm(home_team)), None)
        ml_away = next((r for r in rows if r.market_type == "moneyline" and _norm(r.selection) == _norm(away_team)), None)

        if ml_home and ml_away and ml_home.odds and ml_away.odds:
            hc = _cost_to_win_1(float(ml_home.odds))
            ac = _cost_to_win_1(float(ml_away.odds))
            if hc is not None and ac is not None:
                return SportsbookOdds(
                    outcome_1=away_team,
                    outcome_2=home_team,
                    outcome_1_cost_to_win_1=float(ac),
                    outcome_2_cost_to_win_1=float(hc),
                )
        return None

    def get_spread_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> list[HandicapOdds]:
        """Fetch spread odds for a game."""
        result = self._find_game_and_rows(team_a, team_b, play_date)
        if result is None:
            return []

        away_team, home_team, rows = result

        # Spreads: group by abs(line) and require both teams
        spreads_by_abs: dict[float, dict[str, tuple[float, float]]] = {}
        for r in rows:
            if r.market_type != "spread":
                continue
            if r.line is None or r.odds is None:
                continue
            cost = _cost_to_win_1(float(r.odds))
            if cost is None:
                continue
            pt = float(r.line)
            key = round(abs(pt), 2)
            bucket = spreads_by_abs.setdefault(key, {})
            if _norm(r.selection) == _norm(away_team):
                bucket["away"] = (pt, float(cost))
            elif _norm(r.selection) == _norm(home_team):
                bucket["home"] = (pt, float(cost))

        spreads: list[HandicapOdds] = []
        for _abs, bucket in sorted(spreads_by_abs.items(), key=lambda kv: kv[0]):
            if "away" not in bucket or "home" not in bucket:
                continue
            away_pt, away_cost = bucket["away"]
            home_pt, home_cost = bucket["home"]
            spreads.append(
                HandicapOdds(
                    outcome_1=away_team,
                    outcome_2=home_team,
                    outcome_1_cost_to_win_1=float(away_cost),
                    outcome_2_cost_to_win_1=float(home_cost),
                    point=float(away_pt),
                )
            )
        return spreads

    def get_totals_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> list[TotalOdds]:
        """Fetch totals (over/under) odds for a game. Only returns lines ending in .5"""
        result = self._find_game_and_rows(team_a, team_b, play_date)
        if result is None:
            return []

        away_team, home_team, rows = result

        # Totals: group by total_point and require Over + Under
        totals_by_pt: dict[float, dict[str, float]] = {}
        for r in rows:
            if r.market_type != "totals":
                continue
            if r.line is None or r.odds is None:
                continue
            cost = _cost_to_win_1(float(r.odds))
            if cost is None:
                continue
            pt = float(r.line)
            
            # Only keep lines ending in .5 (e.g., 234.5, not 245)
            if pt % 1 != 0.5:
                continue
                
            key = round(pt, 2)
            bucket = totals_by_pt.setdefault(key, {})
            if _norm(r.selection) == "over":
                bucket["over"] = float(cost)
            elif _norm(r.selection) == "under":
                bucket["under"] = float(cost)

        totals: list[TotalOdds] = []
        for pt, bucket in sorted(totals_by_pt.items(), key=lambda kv: kv[0]):
            if "over" not in bucket or "under" not in bucket:
                continue
            totals.append(
                TotalOdds(
                    outcome_1="Over",
                    outcome_2="Under",
                    outcome_1_cost_to_win_1=float(bucket["over"]),
                    outcome_2_cost_to_win_1=float(bucket["under"]),
                    point=float(pt),
                )
            )
        return totals

    def get_moneyline_spread_totals_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> tuple[Optional[SportsbookOdds], list[HandicapOdds], list[TotalOdds]]:
        """
        Fetch all odds (moneyline, spreads, totals) for a game.
        Convenience method that calls all three individual methods.
        """
        moneyline = self.get_moneyline_odds(team_a, team_b, play_date)
        spreads = self.get_spread_odds(team_a, team_b, play_date)
        totals = self.get_totals_odds(team_a, team_b, play_date)
        return moneyline, spreads, totals

