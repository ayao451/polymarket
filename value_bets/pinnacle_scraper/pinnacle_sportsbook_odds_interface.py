#!/usr/bin/env python3
"""
Pinnacle sportsbook odds adapter for the Polymarket bot.

This replaces `sportsbook_odds_service/*` (The Odds API) by using Pinnacle's
Arcadia guest endpoints via `pinnacle_odds_service.PinnacleBasketballOddsService`.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from .pinnacle_odds_service import PinnacleBasketballOddsService, PinnacleHockeyOddsService, PinnacleSoccerOddsService, PinnacleMMAOddsService, PinnacleTennisOddsService
from .sportsbook_odds import SportsbookOdds, HandicapOdds, TotalOdds, ThreeWayMoneylineOdds

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
            service: Optional pre-instantiated service (basketball, hockey, or soccer). If None, creates based on sport.
            sport: Sport type ("basketball", "hockey", or "soccer"). Ignored if service is provided.
        """
        self.sport = sport.lower()
        if self.sport == "hockey":
            self._svc = PinnacleHockeyOddsService(timeout_ms=timeout_ms)
        elif self.sport == "basketball":
            self._svc = PinnacleBasketballOddsService(timeout_ms=timeout_ms)
        elif self.sport == "soccer":
            self._svc = PinnacleSoccerOddsService(timeout_ms=timeout_ms)
        elif self.sport == "ufc" or self.sport == "mma":
            self._svc = PinnacleMMAOddsService(timeout_ms=timeout_ms)
        elif self.sport == "tennis":
            self._svc = PinnacleTennisOddsService(timeout_ms=timeout_ms)
        else:
            raise ValueError(f"Unsupported sport: {sport}. Must be 'basketball', 'hockey', or 'soccer'")

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

    def get_three_way_moneyline_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> Optional[ThreeWayMoneylineOdds]:
        """Fetch 3-way moneyline odds for a soccer game (away, draw, home)."""
        result = self._find_game_and_rows(team_a, team_b, play_date)
        if result is None:
            return None

        away_team, home_team, rows = result

        ml_away = next((r for r in rows if r.market_type == "moneyline" and _norm(r.selection) == _norm(away_team)), None)
        ml_draw = next((r for r in rows if r.market_type == "moneyline" and (_norm(r.selection) == "draw" or _norm(r.selection) == "tie")), None)
        ml_home = next((r for r in rows if r.market_type == "moneyline" and _norm(r.selection) == _norm(home_team)), None)

        if ml_away and ml_draw and ml_home and ml_away.odds and ml_draw.odds and ml_home.odds:
            ac = _cost_to_win_1(float(ml_away.odds))
            dc = _cost_to_win_1(float(ml_draw.odds))
            hc = _cost_to_win_1(float(ml_home.odds))
            if ac is not None and dc is not None and hc is not None:
                return ThreeWayMoneylineOdds(
                    away_team=away_team,
                    home_team=home_team,
                    outcome_1_cost_to_win_1=float(ac),
                    outcome_2_cost_to_win_1=float(dc),
                    outcome_3_cost_to_win_1=float(hc),
                )
        return None

    def get_spread_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> Optional[list[HandicapOdds]]:
        """Fetch spread odds for a game. Returns None if no odds are available."""
        result = self._find_game_and_rows(team_a, team_b, play_date)
        if result is None:
            return None

        away_team, home_team, rows = result
        
        # If no rows available, return None
        if not rows:
            return None

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
        
        # Return None if no spreads found, otherwise return the list
        return spreads if spreads else None

    @staticmethod
    def _totals_row_matches_type(
        market_type: str,
        line: Optional[float],
        totals_market_type: str,
    ) -> bool:
        """
        Return True if this row belongs to the requested totals market type.
        Pinnacle tennis uses type='total' for both games and sets; we infer from line.
        - totals_sets: line <= 5.5 (e.g. 2.5, 3.5, 4.5, 5.5)
        - totals_games: line > 5.5 (e.g. 21.5, 36.5, 39.5)
        """
        if market_type == totals_market_type:
            return True
        if market_type != "totals" or line is None:
            return False
        pt = float(line)
        if totals_market_type == "totals_sets":
            return pt <= 5.5
        if totals_market_type == "totals_games":
            return pt > 5.5
        return False

    def _get_totals_odds_by_type(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date],
        totals_market_type: str,
    ) -> Optional[list[TotalOdds]]:
        """
        Fetch totals (over/under) odds for a game, filtered by totals market type.
        totals_market_type: "totals" | "totals_games" | "totals_sets"
        Only returns lines ending in .5. Returns None if no odds are available.
        For tennis, type='total' rows are split by line: <=5.5 -> sets, >5.5 -> games.
        """
        result = self._find_game_and_rows(team_a, team_b, play_date)
        if result is None:
            return None

        away_team, home_team, rows = result
        if not rows:
            return None

        totals_by_pt: dict[float, dict[str, float]] = {}
        for r in rows:
            if not self._totals_row_matches_type(
                r.market_type or "",
                r.line,
                totals_market_type,
            ):
                continue
            if r.line is None or r.odds is None:
                continue
            cost = _cost_to_win_1(float(r.odds))
            if cost is None:
                continue
            pt = float(r.line)
            if pt % 1 != 0.5:
                continue
            key = round(pt, 2)
            bucket = totals_by_pt.setdefault(key, {})
            if _norm(r.selection) == "over":
                bucket["over"] = float(cost)
            elif _norm(r.selection) == "under":
                bucket["under"] = float(cost)

        out: list[TotalOdds] = []
        for pt, bucket in sorted(totals_by_pt.items(), key=lambda kv: kv[0]):
            if "over" not in bucket or "under" not in bucket:
                continue
            out.append(
                TotalOdds(
                    outcome_1="Over",
                    outcome_2="Under",
                    outcome_1_cost_to_win_1=float(bucket["over"]),
                    outcome_2_cost_to_win_1=float(bucket["under"]),
                    point=float(pt),
                )
            )
        return out if out else None

    def get_totals_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> Optional[list[TotalOdds]]:
        """Fetch generic totals (over/under) odds. Use totals_games/totals_sets for tennis."""
        return self._get_totals_odds_by_type(team_a, team_b, play_date, "totals")

    def get_totals_games_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> Optional[list[TotalOdds]]:
        """Fetch total games (over/under) odds for tennis."""
        return self._get_totals_odds_by_type(team_a, team_b, play_date, "totals_games")

    def get_totals_sets_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> Optional[list[TotalOdds]]:
        """Fetch total sets (over/under) odds for tennis."""
        return self._get_totals_odds_by_type(team_a, team_b, play_date, "totals_sets")

    def get_moneyline_spread_totals_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> tuple[Optional[SportsbookOdds], Optional[list[HandicapOdds]], Optional[list[TotalOdds]]]:
        """
        Fetch all odds (moneyline, spreads, totals) for a game.
        Convenience method that calls all three individual methods.
        """
        moneyline = self.get_moneyline_odds(team_a, team_b, play_date)
        spreads = self.get_spread_odds(team_a, team_b, play_date)
        totals = self.get_totals_odds(team_a, team_b, play_date)
        return moneyline, spreads, totals

