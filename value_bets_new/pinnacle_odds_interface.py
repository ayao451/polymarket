#!/usr/bin/env python3
"""
Pinnacle sportsbook odds adapter for the Polymarket bot.

This replaces `sportsbook_odds_service/*` (The Odds API) by using Pinnacle's
Arcadia guest endpoints via `pinnacle_odds_service.PinnacleBasketballOddsService`.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from value_bets_new.pinnacle_odds_service import PinnacleBasketballOddsService, PinnacleHockeyOddsService, PinnacleMMAOddsService, PinnacleTennisOddsService, PinnacleSoccerOddsService
from value_bets_new.constants import Sport, SportsbookOdds, HandicapOdds, TotalOdds

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

    def __init__(self, sport: Sport, timeout_ms: int = 45000) -> None:
        """
        Initialize the interface.

        Args:
            sport: Sport enum value (Sport.BASKETBALL, Sport.HOCKEY, Sport.UFC, or Sport.TENNIS).
            timeout_ms: Timeout for API requests
        """
        self.sport = sport
        if sport == Sport.HOCKEY:
            self._svc = PinnacleHockeyOddsService(timeout_ms=timeout_ms)
        elif sport == Sport.BASKETBALL:
            self._svc = PinnacleBasketballOddsService(timeout_ms=timeout_ms)
        elif sport == Sport.UFC:
            self._svc = PinnacleMMAOddsService(timeout_ms=timeout_ms)
        elif sport == Sport.TENNIS:
            self._svc = PinnacleTennisOddsService(timeout_ms=timeout_ms)
        elif sport == Sport.SOCCER:
            self._svc = PinnacleSoccerOddsService(timeout_ms=timeout_ms)
        else:
            raise ValueError(
                f"Unsupported sport: {sport}. Must be Sport.BASKETBALL, Sport.HOCKEY, Sport.UFC, Sport.TENNIS, or Sport.SOCCER"
            )

    def _find_game_and_rows(
        self,
        team_a: str,
        team_b: str,
        play_date: date,
    ) -> Optional[tuple[str, str, list]]:
        """
        Find the matching game and return (away_team, home_team, market_rows).
        Returns None if game not found or odds fetch fails.
        """
        # Try both the play_date and the day before (in case of timezone differences)
        from datetime import timedelta
        dates_to_try = [play_date, play_date - timedelta(days=1), play_date + timedelta(days=1)]
        games = []
        for local_date in dates_to_try:
            date_games = self._svc.list_games_for_date(local_date)
            games.extend(date_games)

        ta = _norm(team_a)
        tb = _norm(team_b)

        def _team_matches(t1: str, t2: str) -> bool:
            """Fuzzy team name matching."""
            n1 = _norm(t1)
            n2 = _norm(t2)
            if n1 == n2:
                return True
            # Check if one contains the other
            if n1 in n2 or n2 in n1:
                return True
            # Check if last word matches (nickname)
            words1 = n1.split()
            words2 = n2.split()
            if words1 and words2 and words1[-1] == words2[-1]:
                # One is just nickname, or first word matches
                if len(words1) == 1 or len(words2) == 1:
                    return True
                if words1[0] == words2[0]:
                    return True
            # Check if first word matches (for school/city names)
            if words1 and words2 and words1[0] == words2[0] and len(words1[0]) > 3:
                return True
            # Check if all words from shorter are in longer
            if len(words1) > 1 and len(words2) > 1:
                shorter = words1 if len(words1) < len(words2) else words2
                longer = words2 if len(words1) < len(words2) else words1
                if all(word in longer for word in shorter if len(word) > 2):
                    return True
            return False
        
        match = None
        for g in games:
            a = _norm(g.away_team)
            h = _norm(g.home_team)
            
            # Try exact match first
            if (a == ta and h == tb) or (a == tb and h == ta) or (h == ta and a == tb) or (h == tb and a == ta):
                match = g
                break
            
            # Try fuzzy match - check all combinations
            ta_matches_a = _team_matches(ta, a)
            ta_matches_h = _team_matches(ta, h)
            tb_matches_a = _team_matches(tb, a)
            tb_matches_h = _team_matches(tb, h)
            
            if (ta_matches_a and tb_matches_h) or \
               (ta_matches_h and tb_matches_a) or \
               (tb_matches_a and ta_matches_h) or \
               (tb_matches_h and ta_matches_a):
                match = g
                break

        if match is None:
            return None

        try:
            res = self._svc.get_game_odds(match.matchup_id, game_info=match)
        except Exception:
            return None

        # Only full-game (period=0) markets are used for Polymarket matching.
        rows = [r for r in (res.markets or []) if int(r.period or 0) == 0 and not bool(r.is_alternate or False)]
        return match.away_team, match.home_team, rows

    def get_moneyline_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: date,
    ) -> Optional[SportsbookOdds]:
        """Fetch moneyline odds for a game."""
        result = self._find_game_and_rows(team_a, team_b, play_date)
        if result is None:
            return None

        away_team, home_team, rows = result

        # Filter for moneyline rows only - ensure market_type is exactly "moneyline"
        # (not "totals" which was the bug before the fix)
        moneyline_rows = [r for r in rows if (r.market_type or "").lower() == "moneyline"]
        
        if not moneyline_rows:
            return None

        ml_home = next((r for r in moneyline_rows if _norm(r.selection) == _norm(home_team)), None)
        ml_away = next((r for r in moneyline_rows if _norm(r.selection) == _norm(away_team)), None)

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
        play_date: date,
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
        play_date: date,
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
        play_date: date,
    ) -> Optional[list[TotalOdds]]:
        """Fetch generic totals (over/under) odds. Use totals_games/totals_sets for tennis."""
        return self._get_totals_odds_by_type(team_a, team_b, play_date, "totals")

    def get_totals_games_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: date,
    ) -> Optional[list[TotalOdds]]:
        """Fetch total games (over/under) odds for tennis."""
        return self._get_totals_odds_by_type(team_a, team_b, play_date, "totals_games")

    def get_totals_sets_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: date,
    ) -> Optional[list[TotalOdds]]:
        """Fetch total sets (over/under) odds for tennis."""
        return self._get_totals_odds_by_type(team_a, team_b, play_date, "totals_sets")

    def get_moneyline_spread_totals_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: date,
    ) -> tuple[Optional[SportsbookOdds], Optional[list[HandicapOdds]], Optional[list[TotalOdds]]]:
        """
        Fetch all odds (moneyline, spreads, totals) for a game.
        Convenience method that calls all three individual methods.
        """
        moneyline = self.get_moneyline_odds(team_a, team_b, play_date)
        spreads = self.get_spread_odds(team_a, team_b, play_date)
        totals = self.get_totals_odds(team_a, team_b, play_date)
        return moneyline, spreads, totals

