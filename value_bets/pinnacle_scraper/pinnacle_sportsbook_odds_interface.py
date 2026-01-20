#!/usr/bin/env python3
"""
Pinnacle sportsbook odds adapter for the Polymarket bot.

This replaces `sportsbook_odds_service/*` (The Odds API) by using Pinnacle's
Arcadia guest endpoints via `pinnacle_odds_service.PinnacleBasketballOddsService`.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from .pinnacle_odds_service import PinnacleBasketballOddsService, PinnacleHockeyOddsService, PinnacleSoccerOddsService
from .sportsbook_odds import SportsbookOdds, HandicapOdds, TotalOdds, PlayerPropOdds, ThreeWayMoneylineOdds

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
        if sport.lower() == "hockey":
            self._svc = PinnacleHockeyOddsService(timeout_ms=timeout_ms)
        elif sport.lower() == "basketball":
            self._svc = PinnacleBasketballOddsService(timeout_ms=timeout_ms)
        elif sport.lower() == "soccer":
            self._svc = PinnacleSoccerOddsService(timeout_ms=timeout_ms)
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

    def get_player_props_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> list[PlayerPropOdds]:
        """
        Fetch player prop odds for a game.
        
        Returns a list of PlayerPropOdds, grouped by player, prop_type, and line.
        """
        result = self._find_game_and_rows(team_a, team_b, play_date)
        if result is None:
            return []

        away_team, home_team, rows = result

        # Filter for player prop markets
        player_prop_rows = [r for r in rows if r.market_type == "player_prop"]
        if not player_prop_rows:
            return []

        # Group by player, prop_type, and line
        # We need to extract player name and prop type from the raw market data
        props_by_key: dict[tuple[str, str, float], dict[str, float]] = {}
        
        for r in player_prop_rows:
            if r.line is None or r.odds is None:
                continue
            
            # Extract player name and prop type from raw market data
            raw_market = r.raw.get("market", {}) if isinstance(r.raw, dict) else {}
            raw_selection = r.raw.get("selection", {}) if isinstance(r.raw, dict) else {}
            
            # Try to get market name/description
            market_name = str(raw_market.get("name") or raw_market.get("marketName") or raw_market.get("description") or "").lower()
            selection_name = str(raw_selection.get("name") or raw_selection.get("participant") or "").lower()
            
            # Try to extract player name and prop type
            # Common patterns:
            # - "Player Name Points Over/Under 25.5"
            # - "Points - Player Name - Over/Under 25.5"
            # - Market name might contain player and prop type
            
            # Look for prop types
            prop_types = ["points", "rebounds", "assists", "threes", "steals", "blocks"]
            prop_type = None
            for pt in prop_types:
                if pt in market_name or pt in selection_name:
                    prop_type = pt
                    break
            
            if prop_type is None:
                continue
            
            # Extract player name - usually before or after prop type
            # Try to find player name in market name
            player_name = None
            # Remove common prefixes/suffixes
            clean_name = market_name.replace(prop_type, "").strip()
            # Remove "over", "under", numbers, common words
            for word in ["over", "under", "o", "u", "total", "line"]:
                clean_name = clean_name.replace(word, "").strip()
            
            # Try to extract player name - it's usually a capitalized name
            # Look for words that look like names (capitalized, multiple words possible)
            words = clean_name.split()
            if words:
                # Player name is usually the longest meaningful word(s)
                # Filter out very short words and numbers
                candidate_words = [w for w in words if len(w) > 2 and not w.replace(".", "").isdigit()]
                if candidate_words:
                    # Take the first substantial word(s) as player name
                    player_name = " ".join(candidate_words[:2]).strip().title()
            
            # Fallback: try selection name if it's not just "Over"/"Under"
            if not player_name or len(player_name) < 3:
                sel_clean = selection_name.replace("over", "").replace("under", "").strip()
                if sel_clean and len(sel_clean) > 2:
                    player_name = sel_clean.title()
            
            if not player_name or prop_type is None:
                continue
            
            # Normalize selection to Over/Under
            selection = _norm(r.selection)
            if "over" in selection.lower():
                selection = "Over"
            elif "under" in selection.lower():
                selection = "Under"
            else:
                continue
            
            key = (player_name, prop_type, float(r.line))
            bucket = props_by_key.setdefault(key, {})
            if selection == "Over":
                bucket["over"] = float(_cost_to_win_1(float(r.odds)) or 0)
            elif selection == "Under":
                bucket["under"] = float(_cost_to_win_1(float(r.odds)) or 0)

        # Build PlayerPropOdds list
        player_props: list[PlayerPropOdds] = []
        for (player_name, prop_type, line), bucket in sorted(props_by_key.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2])):
            if "over" not in bucket or "under" not in bucket:
                continue
            if bucket["over"] <= 0 or bucket["under"] <= 0:
                continue
            player_props.append(
                PlayerPropOdds(
                    player_name=player_name,
                    prop_type=prop_type,
                    line=line,
                    outcome_1="Over",
                    outcome_2="Under",
                    outcome_1_cost_to_win_1=bucket["over"],
                    outcome_2_cost_to_win_1=bucket["under"],
                )
            )

        return player_props

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

