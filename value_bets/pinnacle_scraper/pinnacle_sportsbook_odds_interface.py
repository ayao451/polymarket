#!/usr/bin/env python3
"""
Pinnacle sportsbook odds adapter for the Polymarket bot.

This replaces `sportsbook_odds_service/*` (The Odds API) by using Pinnacle's
Arcadia guest endpoints via `pinnacle_odds_service.PinnacleBasketballOddsService`.

It exposes the same high-level dataclasses previously consumed by the bot:
  - MoneylineOdds
  - SpreadOdds
  - TotalsOdds
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

from .pinnacle_odds_service import PinnacleBasketballOddsService


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _cost_to_win_1(decimal_odds: float) -> Optional[float]:
    try:
        d = float(decimal_odds)
    except Exception:
        return None
    if d <= 0:
        return None
    return 1.0 / d


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


class PinnacleSportsbookOddsInterface:
    """
    Replacement for SportsbookWeightedOddsInterface, backed by Pinnacle.
    """

    def __init__(self, *, timeout_ms: int = 45000) -> None:
        self._svc = PinnacleBasketballOddsService(timeout_ms=timeout_ms)

    def get_moneyline_spread_totals_odds(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
        decimals: int = 6,
        sport_key: str = "PINNACLE",
    ) -> tuple[Optional[MoneylineOdds], list[SpreadOdds], list[TotalsOdds]]:
        # sport_key is accepted for backward compatibility but not used.
        _ = (decimals, sport_key)

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
            return None, [], []

        try:
            res = self._svc.get_game_odds(match.matchup_id, game_info=match)
        except RuntimeError as e:
            # Arcadia can intermittently fail for specific matchups; don't fail the whole run.
            print(
                f"Warning: failed to fetch Pinnacle odds for {match.away_team} @ {match.home_team} "
                f"(matchup_id={match.matchup_id}): {e}"
            )
            return None, [], []
        # Only full-game (period=0) markets are used for Polymarket matching.
        rows = [r for r in (res.markets or []) if int(r.period or 0) == 0 and not bool(r.is_alternate or False)]

        away_team = match.away_team
        home_team = match.home_team

        # Moneyline
        ml_home = next((r for r in rows if r.market_type == "moneyline" and _norm(r.selection) == _norm(home_team)), None)
        ml_away = next((r for r in rows if r.market_type == "moneyline" and _norm(r.selection) == _norm(away_team)), None)
        moneyline: Optional[MoneylineOdds] = None
        if ml_home and ml_away and ml_home.odds and ml_away.odds:
            hc = _cost_to_win_1(float(ml_home.odds))
            ac = _cost_to_win_1(float(ml_away.odds))
            if hc is not None and ac is not None:
                moneyline = MoneylineOdds(
                    away_team=away_team,
                    home_team=home_team,
                    away_cost_to_win_1=float(ac),
                    home_cost_to_win_1=float(hc),
                )

        # Spreads: group by abs(line) and require both teams
        spreads_by_abs: dict[float, dict[str, tuple[float, float]]] = {}
        # value: { "away": (point, cost), "home": (point, cost) }
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

        spreads: list[SpreadOdds] = []
        for _abs, bucket in sorted(spreads_by_abs.items(), key=lambda kv: kv[0]):
            if "away" not in bucket or "home" not in bucket:
                continue
            away_pt, away_cost = bucket["away"]
            home_pt, home_cost = bucket["home"]
            spreads.append(
                SpreadOdds(
                    away_team=away_team,
                    home_team=home_team,
                    away_point=float(away_pt),
                    home_point=float(home_pt),
                    away_cost_to_win_1=float(away_cost),
                    home_cost_to_win_1=float(home_cost),
                )
            )

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
            key = round(pt, 2)
            bucket = totals_by_pt.setdefault(key, {})
            if _norm(r.selection) == "over":
                bucket["over"] = float(cost)
            elif _norm(r.selection) == "under":
                bucket["under"] = float(cost)

        totals: list[TotalsOdds] = []
        for pt, bucket in sorted(totals_by_pt.items(), key=lambda kv: kv[0]):
            if "over" not in bucket or "under" not in bucket:
                continue
            totals.append(
                TotalsOdds(
                    away_team=away_team,
                    home_team=home_team,
                    total_point=float(pt),
                    over_cost_to_win_1=float(bucket["over"]),
                    under_cost_to_win_1=float(bucket["under"]),
                )
            )

        return moneyline, spreads, totals

