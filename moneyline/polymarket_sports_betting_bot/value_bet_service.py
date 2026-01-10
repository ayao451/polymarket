#!/usr/bin/env python3
"""
Value bet discovery service.

A value bet exists when Polymarket's best ask for a side is at least `min_edge`
cheaper than the sportsbook quote for that same side.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from sportsbook_odds_service.sportsbook_weighted_odds_interface import MoneylineOdds
from polymarket_odds_service.polymarket_market_analyzer import MarketOdds


@dataclass(frozen=True)
class ValueBet:
    team: str
    token_id: str
    sportsbook_cost_to_win_1: float
    polymarket_best_ask: float
    edge: float  # sportsbook_cost_to_win_1 - polymarket_best_ask

    def to_string(self, decimals: int = 4) -> str:
        fmt = f".{max(0, int(decimals))}f"
        return (
            f"{self.team}: polymarket_ask={format(self.polymarket_best_ask, fmt)}, "
            f"sportsbook={format(self.sportsbook_cost_to_win_1, fmt)}, "
            f"edge={format(self.edge, fmt)}"
        )


class ValueBetService:
    def __init__(
        self,
        away_team: str,
        home_team: str,
        sportsbook_result: Optional[MoneylineOdds],
        polymarket_results: List[MarketOdds],
    ) -> None:
        self.away_team = away_team
        self.home_team = home_team
        self.sportsbook_result = sportsbook_result
        self.polymarket_results = polymarket_results

    @staticmethod
    def _normalize_team_name(s: str) -> str:
        return " ".join(str(s).strip().lower().split())

    @staticmethod
    def _team_matches_outcome(team_name: str, outcome_team_name: str) -> bool:
        """
        Polymarket outcomes are often just the nickname (e.g. "Heat") while the sportsbook
        side may be the full name (e.g. "Miami Heat"). Consider them a match if:
        - exact normalized match, OR
        - normalized outcome equals the last word of full team name, OR
        - outcome is contained in full name (rare but safe)
        """
        full_key = ValueBetService._normalize_team_name(team_name)
        outcome_key = ValueBetService._normalize_team_name(outcome_team_name)
        if not full_key or not outcome_key:
            return False

        if full_key == outcome_key:
            return True

        full_last = full_key.split()[-1]
        if outcome_key == full_last:
            return True

        if outcome_key in full_key:
            return True

        return False

    @staticmethod
    def _extract_outcome_team(market_label: str) -> Optional[str]:
        """
        Parse Polymarket market label like "Bulls vs. Pistons (Bulls)" -> "Bulls".
        """
        if not market_label:
            return None
        if "(" not in market_label or ")" not in market_label:
            return None
        inside = market_label.split("(", 1)[1].rsplit(")", 1)[0].strip()
        return inside or None

    def _sportsbook_quote_for_outcome(self, outcome_team: str) -> Optional[float]:
        """
        Return sportsbook cost_to_win_1 for this outcome team (away/home), or None if unknown.
        """
        if self.sportsbook_result is None:
            return None

        sportsbook = self.sportsbook_result
        if self._team_matches_outcome(self.away_team, outcome_team):
            return float(sportsbook.away_cost_to_win_1)
        if self._team_matches_outcome(self.home_team, outcome_team):
            return float(sportsbook.home_cost_to_win_1)
        return None

    def discover_value_bets(self, min_edge: float = 0.02) -> List[ValueBet]:
        """
        Discover value bets between sportsbook and Polymarket moneyline.

        A value bet exists when:
            polymarket_best_ask <= sportsbook_cost_to_win_1 - min_edge
        """
        min_edge = float(min_edge)
        if min_edge < 0:
            min_edge = 0.0

        value_bets: List[ValueBet] = []

        for m in self.polymarket_results:
            if m.best_ask is None:
                continue

            outcome_team = self._extract_outcome_team(m.market)
            if not outcome_team:
                continue

            sportsbook_quote = self._sportsbook_quote_for_outcome(outcome_team)
            if sportsbook_quote is None:
                continue

            polymarket_ask = float(m.best_ask)
            edge = sportsbook_quote - polymarket_ask
            if edge >= min_edge:
                value_bets.append(
                    ValueBet(
                        team=outcome_team,
                        token_id=m.token_id,
                        sportsbook_cost_to_win_1=sportsbook_quote,
                        polymarket_best_ask=polymarket_ask,
                        edge=edge,
                    )
                )

        return sorted(value_bets, key=lambda vb: vb.edge, reverse=True)


