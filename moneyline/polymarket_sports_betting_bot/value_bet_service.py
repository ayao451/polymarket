#!/usr/bin/env python3
"""
Value bet discovery service.

A value bet exists when the expected payout exceeds the stake ($1).

We compute "true" (no-vig) probabilities from the sportsbook's two-way odds using
the Power de-vig method, then compare that to the Polymarket price.

If you buy 1 token at price `x` (Polymarket best ask), payout is $1 if it wins.
So the expected payout for a $1 stake (buying ~1/x tokens) is:

    expected_payout = p_true * (1 / x)

If expected_payout > 1, the bet has positive expected value.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional

from sportsbook_odds_service.sportsbook_weighted_odds_interface import MoneylineOdds
from polymarket_odds_service.polymarket_market_analyzer import MarketOdds


@dataclass(frozen=True)
class ValueBet:
    team: str
    token_id: str
    true_prob: float
    polymarket_best_ask: float
    expected_payout_per_1: float  # expected payout for a $1 stake (gross, before fees)

    def to_string(self, decimals: int = 4) -> str:
        fmt = f".{max(0, int(decimals))}f"
        return (
            f"{self.team}: polymarket_ask={format(self.polymarket_best_ask, fmt)}, "
            f"true_prob={format(self.true_prob, fmt)}, "
            f"expected_payout_per_$1={format(self.expected_payout_per_1, fmt)}"
        )


class ValueBetService:
    MIN_TRUE_PROB = 0.05  # don't bet extreme longshots (<5% true win probability)
    MIN_EXPECTED_PAYOUT_PER_1 = 1.01  # require >1% expected edge on $1 stake

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

    @staticmethod
    def _devig(q1: float, q2: float) -> Optional[tuple[float, float]]:
        """
        De-vig for a 2-outcome market using the standard proportional method.

        Inputs q1,q2 are the raw implied probabilities (with vig), e.g. q=1/decimal_odds.
        We normalize them so they sum to 1:

            p1 = q1 / (q1 + q2)
            p2 = q2 / (q1 + q2)

        This matches the "normalize implied probabilities by total overround" method.
        """
        try:
            q1 = float(q1)
            q2 = float(q2)
        except (TypeError, ValueError):
            return None
        if not (math.isfinite(q1) and math.isfinite(q2)):
            return None
        if q1 <= 0 or q2 <= 0:
            return None
        total = q1 + q2
        if total <= 0:
            return None
        return (q1 / total), (q2 / total)

    def _true_prob_for_outcome(self, outcome_team: str) -> Optional[float]:
        """
        Return the no-vig (true) probability for this outcome (away/home), or None if unknown.
        """
        if self.sportsbook_result is None:
            return None

        sb = self.sportsbook_result
        devigged = self._devig(sb.away_cost_to_win_1, sb.home_cost_to_win_1)
        if devigged is None:
            return None
        p_away, p_home = devigged

        if self._team_matches_outcome(self.away_team, outcome_team):
            return float(p_away)
        if self._team_matches_outcome(self.home_team, outcome_team):
            return float(p_home)
        return None

    def discover_value_bets(self) -> List[ValueBet]:
        """
        Discover value bets between sportsbook (de-vigged) and Polymarket moneyline.
        """
        value_bets: List[ValueBet] = []

        for m in self.polymarket_results:
            if m.best_ask is None:
                continue

            outcome_team = self._extract_outcome_team(m.market)
            if not outcome_team:
                continue

            p_true = self._true_prob_for_outcome(outcome_team)
            if p_true is None:
                continue
            if float(p_true) < self.MIN_TRUE_PROB:
                continue

            polymarket_ask = float(m.best_ask)
            if polymarket_ask <= 0:
                continue

            payout_per_1 = 1.0 / polymarket_ask  # $ payout if the $1 stake wins
            expected_payout = float(p_true) * float(payout_per_1)

            # Value bet if expected payout exceeds threshold (stake is $1).
            if expected_payout > self.MIN_EXPECTED_PAYOUT_PER_1:
                value_bets.append(
                    ValueBet(
                        team=outcome_team,
                        token_id=m.token_id,
                        true_prob=float(p_true),
                        polymarket_best_ask=polymarket_ask,
                        expected_payout_per_1=expected_payout,
                    )
                )

        # Sort by highest expected profit per $1 stake.
        return sorted(
            value_bets, key=lambda vb: (vb.expected_payout_per_1 - 1.0), reverse=True
        )


