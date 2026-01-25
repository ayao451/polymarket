from __future__ import annotations

import math
from typing import Optional

from value_bets_new.constants import MarketOdds, SportsbookOdds, ValueBet


class EventProcessor:
    def __init__(self) -> None:
        self.min_true_prob = 0.05
        self.max_expected_payout_per_1 = 1.10

    def process_two_outcome_event(self, team_name : str, polymarket_odds: MarketOdds, sportsbook_odds: SportsbookOdds ) -> Optional[ValueBet]:

        p_true = 1
        payout_per_1 = 1.0 / polymarket_odds.best_ask  # $ payout if the $1 stake wins
        expected_payout = float(p_true) * float(payout_per_1)
        
        if p_true is None or p_true < self.min_true_prob or polymarket_odds.best_ask <= 0:
            return None
        
        payout_per_1 = 1.0 / polymarket_odds.best_ask  # $ payout if the $1 stake wins
        expected_payout = float(p_true) * float(payout_per_1)
        
        if expected_payout > self.max_expected_payout_per_1:
            print("Expected payout exceeds maximum")
            return None

        return ValueBet(
            team=team_name,
            token_id=polymarket_odds.token_id,
            true_prob=float(p_true),
            polymarket_best_ask=polymarket_odds.best_ask,
            expected_payout_per_1=expected_payout,
            condition_id=polymarket_odds.condition_id,
        )


    def _true_prob_for_outcome(self, team_name: str, sportsbook_odds) -> Optional[float]:
        devigged_odds = self._devig(sportsbook_odds.outcome_1_cost_to_win_1, sportsbook_odds.outcome_2_cost_to_win_1)
        if devigged_odds is None:
            return None
        p_outcome_1, p_outcome_2 = devigged_odds


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