from __future__ import annotations

import math
from typing import Optional

from value_bets_new.constants import MarketOdds, SportsbookOdds, ValueBet


class EventProcessor:
    def __init__(self) -> None:
        self.min_true_prob = 0.05
        self.min_expected_payout_per_1 = 1.02
        self.max_expected_payout_per_1 = 1.10

    def process_two_outcome_event(self, team_name : str, polymarket_odds: MarketOdds, sportsbook_odds: SportsbookOdds ) -> Optional[ValueBet]:
        print(f"[DEBUG] [EventProcessor] Processing value bet evaluation for team: {team_name}")
        print(f"[DEBUG] [EventProcessor] Polymarket best_ask: {polymarket_odds.best_ask}")
        
        if polymarket_odds.best_ask is None or polymarket_odds.best_ask <= 0:
            print(f"[DEBUG] [EventProcessor] REJECTED: best_ask is None or <= 0")
            return None
        
        # Calculate true probability for this team
        print(f"[DEBUG] [EventProcessor] Calculating true probability for team: {team_name}")
        print(f"[DEBUG] [EventProcessor] Sportsbook outcome_1: {sportsbook_odds.outcome_1} (cost_to_win_1: {sportsbook_odds.outcome_1_cost_to_win_1})")
        print(f"[DEBUG] [EventProcessor] Sportsbook outcome_2: {sportsbook_odds.outcome_2} (cost_to_win_1: {sportsbook_odds.outcome_2_cost_to_win_1})")
        p_true = self._true_prob_for_outcome(team_name, sportsbook_odds)
        print(f"[DEBUG] [EventProcessor] Calculated true probability: {p_true}")
        
        if p_true is None:
            print(f"[DEBUG] [EventProcessor] REJECTED: p_true is None (team name didn't match)")
            print("\n\n\n")
            print(f"[DEBUG] [EventProcessor] Team name from Polymarket: '{team_name}'")
            print(f"[DEBUG] [EventProcessor] Sportsbook outcome_1: '{sportsbook_odds.outcome_1}'")
            print(f"[DEBUG] [EventProcessor] Sportsbook outcome_2: '{sportsbook_odds.outcome_2}'")
            print("\n\n\n")
            return None
        
        if p_true < self.min_true_prob:
            print(f"[DEBUG] [EventProcessor] REJECTED: p_true ({p_true:.4f}) < min_true_prob ({self.min_true_prob})")
            return None
        
        payout_per_1 = 1.0 / polymarket_odds.best_ask  # $ payout if the $1 stake wins
        print(f"[DEBUG] [EventProcessor] Payout per $1: {payout_per_1:.4f}")
        expected_payout = float(p_true) * float(payout_per_1)
        print(f"[DEBUG] [EventProcessor] Expected payout: {expected_payout:.4f} (p_true={p_true:.4f} * payout_per_1={payout_per_1:.4f})")
        
        if expected_payout < self.min_expected_payout_per_1:
            print(f"[DEBUG] [EventProcessor] REJECTED: expected_payout ({expected_payout:.4f}) < min_expected_payout_per_1 ({self.min_expected_payout_per_1})")
            return None
        
        if expected_payout > self.max_expected_payout_per_1:
            print(f"[DEBUG] [EventProcessor] REJECTED: expected_payout ({expected_payout:.4f}) > max_expected_payout_per_1 ({self.max_expected_payout_per_1})")
            return None

        print(f"[DEBUG] [EventProcessor] VALUE BET ACCEPTED! Creating ValueBet object...")
        value_bet = ValueBet(
            team=team_name,
            token_id=polymarket_odds.token_id,
            true_prob=float(p_true),
            polymarket_best_ask=polymarket_odds.best_ask,
            expected_payout_per_1=expected_payout,
            condition_id=polymarket_odds.condition_id,
        )
        return value_bet


    def _true_prob_for_outcome(self, team_name: str, sportsbook_odds: SportsbookOdds) -> Optional[float]:
        print(f"[DEBUG] [EventProcessor] _true_prob_for_outcome: team_name={team_name}")
        print(f"[DEBUG] [EventProcessor] Devigging odds: q1={sportsbook_odds.outcome_1_cost_to_win_1}, q2={sportsbook_odds.outcome_2_cost_to_win_1}")
        devigged_odds = self._devig(sportsbook_odds.outcome_1_cost_to_win_1, sportsbook_odds.outcome_2_cost_to_win_1)
        if devigged_odds is None:
            print(f"[DEBUG] [EventProcessor] Devig failed - returned None")
            return None
        p_outcome_1, p_outcome_2 = devigged_odds
        print(f"[DEBUG] [EventProcessor] Devigged probabilities: p_outcome_1={p_outcome_1:.4f}, p_outcome_2={p_outcome_2:.4f}")
        
        def _team_matches(t1: str, t2: str) -> bool:
            """Fuzzy team name matching."""
            n1 = " ".join(t1.lower().strip().split())
            n2 = " ".join(t2.lower().strip().split())
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
        
        # Determine which probability corresponds to the team using fuzzy matching
        print(f"[DEBUG] [EventProcessor] Checking if '{team_name}' matches outcome_1: '{sportsbook_odds.outcome_1}'")
        match_1 = _team_matches(team_name, sportsbook_odds.outcome_1)
        print(f"[DEBUG] [EventProcessor] Match with outcome_1: {match_1}")
        
        if match_1:
            print(f"[DEBUG] [EventProcessor] Returning p_outcome_1: {p_outcome_1:.4f}")
            return p_outcome_1
        
        print(f"[DEBUG] [EventProcessor] Checking if '{team_name}' matches outcome_2: '{sportsbook_odds.outcome_2}'")
        match_2 = _team_matches(team_name, sportsbook_odds.outcome_2)
        print(f"[DEBUG] [EventProcessor] Match with outcome_2: {match_2}")
        
        if match_2:
            print(f"[DEBUG] [EventProcessor] Returning p_outcome_2: {p_outcome_2:.4f}")
            return p_outcome_2
        
        print(f"[DEBUG] [EventProcessor] No match found for team '{team_name}' with either outcome")
        return None


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