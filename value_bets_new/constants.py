from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from value_bets_new.polymarket import PolymarketEvent


class MarketType(Enum):
    MONEYLINE = "moneyline"
    SPREADS = "spreads"
    TOTALS = "totals"
    TOTALS_GAMES = "totals_games"
    TOTALS_SETS = "totals_sets"

class Sport(Enum):
    BASKETBALL = "basketball"
    HOCKEY = "hockey"
    TENNIS = "tennis"
    UFC = "ufc"
    SOCCER = "soccer"

@dataclass
class MarketOdds:
    token_id: str
    team_name: str
    best_bid: Optional[float]
    bid_volume: float
    best_ask: Optional[float]
    ask_volume: float
    spread: Optional[float]
    condition_id: Optional[str] = None
    
@dataclass(frozen=True)
class SportsbookOdds:
    """
    Base odds class for sportsbook markets.
    
    outcome_1/outcome_2 are the two sides of the bet.
    """
    outcome_1: str
    outcome_2: str
    outcome_1_cost_to_win_1: float
    outcome_2_cost_to_win_1: float
    point: Optional[float] = None
    
    def to_string(self, decimals: int = 6) -> str:
        decimals = max(0, int(decimals))
        fmt = f".{decimals}f"
        return (
            f"{self.outcome_1}: {format(self.outcome_1_cost_to_win_1, fmt)} to win $1 | "
            f"{self.outcome_2}: {format(self.outcome_2_cost_to_win_1, fmt)} to win $1"
        )

    def __str__(self) -> str:
        return self.to_string()
    
@dataclass(frozen=True)
class HandicapOdds(SportsbookOdds):
    """
    Spread/handicap odds.
    
    outcome_1/outcome_2 are team names.
    point is the handicap line for outcome_1 (outcome_2 has -point).
    """
    
    def to_string(self, decimals: int = 6) -> str:
        decimals = max(0, int(decimals))
        fmt = f".{decimals}f"
        pt = self.point if self.point is not None else 0
        return (
            f"{self.outcome_1} ({pt:+g}): {format(self.outcome_1_cost_to_win_1, fmt)} to win $1 | "
            f"{self.outcome_2} ({-pt:+g}): {format(self.outcome_2_cost_to_win_1, fmt)} to win $1"
        )


@dataclass(frozen=True)
class TotalOdds(SportsbookOdds):
    """
    Over/under totals odds.
    
    outcome_1 = "Over", outcome_2 = "Under".
    point is the total line.
    """
    
    def to_string(self, decimals: int = 6) -> str:
        decimals = max(0, int(decimals))
        fmt = f".{decimals}f"
        pt = self.point if self.point is not None else 0
        return (
            f"O/U {pt:g} | "
            f"Over: {format(self.outcome_1_cost_to_win_1, fmt)} to win $1 | "
            f"Under: {format(self.outcome_2_cost_to_win_1, fmt)} to win $1"
        )

@dataclass(frozen=True)
class ValueBet:
    team: str
    token_id: str
    true_prob: float
    polymarket_best_ask: float
    expected_payout_per_1: float  # expected payout for a $1 stake (gross, before fees)
    condition_id: Optional[str] = None

    def to_string(self, decimals: int = 4) -> str:
        fmt = f".{max(0, int(decimals))}f"
        return (
            f"{self.team}: polymarket_ask={format(self.polymarket_best_ask, fmt)}, "
            f"true_prob={format(self.true_prob, fmt)}, "
            f"expected_payout_per_$1={format(self.expected_payout_per_1, fmt)}"
        )
