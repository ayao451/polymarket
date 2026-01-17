#!/usr/bin/env python3
"""
Sportsbook odds classes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


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
