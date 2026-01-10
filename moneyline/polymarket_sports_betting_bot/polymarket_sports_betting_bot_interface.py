#!/usr/bin/env python3
"""
Polymarket Sports Betting Bot interface.

Provides a class-based API for running NBA moneyline comparison:
- sportsbook (The Odds API) weighted moneyline odds
- Polymarket moneyline market data

This mirrors the behavior of `main.py`, but is usable as a library.
"""

from __future__ import annotations

import sys
from typing import List, Optional

from sportsbook_odds_service.sportsbook_weighted_odds_interface import (
    SportsbookWeightedOddsInterface,
)
from polymarket_odds_service.polymarket_odds_interface import PolymarketOddsInterface
from trade_executor.trade_executor_service import TradeExecutorService

from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import BUY

from cli_helpers import (
    print_polymarket_moneyline,
    print_sportsbook_odds,
    validate_input,
)

from .value_bet_service import ValueBetService


class PolymarketSportsBettingBotInterface:
    """
    Bot interface wrapper.

    Keeps the orchestration in one place and delegates validation + display to helpers.
    """

    @staticmethod
    def _resolve_away_home(team_a: str, team_b: str, sportsbook_result) -> tuple[str, str]:
        """
        Prefer sportsbook's (away, home) labeling when available; otherwise fall back
        to the CLI input order.
        """
        if sportsbook_result is not None:
            return sportsbook_result.away_team, sportsbook_result.home_team
        return team_a, team_b

    @staticmethod
    def _print_value_bets_and_trade(team_a: str, team_b: str, sportsbook_result, polymarket_results) -> None:
        print("Value bets:")

        away_team, home_team = PolymarketSportsBettingBotInterface._resolve_away_home(
            team_a, team_b, sportsbook_result
        )
        bets = ValueBetService(
            away_team, home_team, sportsbook_result, polymarket_results
        ).discover_value_bets()

        if not bets:
            print("  (none)")
            return

        trade_executor = TradeExecutorService()
        for b in bets:
            print(f"  - {b.to_string()}")
            result = trade_executor.execute_trade(
                token_id=b.token_id,
                side=BUY,
                price=b.polymarket_best_ask,
                size=1.0,
                order_type=OrderType.FOK,
            )
            if result.ok:
                print(f"    trade response: {result.response}")
            else:
                print(f"    trade failed: {result.error}")

    def run_nba_moneyline(self, argv: Optional[List[str]] = None) -> int:
        """
        Fetch and print sportsbook moneyline odds + Polymarket moneyline odds for an NBA game.

        Args:
            argv: Optional argv list (defaults to sys.argv). Expected:
                  <team_a> <team_b> [YYYY-MM-DD]

        Returns:
            Process-style exit code (0 success, non-zero failure).
        """
        if argv is None:
            argv = sys.argv

        print("Checking environment configuration...")
        args = validate_input(argv)
        if args is None:
            return 1
        print("✓ Environment configured\n")

        team_a = args.team_a
        team_b = args.team_b
        play_date = args.play_date

        # Sportsbook moneyline
        sportsbook = SportsbookWeightedOddsInterface()
        sportsbook_result = sportsbook.get_moneyline_odds(team_a, team_b, play_date)

        # Polymarket moneyline
        polymarket = PolymarketOddsInterface(team_a, team_b, play_date)
        polymarket_results = polymarket.get_moneyline_odds()

        # Display
        print_sportsbook_odds(sportsbook_result)
        print_polymarket_moneyline(polymarket_results)
        self._print_value_bets_and_trade(team_a, team_b, sportsbook_result, polymarket_results)

        return 0


if __name__ == "__main__":
    raise SystemExit(PolymarketSportsBettingBotInterface().run_nba_moneyline())


