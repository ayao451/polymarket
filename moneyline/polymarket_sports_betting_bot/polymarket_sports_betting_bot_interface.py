#!/usr/bin/env python3
"""
Polymarket Sports Betting Bot interface.

Provides a class-based API for running moneyline comparison:
- sportsbook (The Odds API) weighted moneyline odds
- Polymarket moneyline market data

This mirrors the behavior of `main.py`, but is usable as a library.
"""

from __future__ import annotations

import math
import sys
from typing import List, Optional, Tuple

from sportsbook_odds_service.sportsbook_weighted_odds_interface import (
    SportsbookWeightedOddsInterface,
)
from sportsbook_odds_service.fetch_game_odds import NBA_SPORT_KEY
from polymarket_odds_service.polymarket_odds_interface import PolymarketOddsInterface
from trade_executor.trade_executor_service import TradeExecutorService

from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import BUY

from cli_helpers import (
    print_polymarket_moneyline,
    print_sportsbook_odds,
    validate_input,
)

from .value_bet_service import ValueBet, ValueBetService


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
    def _print_value_bets_and_trade(team_a: str, team_b: str, sportsbook_result, polymarket_results):
        print("Value bets:")

        away_team, home_team = PolymarketSportsBettingBotInterface._resolve_away_home(
            team_a, team_b, sportsbook_result
        )
        bets = ValueBetService(
            away_team, home_team, sportsbook_result, polymarket_results
        ).discover_value_bets()

        if not bets:
            print("  (none)")
            return []

        trade_executor = TradeExecutorService()

        results = []
        for b in bets:
            print(f"  - {b.to_string()}")

            size_tokens, stake_usdc, f_star = PolymarketSportsBettingBotInterface._kelly(
                trade_executor=trade_executor,
                price=float(b.polymarket_best_ask),
                true_prob=float(b.true_prob),
            )
            if size_tokens <= 0:
                continue

            print(
                f"    kelly_f*={f_star:.4f}, stake≈{stake_usdc:.2f}, "
                f"price={float(b.polymarket_best_ask):.4f}, size={size_tokens}"
            )

            result = trade_executor.execute_trade(
                token_id=b.token_id,
                side=BUY,
                price=float(b.polymarket_best_ask),
                size=float(size_tokens),
                order_type=OrderType.FOK,
                team=b.team,
                game=f"{away_team} @ {home_team}",
                expected_payout_per_1=b.expected_payout_per_1,
            )
            results.append(result)
            if result.ok:
                print(f"    trade response: {result.response}")
            else:
                print(f"    trade failed: {result.error}")
        return results

    @staticmethod
    def _kelly(
        *,
        trade_executor: TradeExecutorService,
        price: float,
        true_prob: float,
        fallback_bankroll: float = 1.0,
    ) -> Tuple[int, float, float]:
        """
        Full-Kelly sizing for a $1 payout token bought at `price`.

        f* = (p - x) / (1 - x)
          where p=true_prob, x=price.

        Returns:
            (size_tokens, stake_usdc, f_star)
        """
        bankroll = trade_executor.get_usdc_balance()
        if bankroll is None:
            print("Warning: Could not fetch USDC bankroll; falling back to ~$1 sizing.")
            bankroll = float(fallback_bankroll)
        else:
            print(f"USDC Bankroll: {float(bankroll):.2f}")

        bankroll = float(bankroll)
        price = float(price)
        p = float(true_prob)

        if bankroll <= 0 or not math.isfinite(bankroll):
            return 0, 0.0, 0.0
        if price <= 0 or price >= 1 or not math.isfinite(price):
            return 0, 0.0, 0.0
        if p <= 0 or p >= 1 or not math.isfinite(p):
            return 0, 0.0, 0.0

        denom = 1.0 - price
        f_star = (p - price) / denom if denom > 0 else 0.0
        if not math.isfinite(f_star) or f_star <= 0.0:
            return 0, 0.0, 0.0
        if f_star > 1.0:
            f_star = 1.0

        stake = bankroll * f_star  # full Kelly
        if stake <= 0.0:
            return 0, 0.0, float(f_star)

        size_tokens = math.floor(stake / price)
        if size_tokens <= 0:
            return 0, float(stake), float(f_star)

        return int(size_tokens), float(stake), float(f_star)

    def run_nba_moneyline(
        self, argv: Optional[List[str]] = None, *, sport_key: str = NBA_SPORT_KEY
    ) -> int:
        """
        Fetch and print sportsbook moneyline odds + Polymarket moneyline odds for a game.

        Args:
            argv: Optional argv list (defaults to sys.argv). Expected:
                  <team_a> <team_b> [YYYY-MM-DD]
            sport_key: The Odds API sport key (defaults to NBA).

        Returns:
            Process-style exit code (0 success, non-zero failure).
        """
        if argv is None:
            argv = sys.argv

        # Exposed for callers like `moneyline/main.py` to avoid re-running events that
        # were already successfully traded in this process.
        self.last_run_trade_results = []
        self.last_run_had_successful_trade = False

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
        sportsbook_result = sportsbook.get_moneyline_odds(
            team_a, team_b, play_date, sport_key=sport_key
        )

        # Polymarket moneyline
        away_team, home_team = self._resolve_away_home(team_a, team_b, sportsbook_result)
        polymarket = PolymarketOddsInterface(away_team, home_team, play_date)
        polymarket_results = polymarket.get_moneyline_odds()

        # Display
        print_sportsbook_odds(sportsbook_result)
        print_polymarket_moneyline(polymarket_results)
        self.last_run_trade_results = self._print_value_bets_and_trade(
            team_a, team_b, sportsbook_result, polymarket_results
        )
        self.last_run_had_successful_trade = any(
            getattr(r, "ok", False) for r in (self.last_run_trade_results or [])
        )

        return 0

    # Backwards-compatible alias (name no longer NBA-specific).
    def run_moneyline(self, argv: Optional[List[str]] = None, *, sport_key: str = NBA_SPORT_KEY) -> int:
        return self.run_nba_moneyline(argv, sport_key=sport_key)


if __name__ == "__main__":
    raise SystemExit(PolymarketSportsBettingBotInterface().run_moneyline())


