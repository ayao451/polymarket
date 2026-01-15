#!/usr/bin/env python3
"""
Polymarket Sports Betting Bot interface.

Provides a class-based API for running moneyline comparison:
- sportsbook (Pinnacle) moneyline/spread/totals odds
- Polymarket moneyline market data

This mirrors the behavior of `value_bets.py`, but is usable as a library.
"""

from __future__ import annotations

import math
import sys
from typing import List, Optional, Tuple

from pinnacle_scraper.pinnacle_sportsbook_odds_interface import PinnacleSportsbookOddsInterface
from polymarket_odds_service.polymarket_odds_interface import PolymarketOddsInterface
from trade_executor.trade_executor_service import TradeExecutorService

from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import BUY

from cli_helpers import (
    print_polymarket_moneyline,
    print_sportsbook_odds,
    print_sportsbook_spread_odds,
    print_sportsbook_totals_odds,
    validate_input,
)

from .value_bet_service import (
    SpreadValueBetService,
    TotalsValueBetService,
    ValueBetService,
)


class PolymarketSportsBettingBotInterface:
    """
    Bot interface wrapper.

    Keeps the orchestration in one place and delegates validation + display to helpers.
    """

    def __init__(self, *, enable_trading: bool = True) -> None:
        # When False (e.g. --testing), we never submit orders; we only print what we would do.
        self.enable_trading = bool(enable_trading)
        # Track best "edge" (expected_payout_per_$1 - 1) per event (this process only).
        # Used to prevent repeated trades on the same game unless the new trade is strictly better.
        self._best_edge_by_event_key: dict[str, float] = {}

    @staticmethod
    def _trade_event_key(*, away_team: str, home_team: str, play_date, sport_key: str) -> str:
        """
        Stable-ish key for "this game event" within this process.
        """
        date_part = ""
        try:
            date_part = play_date.isoformat() if play_date is not None else ""
        except Exception:
            date_part = str(play_date or "")
        return f"{sport_key}:{date_part}:{away_team} @ {home_team}"

    def _best_edge_for_event(self, event_key: str) -> float:
        try:
            return float(self._best_edge_by_event_key.get(event_key, float("-inf")))
        except Exception:
            return float("-inf")

    def _record_successful_trade_edge(self, event_key: str, *, expected_payout_per_1: float) -> None:
        edge = float(expected_payout_per_1) - 1.0
        prev = self._best_edge_for_event(event_key)
        if edge > prev:
            self._best_edge_by_event_key[event_key] = edge

    @staticmethod
    def _resolve_away_home(team_a: str, team_b: str, sportsbook_result) -> tuple[str, str]:
        """
        Prefer sportsbook's (away, home) labeling when available; otherwise fall back
        to the CLI input order.
        """
        if sportsbook_result is not None:
            return sportsbook_result.away_team, sportsbook_result.home_team
        return team_a, team_b

    def _print_value_bets_and_trade(
        self,
        team_a: str,
        team_b: str,
        sportsbook_result,
        polymarket_results,
        *,
        do_trade: bool,
        event_key: str,
    ):
        print("Value bets:")

        away_team, home_team = self._resolve_away_home(
            team_a, team_b, sportsbook_result
        )
        bets = ValueBetService(
            away_team, home_team, sportsbook_result, polymarket_results
        ).discover_value_bets()

        if not bets:
            print("  (none)")
            return []

        if not do_trade:
            for b in bets:
                print(f"  - WOULD BUY -> {b.to_string()} token_id={b.token_id}")
            return []

        trade_executor = TradeExecutorService()

        results = []
        for b in bets:
            edge = float(b.expected_payout_per_1) - 1.0
            best_edge = self._best_edge_for_event(event_key)
            if edge <= best_edge:
                print(
                    f"  - SKIP (edge={edge:.4f} <= best_edge={best_edge:.4f} for this game) -> {b.to_string()}"
                )
                continue

            print(f"  - {b.to_string()} (edge={edge:.4f}, best_edge={best_edge:.4f})")

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
                self._record_successful_trade_edge(
                    event_key, expected_payout_per_1=float(b.expected_payout_per_1)
                )
                print(f"    trade response: {result.response}")
            else:
                print(f"    trade failed: {result.error}")
        return results

    def _print_spread_value_bets_and_trade(
        self,
        *,
        away_team: str,
        home_team: str,
        sportsbook_spreads,
        polymarket_spreads,
        do_trade: bool,
        event_key: str,
    ):
        """
        Print (and optionally execute) spread value bets.
        """
        print("Spread value bets:")

        spread_service = SpreadValueBetService(
            sportsbook_spreads=sportsbook_spreads,
            polymarket_spread_results=polymarket_spreads,
        )
        bets = spread_service.discover_value_bets()

        if not bets:
            print("  (none)")
            return []

        if not do_trade:
            for b in bets:
                team_label = f"{b.team} ({b.point:+g})"
                print(
                    f"  - WOULD BUY -> {team_label}: polymarket_ask={b.polymarket_best_ask:.4f}, "
                    f"true_prob={b.true_prob:.4f}, expected_payout_per_$1={b.expected_payout_per_1:.4f} "
                    f"token_id={b.token_id}"
                )
            return []

        trade_executor = TradeExecutorService()
        results = []
        for b in bets:
            team_label = f"{b.team} ({b.point:+g})"
            edge = float(b.expected_payout_per_1) - 1.0
            best_edge = self._best_edge_for_event(event_key)
            if edge <= best_edge:
                print(
                    f"  - SKIP (edge={edge:.4f} <= best_edge={best_edge:.4f} for this game) -> "
                    f"{team_label}: polymarket_ask={b.polymarket_best_ask:.4f}, "
                    f"true_prob={b.true_prob:.4f}, expected_payout_per_$1={b.expected_payout_per_1:.4f}"
                )
                continue

            print(
                f"  - {team_label}: polymarket_ask={b.polymarket_best_ask:.4f}, "
                f"true_prob={b.true_prob:.4f}, expected_payout_per_$1={b.expected_payout_per_1:.4f} "
                f"(edge={edge:.4f}, best_edge={best_edge:.4f})"
            )

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
                team=team_label,
                game=f"{away_team} @ {home_team}",
                expected_payout_per_1=b.expected_payout_per_1,
            )
            results.append(result)
            if result.ok:
                self._record_successful_trade_edge(
                    event_key, expected_payout_per_1=float(b.expected_payout_per_1)
                )
                print(f"    trade response: {result.response}")
            else:
                print(f"    trade failed: {result.error}")

        return results

    def _print_totals_value_bets_and_trade(
        self,
        *,
        away_team: str,
        home_team: str,
        sportsbook_totals,
        polymarket_totals,
        do_trade: bool,
        event_key: str,
    ):
        print("Totals value bets:")

        service = TotalsValueBetService(
            sportsbook_totals=sportsbook_totals,
            polymarket_totals_results=polymarket_totals,
        )
        bets = service.discover_value_bets()

        if not bets:
            print("  (none)")
            return []

        if not do_trade:
            for b in bets:
                print(f"  - WOULD BUY -> {b.to_string()} token_id={b.token_id}")
            return []

        trade_executor = TradeExecutorService()
        results = []
        for b in bets:
            edge = float(b.expected_payout_per_1) - 1.0
            best_edge = self._best_edge_for_event(event_key)
            if edge <= best_edge:
                print(
                    f"  - SKIP (edge={edge:.4f} <= best_edge={best_edge:.4f} for this game) -> {b.to_string()}"
                )
                continue

            print(f"  - {b.to_string()} (edge={edge:.4f}, best_edge={best_edge:.4f})")

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

            team_label = f"{b.side} {b.total_point:g}"
            result = trade_executor.execute_trade(
                token_id=b.token_id,
                side=BUY,
                price=float(b.polymarket_best_ask),
                size=float(size_tokens),
                order_type=OrderType.FOK,
                team=team_label,
                game=f"{away_team} @ {home_team}",
                expected_payout_per_1=b.expected_payout_per_1,
            )
            results.append(result)
            if result.ok:
                self._record_successful_trade_edge(
                    event_key, expected_payout_per_1=float(b.expected_payout_per_1)
                )
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
        self, argv: Optional[List[str]] = None, *, sport_key: str = "PINNACLE"
    ) -> int:
        """
        Fetch and print sportsbook moneyline odds + Polymarket moneyline odds for a game.

        Args:
            argv: Optional argv list (defaults to sys.argv). Expected:
                  <team_a> <team_b> [YYYY-MM-DD]
            sport_key: kept for backward compatibility (ignored; Pinnacle is used).

        Returns:
            Process-style exit code (0 success, non-zero failure).
        """
        if argv is None:
            argv = sys.argv

        # Exposed for callers like `value_bets/value_bets.py` to avoid re-running events that
        # were already successfully traded in this process.
        self.last_run_trade_results = []
        self.last_run_had_successful_trade = False
        self.last_run_spread_trade_results = []
        self.last_run_totals_trade_results = []

        print("Checking environment configuration...")
        args = validate_input(argv)
        if args is None:
            return 1
        print("✓ Environment configured\n")

        # Support a lightweight "do not trade" mode for safety.
        # Note: the flag is primarily handled in `value_bets/value_bets.py`, but we also
        # accept it here in case callers invoke the bot directly.
        enable_trading = self.enable_trading and ("--testing" not in (argv or []))

        team_a = args.team_a
        team_b = args.team_b
        play_date = args.play_date

        # Sportsbook odds (Pinnacle)
        sportsbook = PinnacleSportsbookOddsInterface()
        sportsbook_result, sportsbook_spreads, sportsbook_totals = sportsbook.get_moneyline_spread_totals_odds(
            team_a, team_b, play_date, sport_key=sport_key
        )

        # Polymarket moneyline + spreads + totals
        away_team, home_team = self._resolve_away_home(team_a, team_b, sportsbook_result)
        trade_event_key = self._trade_event_key(
            away_team=away_team, home_team=home_team, play_date=play_date, sport_key=sport_key
        )
        polymarket = PolymarketOddsInterface(away_team, home_team, play_date)
        polymarket_results = polymarket.get_moneyline_odds()
        polymarket_spreads = polymarket.get_spread_odds()
        polymarket_totals = polymarket.get_totals_odds()

        # Display
        print("\n" + "=" * 80)
        print(f"{away_team} vs {home_team}")
        print("=" * 80)
        print_sportsbook_odds(sportsbook_result)
        print_sportsbook_spread_odds(sportsbook_spreads)
        print_sportsbook_totals_odds(sportsbook_totals)
        print_polymarket_moneyline(polymarket_results)
        self.last_run_trade_results = self._print_value_bets_and_trade(
            team_a,
            team_b,
            sportsbook_result,
            polymarket_results,
            do_trade=enable_trading,
            event_key=trade_event_key,
        )
        self.last_run_had_successful_trade = any(
            getattr(r, "ok", False) for r in (self.last_run_trade_results or [])
        )

        # Spread output (no auto-trading yet; just display value-bet status per market)
        print("\n" + "=" * 80)
        print("SPREAD MARKETS")
        print("=" * 80)
        if not sportsbook_spreads:
            print("(no sportsbook spread odds)")
        # Don't print a "no polymarket spreads" placeholder; we'll simply retry on the next scan.
        if sportsbook_spreads and polymarket_spreads:
            spread_service = SpreadValueBetService(
                sportsbook_spreads=sportsbook_spreads,
                polymarket_spread_results=polymarket_spreads,
            )
            spread_evals = spread_service.evaluate()

            # Group by base question (everything before the *last* parenthesized outcome).
            def _base_question(label: str) -> str:
                s = label or ""
                i = s.rfind("(")
                return s[:i].strip() if i != -1 else s.strip()

            def _outcome_label(label: str) -> str:
                s = label or ""
                i = s.rfind("(")
                j = s.rfind(")")
                if i == -1 or j == -1 or j <= i:
                    return ""
                return (s[i + 1 : j] or "").strip()

            groups: dict[str, list] = {}
            for m in polymarket_spreads:
                groups.setdefault(_base_question(m.market), []).append(m)

            for q in sorted(groups.keys()):
                print(f"\n- {q}")
                # stable ordering within a question by outcome label
                for m in sorted(groups[q], key=lambda mm: _outcome_label(mm.market)):
                    bid_s = f"{m.best_bid:.4f}" if m.best_bid is not None else "N/A"
                    ask_s = f"{m.best_ask:.4f}" if m.best_ask is not None else "N/A"
                    spr_s = f"{m.spread:.4f}" if m.spread is not None else "N/A"
                    outcome = _outcome_label(m.market)
                    print(f"  * {outcome}")
                    print(
                        f"    Bid: {bid_s} (vol: {m.bid_volume:.2f}) | "
                        f"Ask: {ask_s} (vol: {m.ask_volume:.2f}) | "
                        f"Spread: {spr_s}"
                    )
                    ev = spread_evals.get(str(m.token_id))
                    if ev is None:
                        print("    Value bet: N/A (evaluation unavailable)")
                    elif not ev.matched_sportsbook_line:
                        print("    Value bet: N/A (no matching sportsbook +line)")
                    elif ev.is_value_bet:
                        # Use the same formatting as SpreadValueBet for the YES case
                        print(
                            f"    Value bet: YES -> {ev.team} ({ev.point:+g}): "
                            f"polymarket_ask={ev.polymarket_best_ask:.4f}, "
                            f"true_prob={ev.true_prob:.4f}, "
                            f"expected_payout_per_$1={ev.expected_payout_per_1:.4f}"
                        )
                    else:
                        tp = f"{ev.true_prob:.4f}" if ev.true_prob is not None else "N/A"
                        ep = (
                            f"{ev.expected_payout_per_1:.4f}"
                            if ev.expected_payout_per_1 is not None
                            else "N/A"
                        )
                        print(f"    Value bet: no (true_prob={tp}, expected_payout_per_$1={ep})")

            # Execute spread value bets after printing the spread markets.
            self.last_run_spread_trade_results = self._print_spread_value_bets_and_trade(
                away_team=away_team,
                home_team=home_team,
                sportsbook_spreads=sportsbook_spreads,
                polymarket_spreads=polymarket_spreads,
                do_trade=enable_trading,
                event_key=trade_event_key,
            )
            if any(getattr(r, "ok", False) for r in (self.last_run_spread_trade_results or [])):
                self.last_run_had_successful_trade = True

        # Totals output (and optional trading)
        print("\n" + "=" * 80)
        print("TOTAL MARKETS")
        print("=" * 80)
        if not sportsbook_totals:
            print("(no sportsbook totals odds)")
        if sportsbook_totals and polymarket_totals:
            # Simple display: group by question (strip last outcome parens)
            def _base_q(label: str) -> str:
                s = label or ""
                i = s.rfind("(")
                return s[:i].strip() if i != -1 else s.strip()

            def _outcome(label: str) -> str:
                s = label or ""
                i = s.rfind("(")
                j = s.rfind(")")
                if i == -1 or j == -1 or j <= i:
                    return ""
                return (s[i + 1 : j] or "").strip()

            groups: dict[str, list] = {}
            for m in polymarket_totals:
                groups.setdefault(_base_q(m.market), []).append(m)

            for q in sorted(groups.keys()):
                print(f"\n- {q}")
                for m in sorted(groups[q], key=lambda mm: _outcome(mm.market)):
                    bid_s = f"{m.best_bid:.4f}" if m.best_bid is not None else "N/A"
                    ask_s = f"{m.best_ask:.4f}" if m.best_ask is not None else "N/A"
                    spr_s = f"{m.spread:.4f}" if m.spread is not None else "N/A"
                    print(f"  * {_outcome(m.market)}")
                    print(
                        f"    Bid: {bid_s} (vol: {m.bid_volume:.2f}) | "
                        f"Ask: {ask_s} (vol: {m.ask_volume:.2f}) | "
                        f"Spread: {spr_s}"
                    )

            self.last_run_totals_trade_results = self._print_totals_value_bets_and_trade(
                away_team=away_team,
                home_team=home_team,
                sportsbook_totals=sportsbook_totals,
                polymarket_totals=polymarket_totals,
                do_trade=enable_trading,
                event_key=trade_event_key,
            )
            if any(getattr(r, "ok", False) for r in (self.last_run_totals_trade_results or [])):
                self.last_run_had_successful_trade = True

        return 0

    # Backwards-compatible alias (name no longer NBA-specific).
    def run_moneyline(self, argv: Optional[List[str]] = None, *, sport_key: str = "PINNACLE") -> int:
        return self.run_nba_moneyline(argv, sport_key=sport_key)


if __name__ == "__main__":
    raise SystemExit(PolymarketSportsBettingBotInterface().run_moneyline())


