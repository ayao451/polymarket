#!/usr/bin/env python3
"""
Trade executor service interface.

Thin wrapper around `PolymarketTrader` to execute trades without refetching
event/market data.

Credentials are read from `config_local.py` (optional) or env vars:
- POLYMARKET_HOST
- POLYMARKET_KEY
- POLYMARKET_CHAIN_ID
- POLYMARKET_PROXY_ADDRESS
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
import os
from datetime import datetime, timezone
from typing import Any, Optional

from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import BUY, SELL

from .execute_trade import PolymarketTrader


@dataclass(frozen=True)
class TradeExecutionResult:
    ok: bool
    token_id: str
    side: str
    price: float
    size: float
    order_type: OrderType
    team: Optional[str] = None
    game: Optional[str] = None
    expected_payout_per_1: Optional[float] = None
    response: Optional[Any] = None
    error: Optional[str] = None


class TradeExecutorService:
    def __init__(self, trader: Optional[PolymarketTrader] = None) -> None:
        # Don't raise on init; keep it safe for callers.
        self._trader = trader
        self._init_error: Optional[str] = None
        if trader is None:
            try:
                self._trader = PolymarketTrader()
            except Exception as e:
                self._trader = None
                self._init_error = str(e)

    def get_usdc_balance(self) -> Optional[float]:
        """
        Best-effort fetch of current collateral (USDC) balance.
        Returns None if unavailable.
        """
        if self._trader is None:
            if self._init_error:
                print(f"Trade executor not initialized: {self._init_error}")
            return None
        try:
            balance = self._trader.get_usdc_balance()
            return float(balance)
        except Exception as e:
            print(f"Error fetching USDC balance: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _fail(
        self,
        *,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_type: OrderType,
        team: Optional[str] = None,
        game: Optional[str] = None,
        expected_payout_per_1: Optional[float] = None,
        error: str,
    ) -> TradeExecutionResult:
        return TradeExecutionResult(
            ok=False,
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            order_type=order_type,
            team=team,
            game=game,
            expected_payout_per_1=expected_payout_per_1,
            response=None,
            error=error,
        )

    @staticmethod
    def _trades_csv_path() -> str:
        # Write next to `value_bets/value_bets.py`
        value_bets_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        return os.path.join(value_bets_root, "trades.csv")

    @classmethod
    def _append_successful_trade(cls, result: TradeExecutionResult) -> None:
        """
        Best-effort append of a successful trade to a CSV log file.
        Never raises.
        """
        try:
            ts = datetime.now(timezone.utc).isoformat()
            # OrderType is an enum, try to get its name
            try:
                order_type = result.order_type.name
            except AttributeError:
                order_type = str(result.order_type)

            status = None
            order_id = None
            tx_hashes = None
            success = None

            resp = result.response
            if isinstance(resp, dict):
                status = resp.get("status")
                order_id = resp.get("orderID") or resp.get("orderId")
                tx_hashes = resp.get("transactionsHashes") or resp.get("transactionHashes")
                success = resp.get("success")

            # Human-readable financials (approx; ignores fees)
            amount = float(result.price) * float(result.size)  # USDC spent for BUY
            payout = float(result.size)  # $1 per token if outcome wins
            profit = payout - amount

            team = (result.team or "").strip() or "UNKNOWN_TEAM"
            game = (result.game or "").strip() if result.game else ""

            # Ensure tx_hashes is a string for CSV.
            if isinstance(tx_hashes, (list, tuple, dict)):
                tx_hashes_s = str(tx_hashes)
            elif tx_hashes is None:
                tx_hashes_s = ""
            else:
                tx_hashes_s = str(tx_hashes)

            row = {
                "ts": ts,
                "team": team,
                "game": game,
                "amount": f"{amount:.2f}",
                "payout": f"{payout:.2f}",
                "profit": f"{profit:.2f}",
                "side": str(result.side),
                "price": f"{float(result.price):.4f}",
                "size": str(result.size),
                "type": str(order_type),
                "expected_payout_per_$1": (
                    f"{float(result.expected_payout_per_1):.4f}"
                    if result.expected_payout_per_1 is not None
                    else ""
                ),
                "success": ("" if success is None else str(success)),
                "status": ("" if status is None else str(status)),
                "order_id": ("" if order_id is None else str(order_id)),
                "tx_hashes": tx_hashes_s,
                "token_id": str(result.token_id),
            }

            path = cls._trades_csv_path()
            fieldnames = list(row.keys())
            needs_header = (not os.path.exists(path)) or (os.path.getsize(path) == 0)
            with open(path, "a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if needs_header:
                    writer.writeheader()
                writer.writerow(row)
        except Exception:
            # Intentionally swallow all logging errors
            return

    def execute_trade(
        self,
        *,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_type: OrderType,
        team: Optional[str] = None,
        game: Optional[str] = None,
        expected_payout_per_1: Optional[float] = None,
    ) -> TradeExecutionResult:
        """
        Execute an order on Polymarket CLOB.

        This method does not refetch any event/market info; it requires `token_id`.
        """
        if self._trader is None:
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                team=team,
                game=game,
                expected_payout_per_1=expected_payout_per_1,
                error=self._init_error or "Trade executor not initialized",
            )

        if side not in (BUY, SELL):
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                team=team,
                game=game,
                expected_payout_per_1=expected_payout_per_1,
                error=f"Invalid side '{side}'. Expected BUY or SELL.",
            )
        if not token_id:
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                team=team,
                game=game,
                expected_payout_per_1=expected_payout_per_1,
                error="token_id is required",
            )
        if price <= 0:
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                team=team,
                game=game,
                expected_payout_per_1=expected_payout_per_1,
                error="price must be > 0",
            )
        if size <= 0:
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                team=team,
                game=game,
                expected_payout_per_1=expected_payout_per_1,
                error="size must be > 0",
            )

        try:
            resp = self._trader.execute_trade(
                side=side,
                price=price,
                size=size,
                token_id=token_id,
                order_type=order_type,
            )
            result = TradeExecutionResult(
                ok=True,
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                team=team,
                game=game,
                expected_payout_per_1=expected_payout_per_1,
                response=resp,
                error=None,
            )
            self._append_successful_trade(result)
            return result
        except Exception as e:
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                team=team,
                game=game,
                expected_payout_per_1=expected_payout_per_1,
                error=f"{e} ({type(e).__name__})",
            )


