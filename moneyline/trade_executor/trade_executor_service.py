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

from dataclasses import dataclass
import json
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

    def _fail(
        self,
        *,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_type: OrderType,
        error: str,
    ) -> TradeExecutionResult:
        return TradeExecutionResult(
            ok=False,
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            order_type=order_type,
            response=None,
            error=error,
        )

    @staticmethod
    def _successful_trades_path() -> str:
        # Write next to `moneyline/main.py`
        moneyline_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        return os.path.join(moneyline_root, "successful_trades.txt")

    @classmethod
    def _append_successful_trade(cls, result: TradeExecutionResult) -> None:
        """
        Best-effort append of a successful trade to a log file (JSONL).
        Never raises.
        """
        try:
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "token_id": result.token_id,
                "side": result.side,
                "price": result.price,
                "size": result.size,
                "order_type": str(result.order_type),
                "response": str(result.response) if result.response is not None else None,
            }
            path = cls._successful_trades_path()
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
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
                error=self._init_error or "Trade executor not initialized",
            )

        if side not in (BUY, SELL):
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                error=f"Invalid side '{side}'. Expected BUY or SELL.",
            )
        if not token_id:
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                error="token_id is required",
            )
        if price <= 0:
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                error="price must be > 0",
            )
        if size <= 0:
            return self._fail(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
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
                error=f"{e} ({type(e).__name__})",
            )


