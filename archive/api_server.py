from __future__ import annotations

from typing import Any, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from execute_trade import PolymarketTrader
from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import BUY, SELL


app = FastAPI(title="Polymarket Trade API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ExecuteTradeRequest(BaseModel):
    action: str = Field(..., description="BUY or SELL")
    event_slug: str
    sub_slug: Optional[str] = None
    outcome: str = Field(..., description="YES or NO (case-insensitive)")
    order_type: str = Field(..., description="GTC, FOK, GTD, or FAK")
    price: float
    size: float


class ExecuteTradeResponse(BaseModel):
    ok: bool
    result: Optional[Any] = None
    error: Optional[str] = None


_trader: Optional[PolymarketTrader] = None


@app.get("/health")
def health() -> dict:
    return {"ok": True}


def _parse_side(action: str):
    a = action.strip().upper()
    if a == "BUY":
        return BUY
    if a == "SELL":
        return SELL
    raise ValueError("action must be BUY or SELL")


def _parse_order_type(order_type: str) -> OrderType:
    ot = order_type.strip().upper()
    try:
        return OrderType[ot]
    except KeyError as e:
        raise ValueError("order_type must be one of: GTC, FOK, GTD, FAK") from e


@app.post("/execute_trade", response_model=ExecuteTradeResponse)
def execute_trade(req: ExecuteTradeRequest) -> ExecuteTradeResponse:
    try:
        global _trader
        if _trader is None:
            # Lazy init: deriving API creds may take time / require network,
            # so we don't block server startup.
            _trader = PolymarketTrader()

        side = _parse_side(req.action)
        order_type = _parse_order_type(req.order_type)

        result = _trader.execute_trade(
            side=side,
            price=req.price,
            size=req.size,
            event_slug=req.event_slug,
            outcome=req.outcome,
            order_type=order_type,
            sub_slug=req.sub_slug,
        )
        return ExecuteTradeResponse(ok=True, result=result)
    except Exception as e:
        return ExecuteTradeResponse(ok=False, error=str(e))


