#!/usr/bin/env python3
"""
Redeem positions module.

Provides a function to redeem a position by creating SELL orders at $1.00,
looping until the position is fully redeemed or 12 hours have passed.
"""

from typing import Optional
from dataclasses import dataclass
import asyncio
from datetime import datetime, timedelta

from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import SELL

from trade_executor.execute_trade import PolymarketTrader


@dataclass
class Position:
    """Represents a position with token_id and number of shares."""
    token_id: str
    number_of_shares: float


async def redeem_position(position: Position) -> Optional[dict]:
    """
    Redeem a position by creating SELL orders at maximum price (0.999 = 99.9 cents).
    
    Loops until the position is successfully redeemed. If the order is only
    partially filled, continues looping until fully filled, or until 12 hours
    have passed. Sleeps for 30 minutes after every iteration of the loop.
    
    Args:
        position: Position object with token_id and number_of_shares
        
    Returns:
        The response dictionary from the final successful order, or None if
        the 12-hour timeout was reached.
    """
    
    if position.number_of_shares <= 0:
        raise ValueError(f"number_of_shares must be > 0, got {position.number_of_shares}")
    
    if not position.token_id or not position.token_id.strip():
        raise ValueError("token_id cannot be empty")
    
    trader = PolymarketTrader()
    start_time = datetime.now()
    max_duration = timedelta(hours=12)
    remaining_shares = position.number_of_shares
    
    while True:
        elapsed_time = datetime.now() - start_time
        if elapsed_time >= max_duration:
            print(f"Timeout reached: 12 hours have passed. Remaining shares: {remaining_shares:.2f}")
            return None
        
        try:
            resp = trader.execute_trade(
                side=SELL,
                price=0.999,  # 99.9 cents (maximum allowed, close to $1.00)
                size=remaining_shares,
                token_id=position.token_id,
                order_type=OrderType.FAK
            )
            
            filled_size: Optional[float] = None
            status = None
            
            if isinstance(resp, dict):
                status = resp.get("status")
                
                if status == "delayed":
                    print(f"Order delayed. Waiting 30 minutes before retrying. Remaining shares: {remaining_shares:.2f}")
                    await asyncio.sleep(30 * 60)  # Sleep for 30 minutes
                    continue
                
                matched = resp.get("matchedAmount") or resp.get("matched_amount") or resp.get("filledAmount") or resp.get("filled_amount")
                if matched is not None:
                    try:
                        filled_size = float(matched)
                    except (ValueError, TypeError):
                        filled_size = None
                
                if filled_size is None and status == "matched":
                    filled_size = remaining_shares
            
            if filled_size is not None:
                if filled_size >= remaining_shares:
                    print(f"Position fully redeemed: {filled_size:.2f} shares")
                    return resp
                else:
                    remaining_shares = remaining_shares - filled_size
                    print(f"Partial fill: {filled_size:.2f} shares filled. "
                          f"Remaining: {remaining_shares:.2f} shares. Waiting 30 minutes before retrying.")
                    await asyncio.sleep(30 * 60)  # Sleep for 30 minutes
                    continue
            else:
                print(f"Order status: {status}. Waiting 30 minutes before retrying. Remaining shares: {remaining_shares:.2f}")
                await asyncio.sleep(30 * 60)  # Sleep for 30 minutes
                continue
                
        except Exception as e:
            error_msg = str(e)
            if "orderbook" in error_msg.lower() and "does not exist" in error_msg.lower():
                print(f"Orderbook does not exist (market may be closed/expired). Waiting 30 minutes before retrying.")
            elif "market not found" in error_msg.lower() or "404" in error_msg:
                print(f"Market not found (invalid token_id: {position.token_id}). Waiting 30 minutes before retrying.")
            elif "400" in error_msg:
                print(f"Bad request: {error_msg}. Waiting 30 minutes before retrying.")
            else:
                print(f"Error: {error_msg}. Waiting 30 minutes before retrying.")
            
            await asyncio.sleep(30 * 60)  # Sleep for 30 minutes
            continue
