#!/usr/bin/env python3
"""
Redeem positions class.

Manages a list of positions (token_id, number_of_shares) and provides
methods to add positions and redeem them by creating SELL orders at $1.00.
"""

from typing import List, Tuple, Optional
from dataclasses import dataclass

from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import SELL

from trade_executor.execute_trade import PolymarketTrader


@dataclass
class Position:
    """Represents a position with token_id and number of shares."""
    token_id: str
    number_of_shares: float


class RedeemPositions:
    """
    Class for managing and redeeming positions.
    
    Maintains a list of positions (token_id, number_of_shares) and provides
    methods to add positions and redeem them by creating SELL orders at $1.00.
    """
    
    def __init__(self):
        """Initialize the RedeemPositions class and set up PolymarketTrader."""
        self.positions: List[Position] = []
        
        # Initialize PolymarketTrader (handles client setup internally)
        self.trader = PolymarketTrader()
    
    def add_position(self, position: Position) -> None:
        """
        Add a position to the list.
        
        Args:
            position: Position object with token_id and number_of_shares
        """
        if not isinstance(position, Position):
            raise TypeError(f"Expected Position, got {type(position)}")
        
        if position.number_of_shares <= 0:
            raise ValueError(f"number_of_shares must be > 0, got {position.number_of_shares}")
        
        if not position.token_id or not position.token_id.strip():
            raise ValueError("token_id cannot be empty")
        
        self.positions.append(position)
    
    def redeem_position(self, position: Position) -> Tuple[Optional[dict], Optional[str]]:
        """
        Redeem a position by creating a SELL order at maximum price (0.999 = 99.9 cents).
        
        Uses PolymarketTrader.execute_trade() to execute the order.
        If the order is only partially filled, adds the remaining position
        back to the list.
        
        Args:
            position: Position object with token_id and number_of_shares
            
        Returns:
            Tuple of (response_dict, error_message). 
            If successful, returns (resp, None).
            If error, returns (None, error_message).
        """
        if not isinstance(position, Position):
            return None, f"Expected Position, got {type(position)}"
        
        try:
            # Use PolymarketTrader to execute the trade
            # Note: Maximum price allowed is 0.999, not 1.0
            resp = self.trader.execute_trade(
                side=SELL,
                price=0.999,  # 99.9 cents (maximum allowed, close to $1.00)
                size=position.number_of_shares,
                token_id=position.token_id,
                order_type=OrderType.FAK
            )
            
            # Check if order was partially filled
            # Note: "delayed" status is treated as success (order accepted but not yet filled)
            filled_size: Optional[float] = None
            status = None
            if isinstance(resp, dict):
                status = resp.get("status")
                # For delayed orders, skip partial fill detection (order hasn't been filled yet)
                if status == "delayed":
                    return resp, None
                
                # Try various field names Polymarket might use for filled amount
                matched = resp.get("matchedAmount") or resp.get("matched_amount") or resp.get("filledAmount") or resp.get("filled_amount")
                if matched is not None:
                    try:
                        filled_size = float(matched)
                    except (ValueError, TypeError):
                        filled_size = None
                
                # If no explicit matched amount but status is "matched", assume full fill
                if filled_size is None and status == "matched":
                    filled_size = position.number_of_shares
            
            # If partially filled, add remaining position back to list
            if filled_size is not None and filled_size < position.number_of_shares:
                remaining_shares = position.number_of_shares - filled_size
                if remaining_shares > 0:
                    remaining_position = Position(
                        token_id=position.token_id,
                        number_of_shares=remaining_shares
                    )
                    self.positions.append(remaining_position)
                    print(f"Partial fill: {filled_size:.2f} of {position.number_of_shares:.2f} shares filled. "
                          f"Added {remaining_shares:.2f} remaining shares back to positions list.")
            
            return resp, None
            
        except Exception as e:
            # Handle PolyApiException and other errors
            error_msg = str(e)
            # Check for various error types
            if "orderbook" in error_msg.lower() and "does not exist" in error_msg.lower():
                return None, f"Orderbook does not exist (market may be closed/expired)"
            if "market not found" in error_msg.lower() or "404" in error_msg:
                return None, f"Market not found (invalid token_id: {position.token_id})"
            if "400" in error_msg:
                return None, f"Bad request: {error_msg}"
            return None, error_msg
    
    def get_positions(self) -> List[Position]:
        """
        Get the list of all positions.
        
        Returns:
            List of Position objects
        """
        return self.positions.copy()
    
    def clear_positions(self) -> None:
        """Clear all positions from the list."""
        self.positions.clear()

