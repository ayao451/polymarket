#!/usr/bin/env python3
"""
Trade executor service interface with Kelly Criterion calculations.

Thin wrapper around `PolymarketTrader` to execute trades without refetching
event/market data. Includes Kelly Criterion bet sizing calculations.

Credentials are read from `config_local.py` (optional) or env vars:
- POLYMARKET_HOST
- POLYMARKET_KEY
- POLYMARKET_CHAIN_ID
- POLYMARKET_PROXY_ADDRESS
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import BUY, SELL

from value_bets_new.trade_executor.execute_trade import PolymarketTrader
from value_bets_new.constants import ValueBet


@dataclass(frozen=True)
class TradeExecutionResult:
    token_id: str
    side: str
    price: float
    size: float  # Requested size
    order_type: OrderType
    team: Optional[str] = None
    game: Optional[str] = None
    expected_payout_per_1: Optional[float] = None
    filled_size: Optional[float] = None  # Actual filled size (for partial fills)
    condition_id: Optional[str] = None  # Polymarket condition ID
    
    @property
    def is_partial_fill(self) -> bool:
        """Returns True if this was a partial fill (filled less than requested)."""
        if self.filled_size is None:
            return False
        return self.filled_size < self.size
    
    @property
    def fill_percentage(self) -> float:
        """Returns the percentage of the order that was filled."""
        if self.filled_size is None or self.size <= 0:
            return 100.0  # Assume full fill if unknown
        return (self.filled_size / self.size) * 100


class TradeExecutorService:
    # Kelly fraction: use fractional Kelly for safety (1.0 = full Kelly)
    KELLY_FRACTION = 1.0
    MIN_BET_SIZE = 1.0  # Minimum bet in USDC
    MAX_BET_FRACTION = 0.10  # Never bet more than 10% of bankroll
    
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

    @staticmethod
    def kelly_criterion(true_prob: float, price: float) -> float:
        """
        Calculate Kelly Criterion bet fraction for Polymarket.
        
        For a bet at price x where you win $1 if correct:
        - Win: profit = (1 - x) per token
        - Lose: loss = x per token
        - Net odds b = (1 - x) / x
        
        Kelly formula: f* = (b*p - q) / b = (p - x) / (1 - x)
        
        Args:
            true_prob: True probability of winning (from devigged sportsbook odds)
            price: Polymarket best ask price
            
        Returns:
            Optimal fraction of bankroll to bet (can be negative if no edge)
        """
        if price <= 0 or price >= 1:
            return 0.0
        
        # f* = (p - x) / (1 - x)
        kelly = (true_prob - price) / (1 - price)
        return max(0.0, kelly)  # Never return negative

    def calculate_bet_size(self, true_prob: float, price: float) -> Tuple[float, float]:
        """
        Calculate the optimal bet size using Kelly Criterion.
        
        Args:
            true_prob: True probability of winning
            price: Polymarket best ask price
            
        Returns:
            Tuple of (bet_size, bankroll) or (0, 0) if can't bet
        """
        bankroll = self.get_usdc_balance()
        if bankroll is None or bankroll <= 0:
            return 0.0, 0.0
        
        full_kelly = self.kelly_criterion(true_prob, price)
        kelly_fraction = full_kelly * self.KELLY_FRACTION  # Use fractional Kelly
        
        # Apply bet size constraints
        max_bet = bankroll * self.MAX_BET_FRACTION
        kelly_bet = bankroll * kelly_fraction
        
        bet_size = min(kelly_bet, max_bet)
        bet_size = max(bet_size, self.MIN_BET_SIZE) if kelly_bet >= self.MIN_BET_SIZE else 0
        
        return bet_size, bankroll

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
            traceback.print_exc()
            return None

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
        condition_id: Optional[str] = None,
    ) -> Optional[TradeExecutionResult]:
        """
        Execute an order on Polymarket CLOB.

        This method does not refetch any event/market info; it requires `token_id`.
        """
        if self._trader is None:
            return None
        if side not in (BUY, SELL):
            return None
        if not token_id:
            return None
        if price <= 0:
            return None
        if size <= 0:
            return None

        try:
            resp = self._trader.execute_trade(
                side=side,
                price=price,
                size=size,
                token_id=token_id,
                order_type=order_type,
            )
            
            # Extract filled size from response (for FAK partial fills)
            filled_size: Optional[float] = None
            if isinstance(resp, dict):
                # Try various field names Polymarket might use
                matched = resp.get("matchedAmount") or resp.get("matched_amount") or resp.get("filledAmount")
                if matched is not None:
                    try:
                        filled_size = float(matched)
                    except (ValueError, TypeError):
                        filled_size = None
                
                # If no explicit matched amount but status is "matched", assume full fill
                if filled_size is None and resp.get("status") == "matched":
                    filled_size = size
            
            result = TradeExecutionResult(
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                order_type=order_type,
                team=team,
                game=game,
                expected_payout_per_1=expected_payout_per_1,
                filled_size=filled_size,
                condition_id=condition_id,
            )
            return result
        except Exception as e:
            return None

    def execute_value_bet(
        self,
        value_bet: ValueBet,
        game_str: Optional[str] = None,
    ) -> Optional[TradeExecutionResult]:
        """
        Execute a value bet using Kelly Criterion sizing.
        
        Args:
            value_bet: The value bet to execute
            game_str: Optional game string (e.g., "Team A @ Team B") for logging
            
        Returns:
            TradeExecutionResult if trade was attempted, None if skipped
        """
        # Calculate Kelly bet size
        bet_size, bankroll = self.calculate_bet_size(
            value_bet.true_prob,
            value_bet.polymarket_best_ask
        )
        
        if bet_size <= 0 or bankroll <= 0:
            print(f"[SKIP TRADE] Bet size too small or no bankroll: bet_size={bet_size}, bankroll={bankroll}")
            return None
        
        # Calculate number of tokens to buy
        num_tokens = math.floor(bet_size / value_bet.polymarket_best_ask) + 1
        price = round(value_bet.polymarket_best_ask, 4)
        
        if num_tokens < 0.01:
            print(f"[SKIP TRADE] Token amount too small: {num_tokens}")
            return None
        
        # Execute the trade
        trade_result = self.execute_trade(
            token_id=value_bet.token_id,
            side=BUY,
            price=price,
            size=num_tokens,
            order_type=OrderType.FOK,
            team=value_bet.team,
            game=game_str,
            expected_payout_per_1=value_bet.expected_payout_per_1,
            condition_id=value_bet.condition_id,
        )
        
        if trade_result is not None:
            print(f"[SUCCESS] Trade executed: {value_bet.team} - ${bet_size:.2f} ({num_tokens:.2f} tokens)")
        else:
            print(f"[FAILED] Trade failed")
        
        return trade_result
