#!/usr/bin/env python3
"""
Base Market class for all market types.

Contains shared functionality:
- Kelly Criterion bet sizing
- Sportsbook/Polymarket/Trade executor initialization
- Trade execution helpers
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional, Union, Set

from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import BUY

from pinnacle_scraper.pinnacle_sportsbook_odds_interface import PinnacleSportsbookOddsInterface
from polymarket_odds_service.polymarket_odds import PolymarketOdds
from polymarket_sports_betting_bot.value_bet_service import ValueBet, SpreadValueBet, TotalsValueBet
from trade_executor.trade_executor_service import TradeExecutorService, TradeExecutionResult


# Union type for all value bet types
AnyValueBet = Union[ValueBet, SpreadValueBet, TotalsValueBet]


class Market(ABC):
    """Abstract base class for market handlers."""
    
    # Kelly fraction: use fractional Kelly for safety (0.25 = quarter Kelly)
    KELLY_FRACTION = 1.0
    MIN_BET_SIZE = 1.0  # Minimum bet in USDC
    MAX_BET_FRACTION = 0.10  # Never bet more than 10% of bankroll
    
    def __init__(self, sport: str = "basketball", verbose: bool = False) -> None:
        self.verbose = verbose
        if self.verbose:
            print(f"\n[MARKET INIT] Initializing {self.__class__.__name__} market handler for sport: {sport}")
        self.sport = sport
        if self.verbose:
            print(f"  -> Creating Pinnacle sportsbook interface...")
        self.sportsbook = PinnacleSportsbookOddsInterface(sport=sport)
        if self.verbose:
            print(f"  -> Creating Polymarket odds connector...")
        self.polymarket = PolymarketOdds()
        if self.verbose:
            print(f"  -> Creating trade executor service...")
        self.trade_executor = TradeExecutorService()
        if self.verbose:
            print(f"  -> {self.__class__.__name__} initialization complete")

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

    def calculate_bet_size(self, true_prob: float, price: float) -> tuple[float, float, float]:
        """
        Calculate the optimal bet size using Kelly Criterion.
        
        Args:
            true_prob: True probability of winning
            price: Polymarket best ask price
            
        Returns:
            Tuple of (bet_size, full_kelly, bankroll) or (0, 0, 0) if can't bet
        """
        if self.verbose:
            print(f"\n[KELLY] Calculating optimal bet size...")
            print(f"  -> True probability: {true_prob:.4f} ({true_prob*100:.2f}%)")
            print(f"  -> Polymarket ask price: {price:.4f} ({price*100:.2f}%)")
            print(f"  -> Fetching USDC balance from wallet...")
        
        bankroll = self.trade_executor.get_usdc_balance()
        if bankroll is None or bankroll <= 0:
            if self.verbose:
                print(f"  -> [ERROR] Cannot fetch bankroll or bankroll is zero")
            return 0.0, 0.0, 0.0
        
        if self.verbose:
            print(f"  -> Current bankroll: ${bankroll:.2f}")
        
        full_kelly = self.kelly_criterion(true_prob, price)
        if self.verbose:
            print(f"  -> Full Kelly fraction: {full_kelly:.4f} ({full_kelly*100:.2f}%)")
        
        kelly_fraction = full_kelly * self.KELLY_FRACTION  # Use fractional Kelly
        if self.verbose:
            print(f"  -> Using {self.KELLY_FRACTION}x fractional Kelly: {kelly_fraction:.4f} ({kelly_fraction*100:.2f}%)")
        
        # Apply bet size constraints
        max_bet = bankroll * self.MAX_BET_FRACTION
        kelly_bet = bankroll * kelly_fraction
        if self.verbose:
            print(f"  -> Kelly bet amount: ${kelly_bet:.2f}")
            print(f"  -> Max bet (10% of bankroll): ${max_bet:.2f}")
        
        bet_size = min(kelly_bet, max_bet)
        bet_size = max(bet_size, self.MIN_BET_SIZE) if kelly_bet >= self.MIN_BET_SIZE else 0
        if self.verbose:
            print(f"  -> Final bet size after constraints: ${bet_size:.2f}")
        
        return bet_size, full_kelly, bankroll

    def print_kelly_info(self, bet_size: float, full_kelly: float, bankroll: float, num_tokens: float) -> None:
        """Print Kelly criterion calculation details."""
        if not self.verbose:
            return
        kelly_fraction = full_kelly * self.KELLY_FRACTION
        print(f"\nKelly Criterion:")
        print(f"  Bankroll: ${bankroll:.2f}")
        print(f"  Full Kelly: {full_kelly:.4f} ({full_kelly*100:.2f}%)")
        print(f"  Fractional Kelly ({self.KELLY_FRACTION}x): {kelly_fraction:.4f}")
        print(f"  Bet size: ${bet_size:.2f} ({bet_size/bankroll*100:.2f}% of bankroll)")
        print(f"  Tokens: {num_tokens:.2f}")

    def execute_value_bet(
        self,
        value_bet: AnyValueBet,
        away_team: str,
        home_team: str,
        event_slug: str,
        market_slug: str,
        traded_markets: Optional[Set[str]] = None,
    ) -> Optional[TradeExecutionResult]:
        """
        Execute a value bet using Kelly Criterion sizing.
        
        Args:
            value_bet: The value bet to execute
            away_team: Away team name
            home_team: Home team name
            event_slug: Polymarket event slug
            market_slug: Polymarket market slug
            traded_markets: Optional set of market slugs to check for duplicates
            
        Returns:
            TradeExecutionResult if trade attempted, None if skipped
        """
        # Check if we've already traded this market slug
        if traded_markets is not None:
            if market_slug in traded_markets:
                if self.verbose:
                    print(f"\n{'='*60}")
                    print(f"[SKIP TRADE] Already traded {market_slug}")
                    print(f"{'='*60}")
                return None
        
        # Add market slug to traded_markets IMMEDIATELY to prevent duplicate trades
        # This must happen before trade execution to prevent race conditions
        if traded_markets is not None:
            traded_markets.add(market_slug)
            if self.verbose:
                print(f"  [TRACKED] Added {market_slug} to traded_markets (before execution)")
        
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"[TRADE EXECUTION] Preparing to execute value bet")
            print(f"{'='*60}")
            print(f"  Game: {away_team} @ {home_team}")
            print(f"  Token ID: {value_bet.token_id}")
            print(f"  True probability: {value_bet.true_prob:.4f} ({value_bet.true_prob*100:.2f}%)")
            print(f"  Polymarket ask: {value_bet.polymarket_best_ask:.4f}")
            print(f"  Expected payout per $1: ${value_bet.expected_payout_per_1:.4f}")
        
        # Calculate Kelly bet size
        bet_size, full_kelly, bankroll = self.calculate_bet_size(
            value_bet.true_prob, value_bet.polymarket_best_ask
        )
        
        if bankroll <= 0:
            print(f"  [SKIP TRADE] Cannot get bankroll - skipping trade")
            # Remove from traded_markets if we're not actually trading
            if traded_markets is not None:
                traded_markets.discard(market_slug)
            return None
        
        if bet_size <= 0:
            print(f"  [SKIP TRADE] Kelly bet size below minimum (full Kelly: {full_kelly:.4f}, bankroll: ${bankroll:.2f})")
            print(f"    -> Calculated bet: ${bankroll * full_kelly * self.KELLY_FRACTION:.2f}, min required: ${self.MIN_BET_SIZE:.2f}")
            # Remove from traded_markets if we're not actually trading
            if traded_markets is not None:
                traded_markets.discard(market_slug)
            return None
        
        # Calculate number of tokens to buy
        # Round to 2 decimals for size (Polymarket API requirement)
        num_tokens = (bet_size // value_bet.polymarket_best_ask)+1
        # Round price to 4 decimals (Polymarket API requirement)
        price = round(value_bet.polymarket_best_ask, 4)
        
        # Ensure we have at least some tokens to buy
        if num_tokens < 0.01:
            print(f"  [SKIP TRADE] Token amount too small: {num_tokens}")
            # Remove from traded_markets if we're not actually trading
            if traded_markets is not None:
                traded_markets.discard(market_slug)
            return None
        
        self.print_kelly_info(bet_size, full_kelly, bankroll, num_tokens)
        
        # Get team/label for logging
        # Use isinstance checks to determine type
        if isinstance(value_bet, (ValueBet, SpreadValueBet)):
            team = value_bet.team
        elif isinstance(value_bet, TotalsValueBet):
            team = value_bet.side
        else:
            # For other custom types, try direct access
            try:
                team = value_bet.team
                if not team:
                    team = "Unknown"
            except AttributeError:
                team = "Unknown"
        
        # Always print that we're attempting to execute
        print(f"\n  [EXECUTING TRADE]")
        print(f"    Team/Outcome: {team}")
        print(f"    Price: {price:.4f}")
        print(f"    Size (tokens): {num_tokens:.2f}")
        print(f"    Cost (USDC): ${num_tokens * price:.2f}")
        print(f"    Bankroll: ${bankroll:.2f}")
        print(f"    Token ID: {value_bet.token_id}")
        
        # Execute trade
        condition_id = getattr(value_bet, 'condition_id', None)
        result = self.trade_executor.execute_trade(
            token_id=value_bet.token_id,
            side=BUY,
            price=price,
            size=num_tokens,
            order_type=OrderType.FAK,  # Fill and Kill - partial fills allowed
            team=team,
            game=f"{away_team} @ {home_team}",
            expected_payout_per_1=value_bet.expected_payout_per_1,
            event_slug=event_slug,
            condition_id=condition_id,
        )
        
        # Always print trade result
        if result.ok:
            print(f"\n{'*'*60}")
            if result.is_partial_fill:
                print(f"*** TRADE PARTIALLY FILLED ***")
            else:
                print(f"*** TRADE EXECUTED SUCCESSFULLY ***")
            print(f"{'*'*60}")
            print(f"  Game: {away_team} @ {home_team}")
            print(f"  Team/Outcome: {team}")
            print(f"  Price: {price:.4f}")
            
            # Show requested vs filled for partial fills
            actual_size = result.filled_size if result.filled_size is not None else num_tokens
            if result.is_partial_fill:
                print(f"  Size requested: {num_tokens:.2f} tokens")
                print(f"  Size filled: {actual_size:.2f} tokens ({result.fill_percentage:.1f}%)")
                print(f"  Cost (USDC): ${actual_size * price:.2f} (of ${num_tokens * price:.2f} requested)")
            else:
                print(f"  Size (tokens): {actual_size:.2f}")
                print(f"  Cost (USDC): ${actual_size * price:.2f}")
            
            print(f"  Expected payout per $1: ${value_bet.expected_payout_per_1:.4f}")
            print(f"  Order ID: {result.response.get('orderID') if result.response else 'N/A'}")
            if self.verbose and result.response:
                print(f"  Response: {result.response}")
        else:
            print(f"\n{'!'*60}")
            print(f"!!! TRADE FAILED !!!")
            print(f"{'!'*60}")
            print(f"  Game: {away_team} @ {home_team}")
            print(f"  Team/Outcome: {team}")
            print(f"  Error: {result.error}")
        
        # Note: market_slug was already added to traded_markets before execution
        # If trade failed, we could remove it, but we'll keep it to prevent retrying failed trades
        if not result.ok and traded_markets is not None:
            # Optionally remove on failure if we want to allow retries
            # For now, we keep it to prevent duplicate attempts
            if self.verbose:
                print(f"  [TRACKED] Keeping {market_slug} in traded_markets (trade failed, but preventing duplicates)")
        
        return result

    @abstractmethod
    def run(
        self,
        away_team: str,
        home_team: str,
        play_date: date,
        event_slug: str,
        market_slug: str,
        traded_markets: Optional[Set[str]] = None,
    ) -> Optional[AnyValueBet]:
        """
        Run the market flow.
        
        Args:
            away_team: Away team name
            home_team: Home team name
            play_date: Date of the game
            event_slug: Polymarket event slug
            market_slug: Polymarket market slug
            traded_markets: Optional set of market slugs to check for duplicates
            
        Returns:
            Value bet if found, None otherwise
        """
        pass
