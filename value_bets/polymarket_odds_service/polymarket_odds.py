from dataclasses import dataclass
from typing import List, Optional
import json
import requests
from py_clob_client.client import ClobClient


class PolymarketOdds:
    
    @dataclass(frozen=True)
    class MarketOdds:
        token_id: str
        market: str
        best_bid: Optional[float]
        bid_volume: float
        best_ask: Optional[float]
        ask_volume: float
        spread: Optional[float]
        
    def __init__(self):
        self.GAMMA_API_BASE = "https://gamma-api.polymarket.com"
        self.CLOB_API_BASE = "https://clob.polymarket.com"
        self.clob_client = ClobClient(
            host=self.CLOB_API_BASE,
            chain_id=137  # Polygon mainnet
        )

    def retrieve_polymarket_odds(self, event_slug: str, market_slug: str) -> List[MarketOdds]:
        """
        Fetch bid-ask spread data for a Polymarket market using event slug and market slug.
        
        Args:
            event_slug: The event slug (e.g., "nba-bos-mia-2026-01-15")
            market_slug: The market slug within the event (e.g., "winner" or "spread-home-4pt5")
            
        Returns:
            List of MarketOdds containing spread data for each token in the market
        """
        # Step 1: Fetch the event using the gamma API
        event_url = f"{self.GAMMA_API_BASE}/events/slug/{event_slug}"
        response = requests.get(event_url)
        response.raise_for_status()
        event_data = response.json()
        
        # Step 2: Find the market matching the market_slug and extract clobTokenIds
        markets = event_data.get("markets", [])
        target_market = None
        for market in markets:
            if market.get("slug") == market_slug:
                target_market = market
                break
        
        if target_market is None:
            raise ValueError(f"Market with slug '{market_slug}' not found in event '{event_slug}'")
        
        # Parse clobTokenIds - may be a JSON string or already a list
        clob_token_ids = target_market.get("clobTokenIds", [])
        if isinstance(clob_token_ids, str):
            clob_token_ids = json.loads(clob_token_ids)
        if not clob_token_ids:
            raise ValueError(f"No clobTokenIds found for market '{market_slug}'")
        
        # Get outcome labels for the tokens (e.g., "Yes", "No" or team names)
        # Parse outcomes - may be a JSON string or already a list
        outcomes = target_market.get("outcomes", [])
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        
        # Step 3: Get spread data for each token using py-clob-client
        odds: List[PolymarketOdds.MarketOdds] = []
        
        for i, token_id in enumerate(clob_token_ids):
            # Get outcome label for this token
            outcome_label = outcomes[i] if i < len(outcomes) else f"Outcome {i}"
            
            # Use get_price for accurate best bid/ask
            try:
                bid_response = self.clob_client.get_price(token_id, side="BUY")
                best_bid = float(bid_response.get("price", 0)) if bid_response else None
            except Exception:
                best_bid = None
                
            try:
                ask_response = self.clob_client.get_price(token_id, side="SELL")
                best_ask = float(ask_response.get("price", 0)) if ask_response else None
            except Exception:
                best_ask = None
            
            # Get order book for volume information
            try:
                order_book = self.clob_client.get_order_book(token_id)
                bid_volume = float(order_book.bids[0].size) if order_book.bids else 0.0
                ask_volume = float(order_book.asks[0].size) if order_book.asks else 0.0
            except Exception:
                bid_volume = 0.0
                ask_volume = 0.0
            
            # Calculate spread
            if best_bid is not None and best_ask is not None:
                spread = best_ask - best_bid
            else:
                spread = None
            
            odds.append(self.MarketOdds(
                token_id=token_id,
                market=outcome_label,
                best_bid=best_bid,
                bid_volume=bid_volume,
                best_ask=best_ask,
                ask_volume=ask_volume,
                spread=spread
            ))

        return odds
