from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
import requests
import json
import os

try:
    # Preferred local config (not checked into git)
    from config_local import host, key, chain_id, POLYMARKET_PROXY_ADDRESS  # type: ignore
except Exception:
    # Fallback to environment variables
    host = os.getenv("POLYMARKET_HOST")
    key = os.getenv("POLYMARKET_KEY")
    chain_id = os.getenv("POLYMARKET_CHAIN_ID")
    POLYMARKET_PROXY_ADDRESS = os.getenv("POLYMARKET_PROXY_ADDRESS")


class PolymarketTrader:
    """
    A class for executing trades on Polymarket.
    Initializes the client and sets API credentials upon instantiation.
    """
    
    def __init__(self):
        """Initialize the Polymarket client and set API credentials."""
        if not host or not key or not chain_id or not POLYMARKET_PROXY_ADDRESS:
            raise RuntimeError(
                "Missing Polymarket CLOB credentials. Provide them via `config_local.py` "
                "or env vars: POLYMARKET_HOST, POLYMARKET_KEY, POLYMARKET_CHAIN_ID, POLYMARKET_PROXY_ADDRESS"
            )
        # Initialize client using a Polymarket Proxy associated with an Email/Magic account
        self.client = ClobClient(
            host,
            key=key,
            chain_id=int(chain_id),
            signature_type=1,
            funder=POLYMARKET_PROXY_ADDRESS,
        )
        
        # Set API credentials
        self.client.set_api_creds(self.client.create_or_derive_api_creds())
    
    def get_token_id(self, event_slug, outcome, sub_slug=None):
        """
        Retrieve the token_id for a specific event, market, and outcome.
        
        Args:
            event_slug: The slug/glob name of the event (e.g., "fed-decision-in-january")
            outcome: The outcome to trade on (e.g., "YES" or "NO")
            sub_slug: Optional slug of the specific market within the event. If not provided, uses the first market.
        
        Returns:
            The token_id string for the specified outcome
        """
        # Fetch event by glob name (slug)
        event_url = f"https://gamma-api.polymarket.com/events/slug/{event_slug}"
        event_response = requests.get(event_url)
        event_data = event_response.json()

        # Extract token_id from the event's markets
        # The event has a markets array, and each market has clobTokenIds as a JSON string
        token_id = None

        if "markets" in event_data and len(event_data["markets"]) > 0:
            # Find the market - either by sub_slug or use the first one
            market = None
            if sub_slug:
                # Find the market that matches the sub_slug
                for m in event_data["markets"]:
                    if m.get("slug") == sub_slug:
                        market = m
                        break
                if not market:
                    available_slugs = [m.get("slug") for m in event_data["markets"]]
                    raise ValueError(f"Market with sub_slug '{sub_slug}' not found. Available markets: {', '.join(available_slugs)}")
            else:
                # Use the first market if no sub_slug provided
                market = event_data["markets"][0]
            
            # Parse outcomes to find the index of the requested outcome
            if "outcomes" in market and "clobTokenIds" in market:
                outcomes = json.loads(market["outcomes"])
                clob_token_ids = json.loads(market["clobTokenIds"])
                
                # Find the index of the requested outcome (case-insensitive)
                outcome_upper = outcome.upper()
                outcome_index = None
                for i, outcome_option in enumerate(outcomes):
                    if outcome_option.upper() == outcome_upper:
                        outcome_index = i
                        break
                
                if outcome_index is not None and outcome_index < len(clob_token_ids):
                    token_id = clob_token_ids[outcome_index]
                    print(f"Found token_id for outcome '{outcome}' from market '{market.get('question', 'unknown')}': {token_id}")
                else:
                    available_outcomes = ", ".join(outcomes)
                    raise ValueError(f"Outcome '{outcome}' not found. Available outcomes: {available_outcomes}")

        if not token_id:
            raise ValueError(f"Could not find token_id for event '{event_slug}'. Markets available: {len(event_data.get('markets', []))}")

        print(f"Using token_id: {token_id}")
        return token_id
    
    def execute_trade(self, side, price, size, token_id, order_type):
        """
        Execute a trade on Polymarket for a given token_id.

        IMPORTANT: This method does NOT refetch event/market data.
        Callers must supply the correct token_id.
        
        Args:
            side: Either BUY or SELL (from py_clob_client.order_builder.constants)
            price: Price per token in USDC (e.g., 0.01 for 1 cent)
            size: Number of tokens to trade
            token_id: The CLOB token id to trade
            order_type: Order type from OrderType (e.g., OrderType.GTC, OrderType.FOK, OrderType.GTD, OrderType.FAK)
        
        Returns:
            The response from posting the order
        """
        # Create order arguments
        order_args = OrderArgs(
            price=price,
            size=size,
            side=side,
            token_id=token_id,
        )
        
        # Create and sign the order
        signed_order = self.client.create_order(order_args)

        # Post the order with the specified order type
        resp = self.client.post_order(signed_order, order_type)
        print(resp)
        
        return resp

    def execute_trade_by_outcome(self, side, price, size, event_slug, outcome, order_type, sub_slug=None):
        """
        Convenience wrapper that DOES refetch event data to resolve token_id.
        Prefer execute_trade(..., token_id, ...) when you already have token_id.
        """
        token_id = self.get_token_id(event_slug, outcome, sub_slug)
        return self.execute_trade(side=side, price=price, size=size, token_id=token_id, order_type=order_type)

