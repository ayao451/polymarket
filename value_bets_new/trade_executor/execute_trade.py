from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
import requests
import json
import os

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    # Load .env from project root (two levels up from this file)
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    load_dotenv(env_path)
    # Also try loading from current directory
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, skip

try:
    # Preferred local config (not checked into git)
    from config_local import host, key, chain_id, POLYMARKET_PROXY_ADDRESS  # type: ignore
except Exception:
    # Fallback to environment variables (now loaded from .env if present)
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
        
        return resp

    def get_usdc_balance(self) -> float:
        """
        Return current collateral (USDC) balance as a float.
        """
        try:
            # Most py_clob_client versions expose these in clob_types
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Cannot import BalanceAllowanceParams/AssetType: {e}")

        response = self.client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        bal = response.get("balance")
        if bal is None:
            raise RuntimeError(f"Unexpected balance response: {response}")
        bal_f = float(bal)
        # Heuristic: if returned in base units (micro-USDC), convert.
        if bal_f > 1_000_000:
            bal_f = bal_f / 1_000_000.0
        return bal_f
