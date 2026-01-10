from execute_trade import PolymarketTrader
from py_clob_client.order_builder.constants import BUY
from py_clob_client.clob_types import OrderType

if __name__ == "__main__":
    # Initialize the trader (this sets up the client and API credentials)
    trader = PolymarketTrader()
    
    # Execute a buy order for the fed-decision-in-january event
    result = trader.execute_trade(
        side=BUY,
        price=0.01,
        size=5.0,
        event_slug="fed-decision-in-january event",
        sub_slug="fed-decreases-interest-rates-by-50-bps-after-january-2026-meeting",
        outcome="YES",
        order_type=OrderType.GTC
    )
    print(f"Trade executed: {result}")

