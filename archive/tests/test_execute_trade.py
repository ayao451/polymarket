import unittest
from unittest.mock import patch, MagicMock
import json
import sys
import os

# Add parent directory to path to import execute_trade
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from execute_trade import PolymarketTrader
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.clob_types import OrderType


class TestExecuteTrade(unittest.TestCase):
    """Test cases for the execute_trade method."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock the client initialization to avoid needing real credentials
        with patch('execute_trade.ClobClient') as mock_client_class:
            self.mock_client = MagicMock()
            mock_client_class.return_value = self.mock_client
            self.mock_client.create_or_derive_api_creds.return_value = ("api_key", "api_secret", "api_passphrase")
            
            with patch('execute_trade.POLYMARKET_PROXY_ADDRESS', '0x123'):
                with patch('execute_trade.host', 'https://clob.polymarket.com'):
                    with patch('execute_trade.key', 'test_key'):
                        with patch('execute_trade.chain_id', 137):
                            self.trader = PolymarketTrader()
        
        # Load test data
        test_json_path = os.path.join(os.path.dirname(__file__), '..', 'test.json')
        with open(test_json_path, 'r') as f:
            self.test_event_data = json.load(f)
    
    @patch('execute_trade.requests.get')
    def test_execute_trade_buy_order(self, mock_get):
        """Test executing a buy order."""
        # Mock the API response for get_token_id
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Mock the client methods
        mock_signed_order = MagicMock()
        self.mock_client.create_order.return_value = mock_signed_order
        self.mock_client.post_order.return_value = {"order_id": "12345", "status": "open"}
        
        # Get expected token_id
        first_market = self.test_event_data["markets"][0]
        clob_token_ids = json.loads(first_market["clobTokenIds"])
        expected_token_id = clob_token_ids[0]
        
        # Execute trade
        result = self.trader.execute_trade(
            side=BUY,
            price=0.01,
            size=5.0,
            event_slug="fed-decision-in-january",
            outcome="YES",
            order_type=OrderType.GTC
        )
        
        # Assertions
        self.mock_client.create_order.assert_called_once()
        order_args = self.mock_client.create_order.call_args[0][0]
        self.assertEqual(order_args.price, 0.01)
        self.assertEqual(order_args.size, 5.0)
        self.assertEqual(order_args.side, BUY)
        self.assertEqual(order_args.token_id, expected_token_id)
        
        self.mock_client.post_order.assert_called_once_with(mock_signed_order, OrderType.GTC)
        self.assertEqual(result, {"order_id": "12345", "status": "open"})
    
    @patch('execute_trade.requests.get')
    def test_execute_trade_sell_order(self, mock_get):
        """Test executing a sell order."""
        # Mock the API response for get_token_id
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Mock the client methods
        mock_signed_order = MagicMock()
        self.mock_client.create_order.return_value = mock_signed_order
        self.mock_client.post_order.return_value = {"order_id": "67890", "status": "open"}
        
        # Execute trade
        result = self.trader.execute_trade(
            side=SELL,
            price=0.50,
            size=10.0,
            event_slug="fed-decision-in-january",
            outcome="NO",
            order_type=OrderType.GTC
        )
        
        # Assertions
        order_args = self.mock_client.create_order.call_args[0][0]
        self.assertEqual(order_args.side, SELL)
        self.assertEqual(order_args.price, 0.50)
        self.assertEqual(order_args.size, 10.0)
        self.mock_client.post_order.assert_called_once_with(mock_signed_order, OrderType.GTC)
    
    @patch('execute_trade.requests.get')
    def test_execute_trade_with_sub_slug(self, mock_get):
        """Test executing a trade with sub_slug."""
        # Mock the API response for get_token_id
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Mock the client methods
        mock_signed_order = MagicMock()
        self.mock_client.create_order.return_value = mock_signed_order
        self.mock_client.post_order.return_value = {"order_id": "11111", "status": "open"}
        
        target_slug = "fed-decreases-interest-rates-by-50-bps-after-january-2026-meeting"
        
        # Execute trade
        result = self.trader.execute_trade(
            side=BUY,
            price=0.02,
            size=3.0,
            event_slug="fed-decision-in-january",
            outcome="YES",
            order_type=OrderType.FOK,
            sub_slug=target_slug
        )
        
        # Assertions
        order_args = self.mock_client.create_order.call_args[0][0]
        self.assertEqual(order_args.price, 0.02)
        self.assertEqual(order_args.size, 3.0)
        self.mock_client.post_order.assert_called_once_with(mock_signed_order, OrderType.FOK)
    
    @patch('execute_trade.requests.get')
    def test_execute_trade_different_order_types(self, mock_get):
        """Test executing trades with different order types."""
        # Mock the API response for get_token_id
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Mock the client methods
        mock_signed_order = MagicMock()
        self.mock_client.create_order.return_value = mock_signed_order
        self.mock_client.post_order.return_value = {"order_id": "22222", "status": "open"}
        
        # Test different order types
        order_types = [OrderType.GTC, OrderType.FOK, OrderType.GTD, OrderType.FAK]
        
        for order_type in order_types:
            self.mock_client.reset_mock()
            
            result = self.trader.execute_trade(
                side=BUY,
                price=0.01,
                size=5.0,
                event_slug="fed-decision-in-january",
                outcome="YES",
                order_type=order_type
            )
            
            self.mock_client.post_order.assert_called_once_with(mock_signed_order, order_type)


if __name__ == '__main__':
    unittest.main()

