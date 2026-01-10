import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add parent directory to path to import execute_trade
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from execute_trade import PolymarketTrader


class TestPolymarketTraderInit(unittest.TestCase):
    """Test cases for PolymarketTrader initialization."""
    
    @patch('execute_trade.ClobClient')
    @patch('execute_trade.POLYMARKET_PROXY_ADDRESS', '0x123')
    @patch('execute_trade.host', 'https://clob.polymarket.com')
    @patch('execute_trade.key', 'test_key')
    @patch('execute_trade.chain_id', 137)
    def test_init_creates_client(self, mock_client_class):
        """Test that __init__ creates a ClobClient with correct parameters."""
        mock_client = MagicMock()
        mock_client.create_or_derive_api_creds.return_value = ("api_key", "api_secret", "api_passphrase")
        mock_client_class.return_value = mock_client
        
        trader = PolymarketTrader()
        
        # Assertions
        mock_client_class.assert_called_once_with(
            'https://clob.polymarket.com',
            key='test_key',
            chain_id=137,
            signature_type=1,
            funder='0x123'
        )
        self.assertEqual(trader.client, mock_client)
    
    @patch('execute_trade.ClobClient')
    @patch('execute_trade.POLYMARKET_PROXY_ADDRESS', '0x123')
    @patch('execute_trade.host', 'https://clob.polymarket.com')
    @patch('execute_trade.key', 'test_key')
    @patch('execute_trade.chain_id', 137)
    def test_init_sets_api_creds(self, mock_client_class):
        """Test that __init__ sets API credentials."""
        mock_client = MagicMock()
        mock_api_creds = ("api_key", "api_secret", "api_passphrase")
        mock_client.create_or_derive_api_creds.return_value = mock_api_creds
        mock_client_class.return_value = mock_client
        
        trader = PolymarketTrader()
        
        # Assertions
        mock_client.create_or_derive_api_creds.assert_called_once()
        mock_client.set_api_creds.assert_called_once_with(mock_api_creds)


if __name__ == '__main__':
    unittest.main()

