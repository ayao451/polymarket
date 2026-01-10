import unittest
from unittest.mock import patch, MagicMock
import json
import sys
import os

# Add parent directory to path to import execute_trade
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from execute_trade import PolymarketTrader


class TestGetTokenId(unittest.TestCase):
    """Test cases for the get_token_id method."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock the client initialization to avoid needing real credentials
        with patch('execute_trade.ClobClient'):
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
    def test_get_token_id_first_market_yes(self, mock_get):
        """Test getting token_id for first market with YES outcome."""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Get the expected token_id from test data
        first_market = self.test_event_data["markets"][0]
        clob_token_ids = json.loads(first_market["clobTokenIds"])
        expected_token_id = clob_token_ids[0]  # First token is YES
        
        # Test
        token_id = self.trader.get_token_id(
            event_slug="fed-decision-in-january",
            outcome="YES"
        )
        
        # Assertions
        self.assertEqual(token_id, expected_token_id)
        mock_get.assert_called_once_with("https://gamma-api.polymarket.com/events/slug/fed-decision-in-january")
    
    @patch('execute_trade.requests.get')
    def test_get_token_id_first_market_no(self, mock_get):
        """Test getting token_id for first market with NO outcome."""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Get the expected token_id from test data
        first_market = self.test_event_data["markets"][0]
        clob_token_ids = json.loads(first_market["clobTokenIds"])
        expected_token_id = clob_token_ids[1]  # Second token is NO
        
        # Test
        token_id = self.trader.get_token_id(
            event_slug="fed-decision-in-january",
            outcome="NO"
        )
        
        # Assertions
        self.assertEqual(token_id, expected_token_id)
    
    @patch('execute_trade.requests.get')
    def test_get_token_id_with_sub_slug(self, mock_get):
        """Test getting token_id with a specific sub_slug."""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Find a specific market by slug
        target_slug = "fed-decreases-interest-rates-by-50-bps-after-january-2026-meeting"
        target_market = None
        for market in self.test_event_data["markets"]:
            if market.get("slug") == target_slug:
                target_market = market
                break
        
        self.assertIsNotNone(target_market, "Target market should exist in test data")
        clob_token_ids = json.loads(target_market["clobTokenIds"])
        expected_token_id = clob_token_ids[0]  # First token is YES
        
        # Test
        token_id = self.trader.get_token_id(
            event_slug="fed-decision-in-january",
            outcome="YES",
            sub_slug=target_slug
        )
        
        # Assertions
        self.assertEqual(token_id, expected_token_id)
    
    @patch('execute_trade.requests.get')
    def test_get_token_id_case_insensitive_outcome(self, mock_get):
        """Test that outcome matching is case-insensitive."""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Get the expected token_id from test data
        first_market = self.test_event_data["markets"][0]
        clob_token_ids = json.loads(first_market["clobTokenIds"])
        expected_token_id = clob_token_ids[0]  # First token is YES
        
        # Test with different case variations
        for outcome_variant in ["yes", "Yes", "YES", "yEs"]:
            token_id = self.trader.get_token_id(
                event_slug="fed-decision-in-january",
                outcome=outcome_variant
            )
            self.assertEqual(token_id, expected_token_id)
    
    @patch('execute_trade.requests.get')
    def test_get_token_id_invalid_sub_slug(self, mock_get):
        """Test that ValueError is raised for invalid sub_slug."""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Test with invalid sub_slug
        with self.assertRaises(ValueError) as context:
            self.trader.get_token_id(
                event_slug="fed-decision-in-january",
                outcome="YES",
                sub_slug="invalid-slug"
            )
        
        self.assertIn("not found", str(context.exception))
        self.assertIn("Available markets", str(context.exception))
    
    @patch('execute_trade.requests.get')
    def test_get_token_id_invalid_outcome(self, mock_get):
        """Test that ValueError is raised for invalid outcome."""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.test_event_data
        mock_get.return_value = mock_response
        
        # Test with invalid outcome
        with self.assertRaises(ValueError) as context:
            self.trader.get_token_id(
                event_slug="fed-decision-in-january",
                outcome="MAYBE"
            )
        
        self.assertIn("not found", str(context.exception))
        self.assertIn("Available outcomes", str(context.exception))
    
    @patch('execute_trade.requests.get')
    def test_get_token_id_no_markets(self, mock_get):
        """Test that ValueError is raised when event has no markets."""
        # Mock the API response with no markets
        event_data_no_markets = self.test_event_data.copy()
        event_data_no_markets["markets"] = []
        
        mock_response = MagicMock()
        mock_response.json.return_value = event_data_no_markets
        mock_get.return_value = mock_response
        
        # Test
        with self.assertRaises(ValueError) as context:
            self.trader.get_token_id(
                event_slug="fed-decision-in-january",
                outcome="YES"
            )
        
        self.assertIn("Could not find token_id", str(context.exception))


if __name__ == '__main__':
    unittest.main()

