#!/usr/bin/env python3
"""
Script to fetch and analyze Polymarket market data.

Fetches all active events from Polymarket's Gamma API, extracts token IDs,
fetches orderbooks from the CLOB API, and displays market statistics.
"""

import requests
import json
import time
from typing import List, Dict, Optional
from tabulate import tabulate
from .find_nba_game import PolymarketGameFinder


class PolymarketMarketAnalyzer:
    """Analyzer for Polymarket market data."""
    
    GAMMA_API_BASE = "https://gamma-api.polymarket.com"
    CLOB_API_BASE = "https://clob.polymarket.com"
    REQUEST_DELAY = 0.1  # Delay between API calls in seconds
    
    def __init__(self):
        self.session = requests.Session()
    
    def fetch_event_by_slug(self, slug: str) -> Optional[Dict]:
        """
        Fetch a specific event by slug from Polymarket's Gamma API.
        
        Args:
            slug: The event slug to fetch
            
        Returns:
            Event dictionary or None if error
        """
        url = f"{self.GAMMA_API_BASE}/events/slug/{slug}"
        
        try:
            print(f"Fetching event: {slug}...")
            response = self.session.get(url)
            response.raise_for_status()
            event_data = response.json()
            print(f"Successfully fetched event: {event_data.get('title', 'Unknown')}\n")
            return event_data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching event: {e}")
            return None
    
    def extract_token_ids(self, event: Dict) -> List[Dict]:
        """
        Extract clobTokenIds from all markets in the event.
        
        Args:
            event: Event dictionary
            
        Returns:
            List of dictionaries with token_id and market info
        """
        token_data = []
        markets = event.get("markets", [])
        
        for market in markets:
            clob_token_ids_str = market.get("clobTokenIds")
            if not clob_token_ids_str:
                continue
            
            try:
                clob_token_ids = json.loads(clob_token_ids_str)
                question = market.get("question", "Unknown")
                event_title = event.get("title", "Unknown Event")
                outcomes = json.loads(market.get("outcomes", "[]"))
                
                for idx, token_id in enumerate(clob_token_ids):
                    outcome = outcomes[idx] if idx < len(outcomes) else f"Outcome {idx + 1}"
                    token_data.append({
                        "token_id": token_id,
                        "question": question,
                        "outcome": outcome,
                        "event_title": event_title,
                        "market_slug": market.get("slug", ""),
                        "event_slug": event.get("slug", "")
                    })
            except json.JSONDecodeError:
                continue
        
        print(f"Extracted {len(token_data)} token IDs from markets\n")
        return token_data
    
    def fetch_orderbook(self, token_id: str) -> Optional[Dict]:
        """
        Fetch orderbook for a given token ID.
        
        Args:
            token_id: The token ID to fetch orderbook for
            
        Returns:
            Dictionary with bids and asks, or None if error
        """
        url = f"{self.CLOB_API_BASE}/book"
        params = {"token_id": token_id}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching orderbook for token {token_id}: {e}")
            return None
    
    def calculate_market_stats(self, orderbook: Dict) -> Dict:
        """
        Calculate market statistics from orderbook data.
        
        Args:
            orderbook: Orderbook dictionary with bids and asks
            
        Returns:
            Dictionary with calculated statistics
        """
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        
        # Best bid (highest price) - find max if not sorted
        best_bid = None
        if bids:
            best_bid = max(float(bid["price"]) for bid in bids)
        
        # Best ask (lowest price) - find min if not sorted
        best_ask = None
        if asks:
            best_ask = min(float(ask["price"]) for ask in asks)
        
        # Total bid volume (sum of all bid sizes)
        bid_volume = sum(float(bid["size"]) for bid in bids) if bids else 0.0
        
        # Total ask volume (sum of all ask sizes)
        ask_volume = sum(float(ask["size"]) for ask in asks) if asks else 0.0
        
        # Spread (best ask - best bid)
        spread = best_ask - best_bid if (best_bid is not None and best_ask is not None) else None
        
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid_volume": bid_volume,
            "ask_volume": ask_volume,
            "spread": spread
        }
    
    def analyze_markets(self, event_slug: str) -> List[Dict]:
        """
        Main method to fetch event by slug, extract tokens, and analyze orderbooks.
        
        Args:
            event_slug: The slug of the event to analyze
            
        Returns:
            List of market analysis dictionaries
        """
        # Fetch event by slug
        event = self.fetch_event_by_slug(event_slug)
        if not event:
            print(f"Failed to fetch event: {event_slug}")
            return []
        
        # Extract token IDs
        token_data = self.extract_token_ids(event)
        
        # Analyze each market
        results = []
        print("Fetching orderbooks and calculating statistics...\n")
        
        for i, token_info in enumerate(token_data, 1):
            token_id = token_info["token_id"]
            print(f"[{i}/{len(token_data)}] Processing token {token_id[:20]}...")
            
            orderbook = self.fetch_orderbook(token_id)
            time.sleep(self.REQUEST_DELAY)
            
            if orderbook:
                stats = self.calculate_market_stats(orderbook)
                
                # Only include markets with valid bid/ask data
                if stats["best_bid"] is not None and stats["best_ask"] is not None:
                    market_label = f"{token_info['question'][:50]} ({token_info.get('outcome', 'Unknown')})"
                    results.append({
                        "market": market_label,
                        "best_bid": stats["best_bid"],
                        "bid_volume": stats["bid_volume"],
                        "best_ask": stats["best_ask"],
                        "ask_volume": stats["ask_volume"],
                        "spread": stats["spread"]
                    })
        
        return results
    
    def display_results(self, results: List[Dict]):
        """
        Display results in a formatted table.
        
        Args:
            results: List of market analysis dictionaries
        """
        if not results:
            print("No market data to display.")
            return
        
        # Prepare table data
        table_data = []
        for result in results:
            table_data.append([
                result["market"],
                f"{result['best_bid']:.4f}" if result['best_bid'] is not None else "N/A",
                f"{result['bid_volume']:.2f}",
                f"{result['best_ask']:.4f}" if result['best_ask'] is not None else "N/A",
                f"{result['ask_volume']:.2f}",
                f"{result['spread']:.4f}" if result['spread'] is not None else "N/A"
            ])
        
        headers = ["Market", "Best Bid", "Bid Volume", "Best Ask", "Ask Volume", "Spread"]
        
        print("\n" + "="*120)
        print("POLYMARKET MARKET ANALYSIS")
        print("="*120)
        print(tabulate(table_data, headers=headers, tablefmt="grid", floatfmt=".4f"))
        print(f"\nTotal markets analyzed: {len(results)}")


def main():
    """Main entry point."""
    # Find today's Pistons vs Bulls game
    print("Finding today's Pistons vs Bulls NBA game...")
    game_finder = PolymarketGameFinder()
    game_info = game_finder.find_pistons_bulls_game()
    
    if not game_info:
        print("Could not find Pistons vs Bulls game. Exiting.")
        return
    
    event_slug = game_info["slug"]
    print(f"\nUsing event slug: {event_slug}\n")
    

    # Analyze markets for the game
    analyzer = PolymarketMarketAnalyzer()
    results = analyzer.analyze_markets(event_slug)
    analyzer.display_results(results)


if __name__ == "__main__":
    main()

