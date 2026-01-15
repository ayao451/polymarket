#!/usr/bin/env python3
"""
Standalone Polymarket Market Analyzer

Fetches event data from Polymarket's Gamma API, extracts token IDs,
fetches orderbooks from the CLOB API, and displays market statistics.

This module is self-contained and can be moved to a new codebase.
"""

from __future__ import annotations

import requests
import json
import sys
from dataclasses import dataclass
from typing import List, Dict, Optional
from tabulate import tabulate


class PolymarketMarketAnalyzer:
    """Analyzer for Polymarket market data."""

    @dataclass(frozen=True)
    class MarketOdds:
        token_id: str
        market: str
        best_bid: Optional[float]
        bid_volume: float
        best_ask: Optional[float]
        ask_volume: float
        spread: Optional[float]
    
    GAMMA_API_BASE = "https://gamma-api.polymarket.com"
    CLOB_API_BASE = "https://clob.polymarket.com"
    def __init__(self):
        """
        Initialize the analyzer.
        """
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

    @staticmethod
    def spread_market_slugs_from_event(event: Dict) -> List[str]:
        """
        Given a Gamma event payload (from /events/slug/{event_slug}), return the list of
        spread market slugs, e.g. "nba-phx-mia-2026-01-13-spread-home-1pt5".

        Heuristics (to avoid missing spread markets):
        - include if `question` contains "spread" (case-insensitive), OR
        - include if `sportsMarketType` contains "spread", OR
        - include if slug contains "-spread-"

        Exclusions:
        - exclude first half spreads (e.g. "1H Spread" / "first_half_spreads" / slug contains "-1h-")
        """
        if not isinstance(event, dict):
            return []

        out: List[str] = []
        markets = event.get("markets", []) or []
        # NOTE: keep this lightweight; callers may print summaries as needed.
        for m in markets:
            try:
                if not isinstance(m, dict):
                    continue
                q = str(m.get("question") or "")
                smt = str(m.get("sportsMarketType") or "")
                slug = str(m.get("slug") or "").strip()

                # Exclude 1H / first half spread markets; we only want full game spreads.
                q_low = q.lower()
                smt_low = smt.lower()
                slug_low = slug.lower()
                is_first_half = (
                    q_low.startswith("1h ")
                    or q_low.startswith("1hspread")
                    or "1h spread" in q_low
                    or "first half" in q_low
                    or "first_half" in smt_low
                    or "first half" in smt_low
                    or "-1h-" in slug_low
                    or slug_low.startswith("1h-")
                )
                if is_first_half:
                    continue

                is_spread = (
                    ("spread" in q_low)
                    or ("spread" in smt_low)
                    or ("-spread-" in slug_low)
                )
                if not is_spread:
                    continue
                if slug:
                    out.append(slug)
            except Exception:
                continue

        # De-dup while keeping order
        seen = set()
        deduped: List[str] = []
        for s in out:
            if s in seen:
                continue
            seen.add(s)
            deduped.append(s)
        return deduped

    @staticmethod
    def totals_market_slugs_from_event(event: Dict) -> List[str]:
        """
        Given a Gamma event payload (from /events/slug/{event_slug}), return the list of
        full-game totals market slugs, e.g. "nba-sas-okc-2026-01-13-total-229pt5".

        Heuristics:
        - include if question contains "o/u" or "total" (case-insensitive), OR
        - include if sportsMarketType contains "total", OR
        - include if slug contains "-total-"

        Exclusions:
        - exclude first half totals (e.g. "1H O/U" / "first_half_totals" / slug contains "-1h-")
        """
        if not isinstance(event, dict):
            return []

        out: List[str] = []
        markets = event.get("markets", []) or []
        for m in markets:
            try:
                if not isinstance(m, dict):
                    continue
                q = str(m.get("question") or "")
                smt = str(m.get("sportsMarketType") or "")
                slug = str(m.get("slug") or "").strip()

                q_low = q.lower()
                smt_low = smt.lower()
                slug_low = slug.lower()

                is_first_half = (
                    q_low.startswith("1h ")
                    or "1h o/u" in q_low
                    or "1h total" in q_low
                    or "first half" in q_low
                    or "first_half" in smt_low
                    or "first half" in smt_low
                    or "-1h-" in slug_low
                    or slug_low.startswith("1h-")
                )
                if is_first_half:
                    continue

                is_total = (
                    ("o/u" in q_low)
                    or ("total" in q_low)
                    or ("total" in smt_low)
                    or ("-total-" in slug_low)
                )
                if not is_total:
                    continue
                if slug:
                    out.append(slug)
            except Exception:
                continue

        seen = set()
        deduped: List[str] = []
        for s in out:
            if s in seen:
                continue
            seen.add(s)
            deduped.append(s)
        return deduped

    def get_spread_market_slugs_for_event(self, event_slug: str) -> List[str]:
        """
        Return all spread market slugs for a given Polymarket sports event slug.

        Gamma API flow:
          1) GET /events/slug/{event_slug}
          2) filter event["markets"] where sportsMarketType == "spread"
          3) return each market["slug"]

        Errors are handled gracefully (returns []).
        """
        if not event_slug:
            return []

        event = self.fetch_event_by_slug(event_slug)
        if not event:
            return []

        return self.spread_market_slugs_from_event(event)
    
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
    
    def analyze_markets(
        self, event_slug: str, market_slug: Optional[str] = None
    ) -> List[MarketOdds]:
        """
        Main method to fetch event by slug, extract tokens, and analyze orderbooks.
        
        Args:
            event_slug: The slug of the event to analyze
            market_slug: If provided, only analyze this specific market within the event
            
        Returns:
            List of market analysis dictionaries
        """
        # Fetch event by slug
        event = self.fetch_event_by_slug(event_slug)
        if not event:
            print(f"Failed to fetch event: {event_slug}")
            return []

        return self.analyze_event(event, market_slug=market_slug)

    def analyze_event(self, event: Dict, market_slug: Optional[str] = None) -> List[MarketOdds]:
        """
        Analyze markets for a pre-fetched Gamma event object.

        Args:
            event: Event dictionary as returned by Gamma API.
            market_slug: If provided, only analyze this specific market within the event.
        """
        event_slug = str(event.get("slug", "") or "")

        # Extract token IDs (for all markets)
        token_data = self.extract_token_ids(event)

        # Optionally filter to a single market
        if market_slug is not None:
            token_data = [t for t in token_data if t.get("market_slug") == market_slug]
            if not token_data:
                print(f"Failed to find market '{market_slug}' in event '{event_slug}'")
                return []

        # Analyze each market
        results: List[MarketOdds] = []

        for token_info in token_data:
            token_id = token_info["token_id"]

            orderbook = self.fetch_orderbook(token_id)

            if orderbook:
                stats = self.calculate_market_stats(orderbook)

                market_label = f"{token_info['question'][:50]} ({token_info.get('outcome', 'Unknown')})"
                results.append(
                    MarketOdds(
                        token_id=str(token_id),
                        market=market_label,
                        best_bid=(float(stats["best_bid"]) if stats["best_bid"] is not None else None),
                        bid_volume=float(stats["bid_volume"] or 0.0),
                        best_ask=(float(stats["best_ask"]) if stats["best_ask"] is not None else None),
                        ask_volume=float(stats["ask_volume"] or 0.0),
                        spread=(float(stats["spread"]) if stats["spread"] is not None else None),
                    )
                )

        return results
    
    def display_results(self, results: List[MarketOdds]):
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
                result.market,
                (f"{result.best_bid:.4f}" if result.best_bid is not None else "N/A"),
                f"{result.bid_volume:.2f}",
                (f"{result.best_ask:.4f}" if result.best_ask is not None else "N/A"),
                f"{result.ask_volume:.2f}",
                (f"{result.spread:.4f}" if result.spread is not None else "N/A"),
            ])
        
        headers = ["Market", "Best Bid", "Bid Volume", "Best Ask", "Ask Volume", "Spread"]
        
        print("\n" + "="*120)
        print("POLYMARKET MARKET ANALYSIS")
        print("="*120)
        print(tabulate(table_data, headers=headers, tablefmt="grid", floatfmt=".4f"))
        print(f"\nTotal markets analyzed: {len(results)}")
    
    def get_market_data(
        self, event_slug: str, market_slug: Optional[str] = None
    ) -> List[MarketOdds]:
        """
        Get market data without displaying it (for programmatic use).
        
        Args:
            event_slug: The slug of the event to analyze
            market_slug: Optional specific market slug within the event
            
        Returns:
            List of market analysis dictionaries
        """
        return self.analyze_markets(event_slug, market_slug)


def main():
    """Main entry point."""
    # Get event slug (and optional market slug) from command line argument or use default
    if len(sys.argv) > 1:
        event_slug = sys.argv[1]
        market_slug = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        # Default example - can be changed or removed
        print("Usage: python polymarket_market_analyzer.py <event_slug> [market_slug]")
        print("Example: python polymarket_market_analyzer.py nba-chi-det-2026-01-07 nba-chi-det-2026-01-07")
        return
    
    # Analyze markets for the event
    analyzer = PolymarketMarketAnalyzer()
    results = analyzer.analyze_markets(event_slug, market_slug)
    analyzer.display_results(results)


if __name__ == "__main__":
    main()


# Public alias so callers can use `MarketOdds` in type hints (no class prefix needed).
MarketOdds = PolymarketMarketAnalyzer.MarketOdds

