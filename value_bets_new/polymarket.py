#!/usr/bin/env python3
"""
Polymarket interface for fetching events.
"""

from __future__ import annotations

import sys
import os
import json
import requests
import time
from datetime import date, datetime, timezone
from typing import List, Dict, Any, Optional
from py_clob_client.client import ClobClient

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from value_bets_new.constants import MarketType, MarketOdds
from value_bets_new.rewrite_later import PolymarketMarketExtractor, PolymarketGameFinder


class PolymarketEvent:
    def __init__(
        self,
        event_slug: str,
        away_team: str,
        home_team: str,
        play_date: date,
        market_slugs_by_event: Dict[MarketType, List[str]],
    ) -> None:
        self.event_slug = event_slug
        self.away_team = away_team
        self.home_team = home_team
        self.play_date = play_date
        self.market_slugs_by_event = market_slugs_by_event

    def __str__(self) -> str:
        return f"{self.away_team} @ {self.home_team} - {self.event_slug}"
    
    def market_slugs_by_event_to_string(self) -> str:
        return "\n".join([f"{market.value}: {slugs}" for market, slugs in self.market_slugs_by_event.items()])


class PolymarketInterface:
    def __init__(self) -> None:
        self.game_finder = PolymarketGameFinder()
        self.GAMMA_API_BASE = "https://gamma-api.polymarket.com"
        self.CLOB_API_BASE = "https://clob.polymarket.com"
        self.session = requests.Session()
        self.clob_client = ClobClient(
            host=self.CLOB_API_BASE,
            chain_id=137  # Polygon mainnet
        )
    
    def _retry_request(self, method: str, url: str, max_retries: int = 3, **kwargs) -> Optional[requests.Response]:
        """
        Retry HTTP requests with exponential backoff for connection errors.
        
        Args:
            method: HTTP method ('get', 'post', etc.)
            url: URL to request
            max_retries: Maximum number of retry attempts
            **kwargs: Additional arguments to pass to requests method
            
        Returns:
            Response object or None if all retries failed
        """
        for attempt in range(max_retries):
            try:
                response = getattr(self.session, method.lower())(url, **kwargs)
                response.raise_for_status()
                return response
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    print(f"[DEBUG] [PolymarketInterface] Connection error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"[DEBUG] [PolymarketInterface] Connection error after {max_retries} attempts: {e}")
                    raise
            except requests.exceptions.HTTPError as e:
                # Don't retry HTTP errors (4xx, 5xx) - these are not transient
                print(f"[DEBUG] [PolymarketInterface] HTTP error: {e}")
                raise
        return None

    def _within_time_contraints(self, event: Dict[str, Any]) -> bool:
        """
        Return True iff event is within time constraints:
        - Event has not started.
        - Event 'slug' is present.
        Note: 12-hour time constraint removed for soccer games.
        """
        start_time = self.game_finder._parse_start_time(event)
        if start_time is None:
            return False

        now = datetime.now(timezone.utc)
        # Ensure start_time is timezone-aware and convert to UTC for comparison
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        else:
            start_time = start_time.astimezone(timezone.utc)
        seconds_until_start = (start_time - now).total_seconds()
        if seconds_until_start < 0:
            # Already started
            return False

        event_slug = event.get("slug") or ""
        if not event_slug:
            return False

        return True

    def fetch_polymarket_events(
        self,
        whitelisted_prefixes: List[str],
        markets: List[MarketType],
    ) -> List[PolymarketEvent]:
        """Fetch Polymarket events for today and tomorrow."""
        polymarket_events = []
        events_checked = 0
        events_filtered_by_prefix = 0
        events_filtered_by_time = 0
        events_filtered_by_title = 0
        
        for page in range(100):  # Search up to 100 pages
            events = self.game_finder.fetch_events_page(
                limit=100,
                offset=page * 100,
                active=True,
                closed=False,
                order="startTime",
                ascending=False,
            )
            if not events:
                break

            for event in events:
                events_checked += 1
                event_slug = event.get("slug")
                
                # Debug: Log all event slugs for tennis debugging
                if ("tennis" in str(whitelisted_prefixes).lower() or "atp" in str(whitelisted_prefixes).lower() or "wta" in str(whitelisted_prefixes).lower()) and event_slug:
                    print(f"[DEBUG] [Tennis] Checking event slug: '{event_slug}'")
                
                if not self._within_time_contraints(event):
                    events_filtered_by_time += 1
                    if event_slug and ("tennis" in str(whitelisted_prefixes).lower() or "atp" in str(whitelisted_prefixes).lower() or "wta" in str(whitelisted_prefixes).lower()):
                        print(f"[DEBUG] [Tennis] Event '{event_slug}' FILTERED - time constraints")
                    continue

                if not event_slug:
                    continue

                # Filter by whitelisted league prefixes (e.g. "nba-", "ufc-", "atp-", "wta-", etc)
                # Check if slug starts with prefix or contains it with dashes (for tennis/other formats)
                matches_prefix = False
                for prefix in whitelisted_prefixes:
                    slug_lower = event_slug.lower()
                    prefix_lower = prefix.lower()
                    if (slug_lower.startswith(prefix_lower + "-") or 
                        f"-{prefix_lower}-" in slug_lower or 
                        slug_lower.startswith(prefix_lower)):
                        matches_prefix = True
                        if "tennis" in str(whitelisted_prefixes).lower() or "atp" in str(whitelisted_prefixes).lower() or "wta" in str(whitelisted_prefixes).lower():
                            print(f"[DEBUG] [Tennis] Event '{event_slug}' MATCHED prefix '{prefix}'")
                        break
                if not matches_prefix:
                    events_filtered_by_prefix += 1
                    if "tennis" in str(whitelisted_prefixes).lower() or "atp" in str(whitelisted_prefixes).lower() or "wta" in str(whitelisted_prefixes).lower():
                        print(f"[DEBUG] [Tennis] Event '{event_slug}' FILTERED - doesn't match prefixes {whitelisted_prefixes}")
                    continue

                event_title = event.get("title")
                if not event_title:
                    events_filtered_by_title += 1
                    if "tennis" in str(whitelisted_prefixes).lower() or "atp" in str(whitelisted_prefixes).lower() or "wta" in str(whitelisted_prefixes).lower():
                        print(f"[DEBUG] [Tennis] Event '{event_slug}' FILTERED - no title")
                    continue

                parts = event_title.replace(" vs. ", " @ ").replace(" vs ", " @ ").split(" @ ", 1)
                if len(parts) != 2:
                    events_filtered_by_title += 1
                    if "tennis" in str(whitelisted_prefixes).lower() or "atp" in str(whitelisted_prefixes).lower() or "wta" in str(whitelisted_prefixes).lower():
                        print(f"[DEBUG] [Tennis] Event '{event_slug}' FILTERED - title doesn't parse: '{event_title}'")
                    continue

                away_team = parts[0]
                home_team = parts[1]

                # Extract play_date from event start time
                start_time = self.game_finder._parse_start_time(event)
                if start_time is None:
                    if "tennis" in str(whitelisted_prefixes).lower() or "atp" in str(whitelisted_prefixes).lower() or "wta" in str(whitelisted_prefixes).lower():
                        print(f"[DEBUG] [Tennis] Event '{event_slug}' FILTERED - no start_time")
                    continue
                # Convert to date object
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                play_date = start_time.astimezone(timezone.utc).date()

                # Market slugs by market type
                market_slugs_by_event = self._fetch_polymarket_market_slugs_given_event_slug(event_slug, markets)

                polymarket_event = PolymarketEvent(
                    event_slug=event_slug,
                    away_team=away_team,
                    home_team=home_team,
                    play_date=play_date,
                    market_slugs_by_event=market_slugs_by_event,
                )
                polymarket_events.append(polymarket_event)
        
        # Debug logging for tennis
        if "tennis" in str(whitelisted_prefixes).lower() or "atp" in str(whitelisted_prefixes).lower() or "wta" in str(whitelisted_prefixes).lower():
            print(f"[DEBUG] [Tennis] Summary - Events checked: {events_checked}, filtered by time: {events_filtered_by_time}, filtered by prefix: {events_filtered_by_prefix}, filtered by title: {events_filtered_by_title}, found: {len(polymarket_events)}")

        return polymarket_events
    
    def _fetch_polymarket_market_slugs_given_event_slug(self, event_slug : str, markets: List[MarketType]) -> Dict[MarketType, List[str]]:

        url = f"{self.GAMMA_API_BASE}/events/slug/{event_slug}"
        
        response = self._retry_request('get', url)
        if response is None:
            return {}
        event = response.json()

        if not event:
            return {}

        market_slugs = {}
        
        for market in markets:
            if market == MarketType.MONEYLINE:
                market_slugs[market] = [event_slug]
            elif market == MarketType.SPREADS:
                market_slugs[market] = PolymarketMarketExtractor.spread_market_slugs_from_event(event)
            elif market == MarketType.TOTALS:
                market_slugs[market] = PolymarketMarketExtractor.totals_market_slugs_from_event(event)
            elif market == MarketType.TOTALS_GAMES:
                market_slugs[market] = PolymarketMarketExtractor.totals_games_market_slugs_from_event(event)
            elif market == MarketType.TOTALS_SETS:
                market_slugs[market] = PolymarketMarketExtractor.totals_sets_market_slugs_from_event(event)

        return market_slugs

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
        response = self._retry_request('get', event_url)
        if response is None:
            raise ValueError(f"Failed to fetch event '{event_slug}' after retries")
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
        
        # Extract condition_id from market data
        condition_id = target_market.get("conditionId") or target_market.get("condition_id")
        if condition_id:
            condition_id = str(condition_id)
        else:
            condition_id = None
        
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
        odds: List[MarketOdds] = []
        
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
            
            odds.append(MarketOdds(
                token_id=token_id,
                team_name=outcome_label,
                best_bid=best_bid,
                bid_volume=bid_volume,
                best_ask=best_ask,
                ask_volume=ask_volume,
                spread=spread,
                condition_id=condition_id
            ))

        return odds
