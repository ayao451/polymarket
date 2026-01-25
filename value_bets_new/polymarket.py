#!/usr/bin/env python3
"""
Polymarket interface for fetching events.
"""

from __future__ import annotations

import sys
import os
import json
import requests
from datetime import date, datetime, timezone
from typing import List, Dict, Any
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

    def _within_time_contraints(self, event: Dict[str, Any]) -> bool:
        """
        Return True iff event is within time constraints:
        - Event has not started.
        - Event starts in <= 12 hours.
        - Event 'slug' is 1esent.
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
        if seconds_until_start < 0 or seconds_until_start > 12 * 3600:
            # Already started, or starts in more than 12 hours
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
                if not self._within_time_contraints(event):
                    continue

                event_slug = event.get("slug")

                # Filter by whitelisted league prefixes (e.g. "nba-", "ufc-", etc)
                if not any([event_slug.startswith(prefix + "-") for prefix in whitelisted_prefixes]):
                    continue

                event_title = event.get("title")
                if not event_title:
                    continue

                parts = event_title.replace(" vs. ", " @ ").split(" @ ", 1)
                if len(parts) != 2:
                    continue

                away_team = parts[0]
                home_team = parts[1]

                # Extract play_date from event start time
                start_time = self.game_finder._parse_start_time(event)
                if start_time is None:
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

        return polymarket_events
    
    def _fetch_polymarket_market_slugs_given_event_slug(self, event_slug : str, markets: List[MarketType]) -> Dict[MarketType, List[str]]:

        url = f"{self.GAMMA_API_BASE}/events/slug/{event_slug}"
        
        response = self.session.get(url)
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
        response = self.session.get(event_url)
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
