#!/usr/bin/env python3
"""
Value bets orchestrator - handles all sports in a unified way.

This orchestrator:
1. Loops through all configured sports
2. For each sport, continuously:
   - Fetches Pinnacle games
   - Fetches Polymarket games
   - Fetches market slugs
   - Matches games between the two platforms
   - Evaluates value bets for each matched game and market
   - Executes trades when value bets are found
"""

from __future__ import annotations

import sys
import os


# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from value_bets_new.constants import MarketType, Sport
from value_bets_new.polymarket import PolymarketInterface, PolymarketEvent
from value_bets_new.pinnacle_odds_service import PinnacleInterface
from value_bets_new.event_processor import EventProcessor
from value_bets_new.pinnacle_odds_interface import PinnacleSportsbookOddsInterface
from value_bets_new.trade_executor.trade_executor_service import TradeExecutorService



class ValueBetsOrchestrator:
    def __init__(self):
        self.polymarket_interface = PolymarketInterface()
        self.pinnacle_interface = PinnacleInterface()
        self.event_processor = EventProcessor()
        self.trade_executor = TradeExecutorService()
        # Create a map of PinnacleSportsbookOddsInterface instances for each sport
        self.pinnacle_odds_interfaces = {
            sport: PinnacleSportsbookOddsInterface(sport=sport)
            for sport in Sport
        }
    
    sports_to_markets = {
        Sport.BASKETBALL: [MarketType.MONEYLINE, MarketType.SPREADS, MarketType.TOTALS],
        Sport.HOCKEY: [MarketType.MONEYLINE, MarketType.SPREADS, MarketType.TOTALS],
        Sport.TENNIS: [MarketType.MONEYLINE, MarketType.SPREADS, MarketType.TOTALS_GAMES, MarketType.TOTALS_SETS],
        Sport.UFC: [MarketType.MONEYLINE],
    }

    sports_to_whitelisted_prefixes = {
        Sport.BASKETBALL: ["nba", "cbb", "bkcl", "bkligend", "bkseriea", "bknbl", "bkcba", "bkfr1", "bkarg", "bkkbl", "euroleague"],
        Sport.HOCKEY: ["nhl"],
        Sport.TENNIS: ["atp", "wta"],
        Sport.UFC: ["ufc"],
    }

    def run(self) -> None:
        for sport, markets in self.sports_to_markets.items():
            while True:
                polymarket_events = self.polymarket_interface.fetch_polymarket_events(
                    whitelisted_prefixes=self.sports_to_whitelisted_prefixes[sport],
                    markets=markets,
                )

                for polymarket_event in polymarket_events:
                    self._process_game(sport, polymarket_event)
                    
    def _process_game(self, sport: Sport, polymarket_event: PolymarketEvent) -> None:
        for market in polymarket_event.market_slugs_by_event:
            event_slugs = polymarket_event.market_slugs_by_event[market]
            for market_slug in event_slugs:
                polymarket_odds_list = self.polymarket_interface.retrieve_polymarket_odds(polymarket_event.event_slug, market_slug)

                sportsbook_odds = self.pinnacle_odds_interfaces[sport].get_moneyline_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
                print(polymarket_event.away_team, polymarket_event.home_team, sportsbook_odds)
                for market_odds in polymarket_odds_list:
                    value_bet = self.event_processor.process_two_outcome_event(market_odds.team_name, market_odds, sportsbook_odds)
                    if value_bet is not None:
                        game_str = f"{polymarket_event.away_team} @ {polymarket_event.home_team}"
                        trade_result = self.trade_executor.execute_value_bet(
                            value_bet,
                            game_str=game_str,
                        )
                        if trade_result is not None:
                            print(trade_result)
                


    def _retrieve_sportsbook_odds(self, sport: Sport, polymarket_event: PolymarketEvent, market_type: MarketType) -> SportsbookOdds:
        if market_type == MarketType.MONEYLINE:
            return self.pinnacle_odds_interfaces[sport].get_moneyline_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        elif market_type == MarketType.SPREADS:
            return self.pinnacle_odds_interfaces[sport].get_spread_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        elif market_type == MarketType.TOTALS:
            return self.pinnacle_odds_interfaces[sport].get_totals_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        elif market_type == MarketType.TOTALS_GAMES:
            return self.pinnacle_odds_interfaces[sport].get_totals_games_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        elif market_type == MarketType.TOTALS_SETS:
            return self.pinnacle_odds_interfaces[sport].get_totals_sets_odds(polymarket_event.away_team, polymarket_event.home_team, polymarket_event.play_date)
        else:
            raise ValueError(f"Invalid market type: {market_type}")


def main() -> int:
    """Main entry point."""
    orchestrator = ValueBetsOrchestrator()
    
    try:
        orchestrator.run()
        return 0
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Script interrupted by user. Exiting gracefully...")
        return 0
    except Exception as e:
        print(f"\n{'!'*60}")
        print(f"!!! UNEXPECTED ERROR !!!")
        print(f"{'!'*60}")
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
