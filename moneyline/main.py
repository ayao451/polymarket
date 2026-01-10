#!/usr/bin/env python3
"""
Main entry point.

Delegates to `PolymarketSportsBettingBotInterface.run_nba_moneyline()` (moneyline runner).
"""

import sys
import time
from datetime import datetime

from polymarket_sports_betting_bot.polymarket_sports_betting_bot_interface import (
    PolymarketSportsBettingBotInterface,
)

from sportsbook_odds_service.fetch_game_odds import (
    NBA_SPORT_KEY,
    MONEYLINE_MARKET_KEY,
    list_events_for_local_date,
)
from sportsbook_odds_service.the_odds_api_connector import theOddsAPIConnector


def main() -> int:
    # Backwards-compatible: if user supplies <away> <home> [YYYY-MM-DD], run once.
    if len(sys.argv) >= 3:
        return PolymarketSportsBettingBotInterface().run_nba_moneyline(sys.argv)

    bot = PolymarketSportsBettingBotInterface()
    connector = theOddsAPIConnector()

    while True:
        now = datetime.now().astimezone()
        today = now.date()
        date_str = today.strftime("%Y-%m-%d")

        print("\n" + "=" * 80)
        print(f"Running moneyline scan for all NBA games on {date_str} (local time)")
        print("=" * 80)

        try:
            events = connector.get_odds(NBA_SPORT_KEY, MONEYLINE_MARKET_KEY)
            todays_events = list_events_for_local_date(events, local_date=today)
        except Exception as e:
            print(f"ERROR: Failed to fetch today's NBA events: {e}")
            todays_events = []

        if not todays_events:
            print("No NBA games found for today (or fetch failed).")
        else:
            for e in todays_events:
                away = str(e.get("away_team", "")).strip()
                home = str(e.get("home_team", "")).strip()
                if not away or not home:
                    continue
                print("\n" + "-" * 80)
                print(f"{away} @ {home}")
                print("-" * 80)
                bot.run_nba_moneyline(["prog", away, home, date_str])

        print("\nSleeping 15 minutes...\n")
        time.sleep(15 * 60)


if __name__ == "__main__":
    raise SystemExit(main())

