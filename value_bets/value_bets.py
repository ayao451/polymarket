#!/usr/bin/env python3
"""
Main entry point for value betting bot.

Delegates to `PolymarketSportsBettingBotInterface.run_moneyline()` to find and execute value bets.
"""

import sys
import time
from datetime import datetime, timedelta
import os
import csv
from typing import Optional

from polymarket_sports_betting_bot.polymarket_sports_betting_bot_interface import (
    PolymarketSportsBettingBotInterface,
)

from pinnacle_scraper.pinnacle_odds_service import PinnacleBasketballOddsService




def _successful_trades_path() -> str:
    # Keep in sync with TradeExecutorService._trades_csv_path()
    value_bets_root = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(value_bets_root, "trades.csv")




def _get_trade_count() -> int:
    """
    Return the current number of trades in the CSV file.
    """
    path = _successful_trades_path()
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return len(list(reader))
    except Exception:
        return 0


def _print_trades_since(start_index: int, testing_mode: bool = False) -> None:
    """
    Print trades added since start_index (0-based row index in CSV) in a human-readable format.
    Always prints something, even if no trades were made.
    
    Args:
        start_index: Index to start reading trades from
        testing_mode: If True, print all trades with "TEST RUN" labels.
                     If False, only print successful trades.
    """
    path = _successful_trades_path()
    
    print("\n" + "=" * 80)
    if testing_mode:
        print("TEST RUN - Trades that would have been made (not actually executed):")
    else:
        print("Trades made during this run:")
    print("=" * 80)
    
    if not os.path.exists(path):
        print("  No trades made in this run." if not testing_mode else "  No trades would have been made in this test run.")
        return

    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"  No trades made in this run. (Error reading trade log: {e})" if not testing_mode else f"  No trades would have been made in this test run. (Error reading trade log: {e})")
        return

    if start_index >= len(rows):
        print("  No trades made in this run." if not testing_mode else "  No trades would have been made in this test run.")
        return

    new_rows = rows[start_index:]
    if not new_rows:
        print("  No trades made in this run." if not testing_mode else "  No trades would have been made in this test run.")
        return

    # In real runs, filter to only successful trades
    if not testing_mode:
        new_rows = [row for row in new_rows if row.get("success", "").strip().lower() == "true"]
        if not new_rows:
            print("  No successful trades made in this run.")
            return

    for i, row in enumerate(new_rows, start=1):
        game = row.get("game", "").strip()
        team = row.get("team", "").strip()
        side = row.get("side", "").strip()
        price = row.get("price", "").strip()
        size = row.get("size", "").strip()
        amount = row.get("amount", "").strip()
        payout = row.get("payout", "").strip()
        profit = row.get("profit", "").strip()
        ev = row.get("expected_payout_per_$1", "").strip()
        ts = row.get("ts", "").strip()
        success = row.get("success", "").strip()
        status = row.get("status", "").strip()
        order_id = row.get("order_id", "").strip()
        
        # Format the trade in a human-readable way
        print(f"\n  Trade #{i}:")
        if testing_mode:
            print(f"    ⚠️  TEST RUN - Trade NOT actually executed")
        if game:
            print(f"    Game:     {game}")
        if team:
            side_str = f" ({side})" if side else ""
            print(f"    Team:     {team}{side_str}")
        if amount:
            print(f"    Amount:   ${amount}")
        if price:
            print(f"    Price:    ${price} per share")
        if payout:
            print(f"    Payout:   ${payout}")
        if profit:
            try:
                profit_val = float(profit)
                profit_sign = "+" if profit_val >= 0 else ""
                print(f"    Profit:   {profit_sign}${profit_val:.2f}")
            except (ValueError, TypeError):
                pass
        if ev:
            try:
                ev_val = float(ev)
                edge_pct = (ev_val - 1.0) * 100
                print(f"    Edge:     {edge_pct:+.2f}% (EV: ${ev_val:.4f} per $1)")
            except (ValueError, TypeError):
                pass
        if success:
            success_indicator = "✓" if success.lower() == "true" else "✗"
            print(f"    Status:   {success_indicator} {status if status else success}")
        if order_id and not testing_mode:
            print(f"    Order ID: {order_id[:20]}..." if len(order_id) > 20 else f"    Order ID: {order_id}")




def main() -> int:
    # Flags:
    # - --tomorrow: run a single scan for tomorrow's (local) games and exit
    run_tomorrow_once = "--tomorrow" in sys.argv[1:]
    # - --testing: do not place trades; only print what we would do
    testing_mode = "--testing" in sys.argv[1:]
    # - --started, --notstarted, --all: filter games by start status (default: --all)
    game_status = "all"  # default
    if "--started" in sys.argv[1:]:
        game_status = "started"
    elif "--notstarted" in sys.argv[1:]:
        game_status = "notstarted"
    elif "--all" in sys.argv[1:]:
        game_status = "all"

    # Scheduling (only used in the "scan mode" loop below)
    RUN_INTERVAL_SECONDS = 2 * 60 * 60  # 2 hours
    MAX_RUNTIME_SECONDS = 6 * 60 * 60  # 6 hours

    # NOTE: Do not do an initial multi-hour sleep; it prevents safe testing and debugging.

    # Backwards-compatible: if user supplies <away> <home> [YYYY-MM-DD], run once.
    # We also allow flags like --testing to appear anywhere.
    positional = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(positional) >= 2:
        bot = PolymarketSportsBettingBotInterface(enable_trading=not testing_mode)
        argv = [sys.argv[0], *positional]
        return bot.run_moneyline(argv)

    bot = PolymarketSportsBettingBotInterface(enable_trading=not testing_mode)
    pinnacle = PinnacleBasketballOddsService(timeout_ms=45000)

    start_time = time.time()
    any_events_seen_overall = False

    while (time.time() - start_time) < MAX_RUNTIME_SECONDS:
        now = datetime.now().astimezone()
        today = now.date()
        if run_tomorrow_once:
            today = today + timedelta(days=1)
        date_str = today.strftime("%Y-%m-%d")

        # Track trades before this iteration
        trade_count_before = _get_trade_count()

        print("\n" + "=" * 80)
        status_label = {"started": "started", "notstarted": "not started yet", "all": "all"}[game_status]
        print(f"Running Pinnacle scan for basketball games on {date_str} (local time) - {status_label}")
        print("=" * 80)

        any_events_today = False
        any_remaining = False

        try:
            games = pinnacle.list_games_for_date(today, game_status=game_status)
        except Exception as e:
            print(f"ERROR: Failed to fetch today's Pinnacle games: {e}")
            games = []

        if games:
            any_events_today = True
            any_events_seen_overall = True

        if games:
            any_remaining = True
            for g in games:
                away = str(getattr(g, "away_team", "")).strip()
                home = str(getattr(g, "home_team", "")).strip()
                if not away or not home:
                    continue

                print("\n" + "-" * 80)
                print(f"{away} @ {home}")
                print("-" * 80)

                # sport_key is kept for backwards compatibility; Pinnacle is used.
                try:
                    bot.run_moneyline(["prog", away, home, date_str], sport_key="PINNACLE")
                except RuntimeError as e:
                    print(f"Skipping game due to sportsbook fetch error: {e}")
                    continue

        # Print trades made during this run
        _print_trades_since(trade_count_before, testing_mode=testing_mode)

        if not any_remaining:
            print("No remaining events to process. Exiting.")
            return 0 if any_events_today else 2

        # In --testing mode we want a single pass and exit (no multi-hour sleep loop).
        if testing_mode:
            print("Finished single testing scan. Exiting.")
            return 0 if any_events_today else 2

        if run_tomorrow_once:
            print("Finished single tomorrow scan. Exiting.")
            return 0 if any_events_today else 2

        remaining_runtime = MAX_RUNTIME_SECONDS - (time.time() - start_time)
        if remaining_runtime <= 0:
            break

        sleep_for = min(RUN_INTERVAL_SECONDS, remaining_runtime)
        sleep_minutes = int(round(sleep_for / 60))
        print(f"\nSleeping {sleep_minutes} minutes...\n")
        time.sleep(sleep_for)

    print(f"Reached max runtime ({MAX_RUNTIME_SECONDS/3600:.0f} hours). Exiting.")
    return 0 if any_events_seen_overall else 2


if __name__ == "__main__":
    raise SystemExit(main())

