#!/usr/bin/env python3
"""
Main entry point.

Delegates to `PolymarketSportsBettingBotInterface.run_moneyline()` (moneyline runner).
"""

import sys
import time
from datetime import datetime, timedelta
import os
import json
import csv
from typing import Any, Dict, List, Optional, Set, Tuple

from polymarket_sports_betting_bot.polymarket_sports_betting_bot_interface import (
    PolymarketSportsBettingBotInterface,
)

from sportsbook_odds_service.fetch_game_odds import (
    NCAA_SPORT_KEY,
    NCAAF_SPORT_KEY,
    NBA_SPORT_KEY,
    NFL_SPORT_KEY,
    NHL_SPORT_KEY,
    MONEYLINE_MARKET_KEY,
    list_events_for_local_date,
)
from sportsbook_odds_service.the_odds_api_connector import theOddsAPIConnector


SPORTS_TO_SCAN: List[Tuple[str, str]] = [
    ("NFL", NFL_SPORT_KEY),
    ("NCAAF", NCAAF_SPORT_KEY),
    ("NBA", NBA_SPORT_KEY),
    ("NCAA", NCAA_SPORT_KEY),
    ("NHL", NHL_SPORT_KEY),
]


def _normalize_team_name(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


def _game_key(away_team: str, home_team: str) -> str:
    """
    Normalized matchup key, directional (away @ home).
    """
    return f"{_normalize_team_name(away_team)} @ {_normalize_team_name(home_team)}"


def _event_key(e: Dict[str, Any], *, sport_key: str) -> str:
    """
    Stable identifier for an Odds API event.
    Prefer the API event id; fall back to a composite key.
    """
    eid = e.get("id")
    if eid:
        return f"{sport_key}:{eid}"
    away = str(e.get("away_team", "")).strip()
    home = str(e.get("home_team", "")).strip()
    commence = str(e.get("commence_time", "")).strip()
    return f"{sport_key}:{away}__{home}__{commence}"


def _successful_trades_path() -> str:
    # Keep in sync with TradeExecutorService._trades_csv_path()
    moneyline_root = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(moneyline_root, "trades.csv")


def _legacy_successful_trades_path() -> str:
    moneyline_root = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(moneyline_root, "successful_trades.txt")


def _parse_game_line(line: str) -> Optional[Tuple[str, str]]:
    """
    Try to extract (away, home) from a line in successful_trades.txt.

    Supports:
    - JSON line format with {"game": "Away @ Home"} or {"matchup": "..."}
    - Pipe-delimited format containing "game=Away @ Home"
    """
    # JSON format
    try:
        payload = json.loads(line)
        game = payload.get("game") or payload.get("matchup")
        if isinstance(game, str) and "@" in game:
            away, home = [p.strip() for p in game.split("@", 1)]
            if away and home:
                return away, home
    except Exception:
        pass

    # Pipe-delimited format
    try:
        parts = [p.strip() for p in line.split("|")]
        game_part = next((p for p in parts if p.startswith("game=")), None)
        if not game_part:
            return None
        game_val = game_part.split("=", 1)[1].strip()
        if "@" not in game_val:
            return None
        away, home = [p.strip() for p in game_val.split("@", 1)]
        if away and home:
            return away, home
    except Exception:
        return None

    return None


def _load_previously_traded_game_keys() -> Set[str]:
    """
    Return a set of normalized matchup keys to skip.

    Primary source:
    - `trades.csv` (current)

    Legacy support:
    - `successful_trades.txt` (older runs) so we don't re-trade historical games.
    """
    keys: Set[str] = set()

    # 1) Current CSV log
    csv_path = _successful_trades_path()
    if os.path.exists(csv_path):
        try:
            with open(csv_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    game = (row.get("game") or "").strip()
                    if "@" not in game:
                        continue
                    away, home = [p.strip() for p in game.split("@", 1)]
                    if not away or not home:
                        continue
                    keys.add(_game_key(away, home))
                    keys.add(_game_key(home, away))
        except Exception:
            pass

    # 2) Legacy TXT log
    legacy_path = _legacy_successful_trades_path()
    if os.path.exists(legacy_path):
        try:
            with open(legacy_path, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f.readlines() if ln.strip()]
        except Exception:
            lines = []
        for line in lines:
            parsed = _parse_game_line(line)
            if not parsed:
                continue
            away, home = parsed
            keys.add(_game_key(away, home))
            keys.add(_game_key(home, away))

    return keys


def _print_all_trades() -> None:
    """
    Print all successful trades from the log (one trade per line).
    """
    path = _successful_trades_path()
    print("\n" + "=" * 80)
    print("Trades (from trades.csv):")
    print("=" * 80)
    if not os.path.exists(path):
        print("(none - log file does not exist yet)")
        return

    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        print(f"(failed to read trade log: {e})")
        return

    if not rows:
        print("(none)")
        return

    for i, row in enumerate(rows, start=1):
        # Keep it compact for logs
        ts = row.get("ts", "")
        game = row.get("game", "")
        team = row.get("team", "")
        price = row.get("price", "")
        size = row.get("size", "")
        ev = row.get("expected_payout_per_$1", "")
        token_id = row.get("token_id", "")
        print(f"{i}. ts={ts} game={game} team={team} price={price} size={size} ev={ev} token_id={token_id}")


def _fetch_todays_events(
    *, connector: theOddsAPIConnector, sport_key: str, local_date
) -> List[Dict[str, Any]]:
    events = connector.get_odds(sport_key, MONEYLINE_MARKET_KEY)
    return list_events_for_local_date(events, local_date=local_date)


def _remaining_events_to_process(
    *,
    todays_events: List[Dict[str, Any]],
    sport_label: str,
    sport_key: str,
    previously_traded_games: Set[str],
    traded_event_keys: Set[str],
) -> List[Dict[str, Any]]:
    remaining: List[Dict[str, Any]] = []
    for event in todays_events:
        away = str(event.get("away_team", "")).strip()
        home = str(event.get("home_team", "")).strip()
        if not away or not home:
            continue

        event_key = _event_key(event, sport_key=sport_key)
        matchup_key = _game_key(away, home)

        if matchup_key in previously_traded_games:
            print(
                f"Skipping previously-traded game (from trades.csv): "
                f"{away} @ {home} (sport={sport_label})"
            )
            traded_event_keys.add(event_key)
            continue

        if event_key in traded_event_keys:
            print(
                f"Skipping already-traded game: {away} @ {home} "
                f"(sport={sport_label}, event_key={event_key})"
            )
            continue

        remaining.append(event)

    return remaining


def main() -> int:
    # Flags:
    # - --tomorrow: run a single scan for tomorrow's (local) games and exit
    run_tomorrow_once = "--tomorrow" in sys.argv[1:]

    # Backwards-compatible: if user supplies <away> <home> [YYYY-MM-DD], run once.
    if len(sys.argv) >= 3:
        return PolymarketSportsBettingBotInterface().run_moneyline(sys.argv)

    bot = PolymarketSportsBettingBotInterface()
    connector = theOddsAPIConnector()
    traded_event_keys: Set[str] = set()

    while True:
        previously_traded_games = _load_previously_traded_game_keys()
        now = datetime.now().astimezone()
        today = now.date()
        if run_tomorrow_once:
            today = today + timedelta(days=1)
        date_str = today.strftime("%Y-%m-%d")

        print("\n" + "=" * 80)
        labels = " + ".join(label for (label, _) in SPORTS_TO_SCAN)
        print(f"Running moneyline scan for {labels} on {date_str} (local time)")
        print("=" * 80)

        any_events_today = False
        any_remaining = False

        for sport_label, sport_key in SPORTS_TO_SCAN:
            print("\n" + "-" * 80)
            print(f"{sport_label} games")
            print("-" * 80)

            try:
                todays_events = _fetch_todays_events(
                    connector=connector, sport_key=sport_key, local_date=today
                )
            except Exception as e:
                print(f"ERROR: Failed to fetch today's {sport_label} events: {e}")
                todays_events = []

            if todays_events:
                any_events_today = True

            remaining_events = _remaining_events_to_process(
                todays_events=todays_events,
                sport_label=sport_label,
                sport_key=sport_key,
                previously_traded_games=previously_traded_games,
                traded_event_keys=traded_event_keys,
            )

            if not remaining_events:
                continue

            any_remaining = True
            for event in remaining_events:
                away = str(event.get("away_team", "")).strip()
                home = str(event.get("home_team", "")).strip()
                if not away or not home:
                    continue

                print("\n" + "-" * 80)
                print(f"{away} @ {home}")
                print("-" * 80)

                event_k = _event_key(event, sport_key=sport_key)
                bot.run_moneyline(["prog", away, home, date_str], sport_key=sport_key)
                if getattr(bot, "last_run_had_successful_trade", False):
                    traded_event_keys.add(event_k)
                    print(
                        f"Marked game as traded; will not run again for "
                        f"(sport={sport_label}, event_key={event_k})"
                    )

        if not any_remaining:
            print("No remaining events to process. Exiting.")
            _print_all_trades()
            return 0 if any_events_today else 2

        if run_tomorrow_once:
            print("Finished single tomorrow scan. Exiting.")
            _print_all_trades()
            return 0 if any_events_today else 2

        print("\nSleeping 15 minutes...\n")
        time.sleep(15 * 60)


if __name__ == "__main__":
    raise SystemExit(main())

