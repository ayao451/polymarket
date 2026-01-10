from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .the_odds_api_connector import theOddsAPIConnector
from .weighted_average import (
    BookLine,
    BookOutcome,
    SPORTSBOOK_WEIGHTS,
    build_weights_for_present_books,
    weighted_average_cost_to_win_1,
)


NBA_SPORT_KEY = "basketball_nba"
MONEYLINE_MARKET_KEY = "h2h"


@dataclass(frozen=True)
class MoneylineOutcome:
    team: str
    # cost_to_win_1 = 1/decimal_odds (stake required to receive $1 total back)
    cost_to_win_1: float


@dataclass(frozen=True)
class BookMoneyline:
    bookmaker_key: str
    bookmaker_title: str
    last_update: str
    outcomes: Tuple[MoneylineOutcome, MoneylineOutcome]


def _parse_iso_z(dt: str) -> datetime:
    return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _normalize_team_name(s: str) -> str:
    return " ".join(s.strip().lower().split())


def _is_same_local_date(utc_dt: datetime, local_date) -> bool:
    return utc_dt.astimezone().date() == local_date


def list_events_for_local_date(
    events: List[Dict[str, Any]],
    *,
    local_date=None,
) -> List[Dict[str, Any]]:
    """
    Filter The Odds API events to those occurring on the provided local_date.
    """
    if local_date is None:
        local_date = datetime.now().astimezone().date()

    matches: List[Dict[str, Any]] = []
    for event in events:
        commence_time = event.get("commence_time")
        if not commence_time:
            continue
        try:
            commence_utc = _parse_iso_z(commence_time)
        except ValueError:
            continue
        if _is_same_local_date(commence_utc, local_date):
            matches.append(event)

    # Sort by commence time for stable iteration
    def _sort_key(e: Dict[str, Any]) -> str:
        return str(e.get("commence_time", ""))

    matches.sort(key=_sort_key)
    return matches


def find_event_for_teams_today(
    events: List[Dict[str, Any]],
    team_a: str,
    team_b: str,
    *,
    local_date=None,
) -> Optional[Dict[str, Any]]:
    if local_date is None:
        local_date = datetime.now().astimezone().date()

    ta = _normalize_team_name(team_a)
    tb = _normalize_team_name(team_b)

    for event in events:
        home = _normalize_team_name(event.get("home_team", ""))
        away = _normalize_team_name(event.get("away_team", ""))
        if not ((home == ta and away == tb) or (home == tb and away == ta)):
            continue

        commence_time = event.get("commence_time")
        if not commence_time:
            continue
        try:
            commence_utc = _parse_iso_z(commence_time)
        except ValueError:
            continue

        if _is_same_local_date(commence_utc, local_date):
            return event

    return None


def extract_moneyline_odds_all_books(event: Dict[str, Any]) -> List[BookMoneyline]:
    books: List[BookMoneyline] = []
    for bookmaker in event.get("bookmakers", []) or []:
        markets = bookmaker.get("markets", []) or []
        h2h = next((m for m in markets if m.get("key") == MONEYLINE_MARKET_KEY), None)
        if not h2h:
            continue

        outcomes = h2h.get("outcomes", []) or []
        if len(outcomes) < 2:
            continue

        parsed_outcomes: List[MoneylineOutcome] = []
        for o in outcomes:
            name = o.get("name")
            price = o.get("price")
            if name is None or price is None:
                continue
            try:
                dec = float(price)
            except (TypeError, ValueError):
                continue
            if dec <= 0:
                continue
            parsed_outcomes.append(MoneylineOutcome(team=str(name), cost_to_win_1=(1.0 / dec)))

        if len(parsed_outcomes) < 2:
            continue

        books.append(
            BookMoneyline(
                bookmaker_key=str(bookmaker.get("key", "")),
                bookmaker_title=str(bookmaker.get("title", "")),
                last_update=str(bookmaker.get("last_update", "")),
                outcomes=(parsed_outcomes[0], parsed_outcomes[1]),
            )
        )

    return books


def _cli() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch moneyline (h2h) odds from all bookmakers for a specific NBA matchup."
    )
    parser.add_argument("--team-a", default="Chicago Bulls")
    parser.add_argument("--team-b", default="Detroit Pistons")
    parser.add_argument("--decimals", type=int, default=6)
    parser.add_argument("--show-weights", action="store_true", default=False)
    args = parser.parse_args()

    connector = theOddsAPIConnector()
    events = connector.get_odds(NBA_SPORT_KEY, MONEYLINE_MARKET_KEY)
    event = find_event_for_teams_today(events, args.team_a, args.team_b)
    if not event:
        print("No matching event found for today.")
        return 2

    books = extract_moneyline_odds_all_books(event)
    if not books:
        print("No bookmaker markets found.")
        return 3

    home_team = str(event.get("home_team", ""))
    away_team = str(event.get("away_team", ""))

    present_keys = [b.bookmaker_key for b in books]
    weights_by_key = build_weights_for_present_books(present_keys, weights_map=SPORTSBOOK_WEIGHTS)
    book_lines: List[BookLine] = [
        BookLine(
            bookmaker_key=b.bookmaker_key,
            outcomes=[BookOutcome(team=o.team, cost_to_win_1=o.cost_to_win_1) for o in b.outcomes],
        )
        for b in books
    ]

    home_avg = weighted_average_cost_to_win_1(book_lines, home_team, weights_by_key=weights_by_key)
    away_avg = weighted_average_cost_to_win_1(book_lines, away_team, weights_by_key=weights_by_key)
    if home_avg is None or away_avg is None:
        print("Could not compute weighted average for both teams.")
        return 4

    decimals = max(0, int(args.decimals))
    fmt = f".{decimals}f"

    line = (
        f"{away_team} @ {home_team} | "
        f"{away_team}: {format(away_avg, fmt)} to win $1 | "
        f"{home_team}: {format(home_avg, fmt)} to win $1"
    )
    if args.show_weights:
        parts = [f"{k}={format(weights_by_key.get(k, 0.0), '.3f')}" for k in sorted(set(present_keys))]
        line += " | weights(" + ",".join(parts) + ")"
    print(line)

    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())


