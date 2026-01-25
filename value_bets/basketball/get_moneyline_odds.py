#!/usr/bin/env python3
"""
Basketball-specific moneyline odds fetcher.

Takes team_a, team_b, and date; retries until moneyline odds are available
from Pinnacle, then returns a SportsbookOdds.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pinnacle_scraper.pinnacle_odds_service import (
    GameInfo,
    PinnacleBasketballOddsService,
)
from pinnacle_scraper.pinnacle_sportsbook_odds_interface import PinnacleSportsbookOddsInterface
from pinnacle_scraper.sportsbook_odds import SportsbookOdds


DEFAULT_MAX_RETRIES = 60
DEFAULT_RETRY_INTERVAL_SECONDS = 60


def parse_date(value: str) -> date:
    """Parse YYYY-MM-DD string to date."""
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date '{value}'; use YYYY-MM-DD") from e


def _norm(s: str) -> str:
    """Normalize team name for matching."""
    return " ".join((s or "").strip().lower().split())


def list_games_for_date(play_date: date, timeout_ms: int = 45000) -> List[tuple[str, str, str]]:
    """
    List basketball games for a given date.

    Returns list of (away_team, home_team, league) tuples.
    """
    svc = PinnacleBasketballOddsService(timeout_ms=timeout_ms)
    games = svc.list_games_for_date(play_date, game_status="all")
    return [(g.away_team, g.home_team, g.league) for g in games]


def _find_matching_game(
    team_a: str,
    team_b: str,
    play_date: date,
    timeout_ms: int = 45000,
) -> Optional[GameInfo]:
    """Find GameInfo for team_a vs team_b on play_date, or None."""
    svc = PinnacleBasketballOddsService(timeout_ms=timeout_ms)
    games = svc.list_games_for_date(play_date, game_status="all")
    ta = _norm(team_a)
    tb = _norm(team_b)
    for g in games:
        a = _norm(g.away_team)
        h = _norm(g.home_team)
        if (a == ta and h == tb) or (a == tb and h == ta):
            return g
    return None


class BasketballMoneylineFetcher:
    """
    Fetches basketball moneyline odds from Pinnacle via the sportsbook interface.

    Retries until odds are available or max_retries is exceeded.
    """

    def __init__(
        self,
        timeout_ms: int = 45000,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_interval_seconds: float = DEFAULT_RETRY_INTERVAL_SECONDS,
        verbose: bool = False,
    ) -> None:
        self._interface = PinnacleSportsbookOddsInterface(sport="basketball", timeout_ms=timeout_ms)
        self._max_retries = max(1, int(max_retries))
        self._retry_interval_seconds = max(1.0, float(retry_interval_seconds))
        self._verbose = bool(verbose)

    def fetch(
        self,
        team_a: str,
        team_b: str,
        play_date: Optional[date] = None,
    ) -> SportsbookOdds:
        """
        Fetch moneyline odds for the given matchup. Retries until odds are
        available or max_retries is exceeded.

        Raises:
            RuntimeError: If odds could not be obtained after all retries.
        """
        d = play_date or date.today()
        last_error: Optional[str] = None

        for attempt in range(1, self._max_retries + 1):
            odds = self._interface.get_moneyline_odds(team_a, team_b, d)
            if odds is not None:
                if self._verbose:
                    print(f"[get_moneyline_odds] Got odds on attempt {attempt}")
                return odds

            last_error = (
                f"No moneyline odds for '{team_a}' vs '{team_b}' on {d}; "
                f"game not found or odds not yet posted."
            )
            if self._verbose:
                print(f"[get_moneyline_odds] Attempt {attempt}/{self._max_retries}: {last_error}")

            if attempt < self._max_retries:
                time.sleep(self._retry_interval_seconds)

        raise RuntimeError(
            f"Could not fetch moneyline odds after {self._max_retries} attempts. {last_error}"
        )


def get_moneyline_odds(
    team_a: str,
    team_b: str,
    play_date: Optional[date] = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_interval_seconds: float = DEFAULT_RETRY_INTERVAL_SECONDS,
    verbose: bool = False,
) -> SportsbookOdds:
    """
    Fetch basketball moneyline odds for team_a vs team_b on play_date.

    Retries until odds are available or max_retries is exceeded. Uses
    Pinnacle (basketball) under the hood.

    Args:
        team_a: First team name (order does not matter).
        team_b: Second team name.
        play_date: Date of the game; defaults to today.
        max_retries: Number of attempts before giving up.
        retry_interval_seconds: Seconds to wait between attempts.
        verbose: If True, log retry attempts.

    Returns:
        SportsbookOdds for the moneyline market.

    Raises:
        RuntimeError: If odds could not be obtained after all retries.
    """
    fetcher = BasketballMoneylineFetcher(
        max_retries=max_retries,
        retry_interval_seconds=retry_interval_seconds,
        verbose=verbose,
    )
    return fetcher.fetch(team_a, team_b, play_date)


def _run_list_games(play_date: date) -> int:
    """Print today's basketball games and exit."""
    games = list_games_for_date(play_date)
    if not games:
        print(f"No basketball games found for {play_date}.")
        return 0
    print(f"Basketball games on {play_date}:")
    for away, home, league in games:
        print(f"  {away} @ {home}  ({league})")
    return 0


def _run_fetch(team_a: str, team_b: str, play_date: date, verbose: bool, max_retries: int, retry_interval: float) -> int:
    """Fetch and print moneyline odds for the given matchup."""
    odds = get_moneyline_odds(
        team_a,
        team_b,
        play_date=play_date,
        max_retries=max_retries,
        retry_interval_seconds=retry_interval,
        verbose=verbose,
    )
    print(odds.to_string())
    return 0


def _fetch_moneyline_spread_totals(
    team_a: str,
    team_b: str,
    play_date: date,
    max_retries: int,
    retry_interval_seconds: float,
    verbose: bool,
):
    """
    Fetch moneyline, spread, and totals odds. Retries until moneyline is available.

    Returns (moneyline, spreads, totals). moneyline is always a SportsbookOdds on success.
    Raises RuntimeError if moneyline could not be obtained after max_retries.
    """
    interface = PinnacleSportsbookOddsInterface(sport="basketball", timeout_ms=45000)
    last_error: Optional[str] = None
    for attempt in range(1, max_retries + 1):
        moneyline, spreads, totals = interface.get_moneyline_spread_totals_odds(
            team_a, team_b, play_date
        )
        if moneyline is not None:
            if verbose:
                print(f"[get_moneyline_odds] Got odds on attempt {attempt}")
            return moneyline, spreads, totals
        last_error = (
            f"No moneyline odds for '{team_a}' vs '{team_b}' on {play_date}; "
            "game not found or odds not yet posted."
        )
        if verbose:
            print(f"[get_moneyline_odds] Attempt {attempt}/{max_retries}: {last_error}")
        if attempt < max_retries:
            time.sleep(retry_interval_seconds)
    raise RuntimeError(
        f"Could not fetch moneyline odds after {max_retries} attempts. {last_error}"
    )


def _format_odds_for_file(moneyline, spreads, totals) -> str:
    """Format moneyline, spreads, and totals as multiline string for file output."""
    lines: List[str] = []
    lines.append("--- MONEYLINE ---")
    lines.append(moneyline.to_string())
    lines.append("")
    lines.append("--- SPREADS ---")
    if spreads:
        for s in spreads:
            lines.append(s.to_string())
    else:
        lines.append("(no spreads)")
    lines.append("")
    lines.append("--- TOTALS ---")
    if totals:
        for t in totals:
            lines.append(t.to_string())
    else:
        lines.append("(no totals)")
    return "\n".join(lines)


def _run_to_file(
    team_a: str,
    team_b: str,
    play_date: date,
    output_path: str,
    verbose: bool,
    max_retries: int,
    retry_interval: float,
) -> int:
    """Fetch moneyline, spread, totals; write to file. Retries until moneyline available."""
    moneyline, spreads, totals = _fetch_moneyline_spread_totals(
        team_a,
        team_b,
        play_date,
        max_retries=max_retries,
        retry_interval_seconds=retry_interval,
        verbose=verbose,
    )
    content = _format_odds_for_file(moneyline, spreads, totals)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Wrote moneyline, spreads, totals to {output_path}")
    return 0


def _run_debug(team_a: str, team_b: str, play_date: date) -> int:
    """Debug: fetch odds via service, print markets, and show why moneyline may be missing."""
    match = _find_matching_game(team_a, team_b, play_date)
    if match is None:
        print(f"[DEBUG] No matching game for '{team_a}' vs '{team_b}' on {play_date}")
        return 1
    print(f"[DEBUG] Found: {match.away_team} @ {match.home_team} (matchup_id={match.matchup_id})")
    svc = PinnacleBasketballOddsService(timeout_ms=45000)
    try:
        res = svc.get_game_odds(match.matchup_id, game_info=match)
    except RuntimeError as e:
        print(f"[DEBUG] get_game_odds raised: {e}")
        return 1
    rows = [r for r in (res.markets or []) if int(r.period or 0) == 0 and not bool(r.is_alternate or False)]
    print(f"[DEBUG] Full-game main markets: {len(rows)} rows")
    for r in rows:
        print(f"  {r.market_type!r} | {r.selection!r} | odds={r.odds} | line={r.line}")
    ml_rows = [r for r in rows if (r.market_type or "").lower() == "moneyline"]
    print(f"[DEBUG] Moneyline rows: {len(ml_rows)}")
    if not ml_rows:
        print("[DEBUG] No moneyline rows -> get_moneyline_odds returns None")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch basketball moneyline odds (team_a vs team_b). Retries until odds are available."
    )
    parser.add_argument("team_a", nargs="?", help="First team name")
    parser.add_argument("team_b", nargs="?", help="Second team name")
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Game date (YYYY-MM-DD). Default: today",
    )
    parser.add_argument(
        "--list-games",
        action="store_true",
        help="List today's basketball games and exit.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug: fetch odds via service and print market rows.",
    )
    parser.add_argument(
        "--to-file",
        action="store_true",
        help="Write moneyline, spread, and totals odds to temp.txt.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Max fetch attempts (default: {DEFAULT_MAX_RETRIES})",
    )
    parser.add_argument(
        "--retry-interval",
        type=float,
        default=DEFAULT_RETRY_INTERVAL_SECONDS,
        help=f"Seconds between retries (default: {DEFAULT_RETRY_INTERVAL_SECONDS})",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    play_date = parse_date(args.date)

    if args.list_games:
        return _run_list_games(play_date)

    if not args.team_a or not args.team_b:
        parser.error("team_a and team_b required unless --list-games is used")
    if args.debug:
        return _run_debug(args.team_a, args.team_b, play_date)
    if args.to_file:
        return _run_to_file(
            args.team_a,
            args.team_b,
            play_date,
            output_path="temp.txt",
            verbose=args.verbose,
            max_retries=args.max_retries,
            retry_interval=args.retry_interval,
        )
    return _run_fetch(
        args.team_a,
        args.team_b,
        play_date,
        verbose=args.verbose,
        max_retries=args.max_retries,
        retry_interval=args.retry_interval,
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Exiting.")
        raise SystemExit(130)
    except Exception as e:
        print(f"Error: {e}")
        raise SystemExit(1)
