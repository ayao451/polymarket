#!/usr/bin/env python3
"""
Data collector for value bets.

Continuously scans games, detects value bets using the same algorithm as value_bets.py,
and logs them to CSV files (one per market slug) without executing any trades.

CSV files are organized as: csvs/{event_slug}/{market_slug}.csv
- csvs/{event_slug}/{event_slug}.csv - moneyline value bets
- csvs/{event_slug}/{spread_market_slug}.csv - spread value bets
- csvs/{event_slug}/{totals_market_slug}.csv - totals value bets

Includes all basketball games (NBA, NCAA, international leagues, etc.).
"""

import sys
import time
import csv
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

# Add parent directory to path to import from value_bets
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from polymarket_sports_betting_bot.polymarket_sports_betting_bot_interface import (
    PolymarketSportsBettingBotInterface,
)
from polymarket_sports_betting_bot.value_bet_service import (
    ValueBet,
    SpreadValueBet,
    TotalsValueBet,
    ValueBetService,
    SpreadValueBetService,
    TotalsValueBetService,
)
from polymarket_odds_service.polymarket_odds_interface import PolymarketOddsInterface
from polymarket_odds_service.find_game import PolymarketGameFinder
from polymarket_odds_service.polymarket_market_analyzer import PolymarketMarketAnalyzer
from pinnacle_scraper.pinnacle_odds_service import PinnacleBasketballOddsService
from pinnacle_scraper.pinnacle_sportsbook_odds_interface import (
    PinnacleSportsbookOddsInterface,
)
from cli_helpers import validate_input


def _get_csv_path_for_market(event_slug: str, market_slug: str) -> str:
    """
    Get the CSV file path for a given event and market slug.
    
    Args:
        event_slug: The Polymarket event slug (e.g., "nba-cha-lal-2026-01-15")
        market_slug: The Polymarket market slug (e.g., "nba-cha-lal-2026-01-15" for moneyline,
                     or "nba-cha-lal-2026-01-15-spread-home-1pt5" for spreads)
    
    Returns:
        Path to CSV file: csvs/{event_slug}/{market_slug}.csv
    """
    collector_dir = os.path.abspath(os.path.dirname(__file__))
    csvs_dir = os.path.join(collector_dir, "csvs")
    # Sanitize event slug for folder name
    safe_event_slug = event_slug.replace("/", "_").replace("\\", "_")
    event_folder = os.path.join(csvs_dir, safe_event_slug)
    # Create event folder if it doesn't exist
    os.makedirs(event_folder, exist_ok=True)
    # Sanitize market slug for filename
    safe_market_slug = market_slug.replace("/", "_").replace("\\", "_")
    return os.path.join(event_folder, f"{safe_market_slug}.csv")


def _ensure_csv_header(csv_path: str) -> None:
    """Ensure CSV file exists with proper header."""
    if os.path.exists(csv_path):
        return
    
    fieldnames = [
        "value_bet",
        "edge",
        "side",
        "event_name",
        "timestamp",
        "team",
        "point",
        "token_id",
        "true_prob",
        "polymarket_best_ask",
        "expected_payout_per_1",
    ]
    try:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
    except Exception:
        pass  # Silently fail on file write errors


def _log_value_bet(
    event_slug: str,
    market_slug: str,
    event_name: str,
    value_bet: ValueBet,
    side: str = "BUY",
) -> None:
    """Log a moneyline value bet to CSV."""
    csv_path = _get_csv_path_for_market(event_slug, market_slug)
    _ensure_csv_header(csv_path)
    
    edge = float(value_bet.expected_payout_per_1) - 1.0
    timestamp = datetime.now(timezone.utc).isoformat()
    
    row = {
        "value_bet": "moneyline",
        "edge": f"{edge:.6f}",
        "side": side,
        "event_name": event_name,
        "timestamp": timestamp,
        "team": value_bet.team,
        "point": "",
        "token_id": value_bet.token_id,
        "true_prob": f"{value_bet.true_prob:.6f}",
        "polymarket_best_ask": f"{value_bet.polymarket_best_ask:.6f}",
        "expected_payout_per_1": f"{value_bet.expected_payout_per_1:.6f}",
    }
    
    try:
        with open(csv_path, "a", encoding="utf-8", newline="") as f:
            fieldnames = list(row.keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row)
    except Exception:
        pass  # Silently fail on file write errors


def _log_spread_value_bet(
    event_slug: str,
    market_slug: str,
    event_name: str,
    value_bet: SpreadValueBet,
    side: str = "BUY",
) -> None:
    """Log a spread value bet to CSV."""
    csv_path = _get_csv_path_for_market(event_slug, market_slug)
    _ensure_csv_header(csv_path)
    
    edge = float(value_bet.expected_payout_per_1) - 1.0
    timestamp = datetime.now(timezone.utc).isoformat()
    
    row = {
        "value_bet": "spread",
        "edge": f"{edge:.6f}",
        "side": side,
        "event_name": event_name,
        "timestamp": timestamp,
        "team": value_bet.team,
        "point": f"{value_bet.point:.1f}",
        "token_id": value_bet.token_id,
        "true_prob": f"{value_bet.true_prob:.6f}",
        "polymarket_best_ask": f"{value_bet.polymarket_best_ask:.6f}",
        "expected_payout_per_1": f"{value_bet.expected_payout_per_1:.6f}",
    }
    
    try:
        with open(csv_path, "a", encoding="utf-8", newline="") as f:
            fieldnames = list(row.keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row)
    except Exception:
        pass  # Silently fail on file write errors


def _log_totals_value_bet(
    event_slug: str,
    market_slug: str,
    event_name: str,
    value_bet: TotalsValueBet,
    side: str = "BUY",
) -> None:
    """Log a totals value bet to CSV."""
    csv_path = _get_csv_path_for_market(event_slug, market_slug)
    _ensure_csv_header(csv_path)
    
    edge = float(value_bet.expected_payout_per_1) - 1.0
    timestamp = datetime.now(timezone.utc).isoformat()
    
    row = {
        "value_bet": "totals",
        "edge": f"{edge:.6f}",
        "side": side,
        "event_name": event_name,
        "timestamp": timestamp,
        "team": value_bet.side,  # "Over" or "Under"
        "point": f"{value_bet.total_point:.1f}",
        "token_id": value_bet.token_id,
        "true_prob": f"{value_bet.true_prob:.6f}",
        "polymarket_best_ask": f"{value_bet.polymarket_best_ask:.6f}",
        "expected_payout_per_1": f"{value_bet.expected_payout_per_1:.6f}",
    }
    
    try:
        with open(csv_path, "a", encoding="utf-8", newline="") as f:
            fieldnames = list(row.keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row)
    except Exception:
        pass  # Silently fail on file write errors


def _fetch_all_polymarket_events_for_date(target_date) -> List[Tuple[str, str, str]]:
    """
    Fetch all Polymarket basketball events for a given date.
    
    Returns:
        List of tuples: (event_slug, away_team, home_team)
    """
    finder = PolymarketGameFinder()
    events_list = []
    
    # Fetch events pages - search more pages to find basketball events
    # Try both active and inactive, and search more pages since basketball events
    # (especially CBB) might be further in the list when sorted by startTime
    for active_only in (True, False):
        for page in range(100):  # Search up to 100 pages (10000 events) to find all CBB games
            events = finder.fetch_events_page(
                limit=100,
                offset=page * 100,
                active=active_only,
                closed=False,
                order="startTime",
                ascending=False,
            )
            if not events:
                break
            
            for event in events:
                # Check if event is on target date
                start = finder._parse_start_time(event)
                if start is None:
                    continue
                
                event_date = start.astimezone().date()
                if event_date != target_date:
                    continue
                
                slug_raw = event.get("slug") or ""
                if not slug_raw:
                    continue
                slug = str(slug_raw).strip()
                slug_lower = slug.lower()
                
                # Exclude WNBA games (prefixed with "cwbb")
                if slug_lower.startswith("cwbb-") or slug_lower.startswith("cwbb"):
                    continue
                
                # Extract title
                title_raw = event.get("title") or ""
                if not title_raw:
                    continue
                title = str(title_raw).strip()
                
                # Filter for basketball events - include all basketball leagues
                # Check slug for basketball patterns (NBA, NCAA/CBB, international leagues, etc.)
                is_basketball = (
                    slug_lower.startswith("nba-") or
                    slug_lower.startswith("ncaa") or
                    slug_lower.startswith("cbb-") or  # College basketball
                    "-nba-" in slug_lower or
                    "-ncaa-" in slug_lower or
                    "-cbb-" in slug_lower or
                    "basketball" in slug_lower or
                    "bball" in slug_lower or
                    slug_lower.startswith("euroleague") or
                    slug_lower.startswith("eurocup") or
                    slug_lower.startswith("fib") or
                    slug_lower.startswith("bkkbl") or
                    slug_lower.startswith("bkarg") or
                    slug_lower.startswith("bkfr1") or
                    slug_lower.startswith("bkcba") or
                    slug_lower.startswith("bknbl") or
                    slug_lower.startswith("bkseriea") or
                    slug_lower.startswith("bkligend") or
                    slug_lower.startswith("bkcl") or
                    "-bkkbl-" in slug_lower or
                    "-bkarg-" in slug_lower or
                    "-bkfr1-" in slug_lower or
                    "-bkcba-" in slug_lower or
                    "-bknbl-" in slug_lower or
                    "-bkseriea-" in slug_lower or
                    "-bkligend-" in slug_lower or
                    "-bkcl-" in slug_lower
                )
                
                # Also check title for basketball keywords
                if not is_basketball:
                    title_lower = title.lower()
                    is_basketball = (
                        "basketball" in title_lower or
                        "nba" in title_lower or
                        "ncaa" in title_lower or
                        "college basketball" in title_lower or
                        "cbb" in title_lower or
                        any(x in title_lower for x in [
                            "celtics", "lakers", "warriors", "raptors", "bucks",
                            "knicks", "bulls", "heat", "mavericks", "nuggets",
                            "suns", "jazz", "clippers", "nets", "76ers"
                        ])
                    )
                
                if not is_basketball:
                    continue
                
                # Exclude esports (shouldn't have nba- prefix, but double-check)
                title_lower = title.lower()
                esports_keywords = [
                    "dota", "valorant", "lol:", "league of legends", 
                    "cs:", "counter-strike", "esports"
                ]
                if any(x in title_lower for x in esports_keywords):
                    continue
                
                # Extract team names from title
                # NBA titles are usually "Team1 vs. Team2" or "Team1 @ Team2"
                # Remove any prefixes
                clean_title = title
                for prefix in ["NBA:", "NCAA:", "NCAAB:", "NBAGL:", "Basketball:"]:
                    if clean_title.startswith(prefix):
                        clean_title = clean_title[len(prefix):].strip()
                        break
                
                # Parse team names
                parts = clean_title.replace(" vs ", " @ ").replace(" vs. ", " @ ").split(" @ ", 1)
                if len(parts) != 2:
                    continue
                
                away = parts[0].strip().rstrip(".")
                home = parts[1].strip().rstrip(".")
                
                if away and home:
                    events_list.append((slug, away, home))
        
        # Continue searching even if we found events (to get all CBB games)
        # Only break if we've checked enough pages
        if page >= 99:  # After checking all pages, break
            break
    
    # Remove duplicates (same slug)
    seen_slugs = set()
    deduped = []
    for slug, away, home in events_list:
        if slug not in seen_slugs:
            seen_slugs.add(slug)
            deduped.append((slug, away, home))
    
    return deduped


def _normalize_team_name(s: str) -> str:
    """Normalize team name for matching. Assumes input is already stripped."""
    return " ".join(str(s).lower().split())


def _teams_match(team1: str, team2: str) -> bool:
    """
    Check if two team names match (fuzzy matching).
    
    Handles cases where:
    - Pinnacle: "Gonzaga" (school name only)
    - Polymarket: "Gonzaga Bulldogs" (school name + team name)
    """
    norm1 = _normalize_team_name(team1)
    norm2 = _normalize_team_name(team2)
    
    if norm1 == norm2:
        return True
    
    # Check if one name starts with the other (for NCAA: school name matches "school name team name")
    # e.g., "gonzaga" matches "gonzaga bulldogs" or "gonzaga bulldogs" matches "gonzaga"
    if norm1.startswith(norm2) or norm2.startswith(norm1):
        return True
    
    # Check if last word matches (nickname matching)
    # e.g., "New York Knicks" matches "Knicks"
    words1 = norm1.split()
    words2 = norm2.split()
    if words1 and words2 and words1[-1] == words2[-1]:
        return True
    
    # Check if one contains the other (bidirectional)
    # e.g., "golden state warriors" contains "warriors"
    if norm1 in norm2 or norm2 in norm1:
        return True
    
    # Check if first word matches (for NCAA: school name is usually first word)
    # e.g., "Gonzaga" matches "Gonzaga Bulldogs"
    if words1 and words2:
        if words1[0] == words2[0] and len(words1[0]) > 3:  # Only if first word is substantial
            return True
    
    # Check if any significant word matches (for multi-word team names)
    # e.g., "Trail Blazers" should match "Portland Trail Blazers"
    if len(words1) > 1 and len(words2) > 1:
        # Check if all words from shorter name are in longer name
        shorter = words1 if len(words1) < len(words2) else words2
        longer = words2 if len(words1) < len(words2) else words1
        if all(word in longer for word in shorter if len(word) > 2):  # Ignore short words like "at", "vs"
            return True
    
    return False


def _match_games(
    polymarket_events: List[Tuple[str, str, str]],
    pinnacle_games: List,
    debug: bool = False,
) -> List[Tuple[str, str, str]]:
    """
    Match Polymarket events with Pinnacle games by team names.
    
    Returns:
        List of (event_slug, away_team, home_team) tuples for matched games.
        Uses Pinnacle team names (more standardized).
    """
    matched = []
    unmatched_pm = []
    
    for event_slug, pm_away, pm_home in polymarket_events:
        found_match = False
        # Try to find matching Pinnacle game
        for pinnacle_game in pinnacle_games:
            pin_away = pinnacle_game.away_team
            pin_home = pinnacle_game.home_team
            
            if not pin_away or not pin_home:
                continue
            
            # Check both possible matchings (normal and swapped)
            # Match 1: pm_away matches pin_away AND pm_home matches pin_home
            match1 = _teams_match(pm_away, pin_away) and _teams_match(pm_home, pin_home)
            # Match 2: pm_away matches pin_home AND pm_home matches pin_away (swapped)
            match2 = _teams_match(pm_away, pin_home) and _teams_match(pm_home, pin_away)
            
            if match1 or match2:
                # Use Pinnacle team names (more standardized)
                matched.append((event_slug, pin_away, pin_home))
                found_match = True
                if debug:
                    print(f"  MATCHED: PM '{pm_away} @ {pm_home}' <-> PIN '{pin_away} @ {pin_home}'")
                break
        
        if not found_match and debug and len(unmatched_pm) < 5:
            unmatched_pm.append((pm_away, pm_home))
    
    if debug and unmatched_pm:
        print(f"\nSample unmatched Polymarket events (first 5):")
        for pm_a, pm_h in unmatched_pm:
            print(f"  '{pm_a} @ {pm_h}'")
    
    return matched


def _process_game_from_event_slug(event_slug: str, away_team: str, home_team: str, play_date) -> None:
    """Process a single game from event slug: detect value bets and log them to CSV."""
    try:
        # Exclude WNBA games (check for wnba or wcbb in slug)
        event_slug_lower = event_slug.lower()
        if "wnba" in event_slug_lower or "wcbb" in event_slug_lower:
            return  # Skip WNBA games
        
        # Get Polymarket odds directly using the event slug
        analyzer = PolymarketMarketAnalyzer()
        event = analyzer.fetch_event_by_slug(event_slug)
        if not event:
            return  # Skip if event fetch failed
        
        # Get sportsbook odds (Pinnacle) - try to match teams
        sportsbook = PinnacleSportsbookOddsInterface()
        sportsbook_result, sportsbook_spreads, sportsbook_totals = (
            sportsbook.get_moneyline_spread_totals_odds(
                away_team, home_team, play_date, sport_key="PINNACLE"
            )
        )
        
        # Resolve away/home from sportsbook result if available
        if sportsbook_result is not None:
            resolved_away = sportsbook_result.away_team
            resolved_home = sportsbook_result.home_team
        else:
            resolved_away = away_team
            resolved_home = home_team
        
        event_name = f"{resolved_away} @ {resolved_home}"
        
        # Get Polymarket moneyline odds
        # For moneyline, the market_slug is the same as event_slug
        moneyline_market_slug = event_slug
        polymarket_moneyline = analyzer.analyze_event(event, market_slug=moneyline_market_slug)
        if polymarket_moneyline and sportsbook_result:
            value_bets = ValueBetService(
                resolved_away, resolved_home, sportsbook_result, polymarket_moneyline
            ).discover_value_bets()
            
            for vb in value_bets:
                _log_value_bet(event_slug, moneyline_market_slug, event_name, vb)
        
        # Spread value bets
        spread_market_slugs = analyzer.spread_market_slugs_from_event(event)
        if spread_market_slugs:
            # Build a map: (team, point) -> market_slug by parsing market labels
            market_slug_map = {}
            polymarket_spreads = []
            for slug in spread_market_slugs:
                spread_results = analyzer.analyze_event(event, market_slug=slug)
                polymarket_spreads.extend(spread_results)
                
                # Map each spread result to its market slug by parsing the market label
                for result in spread_results:
                    # Parse market label like "Spread: Thunder (-7.5) (Thunder)"
                    market_label = result.market or ""
                    if not market_label:
                        continue
                    
                    # Extract outcome label (last parenthesized segment)
                    start = market_label.rfind("(")
                    end = market_label.rfind(")")
                    if start == -1 or end == -1 or end <= start:
                        continue
                    outcome_team = market_label[start + 1 : end].strip()
                    
                    # Extract question text (before last parentheses)
                    question = market_label[:start].strip() if start > 0 else market_label
                    
                    # Parse spread line from question (e.g., "Spread: Thunder (-7.5)")
                    point = None
                    if question:
                        # Look for pattern like "(-7.5)" or "(+7.5)"
                        match = re.search(r'\(([+-]?[\d.]+)\)', question)
                        if match:
                            try:
                                point = float(match.group(1))
                            except ValueError:
                                pass
                    
                    if outcome_team and point is not None:
                        # Normalize team name for matching (outcome_team already extracted/cleaned)
                        norm_team = _normalize_team_name(outcome_team)
                        market_slug_map[(norm_team, point)] = slug
                        # Also map negative point (opposite side)
                        market_slug_map[(norm_team, -point)] = slug
            
            if polymarket_spreads and sportsbook_spreads:
                spread_service = SpreadValueBetService(
                    sportsbook_spreads=sportsbook_spreads,
                    polymarket_spread_results=polymarket_spreads,
                )
                spread_value_bets = spread_service.discover_value_bets()
                
                for vb in spread_value_bets:
                    # Match value bet back to market slug
                    # Normalize team name for matching (vb.team is already a string)
                    norm_team = _normalize_team_name(vb.team)
                    market_slug = market_slug_map.get((norm_team, vb.point), event_slug)
                    _log_spread_value_bet(event_slug, market_slug, event_name, vb)
        
        # Totals value bets
        totals_market_slugs = analyzer.totals_market_slugs_from_event(event)
        if totals_market_slugs:
            # Build a map: (side, point) -> market_slug
            market_slug_map = {}
            polymarket_totals = []
            for slug in totals_market_slugs:
                totals_results = analyzer.analyze_event(event, market_slug=slug)
                polymarket_totals.extend(totals_results)
                
                # Map each totals result to its market slug by parsing the market label
                for result in totals_results:
                    try:
                        # Parse market label like "O/U 229.5 (Over)" or "Total 229.5 (Under)"
                        market_label = result.market or ""
                        if not market_label:
                            continue
                        
                        # Extract outcome label (last parenthesized segment)
                        start = market_label.rfind("(")
                        end = market_label.rfind(")")
                        if start == -1 or end == -1 or end <= start:
                            continue
                        outcome = market_label[start + 1 : end].strip().lower()
                        side = "Over" if "over" in outcome else ("Under" if "under" in outcome else None)
                        
                        # Extract question text (before last parentheses)
                        question = market_label[:start].strip() if start > 0 else market_label
                        
                        # Parse total point from question (e.g., "O/U 229.5" or "Total 229.5")
                        point = None
                        if question:
                            match = re.search(r'([\d.]+)', question)
                            if match:
                                try:
                                    point = float(match.group(1))
                                except ValueError:
                                    pass
                        
                        if side and point is not None:
                            market_slug_map[(side, point)] = slug
                    except AttributeError:
                        continue
            
            if polymarket_totals and sportsbook_totals:
                totals_service = TotalsValueBetService(
                    sportsbook_totals=sportsbook_totals,
                    polymarket_totals_results=polymarket_totals,
                )
                totals_value_bets = totals_service.discover_value_bets()
                
                for vb in totals_value_bets:
                    # Match value bet back to market slug
                    market_slug = market_slug_map.get((vb.side, vb.total_point), event_slug)
                    _log_totals_value_bet(event_slug, market_slug, event_name, vb)
                    
    except Exception:
        # Silently skip on errors to keep the loop running
        pass


def main() -> int:
    """Main entry point - continuously scan and collect value bet data."""
    # Flags
    run_tomorrow_once = "--tomorrow" in sys.argv[1:]
    game_status = "all"
    if "--started" in sys.argv[1:]:
        game_status = "started"
    elif "--notstarted" in sys.argv[1:]:
        game_status = "notstarted"
    elif "--all" in sys.argv[1:]:
        game_status = "all"
    
    # Scheduling
    RECHECK_INTERVAL_SECONDS = 0  # 0 seconds
    
    # Single game mode (backwards compatible)
    positional = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(positional) >= 2:
        from cli_helpers import validate_input
        args = validate_input(["prog"] + positional)
        if args is None:
            return 1
        # For single game mode, we still need to find the event slug
        polymarket_interface = PolymarketOddsInterface(args.team_a, args.team_b, args.play_date)
        if polymarket_interface.event_slug:
            _process_game_from_event_slug(
                polymarket_interface.event_slug, args.team_a, args.team_b, args.play_date
            )
        return 0
    
    # Continuous scan mode
    start_time = time.time()
    now = datetime.now().astimezone()
    target_date = now.date()
    if run_tomorrow_once:
        target_date = target_date + timedelta(days=1)
    
    # Fetch all Polymarket event slugs once at the start
    print(f"Fetching all Polymarket basketball events for {target_date.isoformat()}...")
    polymarket_events = _fetch_all_polymarket_events_for_date(target_date)
    print(f"Found {len(polymarket_events)} Polymarket events")
    if polymarket_events:
        print("\nPolymarket games found:")
        for i, (slug, away, home) in enumerate(polymarket_events, 1):
            print(f"  {i}. {slug} | {away} @ {home}")
    
    # Fetch all Pinnacle games for the date
    print(f"\nFetching all Pinnacle basketball games for {target_date.isoformat()}...")
    pinnacle = PinnacleBasketballOddsService(timeout_ms=45000)
    try:
        pinnacle_games = pinnacle.list_games_for_date(target_date, game_status=game_status)
    except Exception:
        pinnacle_games = []
    print(f"Found {len(pinnacle_games)} Pinnacle games")
    if pinnacle_games:
        print("\nPinnacle games found:")
        for i, game in enumerate(pinnacle_games, 1):
            away = game.away_team
            home = game.home_team
            league = game.league or "Unknown"
            print(f"  {i}. [{league}] {away} @ {home}")
    
    if not polymarket_events or not pinnacle_games:
        print("No events or games found. Exiting.")
        return 2
    
    # Match Polymarket events with Pinnacle games
    print("Matching Polymarket events with Pinnacle games...")
    matched_events = _match_games(polymarket_events, pinnacle_games, debug=False)
    print(f"Found {len(matched_events)} matched games (in both Polymarket and Pinnacle)")
    
    if not matched_events:
        print("No matched games found. Exiting.")
        return 2
    
    # Sort matched events: NBA first, then CBB/NCAA, then the rest
    def _sort_key(event_tuple):
        event_slug, _, _ = event_tuple
        slug_lower = event_slug.lower()
        if "nba-" in slug_lower or slug_lower.startswith("nba-"):
            return (0, event_slug)  # NBA first
        elif "ncaa" in slug_lower or "cbb-" in slug_lower or "-ncaa-" in slug_lower or "-cbb-" in slug_lower:
            return (1, event_slug)  # CBB/NCAA second
        else:
            return (2, event_slug)  # Everything else
    
    matched_events = sorted(matched_events, key=_sort_key)
    
    # Filter by game status if needed (using Pinnacle game times)
    if game_status != "all":
        filtered_events = []
        now_utc = datetime.now(timezone.utc)
        
        for event_slug, away, home in matched_events:
            # Find corresponding Pinnacle game to get start time
            matched_pinnacle = None
            for pg in pinnacle_games:
                pg_away = pg.away_team
                pg_home = pg.home_team
                if _teams_match(away, pg_away) and _teams_match(home, pg_home):
                    matched_pinnacle = pg
                    break
            
            if not matched_pinnacle:
                continue
            
            start_utc = matched_pinnacle.start_time_utc
            if start_utc is None:
                continue
            
            if game_status == "started" and start_utc >= now_utc:
                continue  # Skip games that haven't started
            if game_status == "notstarted" and start_utc < now_utc:
                continue  # Skip games that have started
            
            filtered_events.append((event_slug, away, home))
        
        matched_events = filtered_events
        print(f"After filtering by status '{game_status}': {len(matched_events)} events")
    
    # Print all matched events (after status filtering)
    print("\n" + "=" * 80)
    print("Matched Events (will be processed):")
    print("=" * 80)
    for i, (event_slug, away_team, home_team) in enumerate(matched_events, 1):
        print(f"  {i}. [{event_slug}] {away_team} @ {home_team}")
    print("=" * 80)
    
    # Process all matched events and then continuously recheck forever
    print("\nProcessing matched games for value bets...")
    iteration = 0
    
    while True:
        try:
            iteration += 1
            print(f"\n{'='*80}")
            print(f"Iteration #{iteration} - Processing {len(matched_events)} matched games...")
            print(f"{'='*80}")
            
            # Process all matched events (value bets may change over time)
            for i, (event_slug, away_team, home_team) in enumerate(matched_events, 1):
                print(f"[{i}/{len(matched_events)}] Processing {event_slug}...")
                _process_game_from_event_slug(event_slug, away_team, home_team, target_date)
                # Small delay between games to avoid overwhelming APIs
                time.sleep(1)
            
            if run_tomorrow_once:
                # If --tomorrow flag, run once and exit
                print("\n--tomorrow flag set, exiting after one iteration.")
                break
            
            # Wait 1 minute before rechecking (before starting next iteration)
            print(f"\nWaiting {RECHECK_INTERVAL_SECONDS} seconds before next iteration...")
            time.sleep(RECHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Exiting...")
            raise
        except Exception as e:
            print(f"\n\nError in main loop: {e}")
            print("Continuing loop...")
            import traceback
            traceback.print_exc()
            time.sleep(5)  # Wait a bit before retrying
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

