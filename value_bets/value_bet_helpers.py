#!/usr/bin/env python3
"""
Shared helper functions for value betting bots.
"""

from datetime import datetime, date, timezone
from typing import List, Tuple, Dict, Optional, Any, Union
import sys
import os
import re
import requests
import csv
from polymarket_sports_betting_bot.value_bet_service import ValueBet, SpreadValueBet, TotalsValueBet, PlayerPropValueBet

from polymarket_odds_service.polymarket_odds import (
    PolymarketGameFinder,
    PolymarketMarketExtractor,
    PolymarketOdds,
)


def _get_log_file_path(market_type: str) -> str:
    """Get the path to the log file for a specific market type."""
    value_bets_root = os.path.dirname(os.path.abspath(__file__))
    filename = f"attempted_value_bets_{market_type}.csv"
    return os.path.join(value_bets_root, filename)


def _get_value_bets_csv_path() -> str:
    """Get the path to the value_bets.csv file."""
    value_bets_root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(value_bets_root, "value_bets.csv")


def _sanitize_error_message(error: Optional[str]) -> Optional[str]:
    """
    Sanitize error messages to prevent exceptions/tracebacks from being logged.
    
    Returns None if the error looks like an exception/traceback.
    Returns a simple error message otherwise (truncated to 200 chars).
    """
    if not error:
        return None
    
    error_lower = error.lower()
    
    # Don't log exceptions, tracebacks, or HTML error pages
    if any(indicator in error_lower for indicator in [
        'exception',
        'traceback',
        '<!doctype',
        '<html',
        'cloudflare',
        'file "',
        'line ',
        'traceback (most recent call last)',
    ]):
        return None
    
    # Truncate very long messages (likely exceptions)
    if len(error) > 200:
        return None
    
    # Return sanitized error
    return error.strip()[:200]


def _log_value_bet_to_file(
    path: str,
    value_bet: Union[ValueBet, SpreadValueBet, TotalsValueBet, Any],
    away_team: str,
    home_team: str,
    event_slug: str,
    market_slug: str,
    executed: bool,
    error: Optional[str],
    outcome: str,
    line: Optional[float],
    prop_type: Optional[str],
    player_name: Optional[str],
) -> None:
    """Helper function to write a value bet to a CSV file."""
    file_exists = os.path.exists(path)
    timestamp = datetime.now().isoformat()
    
    # Sanitize error message to prevent exceptions from being logged
    sanitized_error = _sanitize_error_message(error)
    
    row = {
        'timestamp': timestamp,
        'event_slug': event_slug,
        'market_slug': market_slug,
        'away_team': away_team,
        'home_team': home_team,
        'outcome': str(outcome),
        'player_name': str(player_name) if player_name else '',
        'prop_type': str(prop_type) if prop_type else '',
        'line': str(line) if line is not None else '',
        'true_prob': f"{value_bet.true_prob:.6f}",
        'polymarket_ask': f"{value_bet.polymarket_best_ask:.6f}",
        'expected_payout_per_1': f"{value_bet.expected_payout_per_1:.6f}",
        'edge_pct': f"{(value_bet.true_prob - value_bet.polymarket_best_ask) * 100:.2f}",
        'token_id': value_bet.token_id,
        'executed': 'YES' if executed else 'NO',
        'error': sanitized_error or '',
    }
    
    fieldnames = [
        'timestamp', 'event_slug', 'market_slug',
        'away_team', 'home_team', 'outcome', 'player_name', 'prop_type', 'line',
        'true_prob', 'polymarket_ask', 'expected_payout_per_1', 'edge_pct',
        'token_id', 'executed', 'error'
    ]
    
    try:
        with open(path, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[WARNING] Failed to log attempted value bet: {e}")


def log_attempted_moneyline_bet(
    value_bet: ValueBet,
    away_team: str,
    home_team: str,
    event_slug: str,
    market_slug: str,
    executed: bool = False,
    error: Optional[str] = None,
) -> None:
    """Log an attempted moneyline value bet to attempted_value_bets_moneyline.csv."""
    path = _get_log_file_path('moneyline')
    _log_value_bet_to_file(
        path=path,
        value_bet=value_bet,
        away_team=away_team,
        home_team=home_team,
        event_slug=event_slug,
        market_slug=market_slug,
        executed=executed,
        error=error,
        outcome=value_bet.team,
        line=None,
        prop_type=None,
        player_name=None,
    )


def log_attempted_spread_bet(
    value_bet: SpreadValueBet,
    away_team: str,
    home_team: str,
    event_slug: str,
    market_slug: str,
    executed: bool = False,
    error: Optional[str] = None,
) -> None:
    """Log an attempted spread value bet to attempted_value_bets_spreads.csv."""
    path = _get_log_file_path('spreads')
    _log_value_bet_to_file(
        path=path,
        value_bet=value_bet,
        away_team=away_team,
        home_team=home_team,
        event_slug=event_slug,
        market_slug=market_slug,
        executed=executed,
        error=error,
        outcome=value_bet.team,
        line=value_bet.point,
        prop_type=None,
        player_name=None,
    )


def log_attempted_totals_bet(
    value_bet: TotalsValueBet,
    away_team: str,
    home_team: str,
    event_slug: str,
    market_slug: str,
    executed: bool = False,
    error: Optional[str] = None,
) -> None:
    """Log an attempted totals value bet to attempted_value_bets_totals.csv."""
    path = _get_log_file_path('totals')
    _log_value_bet_to_file(
        path=path,
        value_bet=value_bet,
        away_team=away_team,
        home_team=home_team,
        event_slug=event_slug,
        market_slug=market_slug,
        executed=executed,
        error=error,
        outcome=value_bet.side,
        line=value_bet.total_point,
        prop_type=None,
        player_name=None,
    )


def log_attempted_player_prop_bet(
    value_bet: Any,
    away_team: str,
    home_team: str,
    event_slug: str,
    market_slug: str,
    executed: bool = False,
    error: Optional[str] = None,
) -> None:
    """Log an attempted player prop value bet to attempted_value_bets_player_props.csv."""
    # Use isinstance to check type and access attributes directly
    if isinstance(value_bet, PlayerPropValueBet):
        player_name = value_bet.player_name
        line = value_bet.line
        prop_type = value_bet.prop_type
    else:
        # Fallback if structure is different - try direct access with try/except
        try:
            player_name = value_bet.player_name
            line = value_bet.line
            prop_type = value_bet.prop_type
        except AttributeError:
            player_name = 'Unknown'
            line = None
            prop_type = None
    
    path = _get_log_file_path('player_props')
    _log_value_bet_to_file(
        path=path,
        value_bet=value_bet,
        away_team=away_team,
        home_team=home_team,
        event_slug=event_slug,
        market_slug=market_slug,
        executed=executed,
        error=error,
        outcome=player_name,
        line=line,
        prop_type=prop_type,
        player_name=player_name,
    )


def log_value_bet(
    value_bet: Union[ValueBet, SpreadValueBet, TotalsValueBet, Any],
    away_team: str,
    home_team: str,
    play_date: date,
    event_slug: str,
    market_slug: str,
) -> None:
    """
    Log a value bet to value_bets.csv.
    
    This logs all value bets found, regardless of whether they were executed.
    
    Args:
        value_bet: The value bet object
        away_team: Away team name
        home_team: Home team name
        play_date: Date of the game
        event_slug: Polymarket event slug
        market_slug: Polymarket market slug
    """
    path = _get_value_bets_csv_path()
    file_exists = os.path.exists(path)
    
    # Extract outcome based on value bet type
    if isinstance(value_bet, ValueBet):
        outcome = value_bet.team
    elif isinstance(value_bet, SpreadValueBet):
        outcome = f"{value_bet.team} {value_bet.point:+g}"
    elif isinstance(value_bet, TotalsValueBet):
        outcome = f"{value_bet.side} {value_bet.total_point}"
    elif isinstance(value_bet, PlayerPropValueBet):
        side = f" {value_bet.side}" if value_bet.side else ""
        outcome = f"{value_bet.player_name} {value_bet.prop_type} {value_bet.line}{side}"
    else:
        # Fallback for unknown types - try direct access
        try:
            player_name = value_bet.player_name
            prop_type = value_bet.prop_type
            line = value_bet.line
            side = f" {value_bet.side}" if value_bet.side else ""
            outcome = f"{player_name} {prop_type} {line}{side}"
        except AttributeError:
            outcome = 'Unknown'
    
    # Calculate EV percentage: (expected_payout_per_1 - 1) * 100
    ev_percent = (value_bet.expected_payout_per_1 - 1.0) * 100
    
    # Event name: away_team @ home_team
    event_name = f"{away_team} @ {home_team}"
    
    row = {
        'event_name': event_name,
        'outcome': outcome,
        'date': play_date.strftime('%Y-%m-%d'),
        'event_slug': event_slug,
        'market_slug': market_slug,
        'polymarket_odds': f"{value_bet.polymarket_best_ask:.6f}",
        'sportsbook_prob': f"{value_bet.true_prob:.6f}",
        'ev_percent': f"{ev_percent:.2f}",
    }
    
    fieldnames = [
        'event_name', 'outcome', 'date', 'event_slug', 'market_slug',
        'polymarket_odds', 'sportsbook_prob', 'ev_percent'
    ]
    
    try:
        with open(path, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        print(f"[WARNING] Failed to log value bet: {e}")



def normalize_team_name(s: str) -> str:
    """Normalize team name for matching. Assumes input is already stripped."""
    normalized = " ".join(str(s).lower().split())
    # Remove common suffixes for soccer teams
    suffixes = [" fc", " football club", " united", " city", " town", " rovers", " wanderers", " athletic"]
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()
    return normalized


def teams_match(team1: str, team2: str) -> bool:
    """
    Check if two team names match (fuzzy matching).
    
    Handles cases where:
    - Pinnacle: "Gonzaga" (school name only)
    - Polymarket: "Gonzaga Bulldogs" (school name + team name)
    - Soccer: "Brighton & Hove Albion FC" vs "Brighton"
    - Soccer: "AFC Bournemouth" vs "Bournemouth"
    """
    norm1 = normalize_team_name(team1)
    norm2 = normalize_team_name(team2)
    
    if norm1 == norm2:
        return True
    
    # Check if one name starts with the other
    if norm1.startswith(norm2) or norm2.startswith(norm1):
        return True
    
    # For soccer teams, check common patterns
    # Remove "&" and normalize
    norm1_clean = norm1.replace("&", "").replace(" and ", " ")
    norm2_clean = norm2.replace("&", "").replace(" and ", " ")
    if norm1_clean == norm2_clean:
        return True
    if norm1_clean.startswith(norm2_clean) or norm2_clean.startswith(norm1_clean):
        return True
    
    # Check if last word matches (nickname matching)
    words1 = norm1.split()
    words2 = norm2.split()
    if words1 and words2 and words1[-1] == words2[-1]:
        return True
    
    # Check if one contains the other
    if norm1 in norm2 or norm2 in norm1:
        return True
    if norm1_clean in norm2_clean or norm2_clean in norm1_clean:
        return True
    
    # Check if first word matches (for NCAA/international/soccer)
    if words1 and words2:
        if words1[0] == words2[0] and len(words1[0]) > 3:
            return True
        # For soccer: "Brighton & Hove Albion" vs "Brighton"
        if len(words1) > 1 and len(words2) == 1:
            if words1[0] == words2[0]:
                return True
        if len(words2) > 1 and len(words1) == 1:
            if words2[0] == words1[0]:
                return True
    
    # Check if all words from shorter name are in longer name
    if len(words1) > 1 and len(words2) > 1:
        shorter = words1 if len(words1) < len(words2) else words2
        longer = words2 if len(words1) < len(words2) else words1
        if all(word in longer for word in shorter if len(word) > 2):
            return True
    
    # For soccer: check if key words match (e.g., "Brighton" in "Brighton & Hove Albion")
    key_words1 = [w for w in words1 if len(w) > 3]
    key_words2 = [w for w in words2 if len(w) > 3]
    if key_words1 and key_words2:
        # If any key word from one appears in the other
        for kw in key_words1:
            if kw in norm2:
                return True
        for kw in key_words2:
            if kw in norm1:
                return True
    
    return False


def is_slug_basketball(slug_lower: str, include_nba: bool = True, include_ncaa: bool = True, 
                       international_prefixes: Optional[List[str]] = None) -> bool:
    """
    Check if a slug represents a basketball event.
    
    Args:
        slug_lower: Lowercase slug to check
        include_nba: Whether to include NBA games
        include_ncaa: Whether to include NCAA games
        international_prefixes: List of international league prefixes (e.g., ["bkcba", "bknbl"])
    
    Returns:
        True if the slug matches basketball criteria
    """
    is_basketball = False
    
    # NBA checks
    if include_nba:
        if (slug_lower.startswith("nba-") or 
            "-nba-" in slug_lower or 
            "basketball" in slug_lower or 
            "bball" in slug_lower):
            is_basketball = True
    
    # NCAA checks
    if include_ncaa and not is_basketball:
        if (slug_lower.startswith("ncaa") or 
            slug_lower.startswith("cbb-") or 
            "-ncaa-" in slug_lower or 
            "-cbb-" in slug_lower):
            is_basketball = True
    
    # International league checks
    if international_prefixes:
        for prefix in international_prefixes:
            prefix_lower = prefix.lower()
            if (slug_lower.startswith(prefix_lower) or 
                f"-{prefix_lower}-" in slug_lower):
                is_basketball = True
    
    # Generic international basketball leagues
    if not is_basketball:
        generic_prefixes = [
            "euroleague",
            "eurocup",
            "fib",
        ]
        for prefix in generic_prefixes:
            if slug_lower.startswith(prefix):
                is_basketball = True
    
    return is_basketball


def fetch_polymarket_events_for_date(
    target_date: date,
    whitelisted_prefixes: Optional[List[str]] = None,
    verbose: bool = False,
) -> List[Tuple[str, str, str]]:
    """
    Fetch all Polymarket events for a given date, filtered by whitelisted prefixes.
    
    Args:
        target_date: Date to fetch events for
        whitelisted_prefixes: List of prefixes to filter events by (e.g., ["nba", "cbb", "nhl"]).
                              If None, includes all events (caller should filter).
        verbose: If True, print detailed progress information.
    
    Returns:
        List of tuples: (event_slug, away_team, home_team)
    """
    if verbose:
        print(f"\n[POLYMARKET] Fetching events for {target_date}")
        print(f"  Whitelisted prefixes: {whitelisted_prefixes}")
    
    finder = PolymarketGameFinder()
    events_list = []
    seen_slugs = set()
    
    # Fetch events pages
    for active_only in (True, False):
        if verbose:
            print(f"  Searching {'active' if active_only else 'inactive'} events...")
        for page in range(100):  # Search up to 100 pages
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
                
                # Deduplicate
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                
                # Exclude WNBA games
                if slug_lower.startswith("cwbb-") or slug_lower.startswith("cwbb"):
                    continue
                
                # Filter by whitelisted prefixes if specified
                if whitelisted_prefixes:
                    matches_prefix = False
                    for prefix in whitelisted_prefixes:
                        prefix_lower = prefix.lower().strip()
                        if not prefix_lower:
                            continue
                        # Check if slug starts with prefix or contains it with dashes
                        if (slug_lower.startswith(prefix_lower) or 
                            f"-{prefix_lower}-" in slug_lower or
                            slug_lower.startswith(f"{prefix_lower}-")):
                            matches_prefix = True
                            break
                    if not matches_prefix:
                        continue
                
                # Extract title
                title_raw = event.get("title") or ""
                if not title_raw:
                    continue
                title = str(title_raw).strip()
                
                # Exclude esports
                title_lower = title.lower()
                esports_keywords = ["dota", "valorant", "lol:", "league of legends", "cs:", "counter-strike", "esports"]
                if any(x in title_lower for x in esports_keywords):
                    continue
                
                # Parse team names from title
                clean_title = title
                for prefix in ["NBA:", "NCAA:", "NCAAB:", "NBAGL:", "Basketball:"]:
                    if clean_title.startswith(prefix):
                        clean_title = clean_title[len(prefix):].strip()
                        break
                
                parts = clean_title.replace(" vs ", " @ ").replace(" vs. ", " @ ").split(" @ ", 1)
                if len(parts) != 2:
                    continue
                
                away = parts[0].strip().rstrip(".")
                home = parts[1].strip().rstrip(".")
                
                if away and home:
                    events_list.append((slug, away, home))
                    if verbose:
                        print(f"    + Found: {slug} | {away} @ {home}")
    
    if verbose:
        print(f"\n[POLYMARKET] Total events found: {len(events_list)}")
    return events_list


def fetch_market_slugs_by_event(event_slugs: List[str], verbose: bool = False) -> Dict[str, Dict[str, List[str]]]:
    """
    Fetch market slugs for each event slug.
    
    Note: This function fetches event data temporarily to extract market slugs, but
    does not return the event data. Only market slugs are returned.
    
    Args:
        event_slugs: List of event slugs to fetch market slugs for
        verbose: If True, print detailed logs about fetching events
    
    Returns:
        Dict mapping event_slug -> {
            'moneyline': [market_slug],
            'spreads': [market_slug1, market_slug2, ...],
            'totals': [market_slug1, market_slug2, ...]
        }
    """
    if verbose:
        print(f"\n[POLYMARKET] Fetching market slugs for {len(event_slugs)} events...")
    
    odds_service = PolymarketOdds()
    market_slugs_map = {}
    
    for i, event_slug in enumerate(event_slugs, 1):
        try:
            # Fetch event data temporarily to extract market slugs
            event = odds_service.fetch_event_by_slug(event_slug)
            if not event:
                if verbose:
                    print(f"  [{i}/{len(event_slugs)}] {event_slug} - FAILED to fetch")
                continue
            
            # Extract market slugs (event data is discarded after this)
            spreads = PolymarketMarketExtractor.spread_market_slugs_from_event(event)
            totals = PolymarketMarketExtractor.totals_market_slugs_from_event(event)
            player_props = PolymarketMarketExtractor.player_prop_market_slugs_from_event(event)
            
            markets = {
                'moneyline': [event_slug],  # Moneyline uses event_slug
                'spreads': spreads,
                'totals': totals,
                'player_props': player_props,
            }
            market_slugs_map[event_slug] = markets
            
            if verbose:
                print(f"  [{i}/{len(event_slugs)}] {event_slug}")
                print(f"      Moneyline: {event_slug}")
                if spreads:
                    print(f"      Spreads ({len(spreads)}): {spreads[:3]}..." if len(spreads) > 3 else f"      Spreads ({len(spreads)}): {spreads}")
                else:
                    print(f"      Spreads: None")
                if totals:
                    print(f"      Totals ({len(totals)}): {totals[:3]}..." if len(totals) > 3 else f"      Totals ({len(totals)}): {totals}")
                else:
                    print(f"      Totals: None")
                if player_props:
                    print(f"      Player Props ({len(player_props)}): {player_props[:3]}..." if len(player_props) > 3 else f"      Player Props ({len(player_props)}): {player_props}")
                else:
                    print(f"      Player Props: None")
                
        except Exception as e:
            if verbose:
                print(f"  [{i}/{len(event_slugs)}] {event_slug} - ERROR: {e}")
            continue
    
    if verbose:
        print(f"\n[POLYMARKET] Successfully fetched market slugs for {len(market_slugs_map)}/{len(event_slugs)} events")
    return market_slugs_map


def match_games_and_fetch_markets(
    polymarket_events: List[Tuple[str, str, str]],
    pinnacle_games: List,
    verbose: bool = False,
) -> Tuple[List[Tuple[str, str, str, object]], Dict[str, Dict[str, List[str]]]]:
    """
    Match Polymarket events with Pinnacle games by team names, and fetch market slugs for matched events.
    
    Args:
        polymarket_events: List of (event_slug, away_team, home_team) tuples
        pinnacle_games: List of Pinnacle GameInfo objects
        verbose: If True, print detailed logs about fetching events
    
    Returns:
        Tuple of:
        1. List of (event_slug, away_team, home_team, pinnacle_game) tuples for matched games
        2. Dict mapping event_slug -> {
            'moneyline': [market_slug],
            'spreads': [market_slug1, market_slug2, ...],
            'totals': [market_slug1, market_slug2, ...]
        }
    """
    matched = []
    market_slugs_map = {}
    odds_service = PolymarketOdds()
    
    for event_slug, pm_away, pm_home in polymarket_events:
        if verbose:
            print(f"  [MATCH] Trying to match Polymarket: {pm_away} @ {pm_home}")
        # Try to find matching Pinnacle game
        for pinnacle_game in pinnacle_games:
            pin_away = pinnacle_game.away_team
            pin_home = pinnacle_game.home_team
            
            if not pin_away or not pin_home:
                continue
            
            # Check both possible matchings
            match1 = teams_match(pm_away, pin_away) and teams_match(pm_home, pin_home)
            match2 = teams_match(pm_away, pin_home) and teams_match(pm_home, pin_away)
            
            if verbose and not (match1 or match2):
                # Only print first few non-matches to avoid spam
                if len([g for g in pinnacle_games if g == pinnacle_game]) <= 3:
                    print(f"    [NO MATCH] vs Pinnacle: {pin_away} @ {pin_home}")
                    print(f"      pm_away vs pin_away: {teams_match(pm_away, pin_away)}")
                    print(f"      pm_home vs pin_home: {teams_match(pm_home, pin_home)}")
                    print(f"      pm_away vs pin_home: {teams_match(pm_away, pin_home)}")
                    print(f"      pm_home vs pin_away: {teams_match(pm_home, pin_away)}")
            
            if match1 or match2:
                if verbose:
                    print(f"    [MATCH FOUND!] Pinnacle: {pin_away} @ {pin_home}")
                # Use Pinnacle team names and include the full Pinnacle game object
                matched.append((event_slug, pin_away, pin_home, pinnacle_game))
                
                # Fetch event data temporarily to extract market slugs
                try:
                    event = odds_service.fetch_event_by_slug(event_slug)
                    if event:
                        markets = {
                            'moneyline': [event_slug],  # Moneyline uses event_slug
                            'spreads': PolymarketMarketExtractor.spread_market_slugs_from_event(event),
                            'totals': PolymarketMarketExtractor.totals_market_slugs_from_event(event),
                            'player_props': PolymarketMarketExtractor.player_prop_market_slugs_from_event(event),
                        }
                        market_slugs_map[event_slug] = markets
                except Exception:
                    pass
                break
    
    return matched, market_slugs_map


# Backwards-compatible wrapper (for value_bets.py which still uses old flow)
def match_games(
    polymarket_events: List[Tuple[str, str, str]],
    pinnacle_games: List,
    verbose: bool = False,
) -> List[Tuple[str, str, str, object]]:
    """
    Match Polymarket events with Pinnacle games by team names (backwards-compatible).
    Use match_games_and_fetch_markets() if you also need to fetch market slugs.
    
    Returns:
        List of (event_slug, away_team, home_team, pinnacle_game) tuples for matched games.
    """
    matched, _ = match_games_and_fetch_markets(polymarket_events, pinnacle_games, verbose=verbose)
    return matched
