#!/usr/bin/env python3
"""
Shared helper functions for value betting bots.
"""

from datetime import datetime, date
from typing import List, Tuple, Dict, Optional
from polymarket_odds_service.find_game import PolymarketGameFinder
from polymarket_odds_service.polymarket_market_analyzer import PolymarketMarketAnalyzer


def normalize_team_name(s: str) -> str:
    """Normalize team name for matching. Assumes input is already stripped."""
    return " ".join(str(s).lower().split())


def teams_match(team1: str, team2: str) -> bool:
    """
    Check if two team names match (fuzzy matching).
    
    Handles cases where:
    - Pinnacle: "Gonzaga" (school name only)
    - Polymarket: "Gonzaga Bulldogs" (school name + team name)
    """
    norm1 = normalize_team_name(team1)
    norm2 = normalize_team_name(team2)
    
    if norm1 == norm2:
        return True
    
    # Check if one name starts with the other
    if norm1.startswith(norm2) or norm2.startswith(norm1):
        return True
    
    # Check if last word matches (nickname matching)
    words1 = norm1.split()
    words2 = norm2.split()
    if words1 and words2 and words1[-1] == words2[-1]:
        return True
    
    # Check if one contains the other
    if norm1 in norm2 or norm2 in norm1:
        return True
    
    # Check if first word matches (for NCAA/international)
    if words1 and words2:
        if words1[0] == words2[0] and len(words1[0]) > 3:
            return True
    
    # Check if all words from shorter name are in longer name
    if len(words1) > 1 and len(words2) > 1:
        shorter = words1 if len(words1) < len(words2) else words2
        longer = words2 if len(words1) < len(words2) else words1
        if all(word in longer for word in shorter if len(word) > 2):
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
    include_nba: bool = True,
    include_ncaa: bool = True,
    international_prefixes: Optional[List[str]] = None,
    league_filter: Optional[str] = None,
) -> List[Tuple[str, str, str]]:
    """
    Fetch all Polymarket basketball events for a given date.
    
    Args:
        target_date: Date to fetch events for
        include_nba: Whether to include NBA games
        include_ncaa: Whether to include NCAA games
        international_prefixes: List of international league prefixes to include
        league_filter: Optional league filter (e.g., "bkcba" to only include events with this in slug)
    
    Returns:
        List of tuples: (event_slug, away_team, home_team)
    """
    finder = PolymarketGameFinder()
    events_list = []
    seen_slugs = set()
    
    # Default international prefixes if not specified
    if international_prefixes is None:
        international_prefixes = [
            "euroleague",
            "eurocup",
            "fib",
            "bkkbl",
            "bkarg",
            "bkfr1",
            "bkcba",
            "bknbl",
            "bkseriea",
            "bkligend",
            "bkcl",
        ]
    
    # Fetch events pages
    for active_only in (True, False):
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
                
                # Apply league filter if specified
                if league_filter and league_filter.lower() not in slug_lower:
                    continue
                
                # Exclude WNBA games
                if slug_lower.startswith("cwbb-") or slug_lower.startswith("cwbb"):
                    continue
                
                # Extract title
                title_raw = event.get("title") or ""
                if not title_raw:
                    continue
                title = str(title_raw).strip()
                
                # Filter for basketball events
                is_basketball = is_slug_basketball(
                    slug_lower, 
                    include_nba=include_nba,
                    include_ncaa=include_ncaa,
                    international_prefixes=international_prefixes
                )
                
                if not is_basketball:
                    title_lower = title.lower()
                    is_basketball = (
                        "basketball" in title_lower or
                        ("nba" in title_lower and include_nba) or
                        ("ncaa" in title_lower and include_ncaa) or
                        ("college basketball" in title_lower and include_ncaa) or
                        ("cbb" in title_lower and include_ncaa)
                    )
                
                if not is_basketball:
                    continue
                
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
    
    return events_list


def fetch_all_events_and_markets(event_slugs: List[str], verbose: bool = False) -> Tuple[Dict[str, Dict], Dict[str, Dict[str, List[str]]]]:
    """
    Fetch all event data and market slugs for each event upfront.
    
    Args:
        event_slugs: List of event slugs to fetch
        verbose: If True, print detailed logs about fetching events
    
    Returns:
        Tuple of:
        1. Dict mapping event_slug -> event_data (raw event dict from API)
        2. Dict mapping event_slug -> {
            'moneyline': [market_slug],
            'spreads': [market_slug1, market_slug2, ...],
            'totals': [market_slug1, market_slug2, ...]
        }
    """
    analyzer = PolymarketMarketAnalyzer(verbose=verbose)
    events_cache = {}
    market_slugs_map = {}
    
    for event_slug in event_slugs:
        try:
            event = analyzer.fetch_event_by_slug(event_slug)
            if not event:
                continue
            
            # Cache the event data
            events_cache[event_slug] = event
            
            # Extract market slugs
            markets = {
                'moneyline': [event_slug],  # Moneyline uses event_slug
                'spreads': PolymarketMarketAnalyzer.spread_market_slugs_from_event(event),
                'totals': PolymarketMarketAnalyzer.totals_market_slugs_from_event(event),
            }
            market_slugs_map[event_slug] = markets
        except Exception:
            continue
    
    return events_cache, market_slugs_map


def match_games_and_fetch_markets(
    polymarket_events: List[Tuple[str, str, str]],
    pinnacle_games: List,
    verbose: bool = False,
) -> Tuple[List[Tuple[str, str, str, object]], Dict[str, Dict], Dict[str, Dict[str, List[str]]]]:
    """
    Match Polymarket events with Pinnacle games by team names, and fetch market slugs for matched events.
    
    Args:
        polymarket_events: List of (event_slug, away_team, home_team) tuples
        pinnacle_games: List of Pinnacle GameInfo objects
        verbose: If True, print detailed logs about fetching events
    
    Returns:
        Tuple of:
        1. List of (event_slug, away_team, home_team, pinnacle_game) tuples for matched games
        2. Dict mapping event_slug -> event_data (raw event dict from API)
        3. Dict mapping event_slug -> {
            'moneyline': [market_slug],
            'spreads': [market_slug1, market_slug2, ...],
            'totals': [market_slug1, market_slug2, ...]
        }
    """
    matched = []
    events_cache = {}
    market_slugs_map = {}
    analyzer = PolymarketMarketAnalyzer(verbose=verbose)
    
    for event_slug, pm_away, pm_home in polymarket_events:
        # Try to find matching Pinnacle game
        for pinnacle_game in pinnacle_games:
            pin_away = pinnacle_game.away_team
            pin_home = pinnacle_game.home_team
            
            if not pin_away or not pin_home:
                continue
            
            # Check both possible matchings
            match1 = teams_match(pm_away, pin_away) and teams_match(pm_home, pin_home)
            match2 = teams_match(pm_away, pin_home) and teams_match(pm_home, pin_away)
            
            if match1 or match2:
                # Use Pinnacle team names and include the full Pinnacle game object
                matched.append((event_slug, pin_away, pin_home, pinnacle_game))
                
                # Fetch event data and market slugs for this matched event
                try:
                    event = analyzer.fetch_event_by_slug(event_slug)
                    if event:
                        events_cache[event_slug] = event
                        markets = {
                            'moneyline': [event_slug],  # Moneyline uses event_slug
                            'spreads': PolymarketMarketAnalyzer.spread_market_slugs_from_event(event),
                            'totals': PolymarketMarketAnalyzer.totals_market_slugs_from_event(event),
                        }
                        market_slugs_map[event_slug] = markets
                except Exception:
                    pass
                break
    
    return matched, events_cache, market_slugs_map


# Backwards-compatible wrapper (for value_bets.py which still uses old flow)
def match_games(
    polymarket_events: List[Tuple[str, str, str]],
    pinnacle_games: List,
) -> List[Tuple[str, str, str, object]]:
    """
    Match Polymarket events with Pinnacle games by team names (backwards-compatible).
    Use match_games_and_fetch_markets() if you also need to fetch market slugs.
    
    Returns:
        List of (event_slug, away_team, home_team, pinnacle_game) tuples for matched games.
    """
    matched, _, _ = match_games_and_fetch_markets(polymarket_events, pinnacle_games, verbose=False)
    return matched
