from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from datetime import date, datetime
import json
import re
import requests
from py_clob_client.client import ClobClient
class PolymarketMarketExtractor:
    """Helper class for extracting market information from Polymarket events."""

    @staticmethod
    def spread_market_slugs_from_event(event: Dict) -> List[str]:
        """
        Given a Gamma event payload (from /events/slug/{event_slug}), return the list of
        spread market slugs, e.g. "nba-phx-mia-2026-01-13-spread-home-1pt5".

        Heuristics (to avoid missing spread markets):
        - include if `question` contains "spread" (case-insensitive), OR
        - include if `sportsMarketType` contains "spread", OR
        - include if slug contains "-spread-"

        Exclusions:
        - exclude first half spreads (e.g. "1H Spread" / "first_half_spreads" / slug contains "-1h-")
        """
        if not isinstance(event, dict):
            return []

        out: List[str] = []
        markets = event.get("markets", []) or []
        for m in markets:
            try:
                if not isinstance(m, dict):
                    continue
                q = str(m.get("question") or "")
                smt = str(m.get("sportsMarketType") or "")
                slug_raw = m.get("slug") or ""
                if not slug_raw:
                    continue
                # Strip once when extracting from event
                slug = str(slug_raw).strip()

                # Exclude 1H / first half spread markets; we only want full game spreads.
                q_low = q.lower()
                smt_low = smt.lower()
                slug_low = slug.lower()
                is_first_half = (
                    q_low.startswith("1h ")
                    or q_low.startswith("1hspread")
                    or "1h spread" in q_low
                    or "first half" in q_low
                    or "first_half" in smt_low
                    or "first half" in smt_low
                    or "-1h-" in slug_low
                    or slug_low.startswith("1h-")
                )
                if is_first_half:
                    continue

                is_spread = (
                    ("spread" in q_low)
                    or ("spread" in smt_low)
                    or ("-spread-" in slug_low)
                )
                if not is_spread:
                    continue
                if slug:
                    out.append(slug)
            except Exception:
                continue

        # De-dup while keeping order
        seen = set()
        deduped: List[str] = []
        for s in out:
            if s in seen:
                continue
            seen.add(s)
            deduped.append(s)
        return deduped

    @staticmethod
    def totals_market_slugs_from_event(event: Dict) -> List[str]:
        """
        Given a Gamma event payload, return generic totals market slugs (e.g. NBA O/U).
        Excludes total-games, total-sets (tennis), match-total, set-totals, and spread.
        Use totals_games / totals_sets for tennis.
        """
        if not isinstance(event, dict):
            return []

        out: List[str] = []
        for m in event.get("markets", []) or []:
            if not isinstance(m, dict):
                continue
            slug = str(m.get("slug") or "").strip()
            if not slug or "total" not in slug.lower():
                continue
            s = slug.lower()
            if "games" in s or "sets" in s or "spread" in s or "match-total" in s or "set-totals" in s or "first-set" in s or "first_set" in s:
                continue
            out.append(slug)
        return list(dict.fromkeys(out))

    @staticmethod
    def totals_games_market_slugs_from_event(event: Dict) -> List[str]:
        """
        Return total-games market slugs (tennis over/under games).
        Matches: "match-total-X" (Polymarket) or "total" + "games".
        Excludes: sets, spread, first-set (first-set-total = first set games, not match total).
        """
        if not isinstance(event, dict):
            return []

        out: List[str] = []
        for m in event.get("markets", []) or []:
            if not isinstance(m, dict):
                continue
            slug = str(m.get("slug") or "").strip()
            if not slug:
                continue
            s = slug.lower()
            if "sets" in s or "spread" in s or "first-set" in s or "first_set" in s:
                continue
            if "set-totals" in s:
                continue
            if ("match" in s and "total" in s) or ("total" in s and "games" in s):
                out.append(slug)
        return list(dict.fromkeys(out))

    @staticmethod
    def totals_sets_market_slugs_from_event(event: Dict) -> List[str]:
        """
        Return total-sets market slugs (tennis over/under sets).
        Matches: "set-totals-X" (Polymarket) or "total" + "sets".
        Excludes: games, spread.
        """
        if not isinstance(event, dict):
            return []

        out: List[str] = []
        for m in event.get("markets", []) or []:
            if not isinstance(m, dict):
                continue
            slug = str(m.get("slug") or "").strip()
            if not slug:
                continue
            s = slug.lower()
            if "games" in s or "spread" in s:
                continue
            if "set-totals" in s or ("total" in s and "sets" in s):
                out.append(slug)
        return list(dict.fromkeys(out))

class PolymarketGameFinder:
    """
    Finds sports game events on Polymarket (Gamma API).
    
    NOTE: This does not use `/public-search` because that endpoint can return 422s
    for complex queries. We only use `/events` and filter client-side.
    """

    GAMMA_API_BASE = "https://gamma-api.polymarket.com"
    GAME_BETS_TAG_ID = "100639"

    def __init__(self) -> None:
        self.session = requests.Session()
    
    def _retry_request(self, method: str, url: str, max_retries: int = 3, **kwargs) -> Optional[requests.Response]:
        """
        Retry HTTP requests with exponential backoff for connection errors.
        
        Args:
            method: HTTP method ('get', 'post', etc.)
            url: URL to request
            max_retries: Maximum number of retry attempts
            **kwargs: Additional arguments to pass to requests method
            
        Returns:
            Response object or None if all retries failed
        """
        import time
        for attempt in range(max_retries):
            try:
                response = getattr(self.session, method.lower())(url, **kwargs)
                response.raise_for_status()
                return response
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    print(f"[DEBUG] [PolymarketGameFinder] Connection error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"[DEBUG] [PolymarketGameFinder] Connection error after {max_retries} attempts: {e}")
                    raise
            except requests.exceptions.HTTPError as e:
                # Don't retry HTTP errors (4xx, 5xx) - these are not transient
                print(f"[DEBUG] [PolymarketGameFinder] HTTP error: {e}")
                raise
        return None

    def fetch_events_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        active: bool = True,
        closed: bool = False,
        order: str = "startTime",
        ascending: bool = False,
        tag_id: str = GAME_BETS_TAG_ID,
    ) -> List[Dict[str, Any]]:
        """
        Fetch one page of events from Gamma's `/events` endpoint.
        """
        url = f"{self.GAMMA_API_BASE}/events"
        params = {
            "active": "true" if active else "false",
            "closed": "true" if closed else "false",
            "limit": str(int(limit)),
            "offset": str(int(offset)),
            "order": str(order),
            "ascending": "true" if ascending else "false",
            "tag_id": str(tag_id),
        }

        try:
            response = self._retry_request('get', url, params=params)
            if response is None:
                return []
            data = response.json()
            
            if isinstance(data, list):
                return data
            return data.get("data", []) or []
        except requests.exceptions.RequestException:
            return []

    TARGET_SLUG = "cbb-stfpa-chist-2026-01-29"

    def _print_events_readable(self, data: Any) -> None:
        """Print human-readable summary + full JSON only when target slug is present."""
        events = data if isinstance(data, list) else (data.get("data") or data.get("events") or [])
        if not isinstance(events, list):
            return
        has_target = any(
            (evt.get("slug") or "").strip() == self.TARGET_SLUG
            for evt in events
        )
        if not has_target:
            return

    @staticmethod
    def _normalize(s: str) -> str:
        # Assumes input is already stripped
        return " ".join(str(s).lower().split())

    @classmethod
    def _team_tokens(cls, team: str) -> List[str]:
        """
        Tokens to match in Gamma event titles.
        
        Polymarket titles sometimes contain only the team nickname, so we match:
        - last word, and
        - other "meaningful" words (>3 chars)
        """
        words = [w for w in cls._normalize(team).split() if w]
        if not words:
            return []

        last = words[-1]
        meaningful = [w for w in words if len(w) > 3]

        out: List[str] = []
        for w in [last, *meaningful]:
            if w not in out:
                out.append(w)
        return out

    @staticmethod
    def _parse_start_time(event: Dict[str, Any]) -> Optional[datetime]:
        """
        Best-effort parse of `startTime` / `startDate` to a datetime.
        """
        for key in ("startTime", "startDate"):
            raw = event.get(key)
            if not raw:
                continue
            try:
                return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            except Exception:
                continue
        return None

    def find_event_slug(
        self,
        *,
        away_team: str,
        home_team: str,
        play_date: date,
        limit: int = 100,
        max_pages: int = 25,
    ) -> Optional[str]:
        """
        Find the Gamma event slug for a matchup on a given local date.
        """
        # Strip inputs once when they first enter
        away_team = away_team.strip() if away_team else ""
        home_team = home_team.strip() if home_team else ""
        away_tokens = self._team_tokens(away_team)
        home_tokens = self._team_tokens(home_team)
        if not away_tokens or not home_tokens:
            return None

        # Some future events appear as inactive until closer to game time.
        # We try active-only first, then retry with active=false.
        for active_only in (True, False):
            for page in range(max(0, int(max_pages))):
                events = self.fetch_events_page(
                    limit=limit,
                    offset=page * limit,
                    active=active_only,
                    closed=False,
                    order="startTime",
                    ascending=False,  # newest/upcoming first (better for tomorrow)
                )
                if not events:
                    break

                for event in events:
                    title_raw = event.get("title") or ""
                    if not title_raw:
                        continue
                    # Strip once when extracting from event
                    title = self._normalize(str(title_raw).strip())
                    if not title:
                        continue

                    if not any(t in title for t in away_tokens):
                        continue
                    if not any(t in title for t in home_tokens):
                        continue

                    start = self._parse_start_time(event)
                    if start is not None and start.astimezone().date() != play_date:
                        continue

                    slug_raw = event.get("slug") or ""
                    if not slug_raw:
                        continue
                    # Strip once when extracting from event
                    slug = str(slug_raw).strip()
                    if slug:
                        return slug

        return None