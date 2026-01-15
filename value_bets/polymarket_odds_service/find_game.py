#!/usr/bin/env python3
"""
Gamma API event finder (league-agnostic).

Given:
- away team name
- home team name
- local date

Find the matching Gamma event slug by paging through `/events` and filtering client-side.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

import requests


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
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                return data
            return data.get("data", []) or []
        except requests.exceptions.RequestException as e:
            print(f"Error fetching events: {e}")
            return []

    @staticmethod
    def _normalize(s: str) -> str:
        return " ".join(str(s).strip().lower().split())

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
                    title = self._normalize(event.get("title", ""))
                    if not title:
                        continue

                    if not any(t in title for t in away_tokens):
                        continue
                    if not any(t in title for t in home_tokens):
                        continue

                    start = self._parse_start_time(event)
                    if start is not None and start.astimezone().date() != play_date:
                        continue

                    slug = str(event.get("slug") or "").strip()
                    if slug:
                        return slug

        return None

