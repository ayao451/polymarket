#!/usr/bin/env python3
"""
Pinnacle Odds Scraper (Playwright Sync)

Scrapes betting odds by intercepting Pinnacle JSON API responses (preferred).
Falls back to DOM scraping ONLY if no usable API payloads are captured.

Requirements:
  - playwright (sync API)
  - pandas

Run:
  python3 pinnacle_scraper/pinnacle_odds_scraper.py
  python3 pinnacle_scraper/pinnacle_odds_scraper.py --url "https://www.pinnacle.com/en/basketball/nba/..."
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from playwright.sync_api import Browser, BrowserContext, Page, Playwright, sync_playwright


DEFAULT_MATCHUPS_URL = "https://www.pinnacle.com/en/basketball/matchups/"
ARCADIA_BASKETBALL_MATCHUPS_URL = (
    "https://guest.api.arcadia.pinnacle.com/0.1/sports/4/matchups?withSpecials=false&brandId=0"
)
DEFAULT_HOCKEY_MATCHUPS_URL = "https://www.pinnacle.com/en/hockey/matchups/"
ARCADIA_HOCKEY_MATCHUPS_URL = (
    "https://guest.api.arcadia.pinnacle.com/0.1/sports/19/matchups?withSpecials=false&brandId=0"
)
DEFAULT_SOCCER_MATCHUPS_URL = "https://www.pinnacle.com/en/soccer/matchups/"
ARCADIA_SOCCER_MATCHUPS_URL = (
    "https://guest.api.arcadia.pinnacle.com/0.1/sports/29/matchups?withSpecials=false&brandId=0"
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _sleep_human(min_s: float = 0.15, max_s: float = 0.65) -> None:
    time.sleep(random.uniform(min_s, max_s))


def _safe_json(resp_text: str) -> Optional[Any]:
    if not resp_text:
        return None
    try:
        return json.loads(resp_text)
    except Exception:
        return None


def _is_json_response(headers: Dict[str, str]) -> bool:
    ct = (headers.get("content-type") or headers.get("Content-Type") or "").lower()
    return "application/json" in ct or ct.endswith("+json")


def _looks_like_odds_payload(obj: Any) -> bool:
    """
    Heuristic: does this payload look like it contains betting lines / prices?
    We keep this intentionally broad because Pinnacle's internal shapes can vary.
    """
    if not isinstance(obj, (dict, list)):
        return False
    s = json.dumps(obj)[:20000].lower()  # cap to keep it cheap
    keywords = [
        "moneyline",
        "spread",
        "spreads",
        "total",
        "totals",
        "price",
        "prices",
        "odds",
        "selections",
        "markets",
        "period",
        "home",
        "away",
        "participants",
        "team",
    ]
    hits = sum(1 for k in keywords if k in s)
    return hits >= 4


def _iter_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
    """
    Yield dict nodes in a nested JSON structure.
    """
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_dicts(item)


def _norm(s: str) -> str:
    return " ".join((s or "").strip().split())


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _american_to_decimal(american: Any) -> Optional[float]:
    """
    Convert American odds to decimal odds.
    Examples:
      -110 -> 1.9091
      +150 -> 2.5
    """
    a = _to_float(american)
    if a is None or a == 0:
        return None
    if a > 0:
        return 1.0 + (a / 100.0)
    return 1.0 + (100.0 / abs(a))


def _extract_matchup_id_from_url(url: str) -> Optional[int]:
    """
    Extract numeric matchup id from a Pinnacle event URL.
    """
    s = str(url or "")
    m = re.search(r"/(\d{6,})(?:/|#|\\?|$)", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _parse_iso_dt(s: Any) -> Optional[datetime]:
    """
    Parse an Arcadia/Pinnacle ISO timestamp like "2026-01-15T19:30:00Z".
    Returns an aware datetime in UTC.
    """
    if not s:
        return None
    txt = str(s).strip()
    if not txt:
        return None
    try:
        if txt.endswith("Z"):
            dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _format_dt_local(dt_utc: datetime) -> tuple[str, str]:
    """
    Return (local_date_str, local_time_str) for a UTC datetime.
    """
    try:
        local = dt_utc.astimezone()
    except Exception:
        local = dt_utc
    return local.date().isoformat(), local.strftime("%H:%M:%S %Z")


def _format_dt_utc(dt_utc: datetime) -> str:
    try:
        return dt_utc.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(dt_utc)


def _teams_from_matchup_item(m: Dict[str, Any]) -> tuple[str, str]:
    """
    Extract (away, home) names from a matchup listing payload.
    Prefers full names over abbreviations.
    """
    away = ""
    home = ""
    parts = m.get("participants")
    if isinstance(parts, list):
        for p in parts:
            if not isinstance(p, dict):
                continue
            # Prefer full name fields, fall back to name
            name = _norm(str(
                p.get("fullName") or 
                p.get("displayName") or 
                p.get("longName") or 
                p.get("name") or 
                ""
            ))
            align = _norm_key(str(p.get("alignment") or ""))
            if not name or not align:
                continue
            if align == "home" and not home:
                home = name
            elif align == "away" and not away:
                away = name
    return away, home


def _league_name_from_matchup_item(m: Dict[str, Any]) -> str:
    league = m.get("league")
    if isinstance(league, dict):
        return _norm(str(league.get("name") or ""))
    return _norm(str(league or ""))


def _list_basketball_matchups_for_local_date(
    *,
    local_date,
    timeout_s: float = 20.0,
) -> List[Dict[str, Any]]:
    """
    Fetch the Arcadia basketball matchups feed and filter to matchups whose startTime
    falls on the given local_date (system local timezone).
    """
    payload = _arcadia_get_json_requests(ARCADIA_BASKETBALL_MATCHUPS_URL, timeout_s=timeout_s)
    if not isinstance(payload, list):
        return []

    out: List[Dict[str, Any]] = []
    for m in payload:
        if not isinstance(m, dict):
            continue
        st = _parse_iso_dt(m.get("startTime"))
        if st is None:
            continue
        try:
            st_local = st.astimezone()  # system local tz
        except Exception:
            st_local = st
        if st_local.date() != local_date:
            continue
        out.append(m)
    return out


def _list_hockey_matchups_for_local_date(
    *,
    local_date,
    timeout_s: float = 20.0,
) -> List[Dict[str, Any]]:
    """
    Fetch the Arcadia hockey matchups feed and filter to matchups whose startTime
    falls on the given local_date (system local timezone).
    """
    payload = _arcadia_get_json_requests(ARCADIA_HOCKEY_MATCHUPS_URL, timeout_s=timeout_s)
    if not isinstance(payload, list):
        return []

    out: List[Dict[str, Any]] = []
    for m in payload:
        if not isinstance(m, dict):
            continue
        st = _parse_iso_dt(m.get("startTime"))
        if st is None:
            continue
        try:
            st_local = st.astimezone()  # system local tz
        except Exception:
            st_local = st
        if st_local.date() != local_date:
            continue
        out.append(m)
    return out


def _list_soccer_matchups_for_local_date(
    *,
    local_date,
    timeout_s: float = 20.0,
) -> List[Dict[str, Any]]:
    """
    Fetch the Arcadia soccer matchups feed and filter to matchups whose startTime
    falls on the given local_date (system local timezone).
    """
    payload = _arcadia_get_json_requests(ARCADIA_SOCCER_MATCHUPS_URL, timeout_s=timeout_s)
    if not isinstance(payload, list):
        return []

    out: List[Dict[str, Any]] = []
    for m in payload:
        if not isinstance(m, dict):
            continue
        st = _parse_iso_dt(m.get("startTime"))
        if st is None:
            continue
        try:
            st_local = st.astimezone()  # system local tz
        except Exception:
            st_local = st
        if st_local.date() != local_date:
            continue
        out.append(m)
    return out


def _looks_like_matchups_page(url: str) -> bool:
    try:
        p = urlparse(str(url or ""))
    except Exception:
        return False
    path = (p.path or "").lower()
    return path.endswith("/en/basketball/matchups/") or "/en/basketball/matchups" in path


def _extract_game_links_from_matchups_page(
    *,
    matchups_url: str,
    timeout_ms: int,
    headless: bool = True,
) -> List[str]:
    """
    Load the Pinnacle basketball matchups page and extract per-game URLs.

    We avoid scraping odds from HTML; we only collect links containing a numeric matchup id.
    """
    matchups_url = str(matchups_url or DEFAULT_MATCHUPS_URL).strip() or DEFAULT_MATCHUPS_URL

    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )

    def _find_matchup_ids(obj: Any, *, limit: int = 2000) -> List[int]:
        found: List[int] = []

        def rec(x: Any) -> None:
            if len(found) >= limit:
                return
            if isinstance(x, dict):
                for k, v in x.items():
                    if k in ("matchupId", "matchup_id", "matchupID"):
                        try:
                            found.append(int(v))
                        except Exception:
                            pass
                    rec(v)
            elif isinstance(x, list):
                for it in x:
                    rec(it)

        rec(obj)
        return found

    hrefs: List[str] = []
    discovered_matchup_ids: set[int] = set()

    with sync_playwright() as p:
        # For the matchups page specifically, use a plain context (no stealth tweaks).
        # The more aggressive stealth settings can sometimes prevent the SPA from loading matchups data.
        browser = p.chromium.launch(headless=bool(headless))
        context = browser.new_context(
            user_agent=user_agent,
            viewport={"width": random.randint(1180, 1680), "height": random.randint(720, 1020)},
            locale="en-US",
        )
        context.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
        page = context.new_page()

        def on_request(req) -> None:
            try:
                u = req.url
                if "arcadia.pinnacle.com" not in u:
                    return
                m = re.search(r"/matchups/(\d{6,})", u)
                if m:
                    discovered_matchup_ids.add(int(m.group(1)))
            except Exception:
                return

        def on_response(resp) -> None:
            try:
                u = resp.url
                if "arcadia.pinnacle.com" not in u:
                    return
                if resp.status != 200:
                    return
                # Try to parse JSON; ignore failures.
                try:
                    payload = resp.json()
                except Exception:
                    try:
                        payload = _safe_json(resp.text() or "")
                    except Exception:
                        payload = None
                if payload is None:
                    return
                for mid in _find_matchup_ids(payload, limit=500):
                    if mid and mid > 0:
                        discovered_matchup_ids.add(int(mid))
            except Exception:
                return

        context.on("request", on_request)
        context.on("response", on_response)

        try:
            page.goto(matchups_url, wait_until="networkidle", timeout=timeout_ms)
            _try_accept_cookies(page, timeout_ms=5000)

            # Scroll to load more matchups (infinite lists are common).
            for _ in range(14):
                try:
                    page.mouse.wheel(0, 2600)
                except Exception:
                    pass
                _sleep_human(0.4, 1.0)
                try:
                    page.wait_for_load_state("networkidle", timeout=timeout_ms)
                except Exception:
                    pass

            # Give background fetches time to populate (Arcadia calls often happen after scroll).
            _sleep_human(2.5, 4.5)

            # Extract all hrefs on the page; filter to basketball event links that contain an id.
            try:
                hrefs = page.eval_on_selector_all(
                    "a[href]",
                    "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
                )
            except Exception:
                hrefs = []
        finally:
            try:
                context.close()
                browser.close()
            except Exception:
                pass

    out: List[str] = []
    seen = set()
    for h in hrefs or []:
        u = urljoin(matchups_url, str(h))
        if _extract_matchup_id_from_url(u) is None:
            continue
        # Keep only basketball URLs under /en/basketball/
        try:
            p = urlparse(u)
            if "pinnacle.com" not in (p.netloc or ""):
                continue
            if not (p.path or "").lower().startswith("/en/basketball/"):
                continue
        except Exception:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)

    # If the DOM didn't contain useful links, fall back to network-discovered matchup IDs.
    if not out and discovered_matchup_ids:
        for mid in sorted(discovered_matchup_ids):
            # Construct a stable placeholder URL that our parser understands.
            # We only need the numeric id for Arcadia calls.
            u = f"https://www.pinnacle.com/en/basketball/event/{mid}"
            out.append(u)

    return out


def _arcadia_get_json_requests(url: str, *, timeout_s: float = 20.0) -> Optional[Any]:
    """
    Fetch JSON from Arcadia guest endpoints with retries/backoff.
    This avoids any UI/browser navigation entirely.
    """
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.pinnacle.com/",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    }

    for attempt in range(1, 7):
        try:
            r = requests.get(url, headers=headers, timeout=timeout_s)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    return _safe_json(r.text or "")

            # Retry on transient errors / rate limits
            if r.status_code in (408, 425, 429, 500, 502, 503, 504):
                _sleep_human(0.4 * attempt, 1.0 * attempt)
                continue
            return None
        except Exception:
            _sleep_human(0.4 * attempt, 1.0 * attempt)
            continue
    return None


def _scrape_arcadia_matchup_id(
    matchup_id: int,
    *,
    away_team: str = "",
    home_team: str = "",
    league: str = "",
    start_time_utc: Optional[datetime] = None,
    timeout_ms: int,
) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """
    Fetch odds for a matchup id via Arcadia endpoints.
    Uses the listing-provided team names when available to avoid extra /related calls.
    """
    try:
        mid = int(matchup_id)
    except Exception:
        data = {"ok": False, "matchup_id": matchup_id, "error": "Invalid matchup id", "markets": []}
        return data, pd.DataFrame([])

    markets_url = f"https://guest.api.arcadia.pinnacle.com/0.1/matchups/{mid}/markets/related/straight"
    timeout_s = max(1.0, float(timeout_ms) / 1000.0)
    markets_payload = _arcadia_get_json_requests(markets_url, timeout_s=timeout_s)

    away = _norm(away_team)
    home = _norm(home_team)
    if not away or not home:
        # Fallback to /related if names weren't present.
        related_url = f"https://guest.api.arcadia.pinnacle.com/0.1/matchups/{mid}/related"
        related_payload = _arcadia_get_json_requests(related_url, timeout_s=timeout_s)
        away2, home2 = _arcadia_extract_teams_from_related(related_payload)
        away = away or _norm(away2)
        home = home or _norm(home2)

    if not away or not home or markets_payload is None:
        data = {
            "ok": False,
            "matchup_id": mid,
            "error": "Failed to fetch/parse Arcadia odds for matchup",
            "away_team": away,
            "home_team": home,
            "sources": [markets_url],
            "markets": [],
        }
        return data, pd.DataFrame([])

    rows = _arcadia_markets_to_rows(markets_payload, away=away, home=home)

    # Attach start-time metadata when available (used by default printouts).
    start_utc = start_time_utc.astimezone(timezone.utc) if isinstance(start_time_utc, datetime) else None
    start_local_date = ""
    start_local_time = ""
    start_utc_str = ""
    if start_utc is not None:
        start_local_date, start_local_time = _format_dt_local(start_utc)
        start_utc_str = _format_dt_utc(start_utc)

    data = {
        "ok": True,
        "matchup_id": mid,
        "away_team": away,
        "home_team": home,
        "league": _norm(league),
        "start_time_utc": start_utc_str,
        "start_date_local": start_local_date,
        "start_time_local": start_local_time,
        "sources": [markets_url],
        "markets": [r.to_dict() for r in rows],
    }
    df = pd.DataFrame([r.to_dict() for r in rows])
    return data, df


def _scrape_arcadia_only(url: str, *, timeout_ms: int) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """
    Scrape odds without opening any UI: call Arcadia endpoints derived from matchup id.
    """
    matchup_id = _extract_matchup_id_from_url(url)
    if matchup_id is None:
        data = {"ok": False, "url": url, "error": "Could not extract matchup id from URL", "markets": []}
        return data, pd.DataFrame([])

    related_url = f"https://guest.api.arcadia.pinnacle.com/0.1/matchups/{matchup_id}/related"
    markets_url = f"https://guest.api.arcadia.pinnacle.com/0.1/matchups/{matchup_id}/markets/related/straight"

    timeout_s = max(1.0, float(timeout_ms) / 1000.0)
    related_payload = _arcadia_get_json_requests(related_url, timeout_s=timeout_s)
    markets_payload = _arcadia_get_json_requests(markets_url, timeout_s=timeout_s)

    away, home = _arcadia_extract_teams_from_related(related_payload)
    if not away or not home or markets_payload is None:
        data = {
            "ok": False,
            "url": url,
            "error": "Failed to fetch/parse Arcadia endpoints (possibly blocked or matchup unavailable)",
            "sources": [related_url, markets_url],
            "away_team": away or "",
            "home_team": home or "",
            "markets": [],
        }
        return data, pd.DataFrame([])

    rows = _arcadia_markets_to_rows(markets_payload, away=away, home=home)
    data = {
        "ok": True,
        "url": url,
        "away_team": _norm(away),
        "home_team": _norm(home),
        "sources": [related_url, markets_url],
        "markets": [r.to_dict() for r in rows],
    }
    df = pd.DataFrame([r.to_dict() for r in rows])
    return data, df


def _arcadia_fetch_json(page: Page, url: str, *, timeout_ms: int) -> Optional[Any]:
    """
    Fetch JSON via Playwright's APIRequestContext (not subject to browser CORS).
    """
    # Retries help with intermittent connectivity/rate-limits.
    # Note: we keep headers minimal and stable; Arcadia endpoints we use are public.
    last_text = ""
    for attempt in range(1, 6):
        try:
            resp = page.context.request.get(
                url,
                headers={
                    "Accept": "application/json,text/plain,*/*",
                    "Referer": "https://www.pinnacle.com/",
                },
                timeout=timeout_ms,
            )
            if resp.status == 200:
                try:
                    return resp.json()
                except Exception:
                    try:
                        last_text = resp.text() or ""
                    except Exception:
                        last_text = ""
                    parsed = _safe_json(last_text)
                    return parsed

            # Retry on transient errors
            if resp.status in (408, 425, 429, 500, 502, 503, 504):
                _sleep_human(0.4 * attempt, 0.9 * attempt)
                continue
            return None
        except Exception:
            _sleep_human(0.4 * attempt, 0.9 * attempt)
            continue
    return None


def _arcadia_extract_teams_from_related(related_payload: Any) -> Tuple[Optional[str], Optional[str]]:
    away = None
    home = None
    if not isinstance(related_payload, list):
        return None, None
    for item in related_payload:
        if not isinstance(item, dict):
            continue
        parts = item.get("participants")
        if not isinstance(parts, list):
            continue
        for p in parts:
            if not isinstance(p, dict):
                continue
            # Prefer full name fields, fall back to name
            name = _norm(str(
                p.get("fullName") or 
                p.get("displayName") or 
                p.get("longName") or 
                p.get("name") or 
                ""
            ))
            align = _norm_key(str(p.get("alignment") or ""))
            if not name or not align:
                continue
            if align == "home":
                home = home or name
            elif align == "away":
                away = away or name
        if home and away:
            return away, home
    return away, home


def _arcadia_markets_to_rows(markets_payload: Any, *, away: str, home: str) -> List[OddsRow]:
    rows: List[OddsRow] = []
    if not isinstance(markets_payload, list):
        return rows

    away = _norm(away)
    home = _norm(home)
    if not away or not home:
        return rows

    def _period_label(p: Any) -> str:
        try:
            n = int(p)
        except Exception:
            return "Unknown"
        return {
            0: "Game",
            1: "1H",
            2: "2H",
            3: "1Q",
            4: "2Q",
            5: "3Q",
            6: "4Q",
        }.get(n, f"Period {n}")

    # Sort markets to prioritize isAlternate=None over isAlternate=False
    # Process None first (index 0), then False (index 1), then True (index 2)
    def _alternate_sort_key(m: dict) -> int:
        is_alt_raw = m.get("isAlternate")
        if is_alt_raw is None:
            return 0  # Highest priority
        elif is_alt_raw is False:
            return 1  # Second priority
        else:  # True
            return 2  # Lowest priority for main markets
    
    sorted_markets = sorted(markets_payload, key=lambda m: (_alternate_sort_key(m) if isinstance(m, dict) else 999))
    
    for m in sorted_markets:
        if not isinstance(m, dict):
            continue
        # Keep all periods (e.g., 1H) but label them in output.
        try:
            period = int(m.get("period", 0))
        except Exception:
            period = 0
        period_lbl = _period_label(period)
        is_alt_raw = m.get("isAlternate")
        # Treat None as False (default/main market), True as alternate, False as main
        # Prioritize None over False (we sorted markets so None comes first)
        is_alt = bool(is_alt_raw) if is_alt_raw is not None else False

        mt = _norm_key(str(m.get("type") or ""))
        if mt not in ("moneyline", "spread", "total", "totals"):
            continue
        prices = m.get("prices")
        if not isinstance(prices, list):
            continue

        # Normalize market_type naming
        market_type = "totals" if mt in ("total", "totals") else mt

        for p in prices:
            if not isinstance(p, dict):
                continue
            designation = _norm_key(str(p.get("designation") or ""))
            price = p.get("price")
            odds = _american_to_decimal(price)
            line = _to_float(p.get("points"))

            if market_type == "moneyline":
                if designation == "home":
                    sel = home
                elif designation == "away":
                    sel = away
                else:
                    continue
                rows.append(
                    OddsRow(
                        away_team=away,
                        home_team=home,
                        market_type="moneyline",
                        period=period,
                        period_label=period_lbl,
                        is_alternate=is_alt,
                        selection=sel,
                        line=None,
                        odds=odds,
                        american_price=_to_float(price),
                        raw={"market": m, "price": p},
                    )
                )
            elif market_type == "spread":
                if designation == "home":
                    sel = home
                elif designation == "away":
                    sel = away
                else:
                    continue
                if line is None:
                    continue
                rows.append(
                    OddsRow(
                        away_team=away,
                        home_team=home,
                        market_type="spread",
                        period=period,
                        period_label=period_lbl,
                        is_alternate=is_alt,
                        selection=sel,
                        line=float(line),
                        odds=odds,
                        american_price=_to_float(price),
                        raw={"market": m, "price": p},
                    )
                )
            elif market_type == "totals":
                if designation == "over":
                    sel = "Over"
                elif designation == "under":
                    sel = "Under"
                else:
                    continue
                if line is None:
                    continue
                rows.append(
                    OddsRow(
                        away_team=away,
                        home_team=home,
                        market_type="totals",
                        period=period,
                        period_label=period_lbl,
                        is_alternate=is_alt,
                        selection=sel,
                        line=float(line),
                        odds=odds,
                        american_price=_to_float(price),
                        raw={"market": m, "price": p},
                    )
                )

    # Deduplicate
    # For moneyline markets, deduplicate by (market_type, period, is_alternate, selection)
    # without odds, since there should only be one moneyline per team/period/is_alternate.
    # For spreads/totals, include line in the dedup key.
    seen = set()
    out: List[OddsRow] = []
    for r in rows:
        if r.market_type == "moneyline":
            # For moneylines, deduplicate without odds (only one per team/period/is_alternate)
            k = (
                r.market_type,
                int(r.period or 0),
                bool(r.is_alternate or False),
                _norm_key(r.selection),
            )
        else:
            # For spreads/totals, include line (and odds) in dedup key
            k = (
                r.market_type,
                int(r.period or 0),
                bool(r.is_alternate or False),
                _norm_key(r.selection),
                r.line,
                r.odds,
            )
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


@dataclass(frozen=True)
class OddsRow:
    away_team: str
    home_team: str
    market_type: str  # moneyline | spread | totals
    period: int
    period_label: str
    is_alternate: bool
    selection: str  # team name, "Over", "Under"
    line: Optional[float]  # spread points or total points; None for moneyline
    odds: Optional[float]  # decimal odds if available
    american_price: Optional[float]  # raw Arcadia price (American odds), when available
    raw: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "away_team": self.away_team,
            "home_team": self.home_team,
            "market_type": self.market_type,
            "period": self.period,
            "period_label": self.period_label,
            "is_alternate": self.is_alternate,
            "selection": self.selection,
            "line": self.line,
            "odds": self.odds,
            "american_price": self.american_price,
        }




def _extract_teams_from_payload(payload: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Try a few common shapes:
      - participants: [{"name": "...", "alignment":"home/away"}]
      - homeTeam/awayTeam or home/away nested objects
      - team1/team2 naming
    """
    if not isinstance(payload, (dict, list)):
        return None, None

    home = None
    away = None

    # participants[] shape
    for d in _iter_dicts(payload):
        parts = d.get("participants")
        if isinstance(parts, list) and parts:
            for p in parts:
                if not isinstance(p, dict):
                    continue
                # Prefer full name fields, fall back to name/participantName
                name = _norm(str(
                    p.get("fullName") or 
                    p.get("displayName") or 
                    p.get("longName") or 
                    p.get("name") or 
                    p.get("participantName") or 
                    ""
                ))
                if not name:
                    continue
                align = _norm_key(str(p.get("alignment") or p.get("side") or p.get("homeAway") or ""))
                if align in ("home", "h"):
                    home = home or name
                elif align in ("away", "a", "visitor", "visiting"):
                    away = away or name
            if home and away:
                return away, home

    # direct keys
    for d in _iter_dicts(payload):
        # common variants
        ht = d.get("homeTeam") or d.get("home_team") or d.get("home")
        at = d.get("awayTeam") or d.get("away_team") or d.get("away")
        if isinstance(ht, dict):
            # Prefer full name fields, fall back to name/teamName
            ht_name = _norm(str(
                ht.get("fullName") or 
                ht.get("displayName") or 
                ht.get("longName") or 
                ht.get("name") or 
                ht.get("teamName") or 
                ""
            ))
        else:
            ht_name = _norm(str(ht or ""))
        if isinstance(at, dict):
            # Prefer full name fields, fall back to name/teamName
            at_name = _norm(str(
                at.get("fullName") or 
                at.get("displayName") or 
                at.get("longName") or 
                at.get("name") or 
                at.get("teamName") or 
                ""
            ))
        else:
            at_name = _norm(str(at or ""))
        if ht_name and at_name:
            return at_name, ht_name

    # sometimes "team1"/"team2"
    for d in _iter_dicts(payload):
        t1 = _norm(str(d.get("team1") or d.get("competitor1") or ""))
        t2 = _norm(str(d.get("team2") or d.get("competitor2") or ""))
        if t1 and t2:
            # no clear home/away; return in given order
            return t1, t2

    return None, None


def _try_extract_market_rows(payload: Any, *, away: str, home: str) -> List[OddsRow]:
    """
    Best-effort extraction from JSON.
    We look for dicts that resemble a market with a "type"/"key" and a list of prices/selections.
    """
    rows: List[OddsRow] = []
    if not isinstance(payload, (dict, list)):
        return rows

    away_n = _norm(away)
    home_n = _norm(home)
    if not away_n or not home_n:
        return rows

    def market_type_from_node(d: Dict[str, Any]) -> Optional[str]:
        # Try multiple key variants
        mt = d.get("marketType") or d.get("type") or d.get("key") or d.get("marketKey") or d.get("name")
        mt_s = _norm_key(str(mt or ""))
        if not mt_s:
            return None
        if "moneyline" in mt_s or mt_s in ("h2h", "ml"):
            return "moneyline"
        if "spread" in mt_s:
            return "spread"
        if "total" in mt_s or "totals" in mt_s or "over" in mt_s or "under" in mt_s:
            # Check if it's a player prop total (has player name) vs game total
            # For now, we'll check if the market has player-related fields
            # Player props typically have player names in the selection or market name
            has_player = False
            name_str = str(d.get("name") or d.get("marketName") or "").lower()
            # Common player prop indicators
            if any(indicator in name_str for indicator in ["player", "points", "rebounds", "assists", "threes", "steals", "blocks"]):
                # Check if it's not a team total (team totals usually say "team total")
                if "team total" not in name_str and "team points" not in name_str:
                    has_player = True
            if has_player:
                return "player_prop"
            return "totals"
        # Check for player prop indicators in market name/type
        if any(indicator in mt_s for indicator in ["player", "points", "rebounds", "assists", "threes", "steals", "blocks"]):
            if "team" not in mt_s:
                return "player_prop"
        return None

    def iter_selection_like(n: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(n, dict):
            # common list containers
            for k in ("prices", "price", "selections", "outcomes", "participants", "lines", "offers"):
                v = n.get(k)
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            yield item
                elif isinstance(v, dict):
                    yield v
        elif isinstance(n, list):
            for item in n:
                if isinstance(item, dict):
                    yield item

    for d in _iter_dicts(payload):
        mt = market_type_from_node(d)
        if mt is None:
            continue

        # possible line/points keys
        line = _to_float(d.get("points") or d.get("point") or d.get("handicap") or d.get("line"))

        # capture selections/prices
        for sel in iter_selection_like(d):
            # selection name
            name = _norm(str(sel.get("name") or sel.get("participant") or sel.get("team") or sel.get("label") or ""))
            side = _norm_key(str(sel.get("side") or sel.get("designation") or ""))
            if not name and side in ("over", "under"):
                name = "Over" if side == "over" else "Under"

            # odds/price (decimal)
            odds = _to_float(
                sel.get("odds")
                or sel.get("price")
                or sel.get("decimalOdds")
                or sel.get("decimal_odds")
                or sel.get("value")
            )

            # spread/totals line can be inside selection, too
            sel_line = _to_float(sel.get("points") or sel.get("point") or sel.get("handicap") or sel.get("line"))
            final_line = sel_line if sel_line is not None else line

            if mt == "moneyline":
                # selection should match either team name
                if name and _norm_key(name) not in (_norm_key(away_n), _norm_key(home_n)):
                    # sometimes "Home"/"Away" are used
                    if _norm_key(name) == "home":
                        name = home_n
                    elif _norm_key(name) == "away":
                        name = away_n
                    else:
                        continue
                if not name:
                    continue
                rows.append(
                    OddsRow(
                        away_team=away_n,
                        home_team=home_n,
                        market_type="moneyline",
                        selection=name,
                        line=None,
                        odds=odds,
                        raw={"market": d, "selection": sel},
                    )
                )
            elif mt == "spread":
                if not name:
                    continue
                # selection should be home/away team (or "Home"/"Away")
                if _norm_key(name) == "home":
                    name = home_n
                elif _norm_key(name) == "away":
                    name = away_n
                if _norm_key(name) not in (_norm_key(away_n), _norm_key(home_n)):
                    continue
                if final_line is None:
                    continue
                rows.append(
                    OddsRow(
                        away_team=away_n,
                        home_team=home_n,
                        market_type="spread",
                        selection=name,
                        line=float(final_line),
                        odds=odds,
                        raw={"market": d, "selection": sel},
                    )
                )
            elif mt == "totals":
                # totals selection should be Over/Under (or sometimes includes text)
                if not name:
                    continue
                nkey = _norm_key(name)
                if "over" in nkey:
                    name = "Over"
                elif "under" in nkey:
                    name = "Under"
                if name not in ("Over", "Under"):
                    continue
                if final_line is None:
                    continue
                rows.append(
                    OddsRow(
                        away_team=away_n,
                        home_team=home_n,
                        market_type="totals",
                        selection=name,
                        line=float(final_line),
                        odds=odds,
                        raw={"market": d, "selection": sel},
                    )
                )
            elif mt == "player_prop":
                # Player props: selection is usually "Over" or "Under", but may include player name
                # The player name and prop type are typically in the market name/description
                if not name:
                    continue
                nkey = _norm_key(name)
                # Normalize to Over/Under
                if "over" in nkey:
                    name = "Over"
                elif "under" in nkey:
                    name = "Under"
                if name not in ("Over", "Under"):
                    continue
                if final_line is None:
                    continue
                # Store the full market info in raw for later extraction
                rows.append(
                    OddsRow(
                        away_team=away_n,
                        home_team=home_n,
                        market_type="player_prop",
                        selection=name,
                        line=float(final_line),
                        odds=odds,
                        raw={"market": d, "selection": sel},
                    )
                )

    # Dedup rows (keep first occurrence)
    seen = set()
    deduped: List[OddsRow] = []
    for r in rows:
        key = (r.market_type, _norm_key(r.selection), r.line, r.odds)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped


def _stealth_init_js() -> str:
    """
    Lightweight stealth tweaks (best-effort). Not a guaranteed bypass, but helpful.
    """
    return r"""
// webdriver flag
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
// languages
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
// plugins
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
// chrome runtime
window.chrome = window.chrome || { runtime: {} };

// Try to spoof UA-CH in headless (best-effort)
try {
  const uaData = {
    brands: [
      { brand: "Chromium", version: "121" },
      { brand: "Google Chrome", version: "121" },
      { brand: "Not:A-Brand", version: "24" },
    ],
    mobile: false,
    platform: "macOS",
    getHighEntropyValues: async (hints) => {
      const out = {
        architecture: "x86",
        bitness: "64",
        model: "",
        platform: "macOS",
        platformVersion: "10.15.7",
        uaFullVersion: "121.0.0.0",
        fullVersionList: [
          { brand: "Chromium", version: "121.0.0.0" },
          { brand: "Google Chrome", version: "121.0.0.0" },
          { brand: "Not:A-Brand", version: "24.0.0.0" },
        ],
      };
      return out;
    },
  };
  Object.defineProperty(navigator, "userAgentData", { get: () => uaData });
} catch (e) {}

// platform
try { Object.defineProperty(navigator, "platform", { get: () => "MacIntel" }); } catch (e) {}
"""


def _make_context(p: Playwright, *, headless: bool, user_agent: str) -> Tuple[Browser, BrowserContext]:
    browser = p.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )

    # Randomize viewport to reduce fingerprinting consistency
    vw = random.randint(1180, 1680)
    vh = random.randint(720, 1020)

    context = browser.new_context(
        user_agent=user_agent,
        viewport={"width": vw, "height": vh},
        locale="en-US",
        timezone_id="America/New_York",
        java_script_enabled=True,
    )
    context.add_init_script(_stealth_init_js())

    # Realistic headers (best-effort)
    context.set_extra_http_headers(
        {
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",
        }
    )
    return browser, context


def _try_accept_cookies(page: Page, *, timeout_ms: int = 5000) -> None:
    """
    Best-effort click on common cookie consent buttons.
    """
    patterns = [re.compile(r"^\s*accept\s*$", re.IGNORECASE), re.compile(r"accept all", re.IGNORECASE)]
    try:
        for pat in patterns:
            btn = page.get_by_role("button", name=pat)
            if btn.count() > 0:
                btn.first.click(timeout=timeout_ms)
                _sleep_human(0.3, 0.9)
                return
    except Exception:
        pass

    # Some banners use non-button elements; try a text-based locator.
    try:
        loc = page.locator("text=/^\\s*ACCEPT\\s*$/i")
        if loc.count() > 0:
            loc.first.click(timeout=timeout_ms)
            _sleep_human(0.3, 0.9)
            return
    except Exception:
        pass


def _scrape_via_api_interception(page: Page, *, url: str, timeout_ms: int) -> Tuple[Dict[str, Any], pd.DataFrame]:
    captured: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    debug_samples: List[Dict[str, Any]] = []
    debug_pinnacle_requests: List[Dict[str, Any]] = []
    debug_arcadia_responses: List[Dict[str, Any]] = []
    debug_request_failures: List[Dict[str, Any]] = []
    debug_refetch: List[Dict[str, Any]] = []

    def on_request(req) -> None:
        try:
            r_url = req.url
            host = ""
            try:
                host = urlparse(r_url).netloc.lower()
            except Exception:
                host = ""
            if not host.endswith("pinnacle.com"):
                return
            if len(debug_pinnacle_requests) < 60:
                hdrs = {}
                try:
                    hdrs = req.headers or {}
                except Exception:
                    hdrs = {}
                # Only keep a small, potentially relevant subset (avoid huge/noisy headers).
                keep = {}
                for k in ("authorization", "origin", "referer", "user-agent", "x-api-key", "x-authorization", "x-csrf-token"):
                    for kk, vv in hdrs.items():
                        if str(kk).strip().lower() == k:
                            if k in ("authorization", "x-authorization", "x-api-key", "x-csrf-token") and vv:
                                s = str(vv)
                                keep[k] = (s[:4] + "…") if len(s) > 6 else "…"
                            else:
                                keep[k] = vv
                # Get resource_type with try/except instead of hasattr
                resource_type_str = ""
                try:
                    resource_type_str = str(req.resource_type)
                except AttributeError:
                    resource_type_str = ""
                debug_pinnacle_requests.append(
                    {
                        "url": r_url,
                        "method": req.method,
                        "resource_type": resource_type_str,
                        "headers_subset": keep,
                    }
                )
        except Exception:
            return

    def on_request_failed(req) -> None:
        try:
            r_url = req.url
            host = ""
            try:
                host = urlparse(r_url).netloc.lower()
            except Exception:
                host = ""
            if not host.endswith("pinnacle.com"):
                return
            if len(debug_request_failures) >= 40:
                return
            failure = None
            try:
                failure = req.failure
            except Exception:
                failure = None
            err_text = ""
            try:
                if isinstance(failure, dict):
                    err_text = str(failure.get("errorText") or failure.get("error_text") or "")
                else:
                    err_text = str(failure or "")
            except Exception:
                err_text = ""
            # Get resource_type with try/except instead of getattr
            resource_type_str = ""
            try:
                resource_type_str = str(req.resource_type)
            except AttributeError:
                resource_type_str = ""
            debug_request_failures.append(
                {
                    "url": r_url,
                    "method": req.method,
                    "resource_type": resource_type_str,
                    "error": err_text,
                }
            )
        except Exception:
            return

    def on_response(resp) -> None:
        try:
            req = resp.request
            r_url = resp.url
            status = resp.status
            headers = resp.headers or {}
            rtype = ""
            try:
                rtype = str(req.resource_type)
            except AttributeError:
                rtype = ""

            # Track a small sample of XHR/fetch traffic for debugging (without dumping bodies).
            if rtype in ("xhr", "fetch") and len(debug_samples) < 30:
                debug_samples.append(
                    {
                        "url": r_url,
                        "status": status,
                        "resource_type": rtype,
                        "content_type": (headers.get("content-type") or headers.get("Content-Type") or ""),
                    }
                )
            if status >= 400:
                failures.append({"url": r_url, "status": status})
                # Still capture some details for Pinnacle API endpoints.
                host = ""
                try:
                    host = urlparse(r_url).netloc.lower()
                except Exception:
                    host = ""
                if host.endswith("arcadia.pinnacle.com") and len(debug_arcadia_responses) < 20:
                    debug_arcadia_responses.append(
                        {
                            "url": r_url,
                            "status": status,
                            "resource_type": rtype,
                            "content_type": (headers.get("content-type") or headers.get("Content-Type") or ""),
                        }
                    )
                return

            # Prefer JSON content-type, but Pinnacle (and CDNs) sometimes serve JSON as text/plain.
            # So: for XHR/fetch, we attempt JSON parse even when content-type isn't JSON.
            is_json_ct = _is_json_response(headers)
            should_try_parse = is_json_ct or (rtype in ("xhr", "fetch"))
            if not should_try_parse:
                return

            # Avoid parsing huge payloads if it's clearly not relevant.
            # (We don't have a reliable Content-Length here; keep a conservative cap.)
            text = ""
            text_err = None
            try:
                text = resp.text()
            except Exception as e:
                text_err = f"{e} ({type(e).__name__})"

            # Keep a small debug window for Pinnacle API responses regardless of parse success.
            host = ""
            try:
                host = urlparse(r_url).netloc.lower()
            except Exception:
                host = ""
            if host.endswith("arcadia.pinnacle.com") and len(debug_arcadia_responses) < 20:
                debug_arcadia_responses.append(
                    {
                        "url": r_url,
                        "status": status,
                        "resource_type": rtype,
                        "content_type": (headers.get("content-type") or headers.get("Content-Type") or ""),
                        "text_error": text_err,
                        "text_snippet": (text or "")[:300],
                    }
                )

            if not text:
                return
            if len(text) > 5_000_000:
                return

            payload = _safe_json(text)
            if payload is None:
                return

            if _looks_like_odds_payload(payload):
                captured.append(
                    {
                        "url": r_url,
                        "status": status,
                        "method": req.method,
                        "resource_type": rtype,
                        "ts_ms": _now_ms(),
                        "payload": payload,
                    }
                )
        except Exception:
            # swallow; this is a listener
            return

    # Use context-level listeners to reliably capture navigation + subresource requests.
    page.context.on("response", on_response)
    page.context.on("request", on_request)
    page.context.on("requestfailed", on_request_failed)

    # Navigate and wait for JS-rendered content to settle
    page.goto(url, wait_until="networkidle", timeout=timeout_ms)
    _sleep_human(0.8, 1.6)
    page.wait_for_load_state("networkidle", timeout=timeout_ms)
    _sleep_human(0.5, 1.2)

    # Cookie banner can block subsequent data loads; accept if present.
    _try_accept_cookies(page, timeout_ms=5000)
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        pass

    # If the UI can't load the matchup (geo/access restrictions are common),
    # skip waiting on the webapp and fall back to Arcadia API fetch by matchup id.
    try:
        body_text = page.inner_text("body") or ""
        if "matchup not found" in body_text.lower():
            # jump ahead to Arcadia fallback below
            captured.clear()
    except Exception:
        pass

    # Try interacting lightly to trigger additional market loads
    try:
        page.mouse.move(random.randint(20, 300), random.randint(20, 300))
        _sleep_human()
        page.mouse.wheel(0, random.randint(300, 900))
        _sleep_human(0.4, 1.0)
        page.mouse.wheel(0, random.randint(300, 900))
        _sleep_human(0.4, 1.0)
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        pass

    # Give any late fetches a moment to start (helps on slower runs / banner clicks).
    try:
        deadline = time.time() + 8.0
        while time.time() < deadline:
            has_arcadia = False
            for r in debug_pinnacle_requests:
                u = str(r.get("url") or "")
                try:
                    host = urlparse(u).netloc.lower()
                except Exception:
                    host = ""
                if host.endswith("arcadia.pinnacle.com"):
                    has_arcadia = True
                    break
            if has_arcadia:
                break
            _sleep_human(0.3, 0.7)
    except Exception:
        pass

    # If the site initiates API calls but the browser doesn't expose responses (CORS/service worker),
    # re-fetch the discovered API URLs via Playwright's APIRequestContext (no CORS restrictions).
    # This still respects the "use API interception first" constraint.
    try:
        # Wait for Arcadia API URLs to be discovered (they can arrive late).
        deadline = time.time() + 20.0
        arcadia_urls: List[str] = []
        arcadia_headers_by_url: dict[str, dict] = {}
        while time.time() < deadline and not arcadia_urls:
            arcadia_headers_by_url = {}
            tmp: List[str] = []
            for r in debug_pinnacle_requests:
                u = str(r.get("url") or "")
                try:
                    host = urlparse(u).netloc.lower()
                except Exception:
                    host = ""
                if host.endswith("arcadia.pinnacle.com"):
                    tmp.append(u)
                    arcadia_headers_by_url[u] = dict(r.get("headers_subset") or {})
            arcadia_urls = list(dict.fromkeys(tmp))
            if arcadia_urls:
                break
            _sleep_human(0.3, 0.8)

        if arcadia_urls:
            for u in arcadia_urls[:5]:
                try:
                    extra_headers = dict(arcadia_headers_by_url.get(u) or {})
                    # Ensure required headers are present.
                    extra_headers.setdefault("Accept", "application/json,text/plain,*/*")
                    extra_headers.setdefault("Referer", url)
                    resp = page.context.request.get(
                        u,
                        headers=extra_headers,
                        timeout=timeout_ms,
                    )
                    status = resp.status
                    text = ""
                    try:
                        text = resp.text()
                    except Exception:
                        text = ""
                    debug_refetch.append(
                        {"url": u, "status": status, "text_snippet": (text or "")[:300]}
                    )
                    if status == 200:
                        payload = _safe_json(text)
                        if payload is not None and _looks_like_odds_payload(payload):
                            captured.append(
                                {
                                    "url": u,
                                    "status": status,
                                    "method": "GET",
                                    "resource_type": "api_refetch",
                                    "ts_ms": _now_ms(),
                                    "payload": payload,
                                }
                            )
                except Exception as e:
                    debug_refetch.append({"url": u, "error": f"{e} ({type(e).__name__})"})
    except Exception:
        pass

    # Best-effort extraction: choose payload(s) with extractable teams + rows
    best_rows: List[OddsRow] = []
    best_teams: Tuple[Optional[str], Optional[str]] = (None, None)
    best_sources: List[str] = []

    for item in captured:
        payload = item.get("payload")
        away, home = _extract_teams_from_payload(payload)
        if not away or not home:
            continue
        rows = _try_extract_market_rows(payload, away=away, home=home)
        if len(rows) > len(best_rows):
            best_rows = rows
            best_teams = (away, home)
            best_sources = [item.get("url", "")]

    # If no single payload contains everything, merge rows across all payloads using first found teams
    if not best_rows:
        for item in captured:
            payload = item.get("payload")
            away, home = _extract_teams_from_payload(payload)
            if away and home:
                best_teams = (away, home)
                break

        away, home = best_teams
        if away and home:
            merged: List[OddsRow] = []
            sources: List[str] = []
            for item in captured:
                payload = item.get("payload")
                rows = _try_extract_market_rows(payload, away=away, home=home)
                if rows:
                    merged.extend(rows)
                    sources.append(item.get("url", ""))
            # dedup merged
            seen = set()
            out: List[OddsRow] = []
            for r in merged:
                k = (r.market_type, _norm_key(r.selection), r.line, r.odds)
                if k in seen:
                    continue
                seen.add(k)
                out.append(r)
            best_rows = out
            best_sources = sorted(set(sources))

    # Final fallback (API-based, no HTML scraping): directly call Arcadia endpoints by matchup id.
    if not best_rows:
        matchup_id = _extract_matchup_id_from_url(url)
        if matchup_id is not None:
            related_url = f"https://guest.api.arcadia.pinnacle.com/0.1/matchups/{matchup_id}/related"
            markets_url = (
                f"https://guest.api.arcadia.pinnacle.com/0.1/matchups/{matchup_id}/markets/related/straight"
            )
            related_payload = _arcadia_fetch_json(page, related_url, timeout_ms=timeout_ms)
            markets_payload = _arcadia_fetch_json(page, markets_url, timeout_ms=timeout_ms)
            away, home = _arcadia_extract_teams_from_related(related_payload)
            if away and home and markets_payload is not None:
                best_rows = _arcadia_markets_to_rows(markets_payload, away=away, home=home)
                best_teams = (away, home)
                best_sources = [related_url, markets_url]

    away, home = best_teams
    if not away or not home or not best_rows:
        # Include some page-level info to help debug blocking / empty renders
        page_title = ""
        page_url_final = ""
        page_text_snippet = ""
        try:
            page_title = page.title() or ""
        except Exception:
            pass
        try:
            page_url_final = page.url or ""
        except Exception:
            pass
        try:
            # Text-only snippet; avoids scraping odds HTML.
            t = page.inner_text("body")
            page_text_snippet = (t or "")[:800].strip()
        except Exception:
            pass

        # Signal failure to caller (fallback allowed)
        return (
            {
                "ok": False,
                "url": url,
                "error": "No usable JSON odds payloads captured",
                "captured_json_responses": len(captured),
                "debug_sample_xhr_fetch": debug_samples,
                "debug_pinnacle_requests": debug_pinnacle_requests,
                "debug_arcadia_responses": debug_arcadia_responses,
                "debug_request_failures": debug_request_failures,
                "debug_refetch": debug_refetch,
                "page_title": page_title,
                "page_url_final": page_url_final,
                "page_text_snippet": page_text_snippet,
                "failed_requests_sample": failures[:20],
            },
            pd.DataFrame([]),
        )

    data = {
        "ok": True,
        "url": url,
        "away_team": _norm(away),
        "home_team": _norm(home),
        "sources": best_sources,
        "markets": [r.to_dict() for r in best_rows],
        "captured_json_responses": len(captured),
        "debug_sample_xhr_fetch": debug_samples,
        "debug_pinnacle_requests": debug_pinnacle_requests,
        "debug_arcadia_responses": debug_arcadia_responses,
        "debug_request_failures": debug_request_failures,
        "debug_refetch": debug_refetch,
        "failed_requests_sample": failures[:20],
    }
    df = pd.DataFrame([r.to_dict() for r in best_rows])
    return data, df


def _fallback_dom_scrape(page: Page) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """
    Fallback: only use if API interception fails. Kept intentionally minimal and best-effort.
    """
    try:
        title = _norm(page.title() or "")
    except Exception:
        title = ""

    # Team names often appear in the title like "Brooklyn Nets vs New Orleans Pelicans"
    away = None
    home = None
    m = re.search(r"(.+?)\s+vs\s+(.+)", title, flags=re.IGNORECASE)
    if m:
        away = _norm(m.group(1))
        home = _norm(m.group(2))

    data = {
        "ok": False,
        "fallback": "dom",
        "away_team": away or "",
        "home_team": home or "",
        "markets": [],
    }
    return data, pd.DataFrame([])


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Scrape Pinnacle odds via JSON interception (Playwright sync).")
    parser.add_argument(
        "--url",
        default="",
        help="Optional: Pinnacle event URL for a single game (if omitted, prints all games for today, or tomorrow if --tomorrow is used).",
    )
    parser.add_argument("--timeout-ms", type=int, default=45000, help="Request timeout in milliseconds")
    parser.add_argument(
        "--matchups",
        action="store_true",
        help="Start from the basketball matchups page, extract all game links, then print odds for each.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of games processed in --matchups mode (0 = no limit).",
    )
    parser.add_argument(
        "--periods",
        default="0,1",
        help="Comma-separated periods to include when printing each game (default: 0,1 for Game + 1H).",
    )
    parser.add_argument(
        "--include-alternates",
        action="store_true",
        help="Include alternate lines (is_alternate=true). Default prints main lines only.",
    )
    parser.add_argument(
        "--with-ui",
        action="store_true",
        help="(Debug) Use browser UI navigation + interception. Default is API-only (no UI).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="(Only with --with-ui) Run headless Chromium",
    )
    parser.add_argument(
        "--tomorrow",
        action="store_true",
        help="Show games for tomorrow instead of today (default: today).",
    )
    args = parser.parse_args(argv)

    # Realistic UA (desktop Chrome)
    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )

    url = str(args.url or "").strip()
    timeout_ms = int(args.timeout_ms)

    # Default behavior: print odds for ALL basketball games for today (local timezone).
    # Use --tomorrow flag to show games for tomorrow instead.
    # We use the Arcadia matchups feed (no UI) and fall back to crawling the matchups page if needed.
    if not url and not bool(args.with_ui):
        now = datetime.now().astimezone()
        if args.tomorrow:
            target_date = (now + timedelta(days=1)).date()
            date_label = "tomorrow"
        else:
            target_date = now.date()
            date_label = "today"

        matchups = _list_basketball_matchups_for_local_date(
            local_date=target_date,
            timeout_s=max(1.0, float(timeout_ms) / 1000.0),
        )

        def _league_sort_key(name: str) -> tuple[int, str]:
            s = _norm(str(name or ""))
            u = s.upper()
            if u == "NBA" or u.startswith("NBA "):
                return (0, s.lower())
            if u == "NCAA" or u.startswith("NCAA "):
                return (1, s.lower())
            return (2, s.lower())

        # Order: NBA first, NCAA second, then other leagues alphabetically, then start time.
        try:
            matchups.sort(
                key=lambda m: (
                    _league_sort_key(_league_name_from_matchup_item(m)),
                    str(m.get("startTime") or ""),
                    int(m.get("id") or 0),
                )
            )
        except Exception:
            pass
        if not matchups:
            # Fallback to UI crawling if the API feed is blocked/unavailable.
            links = _extract_game_links_from_matchups_page(
                matchups_url=DEFAULT_MATCHUPS_URL, timeout_ms=timeout_ms, headless=True
            )
            if int(args.limit) > 0:
                links = links[: int(args.limit)]
            print(f"Found {len(links)} game links from matchups page.")
            failures = 0
            for i, game_url in enumerate(links, start=1):
                data, df = _scrape_arcadia_only(game_url, timeout_ms=timeout_ms)
                if not data.get("ok"):
                    failures += 1
                    print("\n" + "-" * 80)
                    print(f"[{i}/{len(links)}] FAILED: {game_url}")
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                    continue
                # Filter rows for printing
                if not df.empty:
                    df = df.copy()
                    if "period" in df.columns:
                        df = df[df["period"].isin(sorted({0, 1}))]
                    if not bool(args.include_alternates) and "is_alternate" in df.columns:
                        df = df[df["is_alternate"] == False]  # noqa: E712
                    sort_cols = [c for c in ["period", "market_type", "line", "selection"] if c in df.columns]
                    if sort_cols:
                        df = df.sort_values(sort_cols)

                print("\n" + "=" * 80)
                print(f"[{i}/{len(links)}] {data.get('away_team')} @ {data.get('home_team')}")
                print(f"url={game_url}")
                print("=" * 80)
                if df.empty:
                    print("(no odds rows after filtering)")
                else:
                    cols = [
                        "period_label",
                        "market_type",
                        "selection",
                        "line",
                        "odds",
                        "is_alternate",
                    ]
                    cols = [c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]
                    print(df[cols].to_string(index=False))
                _sleep_human(0.15, 0.45)

            print("\nDone.")
            if failures:
                print(f"Failures: {failures}/{len(links)}")
                return 2
            return 0

        # Parse desired periods
        periods: set[int] = set()
        for part in str(args.periods or "").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                periods.add(int(part))
            except Exception:
                continue
        if not periods:
            periods = {0, 1}
        include_alts = bool(args.include_alternates)

        if int(args.limit) > 0:
            matchups = matchups[: int(args.limit)]

        print(f"Found {len(matchups)} basketball matchups for {date_label} ({target_date.isoformat()} local).")
        failures = 0
        for i, m in enumerate(matchups, start=1):
            mid = m.get("id")
            away, home = _teams_from_matchup_item(m)
            league = _league_name_from_matchup_item(m)
            st_utc = _parse_iso_dt(m.get("startTime"))
            data, df = _scrape_arcadia_matchup_id(
                int(mid),
                away_team=away,
                home_team=home,
                league=league,
                start_time_utc=st_utc,
                timeout_ms=timeout_ms,
            )
            if not data.get("ok"):
                failures += 1
                print("\n" + "-" * 80)
                print(f"[{i}/{len(matchups)}] FAILED: matchup_id={mid} {away} @ {home}")
                print(json.dumps(data, indent=2, ensure_ascii=False))
                continue

            if not df.empty:
                df = df.copy()
                if "period" in df.columns:
                    df = df[df["period"].isin(sorted(periods))]
                if not include_alts and "is_alternate" in df.columns:
                    df = df[df["is_alternate"] == False]  # noqa: E712
                sort_cols = [c for c in ["period", "market_type", "line", "selection"] if c in df.columns]
                if sort_cols:
                    df = df.sort_values(sort_cols)

            print("\n" + "=" * 80)
            print(f"[{i}/{len(matchups)}] {data.get('away_team')} @ {data.get('home_team')}")
            print(f"matchup_id={data.get('matchup_id')}")
            if data.get("league"):
                print(f"league={data.get('league')}")
            if data.get("start_date_local") or data.get("start_time_local"):
                print(
                    f"start_local={data.get('start_date_local')} {data.get('start_time_local')}".strip()
                )
            if data.get("start_time_utc"):
                print(f"start_utc={data.get('start_time_utc')}")
            print("=" * 80)
            if df.empty:
                print("(no odds rows after filtering)")
            else:
                cols = [
                    "period_label",
                    "market_type",
                    "selection",
                    "line",
                    "odds",
                    "is_alternate",
                ]
                cols = [c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]
                print(df[cols].to_string(index=False))
            _sleep_human(0.12, 0.35)

        print("\nDone.")
        if failures:
            print(f"Failures: {failures}/{len(matchups)}")
            return 2
        return 0

    # If the user points at the matchups page (or sets --matchups), crawl links then fetch odds via Arcadia.
    if bool(args.matchups) or (url and _looks_like_matchups_page(url)):
        matchups_url = url if _looks_like_matchups_page(url) else DEFAULT_MATCHUPS_URL
        links = _extract_game_links_from_matchups_page(
            matchups_url=matchups_url, timeout_ms=timeout_ms, headless=True
        )
        if int(args.limit) > 0:
            links = links[: int(args.limit)]

        # Parse desired periods
        periods: set[int] = set()
        for part in str(args.periods or "").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                periods.add(int(part))
            except Exception:
                continue
        if not periods:
            periods = {0, 1}

        include_alts = bool(args.include_alternates)

        print(f"Found {len(links)} game links from matchups page.")
        failures = 0
        for i, game_url in enumerate(links, start=1):
            data, df = _scrape_arcadia_only(game_url, timeout_ms=timeout_ms)
            if not data.get("ok"):
                failures += 1
                print("\n" + "-" * 80)
                print(f"[{i}/{len(links)}] FAILED: {game_url}")
                print(json.dumps(data, indent=2, ensure_ascii=False))
                continue

            # Filter rows for printing
            if not df.empty:
                df = df.copy()
                if "period" in df.columns:
                    df = df[df["period"].isin(sorted(periods))]
                if not include_alts and "is_alternate" in df.columns:
                    df = df[df["is_alternate"] == False]  # noqa: E712
                # Stable ordering
                sort_cols = [c for c in ["period", "market_type", "line", "selection"] if c in df.columns]
                if sort_cols:
                    df = df.sort_values(sort_cols)

            print("\n" + "=" * 80)
            print(f"[{i}/{len(links)}] {data.get('away_team')} @ {data.get('home_team')}")
            print(f"url={game_url}")
            print("=" * 80)
            if df.empty:
                print("(no odds rows after filtering)")
            else:
                cols = [
                    "period_label",
                    "market_type",
                    "selection",
                    "line",
                    "odds",
                    "is_alternate",
                ]
                cols = [c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]
                print(df[cols].to_string(index=False))

            # Throttle a bit to avoid hammering
            _sleep_human(0.15, 0.45)

        print("\nDone.")
        if failures:
            print(f"Failures: {failures}/{len(links)}")
            return 2
        return 0

    if not bool(args.with_ui):
        # No UI: call Arcadia endpoints directly for a single game URL (explicit --url).
        if not url:
            print("Error: provide --url for single-game mode, or run with no args for today's games.")
            return 2
        data, df = _scrape_arcadia_only(url, timeout_ms=timeout_ms)
    else:
        with sync_playwright() as p:
            browser, context = _make_context(p, headless=bool(args.headless), user_agent=user_agent)
            page = context.new_page()

            # Human-ish pacing before navigation
            _sleep_human(0.2, 0.8)

            data, df = _scrape_via_api_interception(page, url=str(args.url), timeout_ms=int(args.timeout_ms))

            # If blocked / no usable JSON, optionally apply extra stealth-ish interactions + retry once
            if not data.get("ok"):
                try:
                    _sleep_human(1.0, 2.0)
                    page.reload(wait_until="networkidle", timeout=int(args.timeout_ms))
                    _sleep_human(0.6, 1.4)
                    data, df = _scrape_via_api_interception(
                        page, url=str(args.url), timeout_ms=int(args.timeout_ms)
                    )
                except Exception:
                    pass

            if not data.get("ok"):
                # API interception failed; fallback allowed by requirements.
                fb_data, fb_df = _fallback_dom_scrape(page)
                data = {**data, **fb_data}
                df = fb_df

            try:
                context.close()
                browser.close()
            except Exception:
                pass

    print(json.dumps(data, indent=2, ensure_ascii=False))
    if not df.empty:
        # stable column ordering
        cols = [
            "away_team",
            "home_team",
            "period_label",
            "market_type",
            "is_alternate",
            "selection",
            "line",
            "odds",
        ]
        cols = [c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]
        df = df[cols]
        print("\nDataFrame:")
        print(df.to_string(index=False))
    else:
        print("\nDataFrame: (empty)")

    return 0 if data.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())

# I love you
