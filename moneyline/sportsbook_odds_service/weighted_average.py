from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Optional, Sequence


# Edit this mapping whenever you want to tune weights.
# - Pinnacle is fixed at 50% by default.
# - Any present sportsbook not listed here will share the remaining weight equally.
SPORTSBOOK_WEIGHTS: Dict[str, float] = {
    "pinnacle": 0.50,
}


def _normalize_key(k: str) -> str:
    return (k or "").strip().lower()


def build_weights_for_present_books(
    present_book_keys: Sequence[str],
    *,
    weights_map: Mapping[str, float] = SPORTSBOOK_WEIGHTS,
    normalize: bool = True,
) -> Dict[str, float]:
    """
    Builds final weights for the given present sportsbook keys.

    Rules:
    - Uses explicit weights from `weights_map` when available (e.g. pinnacle=0.5).
    - Any present book not explicitly weighted shares the remaining weight equally.
    - If explicit weights sum to > 1, then:
      - if normalize=True: normalize explicit weights down to sum to 1 and assign 0 to unspecified.
      - else: keep raw weights and assign 0 to unspecified.
    """
    present = [_normalize_key(k) for k in present_book_keys if _normalize_key(k)]
    if not present:
        return {}

    explicit: Dict[str, float] = {}
    unspecified: list[str] = []

    for k in present:
        if k in weights_map:
            w = float(weights_map[k])
            explicit[k] = max(0.0, w)
        else:
            unspecified.append(k)

    explicit_sum = sum(explicit.values())

    if explicit_sum > 1.0:
        if normalize and explicit_sum > 0:
            explicit = {k: (w / explicit_sum) for k, w in explicit.items()}
        # unspecified books get 0 weight in this case
        return {k: explicit.get(k, 0.0) for k in present}

    remaining = 1.0 - explicit_sum
    weights: Dict[str, float] = {k: explicit.get(k, 0.0) for k in present}

    if unspecified and remaining > 0:
        per = remaining / float(len(unspecified))
        for k in unspecified:
            weights[k] = per

    if normalize:
        total = sum(weights.values())
        if total > 0:
            weights = {k: (w / total) for k, w in weights.items()}

    return weights


@dataclass(frozen=True)
class BookOutcome:
    team: str
    cost_to_win_1: float


@dataclass(frozen=True)
class BookLine:
    bookmaker_key: str
    outcomes: Iterable[BookOutcome]


def _normalize_team_name(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def weighted_average_cost_to_win_1(
    books: Sequence[BookLine],
    team: str,
    *,
    weights_by_key: Mapping[str, float],
) -> Optional[float]:
    """
    Weighted average of cost_to_win_1 across books for a given team.
    Books with missing team outcomes or 0 weight are ignored.
    """
    t_norm = _normalize_team_name(team)
    total_w = 0.0
    total = 0.0

    for b in books:
        w = float(weights_by_key.get(_normalize_key(b.bookmaker_key), 0.0))
        if w <= 0:
            continue

        val: Optional[float] = None
        for o in b.outcomes:
            if _normalize_team_name(o.team) == t_norm:
                val = float(o.cost_to_win_1)
                break
        if val is None:
            continue

        total += w * val
        total_w += w

    if total_w == 0.0:
        return None
    return total / total_w


