#!/usr/bin/env python3
"""
Value bet discovery service.

A value bet exists when the expected payout exceeds the stake ($1).

We compute "true" (no-vig) probabilities from the sportsbook's two-way odds using
the Power de-vig method, then compare that to the Polymarket price.

If you buy 1 token at price `x` (Polymarket best ask), payout is $1 if it wins.
So the expected payout for a $1 stake (buying ~1/x tokens) is:

    expected_payout = p_true * (1 / x)

If expected_payout > 1, the bet has positive expected value.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import List, Optional

from pinnacle_scraper.sportsbook_odds import SportsbookOdds, HandicapOdds, TotalOdds
from polymarket_odds_service.polymarket_odds import PolymarketOdds

MarketOdds = PolymarketOdds.MarketOdds


@dataclass(frozen=True)
class ValueBet:
    team: str
    token_id: str
    true_prob: float
    polymarket_best_ask: float
    expected_payout_per_1: float  # expected payout for a $1 stake (gross, before fees)

    def to_string(self, decimals: int = 4) -> str:
        fmt = f".{max(0, int(decimals))}f"
        return (
            f"{self.team}: polymarket_ask={format(self.polymarket_best_ask, fmt)}, "
            f"true_prob={format(self.true_prob, fmt)}, "
            f"expected_payout_per_$1={format(self.expected_payout_per_1, fmt)}"
        )


class ValueBetService:
    MIN_TRUE_PROB = 0.05  # don't bet extreme longshots (<5% true win probability)
    MIN_EXPECTED_PAYOUT_PER_1 = 1.01  # require >1% expected edge on $1 stake

    def __init__(
        self,
        away_team: str,
        home_team: str,
        sportsbook_result: Optional[SportsbookOdds],
        verbose: bool = False,
    ) -> None:
        self.away_team = away_team
        self.home_team = home_team
        self.sportsbook_result = sportsbook_result
        self.verbose = verbose

    @staticmethod
    def _normalize_team_name(s: str) -> str:
        return " ".join(str(s).strip().lower().split())

    @staticmethod
    def _team_matches_outcome(team_name: str, outcome_team_name: str) -> bool:
        """
        Match team names (similar to _teams_match but for value bet service).
        
        Handles cases like:
        - Polymarket: "Heat" vs Sportsbook: "Miami Heat" (nickname matching)
        - Pinnacle: "Gonzaga" vs Polymarket: "Gonzaga Bulldogs" (NCAA school name matching)
        - Full name variations
        
        Returns True if they match, False otherwise.
        """
        full_key = ValueBetService._normalize_team_name(team_name)
        outcome_key = ValueBetService._normalize_team_name(outcome_team_name)
        if not full_key or not outcome_key:
            return False

        if full_key == outcome_key:
            return True

        # Check if one name starts with the other (important for NCAA: "Gonzaga" matches "Gonzaga Bulldogs")
        if full_key.startswith(outcome_key) or outcome_key.startswith(full_key):
            return True

        # Check if last word matches (nickname matching)
        # e.g., "Miami Heat" matches "Heat", "New York Knicks" matches "Knicks"
        words_full = full_key.split()
        words_outcome = outcome_key.split()
        if words_full and words_outcome and words_full[-1] == words_outcome[-1]:
            return True

        # Check if one contains the other
        if outcome_key in full_key or full_key in outcome_key:
            return True

        # Check if first word matches (for NCAA: school name is usually first word)
        # e.g., "Gonzaga" matches "Gonzaga Bulldogs"
        if words_full and words_outcome:
            if words_full[0] == words_outcome[0] and len(words_full[0]) > 3:
                return True

        # Check if all words from shorter name are in longer name
        # e.g., "Trail Blazers" matches "Portland Trail Blazers"
        if len(words_full) > 1 and len(words_outcome) > 1:
            shorter = words_outcome if len(words_outcome) < len(words_full) else words_full
            longer = words_full if len(words_full) > len(words_outcome) else words_outcome
            if all(word in longer for word in shorter if len(word) > 2):
                return True

        return False

    @staticmethod
    def _extract_outcome_team(market_label: str) -> Optional[str]:
        """
        Parse Polymarket market label.
        
        Handles both:
        - Full format: "Bulls vs. Pistons (Bulls)" -> "Bulls"
        - Simple format: "Heat" -> "Heat"
        """
        if not market_label:
            return None
        
        # First try to extract from parentheses
        if "(" in market_label and ")" in market_label:
            inside = market_label.split("(", 1)[1].rsplit(")", 1)[0].strip()
            if inside:
                return inside
        
        # If no parentheses, return the whole label (it's probably just the team name)
        return market_label.strip() or None

    @staticmethod
    def _devig(q1: float, q2: float) -> Optional[tuple[float, float]]:
        """
        De-vig for a 2-outcome market using the standard proportional method.

        Inputs q1,q2 are the raw implied probabilities (with vig), e.g. q=1/decimal_odds.
        We normalize them so they sum to 1:

            p1 = q1 / (q1 + q2)
            p2 = q2 / (q1 + q2)

        This matches the "normalize implied probabilities by total overround" method.
        """
        try:
            q1 = float(q1)
            q2 = float(q2)
        except (TypeError, ValueError):
            return None
        if not (math.isfinite(q1) and math.isfinite(q2)):
            return None
        if q1 <= 0 or q2 <= 0:
            return None
        total = q1 + q2
        if total <= 0:
            return None
        return (q1 / total), (q2 / total)

    def _true_prob_for_outcome(self, outcome_team: str) -> Optional[float]:
        """
        Return the no-vig (true) probability for this outcome (outcome_1/outcome_2), or None if unknown.
        """
        if self.sportsbook_result is None:
            return None

        sb = self.sportsbook_result
        devigged = self._devig(sb.outcome_1_cost_to_win_1, sb.outcome_2_cost_to_win_1)
        if devigged is None:
            return None
        p_outcome_1, p_outcome_2 = devigged

        if self._team_matches_outcome(self.away_team, outcome_team):
            return float(p_outcome_1)
        if self._team_matches_outcome(self.home_team, outcome_team):
            return float(p_outcome_2)
        return None

    def evaluate_single(self, m: MarketOdds) -> Optional[ValueBet]:
        """
        Evaluate a single MarketOdds against sportsbook odds.
        Returns a ValueBet if it's a value bet, None otherwise.
        """
        if self.verbose:
            print(f"\n    [EVAL] Evaluating outcome: {m.market}")
        
        if m.best_ask is None:
            if self.verbose:
                print(f"    [SKIP] No best_ask available")
            return None

        outcome_team = self._extract_outcome_team(m.market)
        if not outcome_team:
            if self.verbose:
                print(f"    [SKIP] Could not extract outcome team from market label")
            return None
        if self.verbose:
            print(f"    [EVAL] Extracted outcome team: '{outcome_team}'")

        # Show sportsbook odds for devigging
        if self.verbose and self.sportsbook_result:
            sb = self.sportsbook_result
            print(f"    [EVAL] Sportsbook odds: {sb.outcome_1}=${sb.outcome_1_cost_to_win_1:.3f}, {sb.outcome_2}=${sb.outcome_2_cost_to_win_1:.3f}")
            devigged = self._devig(sb.outcome_1_cost_to_win_1, sb.outcome_2_cost_to_win_1)
            if devigged:
                print(f"    [EVAL] Devigged probs: {sb.outcome_1}={devigged[0]*100:.2f}%, {sb.outcome_2}={devigged[1]*100:.2f}%")

        p_true = self._true_prob_for_outcome(outcome_team)
        if p_true is None:
            if self.verbose:
                print(f"    [SKIP] Could not match outcome team '{outcome_team}' to either '{self.away_team}' or '{self.home_team}'")
            return None
        if self.verbose:
            print(f"    [EVAL] True probability for '{outcome_team}': {p_true*100:.2f}%")
        
        if float(p_true) < self.MIN_TRUE_PROB:
            if self.verbose:
                print(f"    [SKIP] True prob {p_true*100:.2f}% below minimum {self.MIN_TRUE_PROB*100:.2f}%")
            return None

        polymarket_ask = float(m.best_ask)
        if polymarket_ask <= 0:
            if self.verbose:
                print(f"    [SKIP] Invalid polymarket ask: {polymarket_ask}")
            return None

        payout_per_1 = 1.0 / polymarket_ask  # $ payout if the $1 stake wins
        expected_payout = float(p_true) * float(payout_per_1)
        
        if self.verbose:
            print(f"    [CALC] Polymarket ask price: ${polymarket_ask:.4f} ({polymarket_ask*100:.2f}%)")
            print(f"    [CALC] Payout per $1 if win: ${payout_per_1:.4f}")
            print(f"    [CALC] Expected payout = true_prob * payout = {p_true:.4f} * {payout_per_1:.4f} = ${expected_payout:.4f}")
            print(f"    [CALC] Threshold: ${self.MIN_EXPECTED_PAYOUT_PER_1:.4f}")
            print(f"    [CALC] Edge: {(expected_payout - 1.0) * 100:.2f}%")

        # Value bet if expected payout exceeds threshold (stake is $1).
        if expected_payout > self.MIN_EXPECTED_PAYOUT_PER_1:
            if self.verbose:
                print(f"    [VALUE BET!] Expected payout ${expected_payout:.4f} > threshold ${self.MIN_EXPECTED_PAYOUT_PER_1:.4f}")
            return ValueBet(
                team=outcome_team,
                token_id=m.token_id,
                true_prob=float(p_true),
                polymarket_best_ask=polymarket_ask,
                expected_payout_per_1=expected_payout,
            )
        if self.verbose:
            print(f"    [NO VALUE] Expected payout ${expected_payout:.4f} <= threshold ${self.MIN_EXPECTED_PAYOUT_PER_1:.4f}")
        return None

    def discover_value_bets(self) -> List[ValueBet]:
        """
        Discover value bets between sportsbook (de-vigged) and Polymarket moneyline.
        """
        value_bets: List[ValueBet] = []

        for m in self.polymarket_results:
            bet = self.evaluate_single(m)
            if bet is not None:
                value_bets.append(bet)

        # Sort by highest expected profit per $1 stake.
        return sorted(
            value_bets, key=lambda vb: (vb.expected_payout_per_1 - 1.0), reverse=True
        )


@dataclass(frozen=True)
class SpreadValueBet:
    team: str
    point: float
    token_id: str
    true_prob: float
    polymarket_best_ask: float
    expected_payout_per_1: float

    def to_string(self, decimals: int = 4) -> str:
        fmt = f".{max(0, int(decimals))}f"
        return (
            f"{self.team} ({self.point:+g}): polymarket_ask={format(self.polymarket_best_ask, fmt)}, "
            f"true_prob={format(self.true_prob, fmt)}, "
            f"expected_payout_per_$1={format(self.expected_payout_per_1, fmt)}"
        )


class SpreadValueBetService:
    """
    Value bet discovery for spread markets.

    Matching rule:
      Polymarket outcome team+line must match sportsbook outcome team+line, e.g.
      "Bulls (+6.5)" == sportsbook Bulls +6.5
    """

    MIN_TRUE_PROB = ValueBetService.MIN_TRUE_PROB
    MIN_EXPECTED_PAYOUT_PER_1 = ValueBetService.MIN_EXPECTED_PAYOUT_PER_1

    def __init__(
        self,
        *,
        sportsbook_spreads: List[HandicapOdds],
        polymarket_spread_results: List[MarketOdds],
        polymarket_spread_side: Optional[str] = None,  # 'away' or 'home'
        polymarket_spread_line: Optional[float] = None,  # e.g., 2.5
        away_team: Optional[str] = None,
        home_team: Optional[str] = None,
        verbose: bool = False,
    ) -> None:
        self.sportsbook_spreads = sportsbook_spreads or []
        self.polymarket_spread_results = polymarket_spread_results or []
        self.polymarket_spread_side = polymarket_spread_side
        self.polymarket_spread_line = polymarket_spread_line
        self.away_team = away_team
        self.home_team = home_team
        self.verbose = verbose

    @staticmethod
    def _normalize_team_name(s: str) -> str:
        return " ".join(str(s).strip().lower().split())

    @staticmethod
    def _extract_outcome_label(market_label: str) -> Optional[str]:
        """
        Parse Polymarket market label like:
          "Spread: Thunder (-7.5) (Thunder)" -> "Thunder"

        IMPORTANT: spread questions themselves contain parentheses for the line, so we
        always take the *last* parenthesized segment as the outcome label.
        """
        if not market_label:
            return None
        start = market_label.rfind("(")
        end = market_label.rfind(")")
        if start == -1 or end == -1 or end <= start:
            return None
        inside = market_label[start + 1 : end].strip()
        return inside or None

    @staticmethod
    def _extract_question_text(market_label: str) -> str:
        """
        For a label "<question> (<outcome>)", return "<question>".
        This uses the *last* parenthesized segment as the outcome.
        """
        if not market_label:
            return ""
        start = market_label.rfind("(")
        if start == -1:
            return market_label.strip()
        return market_label[:start].strip()

    @staticmethod
    def _parse_spread_question(question: str) -> Optional[tuple[str, float]]:
        """
        Parse a Polymarket spread question into (reference_team, reference_line).

        Examples:
          "Spread: Thunder (-7.5)" -> ("Thunder", -7.5)
          "1H Spread: Thunder (-4.5)" -> ("Thunder", -4.5)
        """
        s = str(question or "").strip()
        if not s or "spread" not in s.lower():
            return None

        # team is whatever appears after "Spread:" up to the first "("
        m_team = re.search(r"spread:\s*(?P<team>.+?)\s*\(", s, flags=re.IGNORECASE)
        m_line = re.search(r"\(\s*(?P<pt>[+-]?\d+(?:\.\d+)?)\s*\)", s)
        if not m_team or not m_line:
            return None

        team = (m_team.group("team") or "").strip()
        pt_s = (m_line.group("pt") or "").strip()
        if not team or not pt_s:
            return None
        try:
            pt = float(pt_s)
        except ValueError:
            return None
        return team, float(pt)

    @classmethod
    def _pt_key(cls, pt: float) -> float:
        # Spread lines are typically in 0.5 increments; normalize float noise.
        try:
            return round(float(pt), 2)
        except Exception:
            return float(pt)

    def _build_true_prob_map(self) -> dict[tuple[str, float], float]:
        """
        Builds a mapping:
          (normalized_team, point) -> true_prob
        """
        out: dict[tuple[str, float], float] = {}

        def _keys_for_team(team: str) -> list[str]:
            """
            Generate multiple normalized keys for a team to improve matching.
            For NCAA: "Gonzaga Bulldogs" -> ["gonzaga bulldogs", "bulldogs", "gonzaga"]
            """
            norm = self._normalize_team_name(team)
            if not norm:
                return []
            words = norm.split()
            keys = [norm]  # Full normalized name
            
            # Add last word (nickname) - e.g., "Bulldogs"
            if len(words) > 1:
                last = words[-1]
                if last not in keys:
                    keys.append(last)
            
            # Add first word (for NCAA school names) - e.g., "Gonzaga"
            if len(words) > 1 and len(words[0]) > 3:
                first = words[0]
                if first not in keys:
                    keys.append(first)
            
            return keys

        for s in self.sportsbook_spreads:
            devigged = ValueBetService._devig(s.outcome_1_cost_to_win_1, s.outcome_2_cost_to_win_1)
            if devigged is None:
                continue
            p_outcome_1, p_outcome_2 = devigged
            # outcome_1 has the spread point, outcome_2 has the opposite
            for k in _keys_for_team(s.outcome_1):
                out[(k, self._pt_key(s.point))] = float(p_outcome_1)
            for k in _keys_for_team(s.outcome_2):
                out[(k, self._pt_key(-s.point))] = float(p_outcome_2)
        return out

    @dataclass(frozen=True)
    class SpreadOutcomeEvaluation:
        token_id: str
        team: str
        point: float
        matched_sportsbook_line: bool
        true_prob: Optional[float]
        polymarket_best_ask: Optional[float]
        expected_payout_per_1: Optional[float]
        is_value_bet: bool

    def discover_value_bets(self) -> List[SpreadValueBet]:
        if self.verbose:
            print(f"\n    [SPREAD EVAL] Starting spread value bet discovery")
            print(f"    [SPREAD EVAL] From slug: {self.polymarket_spread_side} team +{self.polymarket_spread_line}")
            print(f"    [SPREAD EVAL] Away team: {self.away_team}, Home team: {self.home_team}")
            print(f"    [SPREAD EVAL] Sportsbook spreads: {len(self.sportsbook_spreads)}")
        
        # Build true prob map
        true_prob_by_team_line = self._build_true_prob_map()
        if self.verbose:
            print(f"    [SPREAD EVAL] True prob map keys: {list(true_prob_by_team_line.keys())}")
            for i, s in enumerate(self.sportsbook_spreads, 1):
                devigged = ValueBetService._devig(s.outcome_1_cost_to_win_1, s.outcome_2_cost_to_win_1)
                if devigged:
                    print(f"      Line {i}: {s.outcome_1} ({s.point:+g}) @ ${s.outcome_1_cost_to_win_1:.3f} -> {devigged[0]*100:.2f}%")
                    print(f"              {s.outcome_2} ({-s.point:+g}) @ ${s.outcome_2_cost_to_win_1:.3f} -> {devigged[1]*100:.2f}%")
            print(f"    [SPREAD EVAL] Polymarket outcomes: {len(self.polymarket_spread_results)}")
            for m in self.polymarket_spread_results:
                print(f"      -> {m.market}: ask={m.best_ask}")
        
        value_bets: List[SpreadValueBet] = []
        
        # Determine the spread line and which team is favored
        if self.polymarket_spread_line is None:
            if self.verbose:
                print(f"    [SPREAD EVAL] No spread line available from slug, cannot evaluate")
            return []
        
        spread_line = self.polymarket_spread_line
        
        for m in self.polymarket_spread_results:
            if self.verbose:
                print(f"\n    [SPREAD EVAL] Processing outcome: {m.market}")
            if m.best_ask is None:
                if self.verbose:
                    print(f"      [SKIP] No best_ask")
                continue
            
            outcome_team = m.market.strip()
            if self.verbose:
                print(f"      Outcome team: '{outcome_team}'")
            
            is_away = ValueBetService._team_matches_outcome(self.away_team or "", outcome_team)
            is_home = ValueBetService._team_matches_outcome(self.home_team or "", outcome_team)
            if self.verbose:
                print(f"      Matches away ({self.away_team}): {is_away}")
                print(f"      Matches home ({self.home_team}): {is_home}")
            
            if not is_away and not is_home:
                if self.verbose:
                    print(f"      [SKIP] Could not match outcome to away or home team")
                continue
            
            # Determine the spread for this outcome
            if self.polymarket_spread_side == 'away':
                if is_away:
                    pt = -spread_line
                else:
                    pt = spread_line
            else:
                if is_home:
                    pt = -spread_line
                else:
                    pt = spread_line
            
            if self.verbose:
                print(f"      Outcome spread: {pt:+g}")
            
            # Try to find matching true probability
            outcome_norm = self._normalize_team_name(outcome_team)
            outcome_words = outcome_norm.split() if outcome_norm else []
            keys_to_try = [outcome_norm]
            if len(outcome_words) > 1:
                if outcome_words[-1] not in keys_to_try:
                    keys_to_try.append(outcome_words[-1])
                if len(outcome_words[0]) > 3 and outcome_words[0] not in keys_to_try:
                    keys_to_try.append(outcome_words[0])
            
            p_true = None
            matched = False
            for key_name in keys_to_try:
                key = (key_name, self._pt_key(pt))
                if self.verbose:
                    print(f"      Trying key: {key}")
                p_true = true_prob_by_team_line.get(key)
                if p_true is not None:
                    matched = True
                    if self.verbose:
                        print(f"      Matched! True prob: {p_true*100:.2f}%")
                    break
            
            if not matched:
                if self.verbose:
                    print(f"      [SKIP] No matching sportsbook line")
                continue
            
            if float(p_true) < self.MIN_TRUE_PROB:
                if self.verbose:
                    print(f"      [SKIP] True prob {p_true*100:.2f}% below min {self.MIN_TRUE_PROB*100:.2f}%")
                continue
            
            ask = float(m.best_ask)
            if ask <= 0:
                if self.verbose:
                    print(f"      [SKIP] Invalid ask: {ask}")
                continue
            
            payout_per_1 = 1.0 / ask
            expected = float(p_true) * payout_per_1
            if self.verbose:
                print(f"      Polymarket ask: ${ask:.4f} ({ask*100:.2f}%)")
                print(f"      Payout per $1 if win: ${payout_per_1:.4f}")
                print(f"      Expected payout: {p_true:.4f} * {payout_per_1:.4f} = ${expected:.4f}")
                print(f"      Edge: {(expected-1)*100:.2f}% (threshold: >{(self.MIN_EXPECTED_PAYOUT_PER_1-1)*100:.2f}%)")
            
            if expected > self.MIN_EXPECTED_PAYOUT_PER_1:
                if self.verbose:
                    print(f"      [VALUE BET!] ${expected:.4f} > ${self.MIN_EXPECTED_PAYOUT_PER_1:.4f}")
                value_bets.append(
                    SpreadValueBet(
                        team=str(outcome_team),
                        point=float(pt),
                        token_id=str(m.token_id),
                        true_prob=float(p_true),
                        polymarket_best_ask=float(ask),
                        expected_payout_per_1=float(expected),
                    )
                )
            else:
                if self.verbose:
                    print(f"      [NO VALUE] ${expected:.4f} <= ${self.MIN_EXPECTED_PAYOUT_PER_1:.4f}")

        if self.verbose:
            print(f"\n    [SPREAD EVAL] Total value bets found: {len(value_bets)}")
        return sorted(value_bets, key=lambda vb: (vb.expected_payout_per_1 - 1.0), reverse=True)

    def evaluate(self) -> dict[str, "SpreadValueBetService.SpreadOutcomeEvaluation"]:
        """
        Evaluate every Polymarket spread outcome:
        - whether it matches a sportsbook team+line
        - true probability (if matched)
        - expected payout per $1 at Polymarket ask (if available)
        - whether it clears the value-bet threshold
        """
        true_prob_by_team_line = self._build_true_prob_map()
        out: dict[str, SpreadValueBetService.SpreadOutcomeEvaluation] = {}

        for m in self.polymarket_spread_results:
            token_id = str(m.token_id)
            outcome_team = self._extract_outcome_label(m.market) or ""
            question = self._extract_question_text(m.market)
            parsed_q = self._parse_spread_question(question) if question else None
            if not outcome_team or not parsed_q:
                out[token_id] = SpreadValueBetService.SpreadOutcomeEvaluation(
                    token_id=token_id,
                    team=outcome_team or "Unknown",
                    point=0.0,
                    matched_sportsbook_line=False,
                    true_prob=None,
                    polymarket_best_ask=(float(m.best_ask) if m.best_ask is not None else None),
                    expected_payout_per_1=None,
                    is_value_bet=False,
                )
                continue

            ref_team, ref_line = parsed_q
            # If the outcome matches the reference team in the question, it gets ref_line;
            # otherwise it gets the opposite line.
            outcome_norm = self._normalize_team_name(outcome_team)
            ref_norm = self._normalize_team_name(ref_team)
            pt = float(ref_line) if (outcome_norm and ref_norm and (outcome_norm == ref_norm)) else -float(ref_line)

            # Try multiple keys for matching (full name, last word, first word for NCAA)
            outcome_words = outcome_norm.split() if outcome_norm else []
            keys_to_try = [outcome_norm]
            if len(outcome_words) > 1:
                if outcome_words[-1] not in keys_to_try:
                    keys_to_try.append(outcome_words[-1])  # Last word (nickname)
                if len(outcome_words[0]) > 3 and outcome_words[0] not in keys_to_try:
                    keys_to_try.append(outcome_words[0])  # First word (for NCAA)
            
            # Try each key until we find a match
            p_true = None
            matched = False
            for key_name in keys_to_try:
                key = (key_name, self._pt_key(pt))
                p_true = true_prob_by_team_line.get(key)
                if p_true is not None:
                    matched = True
                    break

            ask = float(m.best_ask) if m.best_ask is not None else None
            expected: Optional[float] = None
            is_value = False
            if matched and p_true is not None and ask is not None and ask > 0:
                expected = float(p_true) * (1.0 / float(ask))
                if float(p_true) >= self.MIN_TRUE_PROB and expected > self.MIN_EXPECTED_PAYOUT_PER_1:
                    is_value = True

            out[token_id] = SpreadValueBetService.SpreadOutcomeEvaluation(
                token_id=token_id,
                team=str(outcome_team),
                point=float(pt),
                matched_sportsbook_line=bool(matched),
                true_prob=(float(p_true) if p_true is not None else None),
                polymarket_best_ask=ask,
                expected_payout_per_1=expected,
                is_value_bet=bool(is_value),
            )

        return out


@dataclass(frozen=True)
class TotalsValueBet:
    side: str  # "Over" or "Under"
    total_point: float
    token_id: str
    true_prob: float
    polymarket_best_ask: float
    expected_payout_per_1: float

    def to_string(self, decimals: int = 4) -> str:
        fmt = f".{max(0, int(decimals))}f"
        return (
            f"{self.side} {self.total_point:g}: polymarket_ask={format(self.polymarket_best_ask, fmt)}, "
            f"true_prob={format(self.true_prob, fmt)}, "
            f"expected_payout_per_$1={format(self.expected_payout_per_1, fmt)}"
        )


class TotalsValueBetService:
    """
    Value bet discovery for totals (O/U) markets.

    Matching rule:
      Polymarket (Over/Under, line) must match sportsbook (Over/Under, same line).
    """

    MIN_TRUE_PROB = ValueBetService.MIN_TRUE_PROB
    MIN_EXPECTED_PAYOUT_PER_1 = ValueBetService.MIN_EXPECTED_PAYOUT_PER_1

    def __init__(
        self,
        *,
        sportsbook_totals: List[TotalOdds],
        polymarket_totals_results: List[MarketOdds],
        polymarket_line: Optional[float] = None,
        verbose: bool = False,
    ) -> None:
        self.sportsbook_totals = sportsbook_totals or []
        self.polymarket_totals_results = polymarket_totals_results or []
        self.polymarket_line = polymarket_line  # Total line parsed from slug (e.g., 228.5)
        self.verbose = verbose

    @staticmethod
    def _extract_outcome_label(market_label: str) -> Optional[str]:
        """
        Extract outcome label from market label.
        
        Handles:
        - Simple labels: "Over", "Under"
        - Parenthesized: "O/U 228.5 (Over)" -> "Over"
        """
        if not market_label:
            return None
        
        # First try to extract from parentheses
        start = market_label.rfind("(")
        end = market_label.rfind(")")
        if start != -1 and end != -1 and end > start:
            inside = market_label[start + 1 : end].strip()
            if inside:
                return inside
        
        # If no parentheses, return the whole label (it's probably just "Over" or "Under")
        return market_label.strip() or None

    @staticmethod
    def _extract_question_text(market_label: str) -> str:
        if not market_label:
            return ""
        start = market_label.rfind("(")
        if start == -1:
            return market_label.strip()
        return market_label[:start].strip()

    @staticmethod
    def _parse_total_line(question: str) -> Optional[float]:
        s = str(question or "")
        m = re.search(r"o/u\s*([0-9]+(?:\.[0-9]+)?)", s, flags=re.IGNORECASE)
        if not m:
            m = re.search(r"total\s*([0-9]+(?:\.[0-9]+)?)", s, flags=re.IGNORECASE)
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    @staticmethod
    def _pt_key(pt: float) -> float:
        return round(float(pt), 2)

    @staticmethod
    def _side_key(side: str) -> str:
        return (side or "").strip().lower()

    def _build_true_prob_map(self) -> dict[tuple[str, float], float]:
        out: dict[tuple[str, float], float] = {}
        for t in self.sportsbook_totals:
            # outcome_1 = Over, outcome_2 = Under
            devigged = ValueBetService._devig(t.outcome_1_cost_to_win_1, t.outcome_2_cost_to_win_1)
            if devigged is None:
                continue
            p_over, p_under = devigged
            key_pt = self._pt_key(t.point)
            out[(self._side_key("Over"), key_pt)] = float(p_over)
            out[(self._side_key("Under"), key_pt)] = float(p_under)
        return out

    def discover_value_bets(self) -> List[TotalsValueBet]:
        if self.verbose:
            print(f"\n    [TOTALS EVAL] Starting totals value bet discovery")
            print(f"    [TOTALS EVAL] Polymarket line from slug: {self.polymarket_line}")
            print(f"    [TOTALS EVAL] Sportsbook totals: {len(self.sportsbook_totals)}")
            for i, t in enumerate(self.sportsbook_totals, 1):
                devigged = ValueBetService._devig(t.outcome_1_cost_to_win_1, t.outcome_2_cost_to_win_1)
                if devigged:
                    print(f"      Line {i}: Over {t.point} @ ${t.outcome_1_cost_to_win_1:.3f} -> {devigged[0]*100:.2f}%")
                    print(f"              Under {t.point} @ ${t.outcome_2_cost_to_win_1:.3f} -> {devigged[1]*100:.2f}%")
            print(f"    [TOTALS EVAL] Polymarket outcomes: {len(self.polymarket_totals_results)}")
            for m in self.polymarket_totals_results:
                print(f"      -> {m.market}: ask={m.best_ask}")
        
        true_prob_by_side_line = self._build_true_prob_map()
        if self.verbose:
            print(f"    [TOTALS EVAL] True prob map keys: {list(true_prob_by_side_line.keys())}")
        value_bets: List[TotalsValueBet] = []

        # Use the polymarket_line from slug if available
        total_line = self.polymarket_line

        for m in self.polymarket_totals_results:
            if self.verbose:
                print(f"\n    [TOTALS EVAL] Processing: {m.market}")
            if m.best_ask is None:
                if self.verbose:
                    print(f"      [SKIP] No best_ask")
                continue

            # Extract side from market label (should be "Over" or "Under")
            outcome = self._extract_outcome_label(m.market)
            if not outcome:
                if self.verbose:
                    print(f"      [SKIP] Could not extract outcome label from '{m.market}'")
                continue
            side = outcome.strip()
            if self.verbose:
                print(f"      Extracted side: '{side}'")
            if self._side_key(side) not in ("over", "under"):
                if self.verbose:
                    print(f"      [SKIP] Side '{side}' is not over/under")
                continue

            # Use the total line from slug (already parsed)
            if self.verbose:
                print(f"      Using total line from slug: {total_line}")
            if total_line is None:
                # Fallback: try to parse from market label
                question = self._extract_question_text(m.market)
                total_line = self._parse_total_line(question)
                if self.verbose:
                    print(f"      Fallback - parsed from question '{question}': {total_line}")
            
            if total_line is None:
                if self.verbose:
                    print(f"      [SKIP] Could not determine total line")
                continue

            key = (self._side_key(side), self._pt_key(total_line))
            if self.verbose:
                print(f"      Looking up key: {key}")
            p_true = true_prob_by_side_line.get(key)
            if p_true is None:
                if self.verbose:
                    print(f"      [SKIP] No matching sportsbook line for {key}")
                continue
            if self.verbose:
                print(f"      Matched! True prob: {p_true*100:.2f}%")
            if float(p_true) < self.MIN_TRUE_PROB:
                if self.verbose:
                    print(f"      [SKIP] True prob {p_true*100:.2f}% below min {self.MIN_TRUE_PROB*100:.2f}%")
                continue

            ask = float(m.best_ask)
            if ask <= 0:
                if self.verbose:
                    print(f"      [SKIP] Invalid ask: {ask}")
                continue

            payout_per_1 = 1.0 / ask
            expected = float(p_true) * payout_per_1
            if self.verbose:
                print(f"      Polymarket ask: ${ask:.4f} ({ask*100:.2f}%)")
                print(f"      Payout per $1 if win: ${payout_per_1:.4f}")
                print(f"      Expected payout: {p_true:.4f} * {payout_per_1:.4f} = ${expected:.4f}")
                print(f"      Edge: {(expected-1)*100:.2f}% (threshold: >{(self.MIN_EXPECTED_PAYOUT_PER_1-1)*100:.2f}%)")
            
            if expected > self.MIN_EXPECTED_PAYOUT_PER_1:
                if self.verbose:
                    print(f"      [VALUE BET!] ${expected:.4f} > ${self.MIN_EXPECTED_PAYOUT_PER_1:.4f}")
                value_bets.append(
                    TotalsValueBet(
                        side=side,
                        total_point=float(total_line),
                        token_id=str(m.token_id),
                        true_prob=float(p_true),
                        polymarket_best_ask=float(ask),
                        expected_payout_per_1=float(expected),
                    )
                )
            else:
                if self.verbose:
                    print(f"      [NO VALUE] ${expected:.4f} <= ${self.MIN_EXPECTED_PAYOUT_PER_1:.4f}")

        if self.verbose:
            print(f"\n    [TOTALS EVAL] Total value bets found: {len(value_bets)}")
        return sorted(value_bets, key=lambda vb: (vb.expected_payout_per_1 - 1.0), reverse=True)


