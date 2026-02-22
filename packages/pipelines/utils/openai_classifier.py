#!/usr/bin/env python3
"""
Two-stage OpenAI classification pipeline for election markets.

Stage 1: Batch classification (50 items/call) - high recall
Stage 2: Single-item verification for positives - high precision
Stage 3: Tiebreaker for disagreements - majority vote
Stage 4: Web search fallback for unknown candidate parties
"""

import os
import json
import time
import re
from typing import Optional, Dict, List, Any
from openai import OpenAI

_client = None
_search_available = None

def get_client():
    global _client
    if _client is None:
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        _client = OpenAI(api_key=api_key)
    return _client


def is_search_available():
    """Check if web search is available and working."""
    global _search_available
    if _search_available is None:
        try:
            from ddgs import DDGS
            # Test with a simple query
            ddgs = DDGS()
            results = ddgs.text("test", max_results=1)
            _search_available = len(results) > 0
        except Exception:
            try:
                from duckduckgo_search import DDGS
                with DDGS() as ddgs:
                    results = list(ddgs.text("test", max_results=1))
                _search_available = len(results) > 0
            except Exception:
                _search_available = False
    return _search_available


def extract_candidate_name(question: str) -> Optional[str]:
    """Extract candidate name from a market question."""
    # Pattern: "Will [Name] win..."
    match = re.search(r'[Ww]ill\s+([A-Z][a-z]+(?:\s+[A-Z][a-z\']+)+)\s+(?:win|be)', question)
    if match:
        return match.group(1)

    # Pattern: "[Name] (R) vs [Name] (D)"
    match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z\']+)+)\s*\([RD]\)', question)
    if match:
        return match.group(1)

    return None


def extract_location_year(item: dict) -> tuple:
    """Extract location and year from item metadata."""
    location = item.get('location', '')
    year = item.get('election_year', '')
    if year:
        year = str(int(float(year))) if isinstance(year, (int, float)) else str(year)
    return location, year


def search_candidate_party(candidate_name: str, location: str = "", year: str = "",
                          show_progress: bool = True) -> Optional[str]:
    """
    Search for a candidate's party affiliation using OpenAI's web search.

    Returns: "Democrat", "Republican", or None
    """
    context = f" in {location}" if location else ""
    context += f" {year}" if year else ""

    query = f"What is {candidate_name}'s political party affiliation{context}? Is {candidate_name} a Democrat or Republican?"

    try:
        # Use OpenAI's web search via the responses API
        resp = get_client().responses.create(
            model="gpt-4o-mini",
            tools=[{"type": "web_search"}],
            input=query
        )

        # Extract the text response
        response_text = ""
        for item in resp.output:
            if hasattr(item, 'content'):
                for content in item.content:
                    if hasattr(content, 'text'):
                        response_text += content.text

        if response_text:
            # Parse the response for party
            response_lower = response_text.lower()

            # Look for clear party indicators
            democrat_indicators = ['democrat', 'democratic party', 'registered democrat', '(d)', 'dem.']
            republican_indicators = ['republican', 'gop', 'registered republican', '(r)', 'rep.']

            has_democrat = any(ind in response_lower for ind in democrat_indicators)
            has_republican = any(ind in response_lower for ind in republican_indicators)

            if has_democrat and not has_republican:
                if show_progress:
                    print(f"      Web search: {candidate_name} -> Democrat")
                return "Democrat"
            elif has_republican and not has_democrat:
                if show_progress:
                    print(f"      Web search: {candidate_name} -> Republican")
                return "Republican"

    except Exception as e:
        if show_progress:
            print(f"      Web search error for {candidate_name}: {e}")

    # Fallback: Ask GPT directly (uses training data knowledge)
    party = lookup_party_gpt(candidate_name, location, year)
    if party and show_progress:
        print(f"      GPT lookup: {candidate_name} -> {party}")
    return party


def lookup_party_gpt(candidate_name: str, location: str = "", year: str = "") -> Optional[str]:
    """
    Ask GPT to determine party affiliation from its training data.
    More aggressive than the initial classification - encourages GPT to make a determination.
    """
    context = f" running in {location}" if location else ""
    context += f" in {year}" if year else ""

    prompt = f"""What is the political party affiliation of {candidate_name}{context}?

This is for a U.S. election research project. Based on your knowledge:
- If they are a Democrat or ran as a Democrat, return "Democrat"
- If they are a Republican or ran as a Republican, return "Republican"
- If they are clearly independent/third party or you have no information, return null

Many local races (mayors, city council) are officially non-partisan but candidates often have known party affiliations from previous races, endorsements, or public statements.

Return JSON: {{"party": "Democrat"|"Republican"|null, "confidence": float, "reasoning": "brief explanation"}}"""

    try:
        resp = get_client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=200,
            response_format={"type": "json_object"}
        )
        result = json.loads(resp.choices[0].message.content)
        party = result.get("party")
        confidence = result.get("confidence", 0)
        # Only return if reasonably confident
        if party in ["Democrat", "Republican"] and confidence >= 0.6:
            return party
    except Exception:
        pass

    return None


def parse_party_from_search(candidate_name: str, search_text: str) -> Optional[str]:
    """Use GPT to extract party affiliation from search results."""
    prompt = f"""Based on these search results, what is {candidate_name}'s political party affiliation?

Search results:
{search_text[:2000]}

Return JSON: {{"party": "Democrat"|"Republican"|null, "confidence": float, "evidence": "brief quote"}}

Rules:
- Return "Democrat" for Democrats, Democratic Party members
- Return "Republican" for Republicans, GOP members
- Return null if unclear, independent, third party, or no evidence found
"""

    try:
        resp = get_client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=200,
            response_format={"type": "json_object"}
        )
        result = json.loads(resp.choices[0].message.content)
        party = result.get("party")
        if party in ["Democrat", "Republican"]:
            return party
    except:
        pass

    return None

STAGE1_PROMPT = """Classify each market. Return JSON with "results" array.

A "winner market" asks: Will X win? (binary win/lose outcome)

YES - winner market:
- "Will Trump win the 2024 presidential election?"
- "Will DeSantis win the Republican primary?"
- "Will Kari Lake win the Arizona Senate race?"
- "Which party will win the presidency?"
- "Smith (R) vs Jones (D)" - asking who wins

NO - not a winner market:
- "What will Trump's vote share be?" (percentage, not win/lose)
- "Will Trump win by more than 5%?" (margin)
- "Will Biden drop out?" (event, not winning)
- "How many electoral votes will Trump get?" (count)

For party: Who is the market asking about winning?
- "Republican" if asking about a Republican/GOP candidate winning
- "Democrat" if asking about a Democrat candidate winning
- null if neutral ("Which party wins?") or unclear

Be INCLUSIVE in Stage 1.

{"results": [{"index": 0, "is_winner_market": true, "party": "Republican", "confidence": 0.9}, ...]}"""

STAGE2_PROMPT = """VERIFICATION MODE - confirm if this is a winner market.

A winner market asks: Will X win? (binary win/lose outcome)
This includes primaries, nominations, general elections, any contest with a winner.

YES - winner market:
- "Will Trump win the 2024 presidential election?"
- "Will DeSantis win the Republican nomination?"
- "Will the Democrat win the Senate seat?"
- "Kari Lake (R) vs Ruben Gallego (D)"

NO - not a winner market:
- "What will be the vote share?" (percentage)
- "Will X win by more than Y%?" (margin)
- "Will X drop out?" (not about winning)
- "How many electoral votes?" (count)

For party: Who is being asked about winning?
- "Republican" if about Republican candidate
- "Democrat" if about Democrat candidate
- null if neutral or unclear

Return JSON: {"is_winner_market": bool, "party": "Republican"|"Democrat"|null, "confidence": float, "reasoning": "brief"}"""

STAGE3_PROMPT = """TIEBREAKER - is this a winner market?

A winner market = asks "Will X win?" with binary win/lose outcome.
Includes primaries, nominations, general elections.

NOT winner markets: vote share, margins, dropout questions, counts.

Return JSON: {"is_winner_market": bool, "party": "Republican"|"Democrat"|null, "confidence": float, "reasoning": "brief"}"""


def stage1_batch(items, question_key="question", batch_size=50, model="gpt-4o-mini", show_progress=True):
    """Stage 1: Batch classify for high recall."""
    results = []
    total = len(items)

    if show_progress:
        print(f"   Stage 1: {total} items (batch={batch_size})...")

    for start in range(0, total, batch_size):
        batch = items[start:start + batch_size]
        prompt = "Classify:\n" + "\n".join(f"{i}. {item.get(question_key, '[EMPTY]')}" for i, item in enumerate(batch))

        try:
            resp = get_client().chat.completions.create(
                model=model,
                messages=[{"role": "system", "content": STAGE1_PROMPT}, {"role": "user", "content": prompt}],
                temperature=0.0, max_tokens=2000, response_format={"type": "json_object"}
            )
            for r in json.loads(resp.choices[0].message.content).get("results", []):
                results.append({
                    "index": start + r.get("index", 0),
                    "is_winner_market": bool(r.get("is_winner_market", False)),
                    "party": r.get("party") if r.get("party") in ["Republican", "Democrat"] else None,
                    "confidence": float(r.get("confidence", 0.5)), "stage": 1
                })
        except Exception as e:
            for i in range(len(batch)):
                results.append({"index": start + i, "is_winner_market": False, "party": None, "confidence": 0, "stage": 1, "error": str(e)})

        if show_progress and (start + batch_size) % 200 == 0:
            print(f"      {min(start + batch_size, total)}/{total}...")

    results.sort(key=lambda x: x["index"])
    if show_progress:
        pos = sum(1 for r in results if r["is_winner_market"])
        print(f"   Stage 1 done: {pos} positives")
    return results


def stage2_verify(items, stage1_results, question_key="question", model="gpt-4o-mini", delay=0.05, show_progress=True):
    """Stage 2: Verify positives individually."""
    positive_idx = [r["index"] for r in stage1_results if r["is_winner_market"]]
    if show_progress:
        print(f"   Stage 2: Verifying {len(positive_idx)} positives...")

    results = []
    for i, idx in enumerate(positive_idx):
        q = items[idx].get(question_key, "")
        try:
            resp = get_client().chat.completions.create(
                model=model,
                messages=[{"role": "system", "content": STAGE2_PROMPT}, {"role": "user", "content": f'Verify: "{q}"'}],
                temperature=0.0, max_tokens=300, response_format={"type": "json_object"}
            )
            p = json.loads(resp.choices[0].message.content)
            results.append({"index": idx, "is_winner_market": bool(p.get("is_winner_market", False)),
                          "party": p.get("party") if p.get("party") in ["Republican", "Democrat"] else None,
                          "confidence": float(p.get("confidence", 0.5)), "stage": 2, "reasoning": p.get("reasoning", "")})
        except Exception as e:
            results.append({"index": idx, "is_winner_market": False, "party": None, "confidence": 0, "stage": 2, "error": str(e)})

        if delay > 0: time.sleep(delay)
        if show_progress and (i + 1) % 50 == 0:
            print(f"      {i + 1}/{len(positive_idx)}...")

    if show_progress:
        confirmed = sum(1 for r in results if r["is_winner_market"])
        print(f"   Stage 2 done: {confirmed} confirmed")
    return results


def stage3_tiebreak(item, question_key="question", model="gpt-4o-mini"):
    """Stage 3: Tiebreaker."""
    try:
        resp = get_client().chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": STAGE3_PROMPT}, {"role": "user", "content": f'Tiebreaker: "{item.get(question_key, "")}"'}],
            temperature=0.0, max_tokens=300, response_format={"type": "json_object"}
        )
        p = json.loads(resp.choices[0].message.content)
        return {"is_winner_market": bool(p.get("is_winner_market", False)),
                "party": p.get("party") if p.get("party") in ["Republican", "Democrat"] else None,
                "confidence": float(p.get("confidence", 0.5)), "stage": 3}
    except:
        return {"is_winner_market": False, "party": None, "confidence": 0, "stage": 3}


def run_pipeline(items, question_key="question", batch_size=50, model="gpt-4o-mini", delay=0.05,
                 show_progress=True, use_web_search=True):
    """Run full classification pipeline with optional web search for unknown parties."""
    if show_progress:
        print(f"\n{'='*50}\nCLASSIFYING {len(items)} MARKETS\n{'='*50}")

    s1 = stage1_batch(items, question_key, batch_size, model, show_progress)
    s1_lookup = {r["index"]: r for r in s1}

    s2 = stage2_verify(items, s1, question_key, model, delay, show_progress)
    s2_lookup = {r["index"]: r for r in s2}

    final = []
    disagreements = []

    for idx, item in enumerate(items):
        r1 = s1_lookup.get(idx, {})
        r2 = s2_lookup.get(idx)

        if not r1.get("is_winner_market", False):
            final.append({"index": idx, "is_winner_market": False, "party": None, "confidence": r1.get("confidence", 0),
                         "s1": False, "s2": None, "s3": None, "votes": 1})
        elif r2 is not None:
            if r1["is_winner_market"] == r2["is_winner_market"]:
                final.append({"index": idx, "is_winner_market": r2["is_winner_market"],
                             "party": r2.get("party") or r1.get("party"), "confidence": r2.get("confidence", 0.5),
                             "s1": True, "s2": r2["is_winner_market"], "s3": None, "votes": 2, "reasoning": r2.get("reasoning", "")})
            else:
                disagreements.append({"index": idx, "item": item, "s1": True, "s2": r2["is_winner_market"],
                                     "s1_party": r1.get("party"), "s2_party": r2.get("party")})

    if disagreements:
        if show_progress:
            print(f"   Stage 3: {len(disagreements)} tiebreakers...")
        for d in disagreements:
            s3 = stage3_tiebreak(d["item"], question_key, model)
            votes = [d["s1"], d["s2"], s3["is_winner_market"]]
            final_result = sum(votes) >= 2
            final.append({"index": d["index"], "is_winner_market": final_result,
                         "party": s3.get("party") or d.get("s2_party") or d.get("s1_party") if final_result else None,
                         "confidence": s3.get("confidence", 0.5), "s1": d["s1"], "s2": d["s2"], "s3": s3["is_winner_market"],
                         "votes": sum(votes) if final_result else 3 - sum(votes)})
            time.sleep(delay)

    final.sort(key=lambda x: x["index"])

    # Stage 4: Web search for winner markets with unknown party
    if use_web_search and is_search_available():
        unknown_party_idx = [r["index"] for r in final if r["is_winner_market"] and r["party"] is None]

        if unknown_party_idx:
            if show_progress:
                print(f"   Stage 4: Web search for {len(unknown_party_idx)} unknown parties...")

            final_lookup = {r["index"]: r for r in final}
            searched = 0

            for idx in unknown_party_idx:
                item = items[idx]
                question = item.get(question_key, "")

                # Extract candidate name from question
                candidate_name = extract_candidate_name(question)
                if not candidate_name:
                    continue

                # Get location/year from item metadata
                location, year = extract_location_year(item)

                # Search for party
                party = search_candidate_party(candidate_name, location, year, show_progress=show_progress)

                if party:
                    final_lookup[idx]["party"] = party
                    final_lookup[idx]["party_source"] = "web_search"
                    searched += 1

                time.sleep(delay)  # Rate limit

            final = list(final_lookup.values())
            final.sort(key=lambda x: x["index"])

            if show_progress:
                print(f"   Stage 4 done: {searched} parties found via web search")

    if show_progress:
        pos = sum(1 for r in final if r["is_winner_market"])
        with_party = sum(1 for r in final if r["is_winner_market"] and r["party"])
        print(f"\nFINAL: {pos} winner markets ({with_party} with party)\n{'='*50}")

    return final
