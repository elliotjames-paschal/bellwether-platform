#!/usr/bin/env python3
"""
Phase 1B: Build decision tree from extracted fields.

Clusters synonyms into canonical labels and builds the hierarchical tree.
"""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Any, Set

MATCHER_DIR = Path(__file__).parent.parent
DATA_DIR = MATCHER_DIR / "data"

# ============================================================
# AGENT CLUSTERING RULES
# ============================================================

AGENT_CLUSTERS = {
    # People - Politicians
    "TRUMP": ["trump", "donald trump", "president trump", "trump administration", "president"],
    "BIDEN": ["biden", "joe biden", "president biden"],
    "OBAMA": ["obama", "barack obama"],
    "HARRIS": ["harris", "kamala harris"],
    "VANCE": ["vance", "jd vance", "j.d. vance"],
    "DESANTIS": ["desantis", "ron desantis"],
    "NEWSOM": ["newsom", "gavin newsom"],
    "AOC": ["aoc", "ocasio-cortez", "alexandria ocasio-cortez"],

    # People - Officials
    "POWELL": ["powell", "jerome powell", "fed chair powell", "chair powell"],
    "YELLEN": ["yellen", "janet yellen"],
    "HEGSETH": ["hegseth", "pete hegseth"],
    "RUBIO": ["rubio", "marco rubio"],
    "GABBARD": ["gabbard", "tulsi gabbard"],
    "PATEL": ["patel", "kash patel"],

    # People - Business/Tech
    "MUSK": ["elon musk", "musk", "elon", "doge"],
    "BEZOS": ["bezos", "jeff bezos"],
    "ZUCKERBERG": ["zuckerberg", "mark zuckerberg"],
    "ALTMAN": ["altman", "sam altman"],
    "SBF": ["sbf", "sam bankman-fried", "bankman-fried"],

    # People - Foreign Leaders
    "PUTIN": ["putin", "vladimir putin"],
    "ZELENSKY": ["zelensky", "zelenskyy", "volodymyr zelensky"],
    "NETANYAHU": ["netanyahu", "bibi", "benjamin netanyahu"],
    "XI": ["xi", "xi jinping"],
    "KIM": ["kim", "kim jong un", "kim jong-un"],
    "MADURO": ["maduro", "nicolás maduro", "nicolas maduro"],
    "POPE": ["pope", "pope leo", "pope francis"],

    # Institutions - US
    "FED": ["fed", "federal reserve", "the fed", "fomc", "federal reserve board"],
    "SCOTUS": ["supreme court", "scotus", "justices", "the court", "court"],
    "CONGRESS": ["congress", "us congress", "the congress"],
    "SENATE": ["senate", "us senate", "the senate"],
    "HOUSE": ["house", "us house", "house of representatives"],
    "USG": ["federal government", "us government", "government", "federal or state jurisdiction"],

    # Institutions - Foreign Central Banks
    "ECB": ["ecb", "european central bank"],
    "BOE": ["boe", "bank of england"],
    "BOJ": ["boj", "bank of japan"],

    # Parties
    "DEM": ["democratic", "democratic party", "democrats", "democrat", "dems"],
    "GOP": ["republican", "republican party", "republicans", "gop"],

    # Agencies
    "BLS": ["bls", "bureau of labor statistics", "source agencies"],
    "BEA": ["bea", "bureau of economic analysis"],
    "NBER": ["nber", "national bureau of economic research"],
    "DOJ": ["doj", "department of justice", "justice department"],
    "SEC": ["sec", "securities and exchange commission"],
    "FDA": ["fda", "food and drug administration"],
    "DEA": ["dea", "drug enforcement administration"],
    "CBP": ["cbp", "border patrol", "customs and border protection"],
    "ICE": ["ice", "immigration and customs enforcement"],

    # Countries
    "US": ["us", "u.s.", "united states", "usa", "america"],
    "RUSSIA": ["russia", "russian", "russia federation"],
    "CHINA": ["china", "chinese", "prc"],
    "UKRAINE": ["ukraine", "ukrainian"],
    "ISRAEL": ["israel", "israeli"],
    "IRAN": ["iran", "iranian"],
    "UK": ["uk", "united kingdom", "britain", "british"],
    "CANADA": ["canada", "canadian"],
    "MEXICO": ["mexico", "mexican"],
    "GAZA": ["gaza", "hamas", "palestinian"],

    # Companies
    "OPENAI": ["openai", "open ai"],
    "GOOGLE": ["google", "alphabet", "deepmind"],
    "TESLA": ["tesla"],
    "MICROSTRATEGY": ["microstrategy"],
    "META": ["meta", "facebook"],

    # Economic indicators (as agents)
    "GDP": ["gdp", "real gdp", "us gdp", "gdp growth"],
    "CPI": ["cpi", "inflation", "consumer price index"],
    "UNEMPLOYMENT": ["unemployment", "unemployment rate", "jobless rate"],
    "RECESSION": ["recession", "us recession", "economic recession"],
    "GOVT_SPENDING": ["government spending", "federal spending", "spending"],
    "DEBT": ["national debt", "us debt", "debt"],
    "DEFICIT": ["deficit", "budget deficit"],
    "STOCK_INDEX": ["nasdaq", "s&p", "dow", "nasdaq 100"],
}

# ============================================================
# ACTION CLUSTERING RULES
# ============================================================

ACTION_CLUSTERS = {
    "WIN": ["win", "wins", "won", "winning"],
    "CONTROL": ["control", "controls", "take control", "gain control"],
    "CUT": ["cut", "cuts", "lower", "reduce", "decrease"],
    "HIKE": ["hike", "hikes", "raise", "increase"],
    "HOLD": ["hold", "holds", "maintain", "keep", "unchanged"],
    "RESIGN": ["resign", "resigns", "step down", "steps down", "resignation"],
    "LEAVE": ["leave", "leaves", "depart", "departs", "out", "exit"],
    "APPOINT": ["appoint", "appoints", "nominate", "nominates", "name", "names"],
    "CONFIRM": ["confirm", "confirms", "confirmed", "confirmation"],
    "FIRE": ["fire", "fires", "fired", "dismiss", "remove"],
    "VISIT": ["visit", "visits", "travel to", "go to"],
    "MEET": ["meet", "meets", "meeting with", "summit"],
    "SAY": ["say", "says", "said", "announce", "announces", "state", "claim"],
    "ENDORSE": ["endorse", "endorses", "endorsed", "endorsement", "back", "support"],
    "PASS": ["pass", "passes", "passed", "enact", "sign into law"],
    "SIGN": ["sign", "signs", "signed"],
    "VETO": ["veto", "vetoes", "vetoed"],
    "BAN": ["ban", "bans", "banned", "prohibit"],
    "REPORT": ["report", "reports", "release", "publish"],
    "HIT": ["hit", "hits", "reach", "reaches", "achieve"],
    "EXCEED": ["exceed", "exceeds", "surpass", "above", "over"],
    "FALL": ["fall", "falls", "drop", "decline", "below"],
    "RUN": ["run", "runs", "running", "campaign", "seek", "announce candidacy"],
    "ARREST": ["arrest", "arrests", "arrested", "detain"],
    "INDICT": ["indict", "indicts", "indicted", "charge", "charges", "charged"],
    "CONVICT": ["convict", "convicts", "convicted", "guilty"],
    "PARDON": ["pardon", "pardons", "pardoned", "commute"],
    "DIE": ["die", "dies", "died", "death", "pass away"],
    "BE": ["be", "become", "is", "are"],
}

# ============================================================
# CLUSTERING FUNCTIONS
# ============================================================

def normalize(text: str) -> str:
    """Normalize text for matching."""
    if not text:
        return ""
    return text.lower().strip()


def cluster_value(value: str, clusters: Dict[str, List[str]]) -> str:
    """Map a value to its canonical cluster label."""
    norm = normalize(value)
    for label, variants in clusters.items():
        for variant in variants:
            if norm == variant or norm.startswith(variant + " ") or norm.endswith(" " + variant):
                return label
            # Also check if variant is contained in norm
            if len(variant) > 3 and variant in norm:
                return label
    return value.upper().replace(" ", "_")[:30]  # Fallback: uppercase the original


def cluster_agent(agent: str) -> str:
    """Cluster an agent value."""
    return cluster_value(agent, AGENT_CLUSTERS)


def cluster_action(action: str) -> str:
    """Cluster an action value."""
    return cluster_value(action, ACTION_CLUSTERS)


# ============================================================
# TREE BUILDING
# ============================================================

def build_tree(extractions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build the decision tree from extractions.

    Structure:
    {
        "AGENT_LABEL": {
            "aliases": ["original value 1", "original value 2"],
            "count": N,
            "actions": {
                "ACTION_LABEL": {
                    "aliases": [...],
                    "count": N,
                    "targets": {...}
                }
            }
        }
    }
    """
    tree = {}

    for ext in extractions:
        agent_raw = ext.get("agent", "N/A")
        action_raw = ext.get("action", "N/A")
        target_raw = ext.get("target", "N/A")
        mechanism_raw = ext.get("mechanism", "N/A")
        threshold_raw = ext.get("threshold", "any")
        timeframe_raw = ext.get("timeframe", "N/A")

        # Cluster
        agent = cluster_agent(agent_raw)
        action = cluster_action(action_raw)

        # Build tree structure
        if agent not in tree:
            tree[agent] = {"aliases": set(), "count": 0, "actions": {}}
        tree[agent]["aliases"].add(agent_raw)
        tree[agent]["count"] += 1

        if action not in tree[agent]["actions"]:
            tree[agent]["actions"][action] = {"aliases": set(), "count": 0, "targets": {}}
        tree[agent]["actions"][action]["aliases"].add(action_raw)
        tree[agent]["actions"][action]["count"] += 1

        # Add target
        target = target_raw.upper().replace(" ", "_")[:40]
        if target not in tree[agent]["actions"][action]["targets"]:
            tree[agent]["actions"][action]["targets"][target] = {"aliases": set(), "count": 0}
        tree[agent]["actions"][action]["targets"][target]["aliases"].add(target_raw)
        tree[agent]["actions"][action]["targets"][target]["count"] += 1

    # Convert sets to sorted lists for JSON serialization
    def convert_sets(obj):
        if isinstance(obj, dict):
            return {k: convert_sets(v) for k, v in obj.items()}
        elif isinstance(obj, set):
            return sorted(list(obj))
        else:
            return obj

    return convert_sets(tree)


def print_tree_summary(tree: Dict[str, Any]):
    """Print a summary of the tree."""
    print(f"Total agents: {len(tree)}")
    print()

    # Sort by count
    sorted_agents = sorted(tree.items(), key=lambda x: x[1]["count"], reverse=True)

    print("=== Top 20 Clustered Agents ===")
    for agent, data in sorted_agents[:20]:
        actions_summary = ", ".join(
            f"{a}({d['count']})"
            for a, d in sorted(data["actions"].items(), key=lambda x: x[1]["count"], reverse=True)[:3]
        )
        print(f"  {data['count']:4d}  {agent:20s}  → {actions_summary}")

    print()
    print("=== Sample Tree Paths ===")
    for agent, data in sorted_agents[:5]:
        for action, action_data in list(data["actions"].items())[:2]:
            for target, target_data in list(action_data["targets"].items())[:1]:
                print(f"  {agent} → {action} → {target} ({target_data['count']})")


def main():
    # Load extractions
    input_file = DATA_DIR / "extracted_fields.json"
    print(f"Loading extractions from {input_file}...")

    with open(input_file) as f:
        data = json.load(f)

    extractions = data["extractions"]
    print(f"Loaded {len(extractions)} extractions")
    print()

    # Build tree
    print("Building decision tree...")
    tree = build_tree(extractions)

    # Print summary
    print_tree_summary(tree)

    # Save tree
    output_file = DATA_DIR / "decision_tree.json"
    with open(output_file, "w") as f:
        json.dump(tree, f, indent=2)
    print(f"\nSaved decision tree to {output_file}")

    # Also save a flat clustering map for easy lookup
    clustering_map = {
        "agents": {},
        "actions": {}
    }

    for agent, data in tree.items():
        for alias in data["aliases"]:
            clustering_map["agents"][alias] = agent

    # Get all actions across all agents
    all_actions = {}
    for agent, data in tree.items():
        for action, action_data in data["actions"].items():
            if action not in all_actions:
                all_actions[action] = set()
            all_actions[action].update(action_data["aliases"])

    for action, aliases in all_actions.items():
        for alias in aliases:
            clustering_map["actions"][alias] = action

    map_file = DATA_DIR / "clustering_map.json"
    with open(map_file, "w") as f:
        json.dump(clustering_map, f, indent=2)
    print(f"Saved clustering map to {map_file}")


if __name__ == "__main__":
    main()
