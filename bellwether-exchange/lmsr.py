"""
LMSR (Logarithmic Market Scoring Rule) by Robin Hanson.

Pure functions — no database calls, no side effects.
"""

import math


def lmsr_price_yes(q_yes: float, q_no: float, b: float) -> float:
    """Current probability of YES. Range: 0.0 to 1.0"""
    exp_yes = math.exp(q_yes / b)
    exp_no = math.exp(q_no / b)
    return exp_yes / (exp_yes + exp_no)


def lmsr_price_no(q_yes: float, q_no: float, b: float) -> float:
    """Current probability of NO. Always 1 - price_yes."""
    return 1.0 - lmsr_price_yes(q_yes, q_no, b)


def lmsr_cost(q_yes: float, q_no: float, b: float) -> float:
    """LMSR cost function C(q) = b * ln(exp(q_yes/b) + exp(q_no/b))"""
    return b * math.log(math.exp(q_yes / b) + math.exp(q_no / b))


def lmsr_cost_to_buy(side: str, shares: float, q_yes: float, q_no: float, b: float) -> float:
    """
    Cost to buy `shares` of `side` ("yes" or "no").
    Returns the dollar cost of the trade.
    """
    cost_before = lmsr_cost(q_yes, q_no, b)
    if side == "yes":
        cost_after = lmsr_cost(q_yes + shares, q_no, b)
    else:
        cost_after = lmsr_cost(q_yes, q_no + shares, b)
    return cost_after - cost_before


def lmsr_price_after_buy(side: str, shares: float, q_yes: float, q_no: float, b: float) -> float:
    """Price per share (average) for buying `shares` of `side`."""
    cost = lmsr_cost_to_buy(side, shares, q_yes, q_no, b)
    return cost / shares


def b_from_subsidy(subsidy: float) -> float:
    """Compute b parameter from subsidy amount."""
    return subsidy / math.log(2)
