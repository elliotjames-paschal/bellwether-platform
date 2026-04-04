"""
Cross-Platform Probability Standardization: GDP Growth Q1 2026
==============================================================

Fits parametric distributions to Kalshi (exceedance ladder) and Polymarket
(range bucket) contracts on the same underlying variable -- US real GDP growth
in Q1 2026 -- to recover a comparable implied probability distribution from
each platform.

Methodology
-----------
1. Kalshi trades P(X > T) for several thresholds -> survival function S(x).
   We use mid-prices ((bid+ask)/2) weighted by composite(inv_spread, volume).
2. Polymarket trades P(lower <= X < upper) for disjoint buckets -> PMF.
   Raw bucket prices (NOT normalized) weighted by composite(inv_spread, volume).
3. Timestamps aligned to common UTC dates before any cross-platform comparison.
4. Resolution rules extracted and compared from enriched market data.
5. We fit Normal, Skew-Normal, and 2-component Gaussian Mixture using WLS.
   Best model selected per platform via AICc.
6. Bootstrap CIs (1000 resamples) on fitted parameters and derived probabilities.
7. Hybrid CDF: empirical in the observed range, parametric in tails.
8. Kalman filter + exponential smoothing on the daily fitted parameter series.

Usage
-----
    python research/cross_platform_distribution_fit.py
"""

import json
import gzip
import re
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from scipy.optimize import minimize, differential_evolution
from scipy.stats import norm, skewnorm

warnings.filterwarnings("ignore", category=RuntimeWarning)

REPO = Path(__file__).resolve().parent.parent


# =============================================================================
# DATA LOADING
# =============================================================================

def load_kalshi_gdp_q1_2026():
    """Load Kalshi KXGDP-26APR30-T* with mid-price, volume, OI, spread, history."""
    price_file = REPO / "data" / "kalshi_all_political_prices_CORRECTED_v3.json"
    with open(price_file) as f:
        all_prices = json.load(f)

    pattern = re.compile(r"^KXGDP-26APR30-T([\d.]+)$")
    contracts = []

    for market_id, ts_data in all_prices.items():
        m = pattern.match(market_id)
        if not m or not isinstance(ts_data, list) or not ts_data:
            continue

        threshold = float(m.group(1))
        latest = ts_data[-1]
        price_data = latest.get("price", {})
        bid_data = latest.get("yes_bid", {})
        ask_data = latest.get("yes_ask", {})

        close = (price_data.get("close") or 0) / 100.0
        bid = (bid_data.get("close") or 0) / 100.0
        ask = (ask_data.get("close") or 0) / 100.0
        mid = (bid + ask) / 2.0 if (bid + ask) > 0 else close
        spread = max(ask - bid, 0.005)
        volume = latest.get("volume", 0)
        oi = latest.get("open_interest", 0)

        history = []
        for candle in ts_data:
            cd = candle.get("price", {})
            cb = candle.get("yes_bid", {})
            ca = candle.get("yes_ask", {})
            c_close = (cd.get("close") or 0) / 100.0
            c_bid = (cb.get("close") or 0) / 100.0
            c_ask = (ca.get("close") or 0) / 100.0
            c_mid = (c_bid + c_ask) / 2.0 if (c_bid + c_ask) > 0 else c_close
            history.append({
                "ts": candle.get("end_period_ts", 0),
                "date": datetime.fromtimestamp(candle.get("end_period_ts", 0),
                                               tz=timezone.utc).strftime("%Y-%m-%d"),
                "mid": c_mid,
                "close": c_close,
                "volume": candle.get("volume", 0),
                "oi": candle.get("open_interest", 0),
                "spread": max(c_ask - c_bid, 0.005),
            })

        contracts.append({
            "market_id": market_id,
            "threshold": threshold,
            "mid_price": mid,
            "close": close,
            "volume": volume,
            "oi": oi,
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "history": history,
        })

    contracts.sort(key=lambda c: c["threshold"])
    return contracts


def load_polymarket_gdp_q1_2026():
    """Load Polymarket US GDP Q1 2026 buckets with volume, spread, history."""
    enriched_path = REPO / "data" / "enriched_polymarket_markets.json.gz"
    with gzip.open(enriched_path, "rt") as f:
        pm_data = json.load(f)

    market_info = {}
    for mkt_wrapper in pm_data["markets"]:
        mkt = mkt_wrapper.get("api_data", {}).get("market", {})
        q = mkt.get("question", "")
        if "US GDP" not in q or "Q1 2026" not in q:
            continue
        tokens_raw = mkt.get("clobTokenIds", "")
        if isinstance(tokens_raw, str):
            tokens = json.loads(tokens_raw) if tokens_raw else []
        else:
            tokens = tokens_raw
        if not tokens:
            continue
        market_info[q] = {
            "token_id": tokens[0],
            "volume": float(mkt.get("volumeNum", 0) or 0),
            "liquidity": float(mkt.get("liquidityNum", mkt.get("liquidity", 0)) or 0),
            "spread": float(mkt.get("spread", 0.05) or 0.05),
            "bid": float(mkt.get("bestBid", 0) or 0),
            "ask": float(mkt.get("bestAsk", 0) or 0),
        }

    prices_path = REPO / "data" / "polymarket_all_political_prices_CORRECTED.json"
    with open(prices_path) as f:
        all_pm_prices = json.load(f)

    buckets = []
    for question, info in market_info.items():
        ts = all_pm_prices.get(info["token_id"], [])
        if not ts:
            continue

        latest = ts[-1]
        price = latest["p"] if isinstance(latest, dict) else latest

        history = []
        for pt in ts:
            if isinstance(pt, dict):
                history.append({
                    "ts": pt["t"],
                    "date": datetime.fromtimestamp(pt["t"],
                                                   tz=timezone.utc).strftime("%Y-%m-%d"),
                    "price": pt["p"],
                })
            else:
                history.append({"ts": 0, "date": "unknown", "price": pt})

        q_lower = question.lower()
        lower, upper = None, None
        if "less than" in q_lower:
            match = re.search(r"less than ([\d.]+)%", q_lower)
            if match:
                lower, upper = -10.0, float(match.group(1))
        elif "greater than" in q_lower or "at least" in q_lower:
            match = re.search(r"(?:greater than|at least) ([\d.]+)%", q_lower)
            if match:
                lower, upper = float(match.group(1)), 15.0
        elif "between" in q_lower:
            match = re.search(r"between ([\d.]+)% and ([\d.]+)%", q_lower)
            if match:
                lower, upper = float(match.group(1)), float(match.group(2))

        if lower is not None:
            buckets.append({
                "lower": lower, "upper": upper, "price": price,
                "volume": info["volume"], "liquidity": info["liquidity"],
                "spread": max(info["spread"], 0.005),
                "bid": info["bid"], "ask": info["ask"],
                "question": question, "token_id": info["token_id"],
                "history": history,
            })

    buckets.sort(key=lambda b: b["lower"])
    return buckets


# =============================================================================
# FIX 3: TIMESTAMP ALIGNMENT
# =============================================================================

def align_timestamps(kalshi_contracts, pm_buckets):
    """
    Align Kalshi and Polymarket to common UTC dates.

    Returns:
        kalshi_by_date: {date_str: {threshold: candle_dict}}
        pm_by_date: {date_str: {(lower,upper): price}}
        common_dates: sorted list of dates with data on BOTH platforms
        latest_common: most recent common date
    """
    # Kalshi: index by date -> threshold -> candle
    kalshi_by_date = {}
    for c in kalshi_contracts:
        for h in c["history"]:
            d = h["date"]
            if d not in kalshi_by_date:
                kalshi_by_date[d] = {}
            kalshi_by_date[d][c["threshold"]] = h

    # Polymarket: index by date -> (lower,upper) -> price
    # Take last observation per day per bucket
    pm_by_date = {}
    for b in pm_buckets:
        key = (b["lower"], b["upper"])
        for h in b["history"]:
            d = h["date"]
            if d not in pm_by_date:
                pm_by_date[d] = {}
            pm_by_date[d][key] = h["price"]

    k_dates = set(kalshi_by_date.keys())
    pm_dates = set(pm_by_date.keys())
    common = sorted(k_dates & pm_dates)

    latest_common = common[-1] if common else None

    return kalshi_by_date, pm_by_date, common, latest_common


def get_aligned_snapshot(kalshi_contracts, pm_buckets, kalshi_by_date,
                         pm_by_date, target_date):
    """
    Return contract/bucket lists with prices from a specific date.
    Falls back to latest available if a contract has no data on that date.
    """
    aligned_k = []
    for c in kalshi_contracts:
        candle = kalshi_by_date.get(target_date, {}).get(c["threshold"])
        if candle:
            aligned_k.append({**c, "mid_price": candle["mid"],
                              "spread": candle["spread"],
                              "volume": candle["volume"],
                              "oi": candle["oi"]})
        else:
            aligned_k.append(c)  # fallback to latest

    aligned_pm = []
    for b in pm_buckets:
        key = (b["lower"], b["upper"])
        price = pm_by_date.get(target_date, {}).get(key)
        if price is not None:
            aligned_pm.append({**b, "price": price})
        else:
            aligned_pm.append(b)  # fallback

    return aligned_k, aligned_pm


# =============================================================================
# FIX 2: RESOLUTION RULE EXTRACTION
# =============================================================================

def extract_resolution_rules():
    """
    Extract and compare resolution rules for GDP Q1 2026 from both platforms.

    Returns:
        dict with 'kalshi', 'polymarket', 'match' keys
    """
    rules = {"kalshi": [], "polymarket": [], "match": True, "differences": []}

    # Kalshi
    kalshi_path = REPO / "data" / "enriched_kalshi_markets.json.gz"
    if kalshi_path.exists():
        with gzip.open(kalshi_path, "rt") as f:
            k_data = json.load(f)
        markets = k_data.get("markets", k_data if isinstance(k_data, list) else [])
        for mkt_wrapper in markets:
            mkt = mkt_wrapper.get("api_data", {}).get("market", {})
            ticker = mkt.get("ticker", "")
            if "KXGDP-26APR30" in ticker:
                primary = mkt.get("rules_primary", "")
                if primary and primary not in [r["text"] for r in rules["kalshi"]]:
                    rules["kalshi"].append({
                        "ticker": ticker,
                        "text": primary[:300],
                    })
                break  # all contracts share the same rules template

    # Polymarket
    pm_path = REPO / "data" / "enriched_polymarket_markets.json.gz"
    if pm_path.exists():
        with gzip.open(pm_path, "rt") as f:
            pm_data = json.load(f)
        for mkt_wrapper in pm_data.get("markets", []):
            mkt = mkt_wrapper.get("api_data", {}).get("market", {})
            q = mkt.get("question", "")
            if "US GDP" in q and "Q1 2026" in q:
                desc = mkt.get("description", "")
                if desc and desc not in [r["full_text"] for r in rules["polymarket"]]:
                    rules["polymarket"].append({
                        "question": q[:80],
                        "text": desc[:300],
                        "full_text": desc,
                    })
                break

    # Compare key elements (use full text for PM if available)
    k_text = " ".join(r["text"].lower() for r in rules["kalshi"])
    p_text = " ".join(r.get("full_text", r["text"]).lower()
                       for r in rules["polymarket"])

    checks = [
        ("Source (BEA)", "bea" in k_text, "bea" in p_text),
        ("Estimate type (Advance)", "advance" in k_text, "advance" in p_text),
        ("GDP metric", "gdp" in k_text, "gdp" in p_text),
    ]
    for label, k_has, p_has in checks:
        if k_has != p_has:
            rules["match"] = False
            rules["differences"].append(
                f"{label}: Kalshi={'yes' if k_has else 'no'}, "
                f"PM={'yes' if p_has else 'no'}")

    return rules


# =============================================================================
# DISTRIBUTION FITTING (WEIGHTED LEAST SQUARES)
# =============================================================================

def _make_exceedance_weights(contracts, method="composite"):
    """Compute normalized weights for Kalshi exceedance contracts."""
    if method == "composite":
        inv_sp = np.array([1.0 / c["spread"] for c in contracts])
        vol = np.array([max(c["volume"], 1) for c in contracts], dtype=float)
        inv_sp /= inv_sp.max()
        vol /= vol.max()
        raw = np.sqrt(inv_sp * vol)
    elif method == "inv_spread":
        raw = np.array([1.0 / c["spread"] for c in contracts])
    elif method == "volume":
        raw = np.array([max(c["volume"], 1) for c in contracts], dtype=float)
    else:
        raw = np.ones(len(contracts))
    return raw / raw.sum()


def _make_bucket_weights(buckets, method="composite"):
    """Compute normalized weights for Polymarket buckets."""
    if method == "composite":
        inv_sp = np.array([1.0 / b["spread"] for b in buckets])
        vol = np.array([max(b["volume"], 1) for b in buckets], dtype=float)
        inv_sp /= inv_sp.max()
        vol /= vol.max()
        raw = np.sqrt(inv_sp * vol)
    elif method == "inv_spread":
        raw = np.array([1.0 / b["spread"] for b in buckets])
    elif method == "volume":
        raw = np.array([max(b["volume"], 1) for b in buckets], dtype=float)
    else:
        raw = np.ones(len(buckets))
    return raw / raw.sum()


def fit_normal_exceedance(thresholds, prices, weights):
    """Fit N(mu, sigma) to exceedance prices with WLS."""
    def loss(params):
        mu, sigma = params
        predicted = 1.0 - norm.cdf(thresholds, loc=mu, scale=sigma)
        return np.sum(weights * (prices - predicted) ** 2)
    result = minimize(loss, x0=[2.0, 1.5], bounds=[(-5, 10), (0.1, 10)])
    return {"type": "normal", "mu": result.x[0], "sigma": result.x[1],
            "loss": result.fun}


def fit_normal_buckets(lowers, uppers, prices, weights):
    """Fit N(mu, sigma) to bucket probabilities with WLS."""
    def loss(params):
        mu, sigma = params
        predicted = norm.cdf(uppers, loc=mu, scale=sigma) - \
                    norm.cdf(lowers, loc=mu, scale=sigma)
        return np.sum(weights * (prices - predicted) ** 2)
    result = minimize(loss, x0=[2.0, 1.5], bounds=[(-5, 10), (0.1, 10)])
    return {"type": "normal", "mu": result.x[0], "sigma": result.x[1],
            "loss": result.fun}


def fit_skewnorm_exceedance(thresholds, prices, weights):
    """Fit SkewNorm(a, loc, scale) to exceedance prices with WLS."""
    def loss(params):
        a, loc, scale = params
        predicted = 1.0 - skewnorm.cdf(thresholds, a, loc=loc, scale=scale)
        return np.sum(weights * (prices - predicted) ** 2)
    result = minimize(loss, x0=[0.0, 2.0, 1.5],
                      bounds=[(-10, 10), (-5, 10), (0.1, 10)])
    a, loc, scale = result.x
    return {
        "type": "skewnorm", "a": a, "loc": loc, "scale": scale,
        "mu": float(skewnorm.mean(a, loc=loc, scale=scale)),
        "sigma": float(skewnorm.std(a, loc=loc, scale=scale)),
        "loss": result.fun,
    }


def fit_skewnorm_buckets(lowers, uppers, prices, weights):
    """Fit SkewNorm(a, loc, scale) to bucket probabilities with WLS."""
    def loss(params):
        a, loc, scale = params
        predicted = skewnorm.cdf(uppers, a, loc=loc, scale=scale) - \
                    skewnorm.cdf(lowers, a, loc=loc, scale=scale)
        return np.sum(weights * (prices - predicted) ** 2)
    result = minimize(loss, x0=[0.0, 2.0, 1.5],
                      bounds=[(-10, 10), (-5, 10), (0.1, 10)])
    a, loc, scale = result.x
    return {
        "type": "skewnorm", "a": a, "loc": loc, "scale": scale,
        "mu": float(skewnorm.mean(a, loc=loc, scale=scale)),
        "sigma": float(skewnorm.std(a, loc=loc, scale=scale)),
        "loss": result.fun,
    }


def _gmm_cdf(x, w1, mu1, s1, mu2, s2):
    return w1 * norm.cdf(x, mu1, s1) + (1 - w1) * norm.cdf(x, mu2, s2)


def _gmm_survival(x, w1, mu1, s1, mu2, s2):
    return 1.0 - _gmm_cdf(x, w1, mu1, s1, mu2, s2)


def _gmm_moments(w1, mu1, s1, mu2, s2):
    mu = w1 * mu1 + (1 - w1) * mu2
    sigma = np.sqrt(w1 * (s1**2 + mu1**2) + (1 - w1) * (s2**2 + mu2**2) - mu**2)
    return mu, sigma


def fit_gmm_exceedance(thresholds, prices, weights):
    """Fit 2-component Gaussian mixture to exceedance prices with WLS."""
    def loss(params):
        w1, mu1, s1, mu2, s2 = params
        predicted = _gmm_survival(thresholds, w1, mu1, s1, mu2, s2)
        return np.sum(weights * (prices - predicted) ** 2)
    bounds = [(0.1, 0.9), (-3, 5), (0.2, 5), (-3, 5), (0.2, 5)]
    result = differential_evolution(loss, bounds, seed=42, maxiter=500, tol=1e-10)
    w1, mu1, s1, mu2, s2 = result.x
    mu, sigma = _gmm_moments(w1, mu1, s1, mu2, s2)
    return {"type": "gmm", "w1": w1, "mu1": mu1, "s1": s1, "mu2": mu2, "s2": s2,
            "mu": mu, "sigma": sigma, "loss": result.fun}


def fit_gmm_buckets(lowers, uppers, prices, weights):
    """Fit 2-component Gaussian mixture to bucket probabilities with WLS."""
    def loss(params):
        w1, mu1, s1, mu2, s2 = params
        predicted = _gmm_cdf(uppers, w1, mu1, s1, mu2, s2) - \
                    _gmm_cdf(lowers, w1, mu1, s1, mu2, s2)
        return np.sum(weights * (prices - predicted) ** 2)
    bounds = [(0.1, 0.9), (-3, 5), (0.2, 5), (-3, 5), (0.2, 5)]
    result = differential_evolution(loss, bounds, seed=42, maxiter=500, tol=1e-10)
    w1, mu1, s1, mu2, s2 = result.x
    mu, sigma = _gmm_moments(w1, mu1, s1, mu2, s2)
    return {"type": "gmm", "w1": w1, "mu1": mu1, "s1": s1, "mu2": mu2, "s2": s2,
            "mu": mu, "sigma": sigma, "loss": result.fun}


# =============================================================================
# MODEL EVALUATION & SELECTION
# =============================================================================

def survival_fn(fit, x):
    """Compute P(X > x) for any fitted model."""
    x = np.asarray(x, dtype=float)
    if fit["type"] == "normal":
        return 1.0 - norm.cdf(x, fit["mu"], fit["sigma"])
    elif fit["type"] == "skewnorm":
        return 1.0 - skewnorm.cdf(x, fit["a"], loc=fit["loc"], scale=fit["scale"])
    elif fit["type"] == "gmm":
        return _gmm_survival(x, fit["w1"], fit["mu1"], fit["s1"],
                             fit["mu2"], fit["s2"])
    raise ValueError(f"Unknown model type: {fit['type']}")


def cdf_fn(fit, x):
    return 1.0 - survival_fn(fit, x)


def bucket_prob(fit, lower, upper):
    return cdf_fn(fit, upper) - cdf_fn(fit, lower)


def compute_wrmse(fit, data, weights, mode="exceedance"):
    if mode == "exceedance":
        actual = np.array([c["mid_price"] for c in data])
        thresholds = np.array([c["threshold"] for c in data])
        predicted = survival_fn(fit, thresholds)
    else:
        actual = np.array([b["price"] for b in data])
        predicted = np.array([bucket_prob(fit, b["lower"], b["upper"])
                              for b in data])
    residuals = actual - predicted
    wsse = np.sum(weights * residuals ** 2)
    return np.sqrt(wsse / len(actual)), residuals


def select_best_model(fits, data, weights, mode="exceedance"):
    results = []
    for fit in fits:
        wrmse, residuals = compute_wrmse(fit, data, weights, mode)
        n = len(data)
        k = {"normal": 2, "skewnorm": 3, "gmm": 5}[fit["type"]]
        rss = np.sum(residuals ** 2)
        if n > k + 1:
            aic = n * np.log(rss / n + 1e-15) + 2 * k
            aicc = aic + (2 * k * (k + 1)) / (n - k - 1)
        else:
            aicc = float("inf")
        results.append({**fit, "wrmse": wrmse, "aicc": aicc,
                        "residuals": residuals, "k": k})
    results.sort(key=lambda r: r["aicc"])
    return results


# =============================================================================
# FIX 1: BOOTSTRAP CONFIDENCE INTERVALS
# =============================================================================

def bootstrap_fit(data, mode="exceedance", n_boot=1000, seed=42):
    """
    Bootstrap CIs by resampling contracts/buckets with replacement.

    Returns list of fit dicts (one per successful bootstrap iteration).
    """
    rng = np.random.default_rng(seed)
    n = len(data)
    boot_fits = []

    for _ in range(n_boot):
        indices = rng.choice(n, size=n, replace=True)
        sample = [data[i] for i in indices]

        # Require at least 3 unique values to avoid degenerate fits
        if mode == "exceedance":
            unique_vals = len(set(s["threshold"] for s in sample))
        else:
            unique_vals = len(set((s["lower"], s["upper"]) for s in sample))
        if unique_vals < 3:
            continue

        if mode == "exceedance":
            weights = _make_exceedance_weights(sample, method="composite")
            thresholds = np.array([c["threshold"] for c in sample])
            prices = np.array([c["mid_price"] for c in sample])
            fit = fit_normal_exceedance(thresholds, prices, weights)
        else:
            weights = _make_bucket_weights(sample, method="composite")
            lowers = np.array([b["lower"] for b in sample])
            uppers = np.array([b["upper"] for b in sample])
            prices = np.array([b["price"] for b in sample])
            fit = fit_normal_buckets(lowers, uppers, prices, weights)

        boot_fits.append(fit)

    return boot_fits


def bootstrap_ci(boot_fits, key="mu", alpha=0.05):
    """Extract percentile CI for a parameter from bootstrap fits."""
    values = np.array([f[key] for f in boot_fits])
    lo = np.percentile(values, 100 * alpha / 2)
    hi = np.percentile(values, 100 * (1 - alpha / 2))
    return {"mean": np.mean(values), "median": np.median(values),
            "ci_lo": lo, "ci_hi": hi, "std": np.std(values)}


def bootstrap_query(boot_fits, query_fn, alpha=0.05):
    """Compute CI for an arbitrary probability query across bootstrap fits."""
    values = np.array([query_fn(f) for f in boot_fits])
    lo = np.percentile(values, 100 * alpha / 2)
    hi = np.percentile(values, 100 * (1 - alpha / 2))
    return {"mean": np.mean(values), "ci_lo": lo, "ci_hi": hi}


# =============================================================================
# FIX 6: HYBRID NON-PARAMETRIC / PARAMETRIC CDF
# =============================================================================

def build_kalshi_empirical_cdf(contracts):
    """
    Build empirical CDF from Kalshi exceedance prices.
    CDF(T) = 1 - mid_price(T). Linear interpolation between thresholds.

    Returns callable and bounds.
    """
    thresholds = np.array([c["threshold"] for c in contracts])
    cdf_points = np.array([1.0 - c["mid_price"] for c in contracts])

    def interp_cdf(x):
        x = np.asarray(x, dtype=float)
        return np.interp(x, thresholds, cdf_points)

    return interp_cdf, float(thresholds.min()), float(thresholds.max())


def build_pm_empirical_cdf(buckets):
    """
    Build step-function CDF from Polymarket bucket probabilities.

    Returns callable and bounds.
    """
    boundaries = []
    cdf_steps = [0.0]
    cum = 0.0
    for b in buckets:
        cum += b["price"]
        if b["upper"] < 10:
            boundaries.append(b["upper"])
            cdf_steps.append(cum)

    boundaries = np.array(boundaries)
    cdf_steps = np.array(cdf_steps)

    def step_cdf(x):
        x = np.asarray(x, dtype=float)
        return np.interp(x, boundaries, cdf_steps[1:],
                         left=cdf_steps[0], right=cdf_steps[-1])

    lo = float(boundaries.min()) if len(boundaries) > 0 else 0.0
    hi = float(boundaries.max()) if len(boundaries) > 0 else 5.0
    return step_cdf, lo, hi


def build_hybrid_cdf(empirical_cdf, emp_lo, emp_hi, parametric_fit,
                     blend_width=0.5):
    """
    Hybrid CDF: empirical in [emp_lo, emp_hi], parametric in tails,
    sigmoid blending at boundaries.
    """
    def _sigmoid(x, center, width):
        return 1.0 / (1.0 + np.exp(-(x - center) / (width / 4)))

    def hybrid(x):
        x = np.asarray(x, dtype=float)
        emp = empirical_cdf(x)
        par = np.asarray(cdf_fn(parametric_fit, x), dtype=float)

        # Blend: below emp_lo -> parametric; above emp_hi -> parametric
        # In between -> empirical
        # Scale parametric tails to match empirical at boundaries
        emp_at_lo = float(empirical_cdf(np.array([emp_lo]))[0]
                          if hasattr(empirical_cdf(np.array([emp_lo])), '__len__')
                          else empirical_cdf(np.array([emp_lo])))
        par_at_lo = float(cdf_fn(parametric_fit, emp_lo))
        emp_at_hi = float(empirical_cdf(np.array([emp_hi]))[0]
                          if hasattr(empirical_cdf(np.array([emp_hi])), '__len__')
                          else empirical_cdf(np.array([emp_hi])))
        par_at_hi = float(cdf_fn(parametric_fit, emp_hi))

        # Lower tail: scale parametric so it matches empirical at emp_lo
        scale_lo = emp_at_lo / max(par_at_lo, 1e-10)
        par_lower = par * scale_lo

        # Upper tail: scale parametric survival to match at emp_hi
        emp_surv_hi = 1.0 - emp_at_hi
        par_surv_hi = 1.0 - par_at_hi
        scale_hi = emp_surv_hi / max(par_surv_hi, 1e-10)
        par_upper = 1.0 - (1.0 - par) * scale_hi

        # Blending weights
        w_lo = _sigmoid(x, emp_lo, blend_width)  # 0 below emp_lo, 1 above
        w_hi = 1.0 - _sigmoid(x, emp_hi, blend_width)  # 1 below emp_hi, 0 above

        # Combined: empirical in middle, parametric in tails
        result = (1 - w_lo) * par_lower + w_lo * w_hi * emp + (1 - w_hi) * par_upper
        return np.clip(result, 0.0, 1.0)

    return hybrid


# =============================================================================
# TIME SERIES FITTING
# =============================================================================

def fit_daily_timeseries(kalshi_contracts, pm_buckets, kalshi_by_date,
                         pm_by_date, common_dates):
    """
    Fit Normal distribution to each daily snapshot for both platforms.

    Returns:
        list of dicts: {date, k_mu, k_sigma, k_n, pm_mu, pm_sigma, pm_n}
    """
    daily = []

    for date in common_dates:
        row = {"date": date}

        # -- Kalshi --
        k_data = kalshi_by_date.get(date, {})
        if len(k_data) >= 3:
            thresholds = []
            mids = []
            spreads = []
            volumes = []
            for thresh in sorted(k_data.keys()):
                candle = k_data[thresh]
                if candle["mid"] > 0:
                    thresholds.append(thresh)
                    mids.append(candle["mid"])
                    spreads.append(candle["spread"])
                    volumes.append(candle["volume"])

            if len(thresholds) >= 3:
                t_arr = np.array(thresholds)
                m_arr = np.array(mids)
                # Composite weights
                inv_sp = 1.0 / np.array(spreads)
                vol = np.array(volumes, dtype=float)
                inv_sp /= max(inv_sp.max(), 1e-10)
                vol /= max(vol.max(), 1e-10)
                w = np.sqrt(inv_sp * vol)
                w /= w.sum()

                fit = fit_normal_exceedance(t_arr, m_arr, w)
                row["k_mu"] = fit["mu"]
                row["k_sigma"] = fit["sigma"]
                row["k_n"] = len(thresholds)

        # -- Polymarket --
        pm_data = pm_by_date.get(date, {})
        if len(pm_data) >= 3:
            # Reconstruct bucket list for this date
            date_buckets = []
            for b in pm_buckets:
                key = (b["lower"], b["upper"])
                if key in pm_data:
                    date_buckets.append({
                        "lower": b["lower"], "upper": b["upper"],
                        "price": pm_data[key],
                        "spread": b["spread"], "volume": b["volume"],
                    })

            if len(date_buckets) >= 3:
                lowers = np.array([db["lower"] for db in date_buckets])
                uppers = np.array([db["upper"] for db in date_buckets])
                prices = np.array([db["price"] for db in date_buckets])
                w = _make_bucket_weights(date_buckets, method="composite")
                fit = fit_normal_buckets(lowers, uppers, prices, w)
                row["pm_mu"] = fit["mu"]
                row["pm_sigma"] = fit["sigma"]
                row["pm_n"] = len(date_buckets)

        if "k_mu" in row or "pm_mu" in row:
            daily.append(row)

    return daily


# =============================================================================
# FIX 5: KALMAN FILTER + EXPONENTIAL SMOOTHING
# =============================================================================

def exponential_smooth(series, alpha=0.3):
    """Simple exponential smoothing (EMA)."""
    result = np.empty_like(series)
    result[0] = series[0]
    for i in range(1, len(series)):
        result[i] = alpha * series[i] + (1 - alpha) * result[i - 1]
    return result


def kalman_filter_1d(observations, process_var=None, obs_var=None):
    """
    Univariate Kalman filter with random-walk state model.

    Auto-calibrates noise variances if not provided.

    Returns:
        filtered: array of filtered state estimates
        filtered_var: array of filtered state variances (for CI)
    """
    n = len(observations)
    if n < 2:
        return observations.copy(), np.zeros(n)

    diffs = np.diff(observations)
    if process_var is None:
        process_var = float(np.median(diffs ** 2))
    if obs_var is None:
        obs_var = float(np.var(observations) * 0.3)

    # Ensure minimum noise to avoid division by zero
    process_var = max(process_var, 1e-8)
    obs_var = max(obs_var, 1e-8)

    filtered = np.empty(n)
    filtered_var = np.empty(n)

    # Initialize
    state = observations[0]
    state_var = obs_var

    for i in range(n):
        # Predict
        pred_state = state
        pred_var = state_var + process_var

        # Update
        kalman_gain = pred_var / (pred_var + obs_var)
        state = pred_state + kalman_gain * (observations[i] - pred_state)
        state_var = (1 - kalman_gain) * pred_var

        filtered[i] = state
        filtered_var[i] = state_var

    return filtered, filtered_var


# =============================================================================
# PRINT HELPERS
# =============================================================================

def print_resolution_rules(rules):
    print("\n-- Resolution Rules Comparison --")
    if rules["kalshi"]:
        print(f"  Kalshi:      {rules['kalshi'][0]['text'][:200]}")
    else:
        print("  Kalshi:      (no rules found)")
    if rules["polymarket"]:
        print(f"  Polymarket:  {rules['polymarket'][0]['text'][:200]}")
    else:
        print("  Polymarket:  (no rules found)")

    if rules["match"]:
        print("  >> MATCH: Both resolve on same criteria")
    else:
        print("  >> MISMATCH detected:")
        for d in rules["differences"]:
            print(f"     - {d}")


def print_alignment_info(common_dates, latest_common, k_dates, pm_dates):
    print("\n-- Timestamp Alignment --")
    print(f"  Kalshi dates:      {min(k_dates)} to {max(k_dates)} ({len(k_dates)} days)")
    print(f"  Polymarket dates:  {min(pm_dates)} to {max(pm_dates)} ({len(pm_dates)} days)")
    print(f"  Common dates:      {common_dates[0]} to {common_dates[-1]} ({len(common_dates)} days)")
    print(f"  Using snapshot:    {latest_common}")


def print_bootstrap_summary(label, boot_fits, best_fit):
    mu_ci = bootstrap_ci(boot_fits, "mu")
    sig_ci = bootstrap_ci(boot_fits, "sigma")
    print(f"  {label} ({len(boot_fits)} valid resamples):")
    print(f"    mu    = {best_fit['mu']:.3f}%  "
          f"95% CI [{mu_ci['ci_lo']:.3f}, {mu_ci['ci_hi']:.3f}]  "
          f"(SE={mu_ci['std']:.3f})")
    print(f"    sigma = {best_fit['sigma']:.3f}%  "
          f"95% CI [{sig_ci['ci_lo']:.3f}, {sig_ci['ci_hi']:.3f}]  "
          f"(SE={sig_ci['std']:.3f})")
    return mu_ci, sig_ci


def print_smoothed_timeseries(daily, field_prefix="k"):
    """Print raw + EMA + Kalman for mu and sigma."""
    mu_key = f"{field_prefix}_mu"
    sig_key = f"{field_prefix}_sigma"

    # Extract series (skip days with missing data)
    rows = [(d["date"], d[mu_key], d[sig_key])
            for d in daily if mu_key in d]
    if not rows:
        print(f"  No {field_prefix} data for smoothing.")
        return

    dates = [r[0] for r in rows]
    mus = np.array([r[1] for r in rows])
    sigs = np.array([r[2] for r in rows])

    if len(mus) < 3:
        print(f"  Insufficient data for smoothing ({len(mus)} points).")
        return

    ema_mu = exponential_smooth(mus, alpha=0.3)
    ema_sig = exponential_smooth(sigs, alpha=0.3)
    kal_mu, kal_mu_var = kalman_filter_1d(mus)
    kal_sig, kal_sig_var = kalman_filter_1d(sigs)

    label = "Kalshi" if field_prefix == "k" else "Polymarket"
    print(f"\n  {label} daily fits (raw / EMA / Kalman):")
    print(f"  {'Date':>12s}  {'raw_mu':>7s} {'ema_mu':>7s} {'kal_mu':>7s}"
          f"  {'raw_sig':>7s} {'ema_sig':>7s} {'kal_sig':>7s}")
    print(f"  {'-'*12}  {'-'*7} {'-'*7} {'-'*7}"
          f"  {'-'*7} {'-'*7} {'-'*7}")

    for i, d in enumerate(dates):
        print(f"  {d:>12s}  {mus[i]:7.3f} {ema_mu[i]:7.3f} {kal_mu[i]:7.3f}"
              f"  {sigs[i]:7.3f} {ema_sig[i]:7.3f} {kal_sig[i]:7.3f}")

    # Diagnostics
    mu_diffs = np.diff(mus)
    print(f"\n  Kalman diagnostics ({label} mu):")
    print(f"    Process var (auto): {np.median(mu_diffs**2):.4f}")
    print(f"    Obs var (auto):     {np.var(mus) * 0.3:.4f}")
    print(f"    Final Kalman var:   {kal_mu_var[-1]:.4f} "
          f"(+/- {1.96 * np.sqrt(kal_mu_var[-1]):.3f} 95% band)")

    return {
        "dates": dates, "raw_mu": mus, "ema_mu": ema_mu, "kal_mu": kal_mu,
        "kal_mu_var": kal_mu_var, "raw_sigma": sigs, "ema_sigma": ema_sig,
        "kal_sigma": kal_sig, "kal_sigma_var": kal_sig_var,
    }


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 72)
    print("  Cross-Platform Distribution Fit: US GDP Growth Q1 2026")
    print("  WLS + Bootstrap CIs + Hybrid CDF + Kalman Smoothing")
    print("=" * 72)

    # =========================================================================
    # 1. RESOLUTION RULES (Fix 2)
    # =========================================================================
    rules = extract_resolution_rules()
    print_resolution_rules(rules)

    # =========================================================================
    # 2. LOAD DATA
    # =========================================================================
    kalshi = load_kalshi_gdp_q1_2026()
    pm_buckets = load_polymarket_gdp_q1_2026()
    if not kalshi:
        print("ERROR: No Kalshi GDP Q1 2026 data found.")
        sys.exit(1)
    if not pm_buckets:
        print("ERROR: No Polymarket GDP Q1 2026 data found.")
        sys.exit(1)

    # =========================================================================
    # 3. TIMESTAMP ALIGNMENT (Fix 3)
    # =========================================================================
    k_by_date, pm_by_date, common_dates, latest_common = \
        align_timestamps(kalshi, pm_buckets)

    if not common_dates:
        print("ERROR: No common dates between platforms.")
        sys.exit(1)

    print_alignment_info(common_dates, latest_common,
                         sorted(k_by_date.keys()), sorted(pm_by_date.keys()))

    # Use aligned snapshot for cross-platform comparison
    kalshi_aligned, pm_aligned = get_aligned_snapshot(
        kalshi, pm_buckets, k_by_date, pm_by_date, latest_common)

    # =========================================================================
    # 4. RAW DATA TABLES (using aligned prices)
    # =========================================================================
    print(f"\n-- Kalshi Exceedance Contracts (mid-price, {latest_common}) --")
    print(f"  {'Contract':>12s}  {'Mid':>6s}  {'Bid':>5s}  {'Ask':>5s}"
          f"  {'Spread':>6s}  {'Volume':>7s}  {'OI':>7s}")
    print(f"  {'-'*12}  {'-'*6}  {'-'*5}  {'-'*5}  {'-'*6}  {'-'*7}  {'-'*7}")
    for c in kalshi_aligned:
        print(f"  GDP>{c['threshold']:4.1f}%  "
              f"  {c['mid_price']:.3f}  {c['bid']:.2f}  {c['ask']:.2f}  "
              f"{c['spread']:.3f}  {c['volume']:7d}  {c['oi']:7d}")

    print(f"\n-- Polymarket Bucket Contracts ({latest_common}) --")
    print(f"  {'Bucket':>20s}  {'Price':>6s}  {'Bid':>5s}  {'Ask':>5s}"
          f"  {'Spread':>6s}  {'Volume':>8s}")
    print(f"  {'-'*20}  {'-'*6}  {'-'*5}  {'-'*5}  {'-'*6}  {'-'*8}")
    for b in pm_aligned:
        lo = f"{b['lower']:5.1f}" if b["lower"] > -5 else " -inf"
        hi = f"{b['upper']:5.1f}" if b["upper"] < 10 else " +inf"
        label = f"[{lo},{hi})"
        print(f"  {label:>20s}  {b['price']:.3f}  {b['bid']:.2f}"
              f"  {b['ask']:.2f}  {b['spread']:.3f}  {b['volume']:8.0f}")

    # Fix 4: raw prices, no normalization
    pm_total = sum(b["price"] for b in pm_aligned)
    print(f"\n  Bucket sum = {pm_total:.3f} (raw, NOT normalized)")

    # =========================================================================
    # 5. WEIGHTS
    # =========================================================================
    k_weights = _make_exceedance_weights(kalshi_aligned, method="composite")
    pm_weights = _make_bucket_weights(pm_aligned, method="composite")

    print("\n-- Weights (composite: sqrt(inv_spread * volume)) --")
    print("  Kalshi:")
    for c, w in zip(kalshi_aligned, k_weights):
        bar = "#" * int(w * 80)
        print(f"    GDP>{c['threshold']:4.1f}%:  w={w:.3f}  {bar}")
    print("  Polymarket:")
    for b, w in zip(pm_aligned, pm_weights):
        lo = f"{b['lower']:5.1f}" if b["lower"] > -5 else " -inf"
        hi = f"{b['upper']:5.1f}" if b["upper"] < 10 else " +inf"
        bar = "#" * int(w * 80)
        print(f"    [{lo},{hi}):  w={w:.3f}  {bar}")

    # =========================================================================
    # 6. FIT MODELS (Fix 4: raw prices)
    # =========================================================================
    k_thresholds = np.array([c["threshold"] for c in kalshi_aligned])
    k_prices = np.array([c["mid_price"] for c in kalshi_aligned])
    pm_lowers = np.array([b["lower"] for b in pm_aligned])
    pm_uppers = np.array([b["upper"] for b in pm_aligned])
    pm_prices = np.array([b["price"] for b in pm_aligned])

    print("\n-- Fitting distributions (WLS, raw prices) --")
    k_fits = [
        fit_normal_exceedance(k_thresholds, k_prices, k_weights),
        fit_skewnorm_exceedance(k_thresholds, k_prices, k_weights),
        fit_gmm_exceedance(k_thresholds, k_prices, k_weights),
    ]
    pm_fits = [
        fit_normal_buckets(pm_lowers, pm_uppers, pm_prices, pm_weights),
        fit_skewnorm_buckets(pm_lowers, pm_uppers, pm_prices, pm_weights),
        fit_gmm_buckets(pm_lowers, pm_uppers, pm_prices, pm_weights),
    ]

    k_ranked = select_best_model(k_fits, kalshi_aligned, k_weights, "exceedance")
    pm_ranked = select_best_model(pm_fits, pm_aligned, pm_weights, "buckets")

    print(f"\n  Kalshi model ranking (by AICc):")
    print(f"    {'Model':>12s}  {'WRMSE':>8s}  {'AICc':>8s}  {'k':>3s}"
          f"  {'mu':>7s}  {'sigma':>7s}")
    for r in k_ranked:
        print(f"    {r['type']:>12s}  {r['wrmse']:.5f}  {r['aicc']:8.2f}"
              f"  {r['k']:3d}  {r['mu']:7.3f}  {r['sigma']:7.3f}")

    print(f"\n  Polymarket model ranking (by AICc):")
    print(f"    {'Model':>12s}  {'WRMSE':>8s}  {'AICc':>8s}  {'k':>3s}"
          f"  {'mu':>7s}  {'sigma':>7s}")
    for r in pm_ranked:
        print(f"    {r['type']:>12s}  {r['wrmse']:.5f}  {r['aicc']:8.2f}"
              f"  {r['k']:3d}  {r['mu']:7.3f}  {r['sigma']:7.3f}")

    best_k = k_ranked[0]
    best_pm = pm_ranked[0]
    print(f"\n  >> Selected: Kalshi={best_k['type']}, Polymarket={best_pm['type']}")

    # =========================================================================
    # 7. BOOTSTRAP CIs (Fix 1)
    # =========================================================================
    print("\n-- Bootstrap Confidence Intervals (n=1000) --")
    k_boot = bootstrap_fit(kalshi_aligned, mode="exceedance", n_boot=1000)
    pm_boot = bootstrap_fit(pm_aligned, mode="buckets", n_boot=1000)

    k_mu_ci, k_sig_ci = print_bootstrap_summary("Kalshi", k_boot, best_k)
    pm_mu_ci, pm_sig_ci = print_bootstrap_summary("Polymarket", pm_boot, best_pm)

    # Check if cross-platform gap is significant
    mu_gap = best_k["mu"] - best_pm["mu"]
    # Rough significance: do CIs overlap?
    ci_overlap = (k_mu_ci["ci_lo"] < pm_mu_ci["ci_hi"] and
                  pm_mu_ci["ci_lo"] < k_mu_ci["ci_hi"])
    print(f"\n  Cross-platform mu gap: {mu_gap:+.3f}pp")
    if ci_overlap:
        print("  >> CIs OVERLAP: gap is NOT statistically significant at 95%")
    else:
        print("  >> CIs DO NOT overlap: gap IS statistically significant at 95%")

    # =========================================================================
    # 8. RESIDUALS
    # =========================================================================
    print(f"\n-- Residuals (best model) --")
    print(f"  Kalshi ({best_k['type']}):")
    for c, r in zip(kalshi_aligned, best_k["residuals"]):
        print(f"    GDP>{c['threshold']:4.1f}%: actual={c['mid_price']:.3f}  "
              f"fitted={c['mid_price'] - r:.3f}  resid={r:+.4f}")

    print(f"  Polymarket ({best_pm['type']}):")
    for b, r in zip(pm_aligned, best_pm["residuals"]):
        lo = f"{b['lower']:5.1f}" if b["lower"] > -5 else " -inf"
        hi = f"{b['upper']:5.1f}" if b["upper"] < 10 else " +inf"
        print(f"    [{lo},{hi}): actual={b['price']:.3f}  "
              f"fitted={b['price'] - r:.3f}  resid={r:+.4f}")

    # =========================================================================
    # 9. STANDARDIZED COMPARISON WITH CIs
    # =========================================================================
    print(f"\n-- Standardized P(GDP > x) with 95% Bootstrap CIs --")
    print(f"  {'Thresh':>7s}  {'Kalshi':>8s} {'[95% CI]':>18s}  "
          f"{'PM':>8s} {'[95% CI]':>18s}  {'Diff':>7s}")
    print(f"  {'-'*7}  {'-'*8} {'-'*18}  {'-'*8} {'-'*18}  {'-'*7}")

    for x in np.arange(-1.0, 6.1, 0.5):
        pk = float(survival_fn(best_k, x))
        pp = float(survival_fn(best_pm, x))
        d = pk - pp

        k_q = bootstrap_query(k_boot, lambda f, _x=x: float(survival_fn(f, _x)))
        pm_q = bootstrap_query(pm_boot, lambda f, _x=x: float(survival_fn(f, _x)))

        flag = " <--" if abs(d) > 0.05 else ""
        print(f"  {x:7.1f}%  {pk:8.4f} [{k_q['ci_lo']:7.4f},{k_q['ci_hi']:7.4f}]"
              f"  {pp:8.4f} [{pm_q['ci_lo']:7.4f},{pm_q['ci_hi']:7.4f}]"
              f"  {d:+7.4f}{flag}")

    # =========================================================================
    # 10. PROBABILITY QUERIES WITH CIs
    # =========================================================================
    print(f"\n-- Precise Probability Queries with 95% CIs --")
    queries = [
        ("P(GDP > 0%)", lambda f: float(survival_fn(f, 0.0))),
        ("P(GDP > 1%)", lambda f: float(survival_fn(f, 1.0))),
        ("P(GDP > 2%)", lambda f: float(survival_fn(f, 2.0))),
        ("P(GDP > 3%)", lambda f: float(survival_fn(f, 3.0))),
        ("P(0%<GDP<1%)", lambda f: float(bucket_prob(f, 0.0, 1.0))),
        ("P(1%<GDP<2%)", lambda f: float(bucket_prob(f, 1.0, 2.0))),
        ("P(2%<GDP<3%)", lambda f: float(bucket_prob(f, 2.0, 3.0))),
        ("P(1.5%<GDP<2.5%)", lambda f: float(bucket_prob(f, 1.5, 2.5))),
        ("P(GDP < 0%)", lambda f: float(cdf_fn(f, 0.0))),
        ("P(GDP > 5%)", lambda f: float(survival_fn(f, 5.0))),
        ("E[GDP]", lambda f: f["mu"]),
    ]

    print(f"  {'Query':>20s}  {'Kalshi':>8s} {'[95% CI]':>18s}  "
          f"{'PM':>8s} {'[95% CI]':>18s}  {'Diff':>7s}")
    print(f"  {'-'*20}  {'-'*8} {'-'*18}  {'-'*8} {'-'*18}  {'-'*7}")

    for label, fn in queries:
        vk = fn(best_k)
        vp = fn(best_pm)
        d = vk - vp
        k_q = bootstrap_query(k_boot, fn)
        pm_q = bootstrap_query(pm_boot, fn)
        print(f"  {label:>20s}  {vk:8.4f} [{k_q['ci_lo']:7.4f},{k_q['ci_hi']:7.4f}]"
              f"  {vp:8.4f} [{pm_q['ci_lo']:7.4f},{pm_q['ci_hi']:7.4f}]"
              f"  {d:+7.4f}")

    # =========================================================================
    # 11. HYBRID NON-PARAMETRIC COMPARISON (Fix 6)
    # =========================================================================
    print(f"\n-- Hybrid CDF: Empirical (middle) + Parametric (tails) --")

    k_emp_cdf, k_lo, k_hi = build_kalshi_empirical_cdf(kalshi_aligned)
    pm_emp_cdf, pm_lo, pm_hi = build_pm_empirical_cdf(pm_aligned)

    k_hybrid = build_hybrid_cdf(k_emp_cdf, k_lo, k_hi, best_k)
    pm_hybrid = build_hybrid_cdf(pm_emp_cdf, pm_lo, pm_hi, best_pm)

    print(f"  Kalshi empirical range:      [{k_lo:.1f}%, {k_hi:.1f}%]")
    print(f"  Polymarket empirical range:  [{pm_lo:.1f}%, {pm_hi:.1f}%]")
    print(f"\n  {'Thresh':>7s}  {'K_param':>8s} {'K_hybrid':>8s} {'K_delta':>8s}"
          f"  {'PM_param':>8s} {'PM_hybrid':>8s} {'PM_delta':>8s}")
    print(f"  {'-'*7}  {'-'*8} {'-'*8} {'-'*8}  {'-'*8} {'-'*8} {'-'*8}")

    for x in np.arange(-1.0, 6.1, 0.5):
        kp = float(survival_fn(best_k, x))
        kh = float(1.0 - k_hybrid(np.array([x]))[0])
        pp = float(survival_fn(best_pm, x))
        ph = float(1.0 - pm_hybrid(np.array([x]))[0])
        print(f"  {x:7.1f}%  {kp:8.4f} {kh:8.4f} {kh-kp:+8.4f}"
              f"  {pp:8.4f} {ph:8.4f} {ph-pp:+8.4f}")

    # =========================================================================
    # 12. DIVERGENCE MEASURES
    # =========================================================================
    fine_grid = np.linspace(-5, 10, 2000)
    pdf_k = np.diff(np.concatenate([[0], cdf_fn(best_k, fine_grid)]))
    pdf_pm = np.diff(np.concatenate([[0], cdf_fn(best_pm, fine_grid)]))
    eps = 1e-15
    pdf_k = np.clip(pdf_k, eps, None)
    pdf_pm = np.clip(pdf_pm, eps, None)
    pdf_k /= pdf_k.sum()
    pdf_pm /= pdf_pm.sum()
    kl_fwd = float(np.sum(pdf_k * np.log(pdf_k / pdf_pm)))
    kl_rev = float(np.sum(pdf_pm * np.log(pdf_pm / pdf_k)))
    jsd = 0.5 * kl_fwd + 0.5 * kl_rev

    print(f"\n-- Divergence Measures --")
    print(f"  KL(Kalshi || PM)   = {kl_fwd:.6f} nats")
    print(f"  KL(PM || Kalshi)   = {kl_rev:.6f} nats")
    print(f"  Jensen-Shannon     = {jsd:.6f} nats")

    # =========================================================================
    # 13. DAILY TIME SERIES + SMOOTHING (Fix 5)
    # =========================================================================
    print(f"\n-- Daily Time Series (both platforms, common dates) --")
    daily = fit_daily_timeseries(kalshi, pm_buckets, k_by_date,
                                pm_by_date, common_dates)

    k_smooth = print_smoothed_timeseries(daily, field_prefix="k")
    pm_smooth = print_smoothed_timeseries(daily, field_prefix="pm")

    # =========================================================================
    # 14. SUMMARY
    # =========================================================================
    print("\n" + "=" * 72)
    print("  SUMMARY")
    print("=" * 72)
    print(f"""
  Data (aligned to {latest_common}):
    Kalshi:      {len(kalshi_aligned)} exceedance contracts, mid-prices, composite-weighted
    Polymarket:  {len(pm_aligned)} range buckets, RAW prices (sum={pm_total:.3f}), composite-weighted
    Common dates: {len(common_dates)} days ({common_dates[0]} to {common_dates[-1]})

  Resolution: Both resolve on BEA Advance Estimate {'(CONFIRMED)' if rules['match'] else '(MISMATCH!)'}

  Best-fit models (AICc):
    Kalshi ({best_k['type']:>8s}):  E[GDP] = {best_k['mu']:.2f}% [{k_mu_ci['ci_lo']:.2f}, {k_mu_ci['ci_hi']:.2f}]  SD = {best_k['sigma']:.2f}% [{k_sig_ci['ci_lo']:.2f}, {k_sig_ci['ci_hi']:.2f}]
    PM     ({best_pm['type']:>8s}):  E[GDP] = {best_pm['mu']:.2f}% [{pm_mu_ci['ci_lo']:.2f}, {pm_mu_ci['ci_hi']:.2f}]  SD = {best_pm['sigma']:.2f}% [{pm_sig_ci['ci_lo']:.2f}, {pm_sig_ci['ci_hi']:.2f}]

  Cross-platform:
    Mean gap:  {mu_gap:+.2f}pp ({'SIGNIFICANT' if not ci_overlap else 'NOT significant'} at 95%)
    JSD:       {jsd:.4f} nats""")

    if k_smooth:
        k_latest_kal = k_smooth["kal_mu"][-1]
        k_latest_var = k_smooth["kal_mu_var"][-1]
        print(f"""
  Kalman-filtered Kalshi E[GDP] (latest):
    {k_latest_kal:.2f}% +/- {1.96*np.sqrt(k_latest_var):.2f}% (95% band)""")

    print()


if __name__ == "__main__":
    main()
