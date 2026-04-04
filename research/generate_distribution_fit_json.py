"""
Generate distribution_fit.json for the Bellwether research dashboard.

Runs the cross-platform distribution fit pipeline and exports a single JSON
file consumable by the frontend (Plotly charts in research.html).

Usage:
    PYTHONIOENCODING=utf-8 python research/generate_distribution_fit_json.py
"""

import json
import sys
from pathlib import Path

import numpy as np

# Add repo root so we can import the fit module
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "research"))

from cross_platform_distribution_fit import (
    load_kalshi_gdp_q1_2026,
    load_polymarket_gdp_q1_2026,
    align_timestamps,
    get_aligned_snapshot,
    extract_resolution_rules,
    fit_normal_exceedance,
    fit_normal_buckets,
    fit_skewnorm_exceedance,
    fit_skewnorm_buckets,
    fit_gmm_exceedance,
    fit_gmm_buckets,
    select_best_model,
    bootstrap_fit,
    bootstrap_ci,
    bootstrap_query,
    survival_fn,
    cdf_fn,
    bucket_prob,
    build_kalshi_empirical_cdf,
    build_pm_empirical_cdf,
    build_hybrid_cdf,
    fit_daily_timeseries,
    exponential_smooth,
    kalman_filter_1d,
    _make_exceedance_weights,
    _make_bucket_weights,
)

OUTPUT = REPO / "packages" / "docs" / "data" / "distribution_fit.json"


def _f(x):
    """Round a float for JSON output."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return round(float(x), 5)


def main():
    print("Loading data...")
    rules = extract_resolution_rules()
    kalshi = load_kalshi_gdp_q1_2026()
    pm_buckets = load_polymarket_gdp_q1_2026()

    if not kalshi or not pm_buckets:
        print("ERROR: Missing data"); sys.exit(1)

    k_by_date, pm_by_date, common_dates, latest_common = \
        align_timestamps(kalshi, pm_buckets)
    if not common_dates:
        print("ERROR: No common dates"); sys.exit(1)

    kalshi_aligned, pm_aligned = get_aligned_snapshot(
        kalshi, pm_buckets, k_by_date, pm_by_date, latest_common)

    # Weights + arrays
    k_weights = _make_exceedance_weights(kalshi_aligned)
    pm_weights = _make_bucket_weights(pm_aligned)
    k_thresholds = np.array([c["threshold"] for c in kalshi_aligned])
    k_prices = np.array([c["mid_price"] for c in kalshi_aligned])
    pm_lowers = np.array([b["lower"] for b in pm_aligned])
    pm_uppers = np.array([b["upper"] for b in pm_aligned])
    pm_prices = np.array([b["price"] for b in pm_aligned])

    # Fit models
    print("Fitting models...")
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
    best_k = k_ranked[0]
    best_pm = pm_ranked[0]

    # Bootstrap
    print("Bootstrapping (n=1000)...")
    k_boot = bootstrap_fit(kalshi_aligned, mode="exceedance", n_boot=1000)
    pm_boot = bootstrap_fit(pm_aligned, mode="buckets", n_boot=1000)
    k_mu_ci = bootstrap_ci(k_boot, "mu")
    k_sig_ci = bootstrap_ci(k_boot, "sigma")
    pm_mu_ci = bootstrap_ci(pm_boot, "mu")
    pm_sig_ci = bootstrap_ci(pm_boot, "sigma")

    ci_overlap = (k_mu_ci["ci_lo"] < pm_mu_ci["ci_hi"] and
                  pm_mu_ci["ci_lo"] < k_mu_ci["ci_hi"])

    # Empirical + Hybrid CDFs
    k_emp_cdf, k_lo, k_hi = build_kalshi_empirical_cdf(kalshi_aligned)
    pm_emp_cdf, pm_lo, pm_hi = build_pm_empirical_cdf(pm_aligned)
    k_hybrid = build_hybrid_cdf(k_emp_cdf, k_lo, k_hi, best_k)
    pm_hybrid = build_hybrid_cdf(pm_emp_cdf, pm_lo, pm_hi, best_pm)

    # Divergence
    fine_grid = np.linspace(-5, 10, 2000)
    pdf_k = np.diff(np.concatenate([[0], cdf_fn(best_k, fine_grid)]))
    pdf_pm = np.diff(np.concatenate([[0], cdf_fn(best_pm, fine_grid)]))
    eps = 1e-15
    pdf_k = np.clip(pdf_k, eps, None); pdf_k /= pdf_k.sum()
    pdf_pm = np.clip(pdf_pm, eps, None); pdf_pm /= pdf_pm.sum()
    kl_fwd = float(np.sum(pdf_k * np.log(pdf_k / pdf_pm)))
    kl_rev = float(np.sum(pdf_pm * np.log(pdf_pm / pdf_k)))
    jsd = 0.5 * kl_fwd + 0.5 * kl_rev

    # === Build PDF curves for visualization ===
    print("Building visualization data...")
    x_pdf = np.linspace(-3, 8, 300)

    # Parametric PDFs (via numerical differentiation of CDF)
    dx = x_pdf[1] - x_pdf[0]
    k_cdf_vals = np.array([float(cdf_fn(best_k, xi)) for xi in x_pdf])
    pm_cdf_vals = np.array([float(cdf_fn(best_pm, xi)) for xi in x_pdf])
    k_pdf_vals = np.gradient(k_cdf_vals, dx)
    pm_pdf_vals = np.gradient(pm_cdf_vals, dx)

    # Bootstrap CI bands on PDF
    k_pdf_boots = []
    for bf in k_boot:
        cvals = np.array([float(cdf_fn(bf, xi)) for xi in x_pdf])
        k_pdf_boots.append(np.gradient(cvals, dx))
    k_pdf_boots = np.array(k_pdf_boots)
    k_pdf_lo = np.percentile(k_pdf_boots, 2.5, axis=0)
    k_pdf_hi = np.percentile(k_pdf_boots, 97.5, axis=0)

    pm_pdf_boots = []
    for bf in pm_boot:
        cvals = np.array([float(cdf_fn(bf, xi)) for xi in x_pdf])
        pm_pdf_boots.append(np.gradient(cvals, dx))
    pm_pdf_boots = np.array(pm_pdf_boots)
    pm_pdf_lo = np.percentile(pm_pdf_boots, 2.5, axis=0)
    pm_pdf_hi = np.percentile(pm_pdf_boots, 97.5, axis=0)

    # Hybrid CDFs for comparison grid
    comparison_grid = []
    for x in np.arange(-1.0, 6.1, 0.5):
        pk = float(survival_fn(best_k, x))
        pp = float(survival_fn(best_pm, x))
        kh = float(1.0 - k_hybrid(np.array([x]))[0])
        ph = float(1.0 - pm_hybrid(np.array([x]))[0])
        k_q = bootstrap_query(k_boot, lambda f, _x=x: float(survival_fn(f, _x)))
        pm_q = bootstrap_query(pm_boot, lambda f, _x=x: float(survival_fn(f, _x)))
        comparison_grid.append({
            "x": _f(x),
            "k_parametric": _f(pk), "k_hybrid": _f(kh),
            "k_ci": [_f(k_q["ci_lo"]), _f(k_q["ci_hi"])],
            "pm_parametric": _f(pp), "pm_hybrid": _f(ph),
            "pm_ci": [_f(pm_q["ci_lo"]), _f(pm_q["ci_hi"])],
        })

    # Probability queries
    queries = [
        ("P(GDP > 0%)", lambda f: float(survival_fn(f, 0.0))),
        ("P(GDP > 1%)", lambda f: float(survival_fn(f, 1.0))),
        ("P(GDP > 2%)", lambda f: float(survival_fn(f, 2.0))),
        ("P(GDP > 3%)", lambda f: float(survival_fn(f, 3.0))),
        ("P(0% < GDP < 1%)", lambda f: float(bucket_prob(f, 0.0, 1.0))),
        ("P(1% < GDP < 2%)", lambda f: float(bucket_prob(f, 1.0, 2.0))),
        ("P(2% < GDP < 3%)", lambda f: float(bucket_prob(f, 2.0, 3.0))),
        ("P(GDP < 0%)", lambda f: float(cdf_fn(f, 0.0))),
        ("P(GDP > 5%)", lambda f: float(survival_fn(f, 5.0))),
        ("E[GDP]", lambda f: f["mu"]),
    ]
    prob_queries = []
    for label, fn in queries:
        vk = fn(best_k); vp = fn(best_pm)
        k_q = bootstrap_query(k_boot, fn)
        pm_q = bootstrap_query(pm_boot, fn)
        prob_queries.append({
            "label": label,
            "kalshi": _f(vk), "k_ci": [_f(k_q["ci_lo"]), _f(k_q["ci_hi"])],
            "polymarket": _f(vp), "pm_ci": [_f(pm_q["ci_lo"]), _f(pm_q["ci_hi"])],
            "diff": _f(vk - vp),
        })

    # Time series
    print("Fitting daily time series...")
    daily = fit_daily_timeseries(kalshi, pm_buckets, k_by_date,
                                 pm_by_date, common_dates)

    ts_dates = [d["date"] for d in daily]
    k_mu_ts = [d.get("k_mu") for d in daily]
    pm_mu_ts = [d.get("pm_mu") for d in daily]
    k_sigma_ts = [d.get("k_sigma") for d in daily]
    pm_sigma_ts = [d.get("pm_sigma") for d in daily]

    # Kalman filter on mu series
    k_mu_clean = [v for v in k_mu_ts if v is not None]
    pm_mu_clean = [v for v in pm_mu_ts if v is not None]

    k_kal_mu, k_kal_var = kalman_filter_1d(k_mu_clean) if len(k_mu_clean) > 2 else ([], [])
    pm_kal_mu, pm_kal_var = kalman_filter_1d(pm_mu_clean) if len(pm_mu_clean) > 2 else ([], [])

    # Reconstruct Kalman series aligned to full dates
    k_kal_full = []
    k_kal_ci_full = []
    ki = 0
    for v in k_mu_ts:
        if v is not None and ki < len(k_kal_mu):
            k_kal_full.append(_f(k_kal_mu[ki]))
            ci_w = 1.96 * np.sqrt(k_kal_var[ki])
            k_kal_ci_full.append([_f(k_kal_mu[ki] - ci_w), _f(k_kal_mu[ki] + ci_w)])
            ki += 1
        else:
            k_kal_full.append(None)
            k_kal_ci_full.append(None)

    pm_kal_full = []
    pm_kal_ci_full = []
    pi = 0
    for v in pm_mu_ts:
        if v is not None and pi < len(pm_kal_mu):
            pm_kal_full.append(_f(pm_kal_mu[pi]))
            ci_w = 1.96 * np.sqrt(pm_kal_var[pi])
            pm_kal_ci_full.append([_f(pm_kal_mu[pi] - ci_w), _f(pm_kal_mu[pi] + ci_w)])
            pi += 1
        else:
            pm_kal_full.append(None)
            pm_kal_ci_full.append(None)

    # === Assemble JSON ===
    result = {
        "event": "US GDP Q1 2026",
        "resolution_source": "BEA Advance Estimate",
        "resolution_match": rules["match"],
        "latest_date": latest_common,
        "common_dates_count": len(common_dates),
        "date_range": [common_dates[0], common_dates[-1]],
        "kalshi": {
            "model": best_k["type"],
            "mu": _f(best_k["mu"]),
            "mu_ci": [_f(k_mu_ci["ci_lo"]), _f(k_mu_ci["ci_hi"])],
            "sigma": _f(best_k["sigma"]),
            "sigma_ci": [_f(k_sig_ci["ci_lo"]), _f(k_sig_ci["ci_hi"])],
            "contracts": [{
                "threshold": c["threshold"],
                "price": _f(c["mid_price"]),
                "fitted": _f(float(survival_fn(best_k, c["threshold"]))),
                "bid": _f(c["bid"]), "ask": _f(c["ask"]),
                "volume": c["volume"],
            } for c in kalshi_aligned],
        },
        "polymarket": {
            "model": best_pm["type"],
            "mu": _f(best_pm["mu"]),
            "mu_ci": [_f(pm_mu_ci["ci_lo"]), _f(pm_mu_ci["ci_hi"])],
            "sigma": _f(best_pm["sigma"]),
            "sigma_ci": [_f(pm_sig_ci["ci_lo"]), _f(pm_sig_ci["ci_hi"])],
            "buckets": [{
                "label": b.get("label", f"[{b['lower']}, {b['upper']})"),
                "lower": b["lower"], "upper": b["upper"],
                "price": _f(b["price"]),
                "fitted": _f(float(bucket_prob(best_pm, b["lower"], b["upper"]))),
                "volume": _f(b["volume"]),
            } for b in pm_aligned],
            "bucket_sum": _f(sum(b["price"] for b in pm_aligned)),
        },
        "pdf_curves": {
            "x": [_f(v) for v in x_pdf],
            "kalshi_pdf": [_f(v) for v in k_pdf_vals],
            "kalshi_pdf_ci_lo": [_f(v) for v in k_pdf_lo],
            "kalshi_pdf_ci_hi": [_f(v) for v in k_pdf_hi],
            "pm_pdf": [_f(v) for v in pm_pdf_vals],
            "pm_pdf_ci_lo": [_f(v) for v in pm_pdf_lo],
            "pm_pdf_ci_hi": [_f(v) for v in pm_pdf_hi],
        },
        "comparison": {
            "grid": comparison_grid,
            "jsd": _f(jsd),
            "kl_forward": _f(kl_fwd),
            "kl_reverse": _f(kl_rev),
            "mu_gap": _f(best_k["mu"] - best_pm["mu"]),
            "gap_significant": not ci_overlap,
            "prob_queries": prob_queries,
        },
        "timeseries": {
            "dates": ts_dates,
            "kalshi_mu": [_f(v) for v in k_mu_ts],
            "kalshi_mu_kalman": k_kal_full,
            "kalshi_mu_kalman_ci": k_kal_ci_full,
            "pm_mu": [_f(v) for v in pm_mu_ts],
            "pm_mu_kalman": pm_kal_full,
            "pm_mu_kalman_ci": pm_kal_ci_full,
            "kalshi_sigma": [_f(v) for v in k_sigma_ts],
            "pm_sigma": [_f(v) for v in pm_sigma_ts],
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Written to {OUTPUT} ({OUTPUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
