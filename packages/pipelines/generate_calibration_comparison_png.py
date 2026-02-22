#!/usr/bin/env python3
"""
Generate Calibration Comparison: When You Measure Matters

Compares calibration between:
1. Truncated prices (from prediction_accuracy, same as Predicted vs Actual)
2. Resolution prices (true last traded price from resolution_prices.json)
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
FIGURES_DIR = Path(__file__).parent.parent / "figures"

def main():
    print("="*60)
    print("CALIBRATION COMPARISON: When You Measure Matters")
    print("="*60)

    # 1. Load prediction accuracy (Polymarket only)
    print("\n1. Loading prediction accuracy files...")
    pm = pd.read_csv(DATA_DIR / "polymarket_prediction_accuracy_all_political.csv",
                     dtype={'token_id': str}, low_memory=False)

    # Filter to 1 day before (truncated) - Polymarket only
    pm_1d = pm[pm['days_before_event'] == 1].copy()
    pm_1d['id'] = pm_1d['token_id'].astype(str)

    truncated = pm_1d.copy()
    print(f"   Truncated (Polymarket only): {len(truncated):,} markets")

    # 2. Load resolution prices (Polymarket only)
    print("\n2. Loading resolution prices...")
    with open(DATA_DIR / "resolution_prices.json") as f:
        res_data = json.load(f)

    res_lookup = {}
    for key, val in res_data.items():
        if val['platform'] == 'polymarket':
            res_lookup[str(val['token_id'])] = val['resolution_price']

    print(f"   Resolution (Polymarket only): {len(res_lookup):,} markets")

    # 3. Match resolution prices to truncated
    truncated['resolution_price'] = truncated['id'].map(res_lookup)
    matched = truncated[truncated['resolution_price'].notna() & truncated['actual_outcome'].isin([0, 1])].copy()
    print(f"\n3. Matched: {len(matched):,} markets with both prices")

    # 4. Compute calibration bins
    print("\n4. Computing calibration...")
    num_bins = 90

    def compute_bins(df, price_col):
        df_sorted = df.sort_values(price_col).reset_index(drop=True)
        samples_per_bin = max(1, len(df_sorted) // num_bins)
        df_sorted['bin'] = df_sorted.index // samples_per_bin
        df_sorted.loc[df_sorted['bin'] >= num_bins, 'bin'] = num_bins - 1
        return df_sorted.groupby('bin').agg({
            price_col: 'mean',
            'actual_outcome': 'mean'
        }).reset_index().rename(columns={price_col: 'predicted', 'actual_outcome': 'actual'})

    trunc_bins = compute_bins(matched, 'prediction_price')
    res_bins = compute_bins(matched, 'resolution_price')

    # 5. Plot
    print("\n5. Generating figure...")
    FIGURES_DIR.mkdir(exist_ok=True)

    fig, ax = plt.subplots(figsize=(10, 10))

    ax.plot([0, 1], [0, 1], 'k--', linewidth=1.5, alpha=0.7, label='Perfect')
    ax.scatter(trunc_bins['predicted'], trunc_bins['actual'], s=30, alpha=0.6,
               color='#2563eb', label='Truncated', zorder=3)
    ax.scatter(res_bins['predicted'], res_bins['actual'], s=30, alpha=0.6,
               color='#dc2626', label='Resolution', zorder=3)

    # Trend lines
    x = np.linspace(0.02, 0.98, 100)
    z_t = np.polyfit(trunc_bins['predicted'], trunc_bins['actual'], 3)
    z_r = np.polyfit(res_bins['predicted'], res_bins['actual'], 3)
    ax.plot(x, np.clip(np.poly1d(z_t)(x), 0, 1), color='#2563eb', linewidth=2.5, alpha=0.8)
    ax.plot(x, np.clip(np.poly1d(z_r)(x), 0, 1), color='#dc2626', linewidth=2.5, alpha=0.8)

    # Add distribution on right y-axis
    ax2 = ax.twinx()
    bins = np.linspace(0, 1, 41)  # 40 bins

    # Compute histograms
    trunc_hist, _ = np.histogram(matched['prediction_price'], bins=bins)
    res_hist, _ = np.histogram(matched['resolution_price'], bins=bins)

    bin_centers = (bins[:-1] + bins[1:]) / 2
    bar_width = 0.024

    # Overlay with transparency (no side-by-side offset)
    ax2.bar(bin_centers, trunc_hist, width=bar_width, alpha=0.4,
            color='#2563eb', label='Truncated dist.', zorder=1)
    ax2.bar(bin_centers, res_hist, width=bar_width, alpha=0.4,
            color='#dc2626', label='Resolution dist.', zorder=2)

    ax2.set_ylabel('Market Count', fontsize=10, color='gray')
    ax2.tick_params(axis='y', labelcolor='gray', labelsize=9)

    # Stats
    trunc_mae = np.abs(trunc_bins['predicted'] - trunc_bins['actual']).mean()
    res_mae = np.abs(res_bins['predicted'] - res_bins['actual']).mean()

    ax.set_xlabel('Predicted Probability', fontsize=12)
    ax.set_ylabel('Actual Outcome Rate', fontsize=12)
    ax.set_title('Polymarket Calibration: When You Measure Matters', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', fontsize=10)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)

    ax.text(0.98, 0.02, f'Truncated MAE: {trunc_mae:.4f}\nResolution MAE: {res_mae:.4f}\nn={len(matched):,}',
            transform=ax.transAxes, fontsize=9, ha='right', va='bottom',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))

    output = FIGURES_DIR / "calibration_polymarket_only.png"
    plt.savefig(output, dpi=300, facecolor='white')
    plt.close()

    print(f"\n   Saved: {output}")
    print(f"\n   Truncated MAE: {trunc_mae:.4f}")
    print(f"   Resolution MAE: {res_mae:.4f}")
    print(f"   Difference: {trunc_mae - res_mae:+.4f}")

if __name__ == "__main__":
    main()
