#!/usr/bin/env python3
"""
Generate Liquidity Analysis Visualizations

Creates charts comparing liquidity and bid/ask spreads across categories.

Input:
    data/liquidity_metrics_by_market.csv

Output:
    figures/spread_by_category.png
    figures/depth_by_category.png
    figures/liquidity_platform_comparison.png
    data/liquidity_summary_by_category.csv
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys
from datetime import datetime
from pathlib import Path

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR
from category_utils import format_category_name

INPUT_FILE = DATA_DIR / "liquidity_metrics_by_market.csv"
FIGURES_DIR = BASE_DIR / "figures"
SUMMARY_OUTPUT = DATA_DIR / "liquidity_summary_by_category.csv"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def clean_category(cat):
    """Get display name for a category code."""
    return format_category_name(cat)


def plot_spread_by_category(df, output_path):
    """Box plot of relative spread by category."""
    fig, ax = plt.subplots(figsize=(14, 8))

    # Get category stats and sort by median spread
    cat_stats = df.groupby('category')['rel_spread_mean'].agg(['median', 'count'])
    cat_stats = cat_stats[cat_stats['count'] >= 10].sort_values('median')

    categories = cat_stats.index.tolist()

    # Prepare data for box plots
    data = [df[df['category'] == cat]['rel_spread_mean'].dropna().values for cat in categories]
    labels = [f"{clean_category(cat)}\n(n={int(cat_stats.loc[cat, 'count'])})" for cat in categories]

    # Create box plot
    bp = ax.boxplot(data, labels=labels, patch_artist=True)

    # Color by platform mix
    for i, cat in enumerate(categories):
        cat_df = df[df['category'] == cat]
        pm_ratio = (cat_df['platform'] == 'Polymarket').mean()
        color = plt.cm.RdYlBu(pm_ratio)
        bp['boxes'][i].set_facecolor(color)
        bp['boxes'][i].set_alpha(0.7)

    ax.set_ylabel('Relative Spread (%)', fontsize=12)
    ax.set_xlabel('Category', fontsize=12)
    ax.set_title('Bid-Ask Spread by Category\n(Lower = Tighter Spreads = Better Liquidity)', fontsize=14, fontweight='bold')
    ax.tick_params(axis='x', rotation=45)

    # Add color bar for platform mix
    sm = plt.cm.ScalarMappable(cmap='RdYlBu', norm=plt.Normalize(0, 1))
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, shrink=0.5)
    cbar.set_label('Polymarket ratio', fontsize=10)

    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()

    log(f"   Saved: {output_path.name}")


def plot_depth_by_category(df, output_path):
    """Box plot of depth by category."""
    fig, ax = plt.subplots(figsize=(14, 8))

    # Get category stats and sort by median depth (descending - more depth is better)
    cat_stats = df.groupby('category')['depth_mean'].agg(['median', 'count'])
    cat_stats = cat_stats[cat_stats['count'] >= 10].sort_values('median', ascending=False)

    categories = cat_stats.index.tolist()

    # Prepare data
    data = [df[df['category'] == cat]['depth_mean'].dropna().values for cat in categories]
    labels = [f"{clean_category(cat)}\n(n={int(cat_stats.loc[cat, 'count'])})" for cat in categories]

    # Create box plot (log scale for depth)
    bp = ax.boxplot(data, labels=labels, patch_artist=True)

    # Color boxes
    colors = plt.cm.Greens(np.linspace(0.3, 0.9, len(categories)))
    for i, box in enumerate(bp['boxes']):
        box.set_facecolor(colors[i])
        box.set_alpha(0.7)

    ax.set_ylabel('Average Depth (contracts)', fontsize=12)
    ax.set_xlabel('Category', fontsize=12)
    ax.set_title('Order Book Depth by Category\n(Higher = More Liquidity)', fontsize=14, fontweight='bold')
    ax.tick_params(axis='x', rotation=45)
    ax.set_yscale('log')

    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()

    log(f"   Saved: {output_path.name}")


def plot_platform_comparison(df, output_path):
    """Compare spreads and depth between platforms."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Left: Spread comparison
    ax1 = axes[0]
    pm_spreads = df[df['platform'] == 'Polymarket']['rel_spread_mean'].dropna()
    k_spreads = df[df['platform'] == 'Kalshi']['rel_spread_mean'].dropna()

    ax1.hist(pm_spreads, bins=50, alpha=0.6, label=f'Polymarket (n={len(pm_spreads):,})', color='#2563eb', density=True)
    ax1.hist(k_spreads, bins=50, alpha=0.6, label=f'Kalshi (n={len(k_spreads):,})', color='#dc2626', density=True)
    ax1.axvline(pm_spreads.median(), color='#2563eb', linestyle='--', linewidth=2, label=f'PM median: {pm_spreads.median():.2f}%')
    ax1.axvline(k_spreads.median(), color='#dc2626', linestyle='--', linewidth=2, label=f'K median: {k_spreads.median():.2f}%')
    ax1.set_xlabel('Relative Spread (%)', fontsize=12)
    ax1.set_ylabel('Density', fontsize=12)
    ax1.set_title('Spread Distribution by Platform', fontsize=13, fontweight='bold')
    ax1.legend(fontsize=9)
    ax1.set_xlim(0, min(50, max(pm_spreads.quantile(0.95), k_spreads.quantile(0.95)) * 1.2))

    # Right: Depth comparison
    ax2 = axes[1]
    pm_depth = df[df['platform'] == 'Polymarket']['depth_mean'].dropna()
    k_depth = df[df['platform'] == 'Kalshi']['depth_mean'].dropna()

    # Use log scale for depth
    pm_depth_log = np.log10(pm_depth[pm_depth > 0])
    k_depth_log = np.log10(k_depth[k_depth > 0])

    ax2.hist(pm_depth_log, bins=50, alpha=0.6, label=f'Polymarket (n={len(pm_depth):,})', color='#2563eb', density=True)
    ax2.hist(k_depth_log, bins=50, alpha=0.6, label=f'Kalshi (n={len(k_depth):,})', color='#dc2626', density=True)
    ax2.axvline(pm_depth_log.median(), color='#2563eb', linestyle='--', linewidth=2, label=f'PM median: {10**pm_depth_log.median():,.0f}')
    ax2.axvline(k_depth_log.median(), color='#dc2626', linestyle='--', linewidth=2, label=f'K median: {10**k_depth_log.median():,.0f}')
    ax2.set_xlabel('Depth (log10 contracts)', fontsize=12)
    ax2.set_ylabel('Density', fontsize=12)
    ax2.set_title('Depth Distribution by Platform', fontsize=13, fontweight='bold')
    ax2.legend(fontsize=9)

    plt.suptitle('Platform Comparison: Liquidity Metrics', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()

    log(f"   Saved: {output_path.name}")


def plot_spread_vs_volume(df, output_path):
    """Scatter plot of spread vs volume."""
    fig, ax = plt.subplots(figsize=(10, 8))

    # Filter to valid data
    valid = df[(df['rel_spread_mean'].notna()) & (df['volume_usd'] > 0)].copy()

    # Color by platform
    pm = valid[valid['platform'] == 'Polymarket']
    k = valid[valid['platform'] == 'Kalshi']

    ax.scatter(pm['volume_usd'], pm['rel_spread_mean'], alpha=0.5, s=20, c='#2563eb', label='Polymarket')
    ax.scatter(k['volume_usd'], k['rel_spread_mean'], alpha=0.5, s=20, c='#dc2626', label='Kalshi')

    ax.set_xscale('log')
    ax.set_xlabel('Volume (USD)', fontsize=12)
    ax.set_ylabel('Relative Spread (%)', fontsize=12)
    ax.set_title('Spread vs Volume\n(Do higher-volume markets have tighter spreads?)', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Add correlation
    if len(valid) > 10:
        corr = np.corrcoef(np.log10(valid['volume_usd']), valid['rel_spread_mean'])[0, 1]
        ax.text(0.02, 0.98, f'Correlation: {corr:.3f}', transform=ax.transAxes,
                fontsize=10, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()

    log(f"   Saved: {output_path.name}")


def create_summary_table(df, output_path):
    """Create summary table by category."""
    summary = df.groupby('category').agg({
        'market_id': 'count',
        'volume_usd': ['sum', 'mean'],
        'rel_spread_mean': ['mean', 'median', 'std'],
        'depth_mean': ['mean', 'median'],
        'n_snapshots': 'mean'
    }).round(4)

    summary.columns = ['n_markets', 'total_volume', 'avg_volume',
                       'spread_mean', 'spread_median', 'spread_std',
                       'depth_mean', 'depth_median', 'avg_snapshots']

    # Add short names
    summary['category_short'] = summary.index.map(clean_category)
    summary = summary.reset_index()
    summary = summary.sort_values('n_markets', ascending=False)

    summary.to_csv(output_path, index=False)
    log(f"   Saved: {output_path.name}")

    return summary


def main():
    log("=" * 60)
    log("GENERATING LIQUIDITY ANALYSIS")
    log("=" * 60)

    # Load data
    log("\n1. Loading liquidity metrics...")
    if not INPUT_FILE.exists():
        log(f"   ERROR: Input file not found: {INPUT_FILE}")
        log("   Please run calculate_liquidity_metrics.py first.")
        sys.exit(1)

    df = pd.read_csv(INPUT_FILE)
    log(f"   Loaded {len(df):,} markets")
    log(f"   - Polymarket: {(df['platform'] == 'Polymarket').sum():,}")
    log(f"   - Kalshi: {(df['platform'] == 'Kalshi').sum():,}")

    # Create figures directory
    FIGURES_DIR.mkdir(exist_ok=True)

    # Generate visualizations
    log("\n2. Generating visualizations...")

    plot_spread_by_category(df, FIGURES_DIR / "spread_by_category.png")
    plot_depth_by_category(df, FIGURES_DIR / "depth_by_category.png")
    plot_platform_comparison(df, FIGURES_DIR / "liquidity_platform_comparison.png")
    plot_spread_vs_volume(df, FIGURES_DIR / "spread_vs_volume.png")

    # Create summary table
    log("\n3. Creating summary table...")
    summary = create_summary_table(df, SUMMARY_OUTPUT)

    # Print key findings
    log("\n" + "=" * 60)
    log("KEY FINDINGS")
    log("=" * 60)

    # Best/worst spreads by category
    valid_cats = df.groupby('category')['rel_spread_mean'].agg(['median', 'count'])
    valid_cats = valid_cats[valid_cats['count'] >= 10].sort_values('median')

    log("\nTightest Spreads (best liquidity):")
    for cat in valid_cats.head(3).index:
        spread = valid_cats.loc[cat, 'median']
        log(f"  {clean_category(cat)}: {spread:.2f}%")

    log("\nWidest Spreads (worst liquidity):")
    for cat in valid_cats.tail(3).index:
        spread = valid_cats.loc[cat, 'median']
        log(f"  {clean_category(cat)}: {spread:.2f}%")

    # Platform comparison
    log("\nPlatform Comparison:")
    pm = df[df['platform'] == 'Polymarket']['rel_spread_mean'].dropna()
    k = df[df['platform'] == 'Kalshi']['rel_spread_mean'].dropna()
    log(f"  Polymarket median spread: {pm.median():.2f}%")
    log(f"  Kalshi median spread: {k.median():.2f}%")

    if pm.median() < k.median():
        log(f"  => Polymarket has tighter spreads by {k.median() - pm.median():.2f}pp")
    else:
        log(f"  => Kalshi has tighter spreads by {pm.median() - k.median():.2f}pp")

    log("\nDone!")


if __name__ == "__main__":
    main()
