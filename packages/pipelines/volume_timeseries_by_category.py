#!/usr/bin/env python3
"""
Volume Timeseries by Category

Creates a line chart showing trading volume over time, with separate lines
for each major political category.

Note: Since granular volume data may not be available in price JSONs,
this script uses market-level volume and plots by market close date as a proxy.

Usage:
  python volume_timeseries_by_category.py                    # Top 6 categories by volume
  python volume_timeseries_by_category.py --list             # List all available categories
  python volume_timeseries_by_category.py --categories "1. ELECTORAL" "2. MONETARY_POLICY"
  python volume_timeseries_by_category.py --categories "1. ELECTORAL" --add "15. POLITICAL_SPEECH"

Output:
- graphs/combined/volume_timeseries_by_category.png
- data/volume_timeseries_by_category.csv
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import argparse

from config import BASE_DIR, DATA_DIR
from paper_config import load_master_csv, PAPER_GRAPHS_DIR, PAPER_DATA_DIR


def parse_args():
    parser = argparse.ArgumentParser(
        description='Generate trading volume timeseries by political category',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s                                           # Top 6 categories by volume (default)
  %(prog)s --list                                    # List all available categories
  %(prog)s --categories "1. ELECTORAL" "2. MONETARY_POLICY"
  %(prog)s --add "15. POLITICAL_SPEECH"              # Add to default top 6
  %(prog)s --top 10                                  # Show top 10 categories
        '''
    )
    parser.add_argument('--list', action='store_true',
                        help='List all available categories and exit')
    parser.add_argument('--categories', nargs='+', metavar='CAT',
                        help='Specific categories to display (replaces default)')
    parser.add_argument('--add', nargs='+', metavar='CAT',
                        help='Additional categories to add to the default top N')
    parser.add_argument('--top', type=int, default=6,
                        help='Number of top categories by volume (default: 6)')
    parser.add_argument('--platform', choices=['all', 'polymarket', 'kalshi'],
                        default='all', help='Filter by platform')
    return parser.parse_args()

# Paths
GRAPHS_DIR = PAPER_GRAPHS_DIR
GRAPHS_DIR.mkdir(parents=True, exist_ok=True)


def main():
    args = parse_args()

    print("=" * 70)
    print("VOLUME TIMESERIES BY CATEGORY")
    print("=" * 70)

    # Load data
    print(f"\nLoading master CSV...")
    df = load_master_csv()
    df['trading_close_time'] = pd.to_datetime(df['trading_close_time'], format='mixed', utc=True, errors='coerce')
    print(f"  Loaded {len(df):,} total markets")

    # Determine volume column
    volume_col = 'volume_usd' if 'volume_usd' in df.columns else 'volume'
    print(f"  Using volume column: {volume_col}")

    # Filter to markets with valid date and volume
    df_valid = df[
        df['trading_close_time'].notna() &
        df[volume_col].notna() &
        (df[volume_col] > 0)
    ].copy()
    print(f"  Markets with valid date and volume: {len(df_valid):,}")

    # Get all categories with their volumes
    all_categories = df_valid.groupby('political_category')[volume_col].sum().sort_values(ascending=False)

    # Handle --list flag
    if args.list:
        print("\n" + "=" * 70)
        print("AVAILABLE CATEGORIES (sorted by volume)")
        print("=" * 70)
        for i, (cat, vol) in enumerate(all_categories.items(), 1):
            print(f"  {i:2d}. {cat:<35} ${vol/1e6:>10,.1f}M")
        return

    # Extract month for aggregation
    df_valid['month'] = df_valid['trading_close_time'].dt.to_period('M')

    # Determine which categories to display
    if args.categories:
        # User specified exact categories
        selected_categories = args.categories
        # Validate
        invalid = [c for c in selected_categories if c not in all_categories.index]
        if invalid:
            print(f"\nWarning: Unknown categories: {invalid}")
            print("Use --list to see available categories")
        selected_categories = [c for c in selected_categories if c in all_categories.index]
    else:
        # Default: top N categories
        selected_categories = all_categories.nlargest(args.top).index.tolist()

    # Add any additional categories
    if args.add:
        for cat in args.add:
            if cat in all_categories.index and cat not in selected_categories:
                selected_categories.append(cat)
            elif cat not in all_categories.index:
                print(f"Warning: Unknown category '{cat}' (use --list to see available)")

    print(f"\nCategories to display: {selected_categories}")

    # Filter to selected categories
    df_top = df_valid[df_valid['political_category'].isin(selected_categories)].copy()

    # Aggregate volume by month and category
    monthly_volume = df_top.groupby(['month', 'political_category'])[volume_col].sum().unstack(fill_value=0)

    # Convert period index to datetime for plotting
    monthly_volume.index = monthly_volume.index.to_timestamp()

    # Save CSV
    csv_output = PAPER_DATA_DIR / "volume_timeseries_by_category.csv"
    monthly_volume.reset_index().to_csv(csv_output, index=False)
    print(f"\nSaved CSV: {csv_output}")

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))

    # Extended color palette for more categories
    colors = ['#2C3E50', '#E74C3C', '#3498DB', '#2ECC71', '#9B59B6', '#F39C12',
              '#1ABC9C', '#E91E63', '#00BCD4', '#FF5722', '#795548', '#607D8B']

    # Plot each category
    for i, category in enumerate(selected_categories):
        if category in monthly_volume.columns:
            ax.plot(monthly_volume.index, monthly_volume[category] / 1e6,
                    label=category, color=colors[i % len(colors)],
                    linewidth=2, marker='o', markersize=4)

    # Formatting
    ax.set_xlabel('Month', fontsize=12, fontweight='bold')
    ax.set_ylabel('Trading Volume (Millions USD)', fontsize=12, fontweight='bold')
    ax.set_title('Trading Volume Over Time by Political Category\n(All Platforms Combined)',
                 fontsize=14, fontweight='bold', pad=15)

    # Legend - adjust size based on number of categories
    ncol = 2 if len(selected_categories) > 6 else 1
    ax.legend(loc='upper left', fontsize=9, framealpha=0.9, ncol=ncol)

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # Format y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}M'))

    # Rotate x-axis labels
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()

    # Save figure
    output_path = GRAPHS_DIR / "volume_timeseries_by_category.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"Saved figure: {output_path}")

    # Also create separate figures for each platform (unless --platform specified)
    platforms_to_plot = ['Polymarket', 'Kalshi']
    if args.platform != 'all':
        platforms_to_plot = [args.platform.capitalize()]

    for platform in platforms_to_plot:
        df_platform = df_valid[df_valid['platform'] == platform]

        if len(df_platform) == 0:
            continue

        # Use same categories as main plot, or get platform-specific top if using defaults
        if args.categories:
            platform_categories = selected_categories
        else:
            platform_categories = df_platform.groupby('political_category')[volume_col].sum().nlargest(args.top).index.tolist()
            if args.add:
                for cat in args.add:
                    if cat in df_platform['political_category'].values and cat not in platform_categories:
                        platform_categories.append(cat)

        df_platform_top = df_platform[df_platform['political_category'].isin(platform_categories)]

        # Aggregate
        platform_monthly = df_platform_top.groupby(['month', 'political_category'])[volume_col].sum().unstack(fill_value=0)
        platform_monthly.index = platform_monthly.index.to_timestamp()

        # Create figure
        fig, ax = plt.subplots(figsize=(14, 8))

        for i, category in enumerate(platform_categories):
            if category in platform_monthly.columns:
                ax.plot(platform_monthly.index, platform_monthly[category] / 1e6,
                        label=category, color=colors[i % len(colors)],
                        linewidth=2, marker='o', markersize=4)

        ax.set_xlabel('Month', fontsize=12, fontweight='bold')
        ax.set_ylabel('Trading Volume (Millions USD)', fontsize=12, fontweight='bold')
        ax.set_title(f'Trading Volume Over Time by Political Category\n({platform})',
                     fontsize=14, fontweight='bold', pad=15)
        ncol = 2 if len(platform_categories) > 6 else 1
        ax.legend(loc='upper left', fontsize=9, framealpha=0.9, ncol=ncol)
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}M'))
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        platform_output = GRAPHS_DIR / f"volume_timeseries_by_category_{platform.lower()}.png"
        plt.savefig(platform_output, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f"Saved figure: {platform_output}")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
