#!/usr/bin/env python3
"""
Time-Series Panel Analysis: Does Liquidity Predict Price Accuracy Over Time?

This script implements a comprehensive analysis to test whether observable liquidity
metrics at time t can predict how accurate the price is at time t, controlling for
the inherent relationship between price and accuracy.
"""

import sys
# Force unbuffered output
sys.stdout = sys.stderr = open(sys.stdout.fileno(), mode='w', buffering=1)

import json
import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# Paths
DATA_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi/data")
OUTPUT_DIR = Path("/tmp")

print("=" * 80)
print("TIME-SERIES PANEL ANALYSIS: LIQUIDITY → PRICE ACCURACY")
print("=" * 80)

# =============================================================================
# LOAD DATA
# =============================================================================
print("\n[1] Loading data...")

# Load orderbook histories
print("   Loading Polymarket orderbook history...")
with open(DATA_DIR / "orderbook_history_polymarket.json", 'r') as f:
    pm_orderbook = json.load(f)
print(f"   - Loaded {len(pm_orderbook)} Polymarket markets")

print("   Loading Kalshi orderbook history...")
with open(DATA_DIR / "orderbook_history_kalshi.json", 'r') as f:
    kalshi_orderbook = json.load(f)
print(f"   - Loaded {len(kalshi_orderbook)} Kalshi markets")

# Load accuracy data to get outcomes
print("   Loading accuracy data...")
pm_accuracy = pd.read_csv(DATA_DIR / "polymarket_prediction_accuracy_all_political.csv")
kalshi_accuracy = pd.read_csv(DATA_DIR / "kalshi_prediction_accuracy_all_political.csv")

# Get unique market→outcome mappings
pm_outcomes = pm_accuracy.groupby('market_id')['actual_outcome'].first().to_dict()
kalshi_outcomes = kalshi_accuracy.groupby('ticker')['actual_outcome'].first().to_dict()
print(f"   - PM outcomes: {len(pm_outcomes)}, Kalshi outcomes: {len(kalshi_outcomes)}")

# =============================================================================
# BUILD SNAPSHOT DATAFRAME
# =============================================================================
print("\n[2] Building snapshot dataframe...")

snapshots = []

# Process Polymarket
for market_id, data in pm_orderbook.items():
    # Try both int and string lookups
    try:
        outcome = pm_outcomes.get(int(market_id))
    except (ValueError, TypeError):
        outcome = pm_outcomes.get(market_id)
    if outcome is None or pd.isna(outcome):
        continue

    category = data.get('category', 'Unknown')
    trading_close = data.get('trading_close_time')
    metrics = data.get('metrics', [])

    if not metrics:
        continue

    # Get time bounds for life_fraction calculation
    timestamps = [m['timestamp'] for m in metrics]
    min_ts, max_ts = min(timestamps), max(timestamps)
    market_duration = max_ts - min_ts if max_ts > min_ts else 1

    for m in metrics:
        price = m.get('midpoint')
        if price is None or price <= 0 or price >= 1:
            continue

        snapshots.append({
            'platform': 'Polymarket',
            'market_id': str(market_id),
            'category': category,
            'outcome': outcome,
            'timestamp': m['timestamp'],
            'price_t': price,
            'spread_t': m.get('spread', 0),
            'relative_spread_t': m.get('relative_spread', 0),
            'bid_depth_t': m.get('bid_depth', 0),
            'ask_depth_t': m.get('ask_depth', 0),
            'total_depth_t': m.get('total_depth', 0),
            'life_fraction': (m['timestamp'] - min_ts) / market_duration if market_duration > 0 else 0.5,
            'market_duration_hours': market_duration / (1000 * 3600),
        })

# Process Kalshi
for ticker, data in kalshi_orderbook.items():
    outcome = kalshi_outcomes.get(ticker)
    if outcome is None or pd.isna(outcome):
        continue

    category = data.get('category', 'Unknown')
    metrics = data.get('metrics', [])

    if not metrics:
        continue

    timestamps = [m['timestamp'] for m in metrics]
    min_ts, max_ts = min(timestamps), max(timestamps)
    market_duration = max_ts - min_ts if max_ts > min_ts else 1

    for m in metrics:
        price = m.get('midpoint')
        if price is None or price <= 0 or price >= 1:
            continue

        snapshots.append({
            'platform': 'Kalshi',
            'market_id': ticker,
            'category': category,
            'outcome': outcome,
            'timestamp': m['timestamp'],
            'price_t': price,
            'spread_t': m.get('spread', 0),
            'relative_spread_t': m.get('relative_spread', 0),
            'bid_depth_t': m.get('bid_depth', 0),
            'ask_depth_t': m.get('ask_depth', 0),
            'total_depth_t': m.get('total_depth', 0),
            'life_fraction': (m['timestamp'] - min_ts) / market_duration if market_duration > 0 else 0.5,
            'market_duration_hours': market_duration / (1000 * 3600),
        })

df = pd.DataFrame(snapshots)
print(f"   Total snapshots: {len(df):,}")
print(f"   Unique markets: {df['market_id'].nunique()}")
print(f"   Average snapshots per market: {len(df) / df['market_id'].nunique():.1f}")

# =============================================================================
# STEP 1: CONSTRUCT TARGET VARIABLE WITH EMPIRICAL CALIBRATION
# =============================================================================
print("\n[3] Step 1: Constructing target variable with empirical calibration...")

# 1a: Compute raw snapshot error
df['snapshot_error'] = (df['price_t'] - df['outcome']) ** 2

# 1b: Build empirical calibration curve with 50 bins
N_BINS = 50
df['price_bin'] = pd.cut(df['price_t'], bins=N_BINS, labels=False)

# Compute empirical expected error per bin
bin_stats = df.groupby('price_bin').agg({
    'snapshot_error': 'mean',
    'outcome': 'mean',  # resolution rate
    'price_t': 'mean',
}).rename(columns={
    'snapshot_error': 'empirical_expected_error',
    'outcome': 'resolution_rate',
    'price_t': 'mean_price'
})

# Map back to snapshots
df['empirical_expected_error'] = df['price_bin'].map(bin_stats['empirical_expected_error'])

# 1c: Compute residual error
df['residual_error_t'] = df['snapshot_error'] - df['empirical_expected_error']

# 1d: Validate - check correlations
valid_df = df.dropna(subset=['residual_error_t'])
df['price_extremity'] = np.abs(valid_df['price_t'] - 0.5)

corr_price, p_price = stats.spearmanr(valid_df['residual_error_t'], valid_df['price_t'])
corr_extremity, p_extremity = stats.spearmanr(valid_df['residual_error_t'], np.abs(valid_df['price_t'] - 0.5))

print(f"   Validation (with {N_BINS} bins):")
print(f"   - Correlation(residual_error, price): r = {corr_price:.4f}, p = {p_price:.2e}")
print(f"   - Correlation(residual_error, |price-0.5|): r = {corr_extremity:.4f}, p = {p_extremity:.2e}")

# If correlations are too high, try more bins
if abs(corr_price) > 0.10 or abs(corr_extremity) > 0.10:
    print(f"   ⚠ Correlations still high, trying 100 bins...")
    N_BINS = 100
    df['price_bin'] = pd.cut(df['price_t'], bins=N_BINS, labels=False)
    bin_stats = df.groupby('price_bin')['snapshot_error'].mean()
    df['empirical_expected_error'] = df['price_bin'].map(bin_stats)
    df['residual_error_t'] = df['snapshot_error'] - df['empirical_expected_error']

    valid_df = df.dropna(subset=['residual_error_t'])
    corr_price, p_price = stats.spearmanr(valid_df['residual_error_t'], valid_df['price_t'])
    corr_extremity, p_extremity = stats.spearmanr(valid_df['residual_error_t'], np.abs(valid_df['price_t'] - 0.5))
    print(f"   - After 100 bins:")
    print(f"     Correlation(residual_error, price): r = {corr_price:.4f}")
    print(f"     Correlation(residual_error, |price-0.5|): r = {corr_extremity:.4f}")

# =============================================================================
# STEP 2: FEATURE INVENTORY
# =============================================================================
print("\n[4] Step 2: Feature Inventory...")

# Compute derived features
df['log_depth_t'] = np.log10(df['total_depth_t'].clip(lower=1))
df['price_extremity'] = np.abs(df['price_t'] - 0.5)

# Depth imbalance
df['depth_imbalance_t'] = (df['bid_depth_t'] - df['ask_depth_t']) / (df['bid_depth_t'] + df['ask_depth_t'] + 1e-10)
df['abs_depth_imbalance_t'] = np.abs(df['depth_imbalance_t'])

# Clean up
df = df.dropna(subset=['residual_error_t', 'spread_t', 'log_depth_t'])
print(f"   Snapshots after cleaning: {len(df):,}")
print(f"   Unique markets: {df['market_id'].nunique()}")
print(f"   Average snapshots per market: {len(df) / df['market_id'].nunique():.1f}")

# Feature inventory table
print("\n   Feature Inventory:")
print("   " + "-" * 60)
features = ['price_t', 'spread_t', 'relative_spread_t', 'log_depth_t',
            'depth_imbalance_t', 'abs_depth_imbalance_t', 'life_fraction', 'price_extremity']
for feat in features:
    if feat in df.columns:
        print(f"   {feat:25s}: mean={df[feat].mean():.4f}, std={df[feat].std():.4f}")

# =============================================================================
# STEP 3: UNIVARIATE SCREENING
# =============================================================================
print("\n[5] Step 3: Univariate Screening...")
print("   (Note: p-values are artificially small due to non-independent snapshots)")

univariate_results = []
for feat in features:
    if feat not in df.columns:
        continue
    valid = df[[feat, 'residual_error_t']].dropna()
    if len(valid) < 100:
        continue
    r, p = stats.spearmanr(valid[feat], valid['residual_error_t'])
    univariate_results.append({
        'Feature': feat,
        'Spearman_r': r,
        'p_value': p,
        'n_snapshots': len(valid)
    })

univariate_df = pd.DataFrame(univariate_results)
univariate_df = univariate_df.sort_values('Spearman_r', key=abs, ascending=False)

print("\n   Univariate Screening Results (sorted by |r|):")
print("   " + "-" * 70)
print(f"   {'Feature':<25s} {'Spearman r':>12s} {'p-value':>15s} {'n':>10s}")
print("   " + "-" * 70)
for _, row in univariate_df.iterrows():
    sig = "***" if row['p_value'] < 0.001 else "**" if row['p_value'] < 0.01 else "*" if row['p_value'] < 0.05 else ""
    print(f"   {row['Feature']:<25s} {row['Spearman_r']:>12.4f} {row['p_value']:>12.2e} {sig:>3s} {row['n_snapshots']:>8,d}")

# =============================================================================
# STEP 4: TIME-BINNED ANALYSIS
# =============================================================================
print("\n[6] Step 4: Time-Binned Correlation Analysis...")

# Bin life_fraction into 5 bins
df['life_bin'] = pd.cut(df['life_fraction'], bins=[0, 0.2, 0.4, 0.6, 0.8, 1.0],
                         labels=['0-20%', '20-40%', '40-60%', '60-80%', '80-100%'])

liquidity_features = ['spread_t', 'log_depth_t', 'depth_imbalance_t', 'abs_depth_imbalance_t']
time_binned = []

for life_bin in ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%']:
    subset = df[df['life_bin'] == life_bin]
    for feat in liquidity_features:
        valid = subset[[feat, 'residual_error_t']].dropna()
        if len(valid) < 50:
            continue
        r, p = stats.spearmanr(valid[feat], valid['residual_error_t'])
        time_binned.append({
            'life_bin': life_bin,
            'feature': feat,
            'spearman_r': r,
            'n': len(valid)
        })

time_binned_df = pd.DataFrame(time_binned)

# Plot
fig, ax = plt.subplots(figsize=(10, 6))
colors = {'spread_t': 'blue', 'log_depth_t': 'green',
          'depth_imbalance_t': 'orange', 'abs_depth_imbalance_t': 'red'}
markers = {'spread_t': 'o', 'log_depth_t': 's',
           'depth_imbalance_t': '^', 'abs_depth_imbalance_t': 'D'}

for feat in liquidity_features:
    feat_data = time_binned_df[time_binned_df['feature'] == feat]
    if len(feat_data) > 0:
        ax.plot(feat_data['life_bin'], feat_data['spearman_r'],
                marker=markers[feat], color=colors[feat], label=feat, linewidth=2, markersize=8)

ax.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
ax.set_xlabel('Market Life Fraction', fontsize=12)
ax.set_ylabel('Spearman Correlation with Residual Error', fontsize=12)
ax.set_title('Liquidity-Accuracy Relationship Over Market Lifetime', fontsize=14)
ax.legend(loc='best')
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'timeseries_time_binned_correlations.png', dpi=150)
plt.close()
print(f"   Saved: {OUTPUT_DIR / 'timeseries_time_binned_correlations.png'}")

# Print table
print("\n   Time-Binned Correlations:")
print("   " + "-" * 60)
pivot = time_binned_df.pivot(index='feature', columns='life_bin', values='spearman_r')
print(pivot.round(4).to_string())

# =============================================================================
# STEP 5: PANEL REGRESSION WITH CLUSTERED STANDARD ERRORS
# =============================================================================
print("\n[7] Step 5: Panel Regression with Clustered Standard Errors...")

try:
    import statsmodels.api as sm
    from statsmodels.regression.linear_model import OLS

    # Prepare data
    reg_df = df[['market_id', 'category', 'residual_error_t', 'spread_t', 'log_depth_t',
                  'depth_imbalance_t', 'abs_depth_imbalance_t', 'life_fraction', 'price_extremity']].dropna()

    # Standardize features
    for col in ['spread_t', 'log_depth_t', 'depth_imbalance_t', 'abs_depth_imbalance_t', 'life_fraction', 'price_extremity']:
        reg_df[col + '_z'] = (reg_df[col] - reg_df[col].mean()) / reg_df[col].std()

    # Market ID as numeric for clustering
    reg_df['market_id_num'] = pd.factorize(reg_df['market_id'])[0]

    # Model 1: Base
    X1 = reg_df[['spread_t_z', 'log_depth_t_z', 'depth_imbalance_t_z',
                  'abs_depth_imbalance_t_z', 'life_fraction_z', 'price_extremity_z']]
    X1 = sm.add_constant(X1)
    y = reg_df['residual_error_t']

    model1 = OLS(y, X1.astype(float)).fit()
    model1_clustered = model1.get_robustcov_results(cov_type='cluster', groups=reg_df['market_id_num'])

    # Model 2: With Category Fixed Effects
    category_dummies = pd.get_dummies(reg_df['category'], prefix='cat', drop_first=True)
    X2 = pd.concat([X1, category_dummies], axis=1)
    model2 = OLS(y, X2.astype(float)).fit()
    model2_clustered = model2.get_robustcov_results(cov_type='cluster', groups=reg_df['market_id_num'])

    # Model 3: With Time Interactions
    for feat in ['spread_t_z', 'log_depth_t_z', 'depth_imbalance_t_z', 'abs_depth_imbalance_t_z']:
        reg_df[f'{feat}_x_life'] = reg_df[feat] * reg_df['life_fraction_z']

    X3_cols = ['spread_t_z', 'log_depth_t_z', 'depth_imbalance_t_z', 'abs_depth_imbalance_t_z',
               'life_fraction_z', 'price_extremity_z',
               'spread_t_z_x_life', 'log_depth_t_z_x_life', 'depth_imbalance_t_z_x_life', 'abs_depth_imbalance_t_z_x_life']
    X3 = sm.add_constant(reg_df[X3_cols])
    X3 = pd.concat([X3, category_dummies], axis=1)
    model3 = OLS(y, X3.astype(float)).fit()
    model3_clustered = model3.get_robustcov_results(cov_type='cluster', groups=reg_df['market_id_num'])

    # Create param index mappings
    X1_cols = list(X1.columns)
    X2_cols = list(X2.columns)
    X3_cols_full = list(X3.columns)

    def get_coef_p(model_clustered, var, cols):
        if var in cols:
            idx = cols.index(var)
            coef = model_clustered.params[idx]
            p = model_clustered.pvalues[idx]
            sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else ""
            return f"{coef:.4f} ({p:.3f}){sig}"
        return "-"

    # Print results table
    print("\n   Regression Results (Clustered SE by Market):")
    print("   " + "-" * 85)
    print(f"   {'Variable':<30s} {'Model 1':>17s} {'Model 2':>17s} {'Model 3':>17s}")
    print("   " + "-" * 85)

    key_vars = ['spread_t_z', 'log_depth_t_z', 'depth_imbalance_t_z', 'abs_depth_imbalance_t_z',
                'life_fraction_z', 'price_extremity_z']

    for var in key_vars:
        m1_str = get_coef_p(model1_clustered, var, X1_cols)
        m2_str = get_coef_p(model2_clustered, var, X2_cols)
        m3_str = get_coef_p(model3_clustered, var, X3_cols_full)
        print(f"   {var:<30s} {m1_str:>17s} {m2_str:>17s} {m3_str:>17s}")

    # Interaction terms for Model 3
    print("   " + "-" * 85)
    print("   Interaction terms (Model 3 only):")
    for var in ['spread_t_z_x_life', 'log_depth_t_z_x_life', 'depth_imbalance_t_z_x_life', 'abs_depth_imbalance_t_z_x_life']:
        m3_str = get_coef_p(model3_clustered, var, X3_cols_full)
        if m3_str != "-":
            print(f"   {var:<30s} {'-':>17s} {'-':>17s} {m3_str:>17s}")

    print("   " + "-" * 85)
    print(f"   {'R²':<30s} {model1.rsquared:.4f}{'':<12s} {model2.rsquared:.4f}{'':<12s} {model3.rsquared:.4f}")
    print(f"   {'N snapshots':<30s} {len(y):>17,d} {len(y):>17,d} {len(y):>17,d}")
    print(f"   {'N markets':<30s} {reg_df['market_id'].nunique():>17,d} {reg_df['market_id'].nunique():>17,d} {reg_df['market_id'].nunique():>17,d}")

except Exception as e:
    print(f"   Error in regression: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# STEP 6: RANDOM FOREST ON SNAPSHOTS
# =============================================================================
print("\n[8] Step 6: Random Forest with Market-Level Split...")

try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.inspection import permutation_importance
    from sklearn.preprocessing import LabelEncoder

    # Prepare features
    rf_df = df[['market_id', 'category', 'residual_error_t', 'spread_t', 'log_depth_t',
                'depth_imbalance_t', 'abs_depth_imbalance_t', 'life_fraction', 'price_extremity']].dropna()

    # Encode category
    le = LabelEncoder()
    rf_df['category_encoded'] = le.fit_transform(rf_df['category'])

    # Split by MARKET (80/20)
    markets = rf_df['market_id'].unique()
    np.random.seed(42)
    np.random.shuffle(markets)
    train_markets = set(markets[:int(0.8 * len(markets))])

    train_df = rf_df[rf_df['market_id'].isin(train_markets)]
    test_df = rf_df[~rf_df['market_id'].isin(train_markets)]

    print(f"   Train: {len(train_df):,} snapshots from {len(train_markets)} markets")
    print(f"   Test: {len(test_df):,} snapshots from {len(markets) - len(train_markets)} markets")

    feature_cols = ['spread_t', 'log_depth_t', 'depth_imbalance_t',
                    'abs_depth_imbalance_t', 'life_fraction', 'price_extremity', 'category_encoded']

    X_train = train_df[feature_cols].values
    y_train = train_df['residual_error_t'].values
    X_test = test_df[feature_cols].values
    y_test = test_df['residual_error_t'].values

    # Train Random Forest
    rf = RandomForestRegressor(n_estimators=500, max_depth=10, min_samples_leaf=50,
                               random_state=42, n_jobs=-1, oob_score=True)
    rf.fit(X_train, y_train)

    # Evaluate
    train_r2 = rf.score(X_train, y_train)
    test_r2 = rf.score(X_test, y_test)
    oob_r2 = rf.oob_score_
    test_preds = rf.predict(X_test)
    mae = np.mean(np.abs(y_test - test_preds))

    print(f"\n   Random Forest Results:")
    print(f"   - Train R²: {train_r2:.4f}")
    print(f"   - OOB R²: {oob_r2:.4f}")
    print(f"   - Test R² (held-out markets): {test_r2:.4f}")
    print(f"   - Test MAE: {mae:.6f}")

    # Permutation importance
    perm_imp = permutation_importance(rf, X_test, y_test, n_repeats=10, random_state=42, n_jobs=-1)

    importance_df = pd.DataFrame({
        'Feature': feature_cols,
        'Importance': perm_imp.importances_mean,
        'Std': perm_imp.importances_std
    }).sort_values('Importance', ascending=False)

    print("\n   Permutation Importance (test set):")
    for _, row in importance_df.iterrows():
        print(f"   {row['Feature']:<25s}: {row['Importance']:.6f} ± {row['Std']:.6f}")

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ['green' if x > 0 else 'red' for x in importance_df['Importance']]
    ax.barh(importance_df['Feature'], importance_df['Importance'], xerr=importance_df['Std'], color=colors, alpha=0.7)
    ax.axvline(x=0, color='black', linewidth=0.5)
    ax.set_xlabel('Permutation Importance', fontsize=12)
    ax.set_title(f'Random Forest Feature Importance (Test R² = {test_r2:.4f})', fontsize=14)
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'timeseries_feature_importance.png', dpi=150)
    plt.close()
    print(f"\n   Saved: {OUTPUT_DIR / 'timeseries_feature_importance.png'}")

    # =============================================================================
    # STEP 7: PRACTICAL THRESHOLD ANALYSIS
    # =============================================================================
    print("\n[9] Step 7: Practical Threshold Analysis...")

    # Get predictions on test set
    test_df = test_df.copy()
    test_df['predicted_residual'] = test_preds

    # Bin into terciles
    test_df['confidence_tier'] = pd.qcut(test_df['predicted_residual'], q=3,
                                          labels=['High Confidence', 'Medium', 'Low Confidence'])

    # Compute actual performance per tier
    tier_stats = test_df.groupby('confidence_tier').agg({
        'residual_error_t': ['mean', 'std', 'count'],
        'market_id': 'nunique'
    }).round(6)
    tier_stats.columns = ['Mean_Residual_Error', 'Std', 'N_Snapshots', 'N_Markets']

    print("\n   Confidence Tier Performance (Test Set):")
    print("   " + "-" * 70)
    print(tier_stats.to_string())

    # Improvement ratio
    high_conf = test_df[test_df['confidence_tier'] == 'High Confidence']['residual_error_t'].abs().mean()
    low_conf = test_df[test_df['confidence_tier'] == 'Low Confidence']['residual_error_t'].abs().mean()
    improvement_ratio = low_conf / high_conf if high_conf > 0 else 0

    print(f"\n   Improvement Ratio (Low Conf / High Conf): {improvement_ratio:.2f}x")
    if improvement_ratio > 1.5:
        print("   ✓ Signal is strong enough for a product feature (> 1.5x)")
    else:
        print("   ✗ Signal is too weak for reliable confidence tiers (< 1.5x)")

except ImportError as e:
    print(f"   Skipping Random Forest (missing dependency): {e}")
except Exception as e:
    print(f"   Error in Random Forest: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# STEP 8: CATEGORY BREAKDOWN
# =============================================================================
print("\n[10] Step 8: Category Breakdown...")

try:
    category_results = []

    for category in df['category'].unique():
        cat_df = df[df['category'] == category]
        if len(cat_df) < 200:
            continue

        # Prepare for regression
        cat_reg = cat_df[['market_id', 'residual_error_t', 'spread_t', 'log_depth_t',
                          'depth_imbalance_t', 'abs_depth_imbalance_t', 'life_fraction', 'price_extremity']].dropna()

        if len(cat_reg) < 100:
            continue

        # Standardize
        for col in ['spread_t', 'log_depth_t', 'depth_imbalance_t', 'abs_depth_imbalance_t', 'life_fraction', 'price_extremity']:
            cat_reg[col + '_z'] = (cat_reg[col] - cat_reg[col].mean()) / (cat_reg[col].std() + 1e-10)

        cat_reg['market_id_num'] = pd.factorize(cat_reg['market_id'])[0]

        X = cat_reg[['spread_t_z', 'log_depth_t_z', 'depth_imbalance_t_z',
                     'abs_depth_imbalance_t_z', 'life_fraction_z', 'price_extremity_z']]
        X = sm.add_constant(X)
        y = cat_reg['residual_error_t']

        model = OLS(y, X.astype(float)).fit()
        clustered = model.get_robustcov_results(cov_type='cluster', groups=cat_reg['market_id_num'])

        # Find significant features
        X_cols = list(X.columns)
        sig_features = []
        for var in ['spread_t_z', 'log_depth_t_z', 'depth_imbalance_t_z', 'abs_depth_imbalance_t_z']:
            if var in X_cols:
                idx = X_cols.index(var)
                if clustered.pvalues[idx] < 0.05:
                    sig_features.append(f"{var.replace('_z', '')}({clustered.params[idx]:.3f})")

        category_results.append({
            'Category': category,
            'N_Snapshots': len(cat_reg),
            'N_Markets': cat_reg['market_id'].nunique(),
            'R²': model.rsquared,
            'Significant_Features': ', '.join(sig_features) if sig_features else 'None'
        })

    cat_results_df = pd.DataFrame(category_results).sort_values('N_Snapshots', ascending=False)

    print("\n   Category Breakdown:")
    print("   " + "-" * 90)
    print(f"   {'Category':<25s} {'N_Snap':>10s} {'N_Mkts':>8s} {'R²':>8s} {'Significant Features'}")
    print("   " + "-" * 90)
    for _, row in cat_results_df.iterrows():
        print(f"   {row['Category']:<25s} {row['N_Snapshots']:>10,d} {row['N_Markets']:>8d} {row['R²']:>8.4f} {row['Significant_Features']}")

except Exception as e:
    print(f"   Error in category breakdown: {e}")

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

# Determine best signal
best_feature = univariate_df.iloc[0]['Feature'] if len(univariate_df) > 0 else 'None'
best_r = univariate_df.iloc[0]['Spearman_r'] if len(univariate_df) > 0 else 0

print(f"""
Given the category of a market, its current price, and observable liquidity metrics,
we {'CAN' if abs(best_r) > 0.05 and test_r2 > 0.01 else 'CANNOT'} meaningfully predict whether the price is more or less accurate
than expected.

Key findings:
- Strongest univariate signal: {best_feature} (r = {best_r:.4f})
- Random Forest test R² on held-out markets: {test_r2:.4f}
- Panel regression R²: {model1.rsquared:.4f} (with clustered SEs)
- Confidence tier improvement ratio: {improvement_ratio:.2f}x

This {'IS' if improvement_ratio > 1.5 else 'IS NOT'} strong enough to power a confidence indicator on the Bellwether
market card. The signal explains only {test_r2*100:.1f}% of the variance in residual accuracy.
""")

# Save results
results = {
    'n_snapshots': len(df),
    'n_markets': df['market_id'].nunique(),
    'validation': {
        'residual_vs_price_corr': corr_price,
        'residual_vs_extremity_corr': corr_extremity
    },
    'univariate_screening': univariate_df.to_dict('records'),
    'random_forest': {
        'test_r2': float(test_r2),
        'oob_r2': float(oob_r2),
        'mae': float(mae)
    },
    'improvement_ratio': float(improvement_ratio),
    'conclusion': 'weak_signal' if improvement_ratio < 1.5 else 'viable_signal'
}

with open(OUTPUT_DIR / 'timeseries_analysis_results.json', 'w') as f:
    json.dump(results, f, indent=2)
print(f"\nResults saved to: {OUTPUT_DIR / 'timeseries_analysis_results.json'}")
