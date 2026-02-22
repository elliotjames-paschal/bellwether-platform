#!/usr/bin/env python3
"""
Fetch V-Dem Democracy Index Data

Processes the V-Dem dataset and generates a JSON file with democracy scores
for choropleth rendering on the globe.

Usage:
    python fetch_vdem_data.py

Output:
    website/data/vdem_scores.json
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, WEBSITE_DIR

# V-Dem data file
VDEM_FILE = DATA_DIR / "vdem" / "V-Dem-CY-Full+Others-v15.csv"
OUTPUT_FILE = WEBSITE_DIR / "data" / "vdem_scores.json"

# Key V-Dem indices to include
VDEM_INDICES = {
    # Core Democracy Indices
    'v2x_polyarchy': {
        'name': 'Electoral Democracy',
        'description': 'Measures responsiveness of rulers to citizens through electoral competition, free and fair elections, and freedom of association',
        'invert': False,
    },
    'v2x_libdem': {
        'name': 'Liberal Democracy',
        'description': 'Combines electoral democracy with rule of law, civil liberties, independent judiciary, and limits on executive power',
        'invert': False,
    },
    'v2x_partipdem': {
        'name': 'Participatory Democracy',
        'description': 'Emphasizes citizen participation beyond elections: civil society, direct democracy, local government',
        'invert': False,
    },
    'v2x_delibdem': {
        'name': 'Deliberative Democracy',
        'description': 'Emphasizes reasoned debate, justification of decisions, and respect for counterarguments in political discourse',
        'invert': False,
    },
    'v2x_egaldem': {
        'name': 'Egalitarian Democracy',
        'description': 'Focus on equal distribution of political power and resources across social groups',
        'invert': False,
    },
    # Elections & Governance
    'v2xel_frefair': {
        'name': 'Clean Elections',
        'description': 'Electoral integrity - free and fair elections without fraud, intimidation, or irregularities',
        'invert': False,
    },
    'v2x_rule': {
        'name': 'Rule of Law',
        'description': 'Transparent, predictable, and impartially enforced laws that apply equally to all',
        'invert': False,
    },
    'v2x_jucon': {
        'name': 'Judicial Constraints',
        'description': 'Extent to which executive power is constrained by an independent judiciary',
        'invert': False,
    },
    'v2x_corr': {
        'name': 'Political Corruption',
        'description': 'Corruption in public sector, executive, legislature, and judiciary',
        'invert': True,  # Higher is worse
    },
    # Rights & Freedoms
    'v2x_civlib': {
        'name': 'Civil Liberties',
        'description': 'Personal freedoms including movement, religion, expression, and personal autonomy',
        'invert': False,
    },
    'v2x_freexp_altinf': {
        'name': 'Freedom of Expression',
        'description': 'Freedom of expression and access to alternative sources of information',
        'invert': False,
    },
    'v2x_clphy': {
        'name': 'Physical Integrity',
        'description': 'Freedom from political killings, torture, and physical violence by the state',
        'invert': False,
    },
    'v2x_clpriv': {
        'name': 'Private Liberties',
        'description': 'Freedom from government interference in private life, property rights',
        'invert': False,
    },
    # Society & Inclusion
    'v2x_gender': {
        'name': 'Women\'s Empowerment',
        'description': 'Women\'s political empowerment including civil liberties, participation, and representation',
        'invert': False,
    },
    'v2xcs_ccsi': {
        'name': 'Civil Society',
        'description': 'Strength and independence of civil society organizations',
        'invert': False,
    },
    'v2xeg_eqdr': {
        'name': 'Equal Distribution',
        'description': 'Equal distribution of political power across social groups',
        'invert': False,
    },
    # Media & Information
    'v2xme_altinf': {
        'name': 'Media Freedom',
        'description': 'Availability of alternative sources of information and media independence',
        'invert': False,
    },
    'v2x_freexp': {
        'name': 'Freedom of Expression (narrow)',
        'description': 'Freedom of academic and cultural expression, discussion of political issues',
        'invert': False,
    },
}

# Country name mapping (V-Dem name -> ISO 3166 / common name)
COUNTRY_NAME_MAP = {
    'United States of America': 'United States',
    'United Kingdom': 'United Kingdom',
    'Russia': 'Russia',
    'South Korea': 'South Korea',
    'North Korea': 'North Korea',
    'Czech Republic': 'Czech Republic',
    'Czechia': 'Czech Republic',
    'Burma/Myanmar': 'Myanmar',
    'Democratic Republic of the Congo': 'DR Congo',
    'Republic of the Congo': 'Congo',
    'Ivory Coast': 'Ivory Coast',
    'Cote d\'Ivoire': 'Ivory Coast',
}


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def get_latest_year_data(df, year=None):
    """Get the most recent year's data for each country."""
    if year is None:
        year = df['year'].max()

    latest = df[df['year'] == year].copy()
    log(f"  Using data from year {year} ({len(latest)} countries)")
    return latest


def normalize_country_name(name):
    """Normalize country name for matching."""
    return COUNTRY_NAME_MAP.get(name, name)


def generate_vdem_scores():
    """Generate V-Dem democracy scores JSON for globe choropleth."""
    log("Generating V-Dem democracy scores...")

    if not VDEM_FILE.exists():
        log(f"  ERROR: V-Dem data file not found: {VDEM_FILE}")
        log("  Please download from https://v-dem.net/data/")
        return

    # Load V-Dem data
    log(f"  Loading V-Dem data from {VDEM_FILE.name}...")

    # Only load the columns we need
    cols_needed = ['country_name', 'country_text_id', 'year'] + list(VDEM_INDICES.keys())
    df = pd.read_csv(VDEM_FILE, low_memory=False, usecols=cols_needed)

    log(f"  Loaded {len(df):,} country-year observations")
    log(f"  Years: {df['year'].min()} - {df['year'].max()}")
    log(f"  Countries: {df['country_name'].nunique()}")

    # Get latest year data (usually current year or previous)
    latest_year = df['year'].max()
    latest = get_latest_year_data(df, latest_year)

    # Build country scores
    country_scores = {}

    for _, row in latest.iterrows():
        country_name = normalize_country_name(row['country_name'])
        country_code = row['country_text_id']

        scores = {}
        for idx_key, idx_info in VDEM_INDICES.items():
            value = row.get(idx_key)
            if pd.notna(value):
                scores[idx_key] = {
                    'value': round(float(value), 3),
                    'name': idx_info['name'],
                }

        if scores:
            country_scores[country_name] = {
                'code': country_code,
                'year': int(latest_year),
                'scores': scores,
            }

    log(f"  Processed {len(country_scores)} countries with scores")

    # Build output
    output = {
        'generated_at': datetime.now().isoformat(),
        'source': 'V-Dem v15',
        'source_url': 'https://v-dem.net/',
        'data_year': int(latest_year),
        'indices': {k: {
            'name': v['name'],
            'description': v['description'],
            'invert': v['invert'],
        } for k, v in VDEM_INDICES.items()},
        'countries': country_scores,
    }

    # Save output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    log(f"  Saved to {OUTPUT_FILE}")

    # Show sample
    log("  Sample scores:")
    for country in ['United States', 'Germany', 'China', 'Russia', 'Brazil'][:5]:
        if country in country_scores:
            scores = country_scores[country]['scores']
            dem = scores.get('v2x_polyarchy', {}).get('value', 'N/A')
            corr = scores.get('v2x_corr', {}).get('value', 'N/A')
            log(f"    {country}: Democracy={dem}, Corruption={corr}")


if __name__ == "__main__":
    generate_vdem_scores()
