#!/usr/bin/env python3
"""
Generate Web Data for Dashboard

Creates JSON files from analysis outputs for the static website.
These JSON files are loaded by the dashboard JavaScript to render charts.
"""

import pandas as pd
import numpy as np
import json
import os
import re
import glob
from datetime import datetime

# Import audit system
try:
    import sys as _sys_audit
    _sys_audit.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from audit.audit_validator import DataValidator
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False

from category_utils import (
    CATEGORY_DISPLAY_NAMES, CATEGORY_COLORS, OLD_TO_NEW_CATEGORY,
    format_category_name, old_to_new_category
)

# Paths
import sys as _sys
_sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR, WEBSITE_DIR
try:
    from paper_config import PAPER_DATA_DIR
    if str(PAPER_DATA_DIR) != str(DATA_DIR):
        print(f"WARNING: PAPER_DATA_DIR ({PAPER_DATA_DIR}) != DATA_DIR ({DATA_DIR}). "
              "Some analysis outputs may be in PAPER_DATA_DIR. "
              "Run analysis scripts without BELLWETHER_OUTPUT_DIR to write to DATA_DIR.")
except ImportError:
    PAPER_DATA_DIR = DATA_DIR
WEB_DATA_DIR = str(WEBSITE_DIR / "data")

# Ensure output directory exists
os.makedirs(WEB_DATA_DIR, exist_ok=True)


# ============================================================
# TICKER DATA LOADERS
# ============================================================

_ticker_data_cache = None
_market_map_cache = None


def load_ticker_data():
    """Load market_id -> ticker dict from postprocessed tickers.

    Returns dict mapping market_id (str) -> ticker record with category, action, etc.
    Returns empty dict if file doesn't exist (fallback to old system).
    """
    global _ticker_data_cache
    if _ticker_data_cache is not None:
        return _ticker_data_cache

    tickers_file = os.path.join(str(DATA_DIR), "tickers_postprocessed.json")
    if not os.path.exists(tickers_file):
        log("WARNING: tickers_postprocessed.json not found, falling back to political_category")
        _ticker_data_cache = {}
        return _ticker_data_cache

    with open(tickers_file) as f:
        data = json.load(f)

    mapping = {}
    for t in data.get("tickers", []):
        mid = str(t.get("market_id", ""))
        if mid:
            mapping[mid] = t

    log(f"  Loaded ticker data for {len(mapping):,} markets")
    _ticker_data_cache = mapping
    return mapping


def load_market_map():
    """Load cross-platform market map.

    Returns dict mapping ticker string -> market map entry.
    """
    global _market_map_cache
    if _market_map_cache is not None:
        return _market_map_cache

    map_file = os.path.join(WEB_DATA_DIR, "market_map.json")
    if not os.path.exists(map_file):
        _market_map_cache = {}
        return _market_map_cache

    with open(map_file) as f:
        data = json.load(f)

    # Handle both formats: list of entries or dict with 'markets' key
    if isinstance(data, dict) and 'markets' in data:
        entries = data['markets']
    elif isinstance(data, list):
        entries = data
    else:
        _market_map_cache = data
        return _market_map_cache

    _market_map_cache = {entry.get("ticker", ""): entry for entry in entries}

    return _market_map_cache


def add_ticker_category_column(df, ticker_data=None):
    """Add a 'category' column to a DataFrame using ticker-derived categories.

    Falls back to old political_category if ticker data is unavailable.
    """
    if ticker_data is None:
        ticker_data = load_ticker_data()

    if ticker_data:
        df = df.copy()
        df['category'] = df['market_id'].astype(str).map(
            lambda mid: ticker_data.get(mid, {}).get('category', '')
        )
        # Fallback: if ticker has no category, use old political_category
        if 'political_category' in df.columns:
            mask = df['category'] == ''
            df.loc[mask, 'category'] = df.loc[mask, 'political_category'].map(
                lambda x: OLD_TO_NEW_CATEGORY.get(str(x), 'MISC')
            )
        df.loc[df['category'] == '', 'category'] = 'MISC'
    else:
        # No ticker data — use old system
        if 'political_category' in df.columns:
            df = df.copy()
            df['category'] = df['political_category'].map(
                lambda x: OLD_TO_NEW_CATEGORY.get(str(x), 'MISC')
            )
        else:
            df = df.copy()
            df['category'] = 'MISC'

    return df


# --- Globe election coordinate lookup ---
# Maps (country, location) -> (lat, lng) extracted from existing globe data.
# US House districts (e.g. "AZ-1") are resolved dynamically via US_STATE_ABBREVS.
US_STATE_ABBREVS = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
}

LOCATION_COORDS = {
    ("Albania", "Albania"): (41.15, 20.17),
    ("Algeria", "Algeria"): (36.75, 3.06),
    ("Argentina", "Argentina"): (-34.6, -58.38),
    ("Armenia", "Armenia"): (40.18, 44.51),
    ("Australia", "Australia"): (-33.87, 151.21),
    ("Australia", "Bennelong"): (-33.80, 151.10),
    ("Australia", "Brisbane"): (-27.47, 153.03),
    ("Australia", "Canberra"): (-35.28, 149.13),
    ("Australia", "Melbourne"): (-37.81, 144.96),
    ("Australia", "New South Wales"): (-33.87, 151.21),
    ("Australia", "Northern Territory"): (-12.46, 130.84),
    ("Australia", "Perth"): (-31.95, 115.86),
    ("Australia", "Queensland"): (-27.47, 153.03),
    ("Australia", "South Australia"): (-34.93, 138.60),
    ("Australia", "Sydney"): (-33.87, 151.21),
    ("Australia", "Tasmania"): (-42.88, 147.33),
    ("Australia", "Victoria"): (-37.81, 144.96),
    ("Australia", "Western Australia"): (-31.95, 115.86),
    ("Austria", "Austria"): (48.21, 16.37),
    ("Bangladesh", "Bangladesh"): (23.81, 90.41),
    ("Belarus", "Belarus"): (53.9, 27.57),
    ("Belgium", "Belgium"): (50.85, 4.35),
    ("Bolivia", "Bolivia"): (-16.5, -68.15),
    ("Bolivia", "Cochabamba"): (-17.39, -66.16),
    ("Bolivia", "La Paz"): (-16.5, -68.15),
    ("Bolivia", "Santa Cruz"): (-16.5, -68.15),
    ("Bolivia", "Santa Cruz de la Sierra"): (-17.78, -63.18),
    ("Bolivia", "Sucre"): (-16.5, -68.15),
    ("Brazil", "Bahia"): (-12.97, -38.51),
    ("Brazil", "Belo Horizonte"): (-19.92, -43.94),
    ("Brazil", "Brasília"): (-15.79, -47.88),
    ("Brazil", "Brazil"): (-23.55, -46.64),
    ("Brazil", "Curitiba"): (-25.43, -49.27),
    ("Brazil", "Fortaleza"): (-3.72, -38.54),
    ("Brazil", "Minas Gerais"): (-19.92, -43.94),
    ("Brazil", "Paraná"): (-25.43, -49.27),
    ("Brazil", "Pernambuco"): (-8.05, -34.88),
    ("Brazil", "Porto Alegre"): (-30.03, -51.23),
    ("Brazil", "Recife"): (-8.05, -34.88),
    ("Brazil", "Rio de Janeiro"): (-22.91, -43.17),
    ("Brazil", "Rio Grande do Sul"): (-30.03, -51.23),
    ("Brazil", "Salvador"): (-12.97, -38.51),
    ("Brazil", "Santa Catarina"): (-27.60, -48.55),
    ("Brazil", "São Paulo"): (-23.55, -46.64),
    ("Bulgaria", "Bulgaria"): (42.7, 23.32),
    ("Burkina Faso", "Burkina Faso"): (12.37, -1.52),
    ("Burundi", "Burundi"): (-3.38, 29.36),
    ("Cameroon", "Cameroon"): (3.87, 11.52),
    ("Canada", "Alberta"): (53.93, -116.58),
    ("Canada", "Atlantic Canada"): (46.50, -66.16),
    ("Canada", "British Columbia"): (49.28, -123.12),
    ("Canada", "Calgary Centre"): (51.05, -114.07),
    ("Canada", "Canada"): (45.42, -75.69),
    ("Canada", "Chicoutimi—Le Fjord"): (48.43, -71.07),
    ("Canada", "Fundy Royal"): (45.27, -66.06),
    ("Canada", "Hamilton Centre"): (43.26, -79.87),
    ("Canada", "Kildonan—St. Paul"): (49.90, -97.14),
    ("Canada", "Kitchener South—Hespeler"): (43.45, -80.49),
    ("Canada", "La Prairie—Atateken"): (45.42, -73.50),
    ("Canada", "London-Fanshawe"): (42.98, -81.25),
    ("Canada", "Manitoba"): (49.90, -97.14),
    ("Canada", "New Brunswick"): (46.50, -66.16),
    ("Canada", "Newfoundland and Labrador"): (47.57, -52.71),
    ("Canada", "Niagara South"): (43.06, -79.29),
    ("Canada", "Northwest Territories"): (62.45, -114.37),
    ("Canada", "Nova Scotia"): (44.65, -63.58),
    ("Canada", "Nunavut"): (63.75, -68.52),
    ("Canada", "Ontario"): (43.65, -79.38),
    ("Canada", "Ottawa"): (45.42, -75.69),
    ("Canada", "Prince Edward Island"): (46.24, -63.13),
    ("Canada", "Quebec"): (46.81, -71.21),
    ("Canada", "Quebec City"): (46.81, -71.21),
    ("Canada", "Regina-Wascana"): (50.45, -104.62),
    ("Canada", "Saint John—St. Croix"): (45.27, -66.06),
    ("Canada", "Saskatchewan"): (52.13, -106.67),
    ("Canada", "Toronto"): (43.65, -79.38),
    ("Canada", "Trois-Rivières"): (46.35, -72.55),
    ("Canada", "Vancouver"): (49.28, -123.12),
    ("Canada", "Windsor West"): (42.32, -83.04),
    ("Canada", "Winnipeg Centre"): (49.90, -97.14),
    ("Canada", "Yukon"): (60.72, -135.05),
    ("Central African Republic", "Central African Republic"): (4.36, 18.56),
    ("Chad", "Chad"): (12.13, 15.06),
    ("Chile", "Chile"): (-33.45, -70.67),
    ("China", "Beijing"): (39.90, 116.40),
    ("China", "China"): (31.22, 121.46),
    ("China", "Chongqing"): (29.56, 106.55),
    ("China", "Guangdong"): (23.13, 113.26),
    ("China", "Guangzhou"): (23.13, 113.26),
    ("China", "Hong Kong"): (22.28, 114.16),
    ("China", "Shanghai"): (31.22, 121.46),
    ("China", "Shenzhen"): (22.54, 114.06),
    ("China", "Sichuan"): (30.57, 104.07),
    ("China", "Tibet"): (29.65, 91.10),
    ("China", "Xinjiang"): (43.79, 87.60),
    ("China", "Zhejiang"): (30.27, 120.15),
    ("Colombia", "Colombia"): (4.71, -74.07),
    ("Costa Rica", "Costa Rica"): (9.93, -84.08),
    ("Croatia", "Croatia"): (45.81, 15.98),
    ("Cuba", "Cuba"): (23.11, -82.37),
    ("Cyprus", "Cyprus"): (35.17, 33.36),
    ("Czech Republic", "Czech Republic"): (50.08, 14.44),
    ("Democratic Republic of the Congo", "Democratic Republic of the Congo"): (-4.44, 15.27),
    ("Denmark", "Denmark"): (55.68, 12.57),
    ("Dominican Republic", "Dominican Republic"): (18.47, -69.9),
    ("Ecuador", "Ecuador"): (-0.18, -78.47),
    ("Egypt", "Alexandria"): (31.20, 29.92),
    ("Egypt", "Cairo"): (30.04, 31.24),
    ("Egypt", "Egypt"): (30.04, 31.24),
    ("Egypt", "Giza"): (30.01, 31.21),
    ("El Salvador", "El Salvador"): (13.69, -89.22),
    ("Ethiopia", "Ethiopia"): (9.01, 38.75),
    ("European Union", "European Union"): (50.84, 4.37),
    ("Finland", "Finland"): (60.17, 24.94),
    ("France", "France"): (48.86, 2.35),
    ("France", "Lyon"): (45.76, 4.84),
    ("France", "Marseille"): (43.3, 5.37),
    ("France", "Nice"): (43.71, 7.26),
    ("France", "Paris"): (48.86, 2.35),
    ("Gabon", "Gabon"): (0.39, 9.45),
    ("Gambia", "Gambia"): (13.45, -16.58),
    ("Georgia", "Georgia"): (41.72, 44.79),
    ("Germany", "Baden-Württemberg"): (48.78, 9.18),
    ("Germany", "Bavaria"): (48.14, 11.58),
    ("Germany", "Berlin"): (52.52, 13.41),
    ("Germany", "Brandenburg"): (52.41, 13.07),
    ("Germany", "Bremen"): (53.08, 8.81),
    ("Germany", "Frankfurt"): (50.11, 8.68),
    ("Germany", "Germany"): (52.52, 13.41),
    ("Germany", "Hamburg"): (53.55, 9.99),
    ("Germany", "Hesse"): (50.11, 8.68),
    ("Germany", "Lower Saxony"): (52.37, 9.74),
    ("Germany", "Munich"): (48.14, 11.58),
    ("Germany", "North Rhine-Westphalia"): (51.23, 6.78),
    ("Germany", "Rhineland-Palatinate"): (49.99, 8.25),
    ("Germany", "Saxony"): (51.05, 13.74),
    ("Germany", "Schleswig-Holstein"): (54.32, 10.14),
    ("Germany", "Thuringia"): (50.98, 11.03),
    ("Ghana", "Ghana"): (5.56, -0.19),
    ("Greece", "Greece"): (37.98, 23.73),
    ("Greenland", "Greenland"): (64.17, -51.74),
    ("Guatemala", "Guatemala"): (14.63, -90.51),
    ("Guinea", "Guinea"): (9.65, -13.58),
    ("Guinea-Bissau", "Guinea-Bissau"): (11.86, -15.6),
    ("Guyana", "Guyana"): (6.8, -58.16),
    ("Honduras", "Honduras"): (14.07, -87.19),
    ("Hungary", "Hungary"): (47.5, 19.04),
    ("Iceland", "Iceland"): (64.15, -21.94),
    ("India", "Andhra Pradesh"): (17.39, 78.49),
    ("India", "Assam"): (26.14, 91.74),
    ("India", "Bihar"): (25.60, 85.14),
    ("India", "Chandigarh"): (30.73, 76.78),
    ("India", "Chennai"): (13.08, 80.27),
    ("India", "Delhi"): (28.61, 77.21),
    ("India", "Gujarat"): (23.02, 72.57),
    ("India", "Hyderabad"): (17.39, 78.49),
    ("India", "India"): (19.08, 72.88),
    ("India", "Jammu and Kashmir"): (34.08, 74.80),
    ("India", "Karnataka"): (12.97, 77.59),
    ("India", "Kerala"): (9.93, 76.26),
    ("India", "Kolkata"): (22.57, 88.36),
    ("India", "Madhya Pradesh"): (23.26, 77.41),
    ("India", "Maharashtra"): (19.08, 72.88),
    ("India", "Mumbai"): (19.08, 72.88),
    ("India", "New Delhi"): (28.61, 77.21),
    ("India", "Puducherry"): (11.93, 79.83),
    ("India", "Punjab"): (30.73, 76.78),
    ("India", "Rajasthan"): (26.92, 75.79),
    ("India", "Tamil Nadu"): (13.08, 80.27),
    ("India", "Telangana"): (17.39, 78.49),
    ("India", "Uttar Pradesh"): (26.85, 80.95),
    ("India", "West Bengal"): (22.57, 88.36),
    ("Indonesia", "Bali"): (-8.34, 115.09),
    ("Indonesia", "Indonesia"): (-6.21, 106.85),
    ("Indonesia", "Jakarta"): (-6.21, 106.85),
    ("Indonesia", "Java"): (-6.21, 106.85),
    ("Indonesia", "Surabaya"): (-7.25, 112.75),
    ("Indonesia", "Sumatra"): (3.59, 98.67),
    ("Iran", "Iran"): (35.69, 51.39),
    ("Ireland", "Ireland"): (53.35, -6.26),
    ("Israel", "Israel"): (31.77, 35.22),
    ("Italy", "Calabria"): (41.9, 12.5),
    ("Italy", "Italy"): (41.9, 12.5),
    ("Italy", "Tuscany"): (41.9, 12.5),
    ("Ivory Coast", "Ivory Coast"): (6.85, -5.3),
    ("Jamaica", "Jamaica"): (18.11, -76.79),
    ("Japan", "Fukuoka"): (33.59, 130.40),
    ("Japan", "Hiroshima"): (34.40, 132.46),
    ("Japan", "Japan"): (35.68, 139.69),
    ("Japan", "Kyoto"): (35.01, 135.77),
    ("Japan", "Nagoya"): (35.18, 136.91),
    ("Japan", "Osaka"): (34.69, 135.50),
    ("Japan", "Sapporo"): (43.06, 141.35),
    ("Japan", "Tokyo"): (35.68, 139.69),
    ("Japan", "Yokohama"): (35.44, 139.64),
    ("Kenya", "Kenya"): (-1.29, 36.82),
    ("Kenya", "Mombasa"): (-4.05, 39.67),
    ("Kenya", "Nairobi"): (-1.29, 36.82),
    ("Kosovo", "Kosovo"): (42.66, 21.17),
    ("Latvia", "Latvia"): (56.95, 24.11),
    ("Lebanon", "Lebanon"): (33.89, 35.5),
    ("Liechtenstein", "Liechtenstein"): (47.14, 9.52),
    ("Lithuania", "Lithuania"): (54.69, 25.28),
    ("Malawi", "Malawi"): (-13.97, 33.79),
    ("Malaysia", "Malaysia"): (3.14, 101.69),
    ("Mexico", "Mexico"): (19.43, -99.13),
    ("Moldova", "Moldova"): (47.01, 28.86),
    ("Mongolia", "Mongolia"): (47.89, 106.91),
    ("Mozambique", "Mozambique"): (-25.97, 32.57),
    ("Namibia", "Namibia"): (-22.56, 17.08),
    ("Nepal", "Nepal"): (27.72, 85.32),
    ("Netherlands", "Netherlands"): (52.37, 4.9),
    ("New Zealand", "New Zealand"): (-41.29, 174.78),
    ("Nigeria", "Abuja"): (9.08, 7.49),
    ("Nigeria", "Kaduna"): (10.52, 7.44),
    ("Nigeria", "Kano"): (12.00, 8.52),
    ("Nigeria", "Lagos"): (6.45, 3.39),
    ("Nigeria", "Nigeria"): (6.45, 3.39),
    ("Nigeria", "Port Harcourt"): (4.78, 7.01),
    ("North Korea", "North Korea"): (39.02, 125.75),
    ("North Macedonia", "North Macedonia"): (41.99, 21.43),
    ("Norway", "Norway"): (59.91, 10.75),
    ("Panama", "Panama"): (8.98, -79.52),
    ("Paraguay", "Paraguay"): (-25.26, -57.58),
    ("Peru", "Peru"): (-12.05, -77.04),
    ("Philippines", "Philippines"): (14.6, 120.98),
    ("Poland", "Dolnośląskie"): (52.23, 21.01),
    ("Poland", "Gdańsk"): (52.23, 21.01),
    ("Poland", "Kraków"): (52.23, 21.01),
    ("Poland", "Kujawsko-Pomorskie"): (52.23, 21.01),
    ("Poland", "Lubelskie"): (52.23, 21.01),
    ("Poland", "Lubuskie"): (52.23, 21.01),
    ("Poland", "Mazowieckie"): (52.23, 21.01),
    ("Poland", "Małopolskie"): (52.23, 21.01),
    ("Poland", "Opolskie"): (52.23, 21.01),
    ("Poland", "Podkarpackie"): (52.23, 21.01),
    ("Poland", "Podlaskie"): (52.23, 21.01),
    ("Poland", "Poland"): (52.23, 21.01),
    ("Poland", "Pomorskie"): (52.23, 21.01),
    ("Poland", "Warmińsko-Mazurskie"): (52.23, 21.01),
    ("Poland", "Warsaw"): (52.23, 21.01),
    ("Poland", "Wielkopolskie"): (52.23, 21.01),
    ("Poland", "Wrocław"): (52.23, 21.01),
    ("Poland", "Zachodniopomorskie"): (52.23, 21.01),
    ("Poland", "Łódzkie"): (52.23, 21.01),
    ("Poland", "Łódź"): (52.23, 21.01),
    ("Poland", "Śląskie"): (52.23, 21.01),
    ("Poland", "Świętokrzyskie"): (52.23, 21.01),
    ("Portugal", "Lisbon"): (38.72, -9.14),
    ("Portugal", "Portugal"): (38.72, -9.14),
    ("Republic of the Congo", "Republic of the Congo"): (-4.27, 15.28),
    ("Romania", "Bucharest"): (44.43, 26.1),
    ("Romania", "Romania"): (44.43, 26.1),
    ("Russia", "Chechen Republic"): (43.32, 45.68),
    ("Russia", "Russia"): (55.76, 37.62),
    ("Rwanda", "Rwanda"): (-1.94, 29.87),
    ("Samoa", "Samoa"): (-13.83, -171.76),
    ("Senegal", "Senegal"): (14.72, -17.47),
    ("Serbia", "Serbia"): (44.79, 20.45),
    ("Seychelles", "Seychelles"): (-4.68, 55.47),
    ("Sierra Leone", "Sierra Leone"): (8.48, -13.23),
    ("Singapore", "Singapore"): (1.35, 103.82),
    ("Slovakia", "Slovakia"): (48.15, 17.11),
    ("Slovenia", "Slovenia"): (46.06, 14.51),
    ("Somaliland", "Somaliland"): (9.56, 44.06),
    ("South Africa", "Cape Town"): (-33.93, 18.42),
    ("South Africa", "Durban"): (-29.86, 31.02),
    ("South Africa", "Eastern Cape"): (-33.96, 25.60),
    ("South Africa", "Gauteng"): (-26.20, 28.04),
    ("South Africa", "Johannesburg"): (-26.20, 28.04),
    ("South Africa", "KwaZulu-Natal"): (-29.86, 31.02),
    ("South Africa", "Pretoria"): (-25.75, 28.19),
    ("South Africa", "South Africa"): (-26.20, 28.04),
    ("South Africa", "Western Cape"): (-33.93, 18.42),
    ("South Korea", "Busan"): (35.18, 129.08),
    ("South Korea", "Chungbuk State"): (36.64, 127.49),
    ("South Korea", "Chungcheongbuk Province"): (36.64, 127.49),
    ("South Korea", "Chungcheongnam Province"): (36.66, 126.67),
    ("South Korea", "Chungnam State"): (36.66, 126.67),
    ("South Korea", "Daegu"): (35.87, 128.60),
    ("South Korea", "Daejeon"): (36.35, 127.38),
    ("South Korea", "Gangwon Province"): (37.88, 127.73),
    ("South Korea", "Gangwon State"): (37.88, 127.73),
    ("South Korea", "Gwangju"): (35.16, 126.85),
    ("South Korea", "Gyeongbuk State"): (36.57, 128.51),
    ("South Korea", "Gyeonggi Province"): (37.27, 127.01),
    ("South Korea", "Gyeongnam State"): (35.18, 128.69),
    ("South Korea", "Incheon"): (37.46, 126.71),
    ("South Korea", "Jeju Province"): (33.50, 126.53),
    ("South Korea", "Jeonbuk State"): (35.82, 127.11),
    ("South Korea", "Jeonnam State"): (34.82, 126.89),
    ("South Korea", "Sejong"): (36.48, 127.26),
    ("South Korea", "Seoul"): (37.57, 126.98),
    ("South Korea", "South Korea"): (37.57, 126.98),
    ("South Korea", "Ulsan"): (35.54, 129.31),
    ("Spain", "Aragon"): (41.65, -0.88),
    ("Spain", "Castilla y León"): (41.65, -4.73),
    ("Spain", "Spain"): (40.42, -3.7),
    ("Sri Lanka", "Sri Lanka"): (6.93, 79.85),
    ("Suriname", "Suriname"): (5.82, -55.17),
    ("Sweden", "Sweden"): (59.33, 18.07),
    ("Switzerland", "Switzerland"): (46.95, 7.45),
    ("Syria", "Syria"): (33.51, 36.29),
    ("Taiwan", "Taiwan"): (25.03, 121.57),
    ("Tanzania", "Tanzania"): (-6.79, 39.28),
    ("Thailand", "Thailand"): (13.76, 100.5),
    ("Tonga", "Tonga"): (-21.21, -175.15),
    ("Tunisia", "Tunisia"): (36.81, 10.17),
    ("Turkey", "Istanbul"): (39.93, 32.85),
    ("Turkey", "Turkey"): (39.93, 32.85),
    ("Ukraine", "Ukraine"): (50.45, 30.52),
    ("United Kingdom", "Belfast"): (54.60, -5.93),
    ("United Kingdom", "Birmingham"): (52.48, -1.90),
    ("United Kingdom", "Cardiff"): (51.48, -3.18),
    ("United Kingdom", "Edinburgh"): (55.95, -3.19),
    ("United Kingdom", "England"): (51.51, -0.13),
    ("United Kingdom", "Glasgow"): (55.86, -4.25),
    ("United Kingdom", "Gorton and Denton"): (53.47, -2.16),
    ("United Kingdom", "Leeds"): (53.80, -1.55),
    ("United Kingdom", "Liverpool"): (53.41, -2.98),
    ("United Kingdom", "London"): (51.51, -0.13),
    ("United Kingdom", "Manchester"): (53.48, -2.24),
    ("United Kingdom", "Newcastle"): (54.98, -1.61),
    ("United Kingdom", "Northern Ireland"): (54.60, -5.93),
    ("United Kingdom", "Scotland"): (55.95, -3.19),
    ("United Kingdom", "Sheffield"): (53.38, -1.47),
    ("United Kingdom", "United Kingdom"): (51.51, -0.13),
    ("United Kingdom", "Wales"): (51.48, -3.18),
    ("United States", "Alabama"): (32.36, -86.3),
    ("United States", "Alaska"): (64.2, -152.49),
    ("United States", "Albuquerque"): (35.08, -106.65),
    ("United States", "Arizona"): (34.05, -111.09),
    ("United States", "Arkansas"): (34.8, -92.2),
    ("United States", "Atlanta"): (33.75, -84.39),
    ("United States", "Baltimore"): (38.9, -77.04),
    ("United States", "Boston"): (42.36, -71.06),
    ("United States", "Bronx Borough"): (40.84, -73.86),
    ("United States", "Buffalo"): (42.89, -78.88),
    ("United States", "California"): (36.78, -119.42),
    ("United States", "Charlotte"): (35.23, -80.84),
    ("United States", "Chicago"): (38.9, -77.04),
    ("United States", "Cincinnati"): (39.1, -84.51),
    ("United States", "Clark County"): (38.9, -77.04),
    ("United States", "Cleveland"): (41.5, -81.69),
    ("United States", "Colorado"): (39.55, -105.78),
    ("United States", "Connecticut"): (41.6, -72.76),
    ("United States", "D.C."): (38.9, -77.04),
    ("United States", "Delaware"): (38.91, -75.53),
    ("United States", "Detroit"): (42.33, -83.05),
    ("United States", "Florida"): (27.99, -81.76),
    ("United States", "Fort Worth"): (32.76, -97.33),
    ("United States", "Georgia"): (32.68, -83.22),
    ("United States", "Guam"): (13.44, 144.79),
    ("United States", "Hawaii"): (19.9, -155.58),
    ("United States", "Idaho"): (44.07, -114.74),
    ("United States", "Illinois"): (40.63, -89.4),
    ("United States", "Indiana"): (40.27, -86.13),
    ("United States", "Iowa"): (41.88, -93.1),
    ("United States", "Jersey City"): (40.73, -74.04),
    ("United States", "Kansas"): (39.01, -98.48),
    ("United States", "Kentucky"): (37.67, -84.67),
    ("United States", "King County, WA"): (47.49, -121.84),
    ("United States", "Los Angeles"): (34.05, -118.24),
    ("United States", "Louisiana"): (30.98, -91.96),
    ("United States", "Maine"): (45.37, -69.6),
    ("United States", "Maryland"): (39.05, -76.64),
    ("United States", "Massachusetts"): (42.41, -71.38),
    ("United States", "Miami"): (25.76, -80.19),
    ("United States", "Michigan"): (44.31, -85.6),
    ("United States", "Minneapolis"): (44.98, -93.27),
    ("United States", "Minnesota"): (46.73, -94.69),
    ("United States", "Mississippi"): (32.35, -89.4),
    ("United States", "Missouri"): (38.46, -92.29),
    ("United States", "Montana"): (46.8, -110.36),
    ("United States", "Nebraska"): (41.49, -99.9),
    ("United States", "Nevada"): (38.8, -116.42),
    ("United States", "New Hampshire"): (43.19, -71.57),
    ("United States", "New Jersey"): (40.06, -74.41),
    ("United States", "New Mexico"): (34.52, -105.87),
    ("United States", "New Orleans"): (29.95, -90.07),
    ("United States", "New York"): (42.17, -74.95),
    ("United States", "New York City"): (40.71, -74.01),
    ("United States", "North Carolina"): (35.76, -79.02),
    ("United States", "North Dakota"): (47.55, -101.0),
    ("United States", "Oakland"): (37.8, -122.27),
    ("United States", "Ohio"): (40.42, -82.91),
    ("United States", "Oklahoma"): (35.47, -97.52),
    ("United States", "Omaha"): (41.26, -95.94),
    ("United States", "Oregon"): (43.8, -120.55),
    ("United States", "Pennsylvania"): (41.2, -77.19),
    ("United States", "Philadelphia"): (39.95, -75.17),
    ("United States", "Pittsburgh"): (40.44, -80.0),
    ("United States", "Rhode Island"): (41.58, -71.48),
    ("United States", "San Antonio"): (38.9, -77.04),
    ("United States", "San Francisco"): (37.77, -122.42),
    ("United States", "Seattle"): (47.61, -122.33),
    ("United States", "South Carolina"): (33.84, -81.16),
    ("United States", "South Dakota"): (43.97, -99.9),
    ("United States", "Tennessee"): (35.52, -86.58),
    ("United States", "Texas"): (31.97, -99.9),
    ("United States", "United States"): (38.9, -77.04),
    ("United States", "Utah"): (39.32, -111.09),
    ("United States", "Vermont"): (44.56, -72.58),
    ("United States", "Virginia"): (37.43, -78.66),
    ("United States", "Washington"): (47.75, -120.74),
    ("United States", "Washington D.C."): (38.9, -77.04),
    ("United States", "Washington, D.C."): (38.9, -77.04),
    ("United States", "West Virginia"): (38.6, -80.45),
    ("United States", "Wisconsin"): (43.78, -88.79),
    ("United States", "Wyoming"): (43.08, -107.29),
    ("Uruguay", "Uruguay"): (-34.88, -56.18),
    ("Vatican City", "Vatican City"): (41.9, 12.45),
    ("Venezuela", "Sucre"): (10.46, -64.17),
    ("Venezuela", "Venezuela"): (10.49, -66.88),
    ("Vietnam", "Vietnam"): (21.03, 105.85),
    ("Zambia", "Zambia"): (-15.39, 28.32),
    ("Zimbabwe", "Zimbabwe"): (-17.83, 31.05),
}

def get_latest_file(pattern):
    """Find the most recent file matching the pattern.

    First tries pattern without timestamp, then with timestamp.
    """
    # Try fixed filename first (new format)
    fixed_name = pattern.replace('_*.csv', '.csv')
    fixed_path = f"{DATA_DIR}/{fixed_name}"
    if os.path.exists(fixed_path):
        return fixed_path

    # Fall back to timestamped pattern
    files = glob.glob(f"{DATA_DIR}/{pattern}")
    if not files:
        return None
    return max(files, key=os.path.getmtime)

def log(msg):
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    except UnicodeEncodeError:
        safe_msg = msg.encode('ascii', 'replace').decode('ascii')
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {safe_msg}")

def safe_round(val, decimals=4):
    """Round a value, returning 0 if NaN/Inf/None."""
    if val is None:
        return 0
    try:
        f = float(val)
        return round(f, decimals) if np.isfinite(f) else 0
    except (ValueError, TypeError):
        return 0

def generate_summary_stats():
    """Generate summary statistics for the dashboard header."""
    log("Generating summary stats...")

    # Load master data
    df = pd.read_csv(f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv", low_memory=False)

    # Load prediction accuracy data (find most recent files)
    pm_file = get_latest_file("polymarket_prediction_accuracy_all_political*.csv")
    kalshi_file = get_latest_file("kalshi_prediction_accuracy_all_political*.csv")

    if not pm_file or not kalshi_file:
        log("  ⚠ No prediction accuracy files found")
        return None

    log(f"  Using: {os.path.basename(pm_file)}")
    pm_pred = pd.read_csv(pm_file)
    kalshi_pred = pd.read_csv(kalshi_file)

    # Calculate stats
    pm_count = len(df[df['platform'] == 'Polymarket'])
    kalshi_count = len(df[df['platform'] == 'Kalshi'])

    # Brier scores (1 day before), filtered to shared categories for fair comparison
    pm_1d = pm_pred[pm_pred['days_before_event'] == 1].copy()
    kalshi_1d = kalshi_pred[kalshi_pred['days_before_event'] == 1].copy()

    # Map categories onto predictions for fair comparison
    df_cats = add_ticker_category_column(df.copy())
    cat_lookup = dict(zip(df_cats['market_id'].astype(str), df_cats['category']))
    pm_1d['category'] = pm_1d['market_id'].astype(str).map(lambda mid: cat_lookup.get(str(mid), 'MISC'))
    kalshi_1d['category'] = kalshi_1d['ticker'].astype(str).map(lambda mid: cat_lookup.get(str(mid), 'MISC'))

    # Only compare categories where both platforms have predictions
    pm_cats = set(pm_1d['category'].unique())
    kalshi_cats = set(kalshi_1d['category'].unique())
    shared_cats = pm_cats & kalshi_cats

    pm_shared = pm_1d[pm_1d['category'].isin(shared_cats)]
    kalshi_shared = kalshi_1d[kalshi_1d['category'].isin(shared_cats)]

    pm_brier = pm_shared['brier_score'].mean() if len(pm_shared) > 0 else None
    kalshi_brier = kalshi_shared['brier_score'].mean() if len(kalshi_shared) > 0 else None

    # Combined Brier score
    combined_1d = pd.concat([pm_shared, kalshi_shared])
    combined_brier = combined_1d['brier_score'].mean() if len(combined_1d) > 0 else None

    # Electoral markets (use ticker category if available)
    df = add_ticker_category_column(df)
    electoral = df[df['category'] == 'ELEC']

    # Count unique elections (group by country, location, office, year, is_primary)
    election_cols = ['country', 'location', 'office', 'election_year', 'is_primary']
    elections_with_data = electoral[electoral['country'].notna() & electoral['office'].notna()]
    unique_elections = elections_with_data.drop_duplicates(subset=election_cols)

    # Count overlapping elections (elections with markets on BOTH platforms)
    pm_electoral = electoral[electoral['platform'] == 'Polymarket']
    kalshi_electoral = electoral[electoral['platform'] == 'Kalshi']
    pm_elections = set(pm_electoral[pm_electoral['country'].notna()].apply(
        lambda r: (r['country'], r.get('location', ''), r['office'], r.get('election_year', ''), r.get('is_primary', False)), axis=1
    ))
    kalshi_elections = set(kalshi_electoral[kalshi_electoral['country'].notna()].apply(
        lambda r: (r['country'], r.get('location', ''), r['office'], r.get('election_year', ''), r.get('is_primary', False)), axis=1
    ))
    overlapping_elections = len(pm_elections & kalshi_elections)

    # Resolved markets (closed with known outcome)
    resolved_count = int(df['is_closed'].sum()) if 'is_closed' in df.columns else 0

    # Directional accuracy: % of markets where price direction matched outcome
    # Get final price for each market (closest to event)
    def compute_accuracy(pred_df, id_col):
        final = pred_df.loc[pred_df.groupby(id_col)['days_before_event'].idxmin()]
        def is_correct(row):
            price = row['prediction_price']
            outcome = row['actual_outcome']
            if price > 0.5:
                return outcome == 1
            elif price < 0.5:
                return outcome == 0
            return None  # Exclude 50/50
        final['correct'] = final.apply(is_correct, axis=1)
        valid = final['correct'].dropna()
        return valid.sum(), len(valid)

    pm_correct, pm_total = compute_accuracy(pm_pred, 'market_id')
    kalshi_correct, kalshi_total = compute_accuracy(kalshi_pred, 'ticker')
    total_correct = pm_correct + kalshi_correct
    total_markets_for_acc = pm_total + kalshi_total
    directional_accuracy = (total_correct / total_markets_for_acc * 100) if total_markets_for_acc > 0 else None

    # Price observations (count across both price history files)
    price_obs = 0
    pm_prices_file = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED.json"
    kalshi_prices_file = f"{DATA_DIR}/kalshi_all_political_prices_CORRECTED_v3.json"
    for pf in [pm_prices_file, kalshi_prices_file]:
        if os.path.exists(pf):
            with open(pf, 'r') as pfile:
                prices = json.load(pfile)
                price_obs += sum(len(v) if isinstance(v, list) else 1 for v in prices.values())

    summary = {
        'last_updated': datetime.now().isoformat(),
        'total_markets': pm_count + kalshi_count,
        'polymarket_markets': pm_count,
        'kalshi_markets': kalshi_count,
        'resolved_markets': resolved_count,
        'price_observations': price_obs,
        'polymarket_brier': round(pm_brier, 4) if pm_brier else None,
        'kalshi_brier': round(kalshi_brier, 4) if kalshi_brier else None,
        'combined_brier': round(combined_brier, 4) if combined_brier else None,
        'brier_shared_categories': len(shared_cats),
        'brier_pm_n': len(pm_shared),
        'brier_kalshi_n': len(kalshi_shared),
        'electoral_markets': len(electoral),
        'unique_elections': len(unique_elections),
        'overlapping_elections': overlapping_elections,
        'directional_accuracy': round(directional_accuracy, 1) if directional_accuracy else None,
        'accuracy_markets': total_markets_for_acc
    }

    with open(f"{WEB_DATA_DIR}/summary.json", 'w') as f:
        json.dump(summary, f, indent=2, allow_nan=False)

    log(f"  ✓ Summary stats saved")
    return summary

def generate_brier_by_category():
    """Generate Brier score by political category data."""
    log("Generating Brier by category...")

    # Load master data for category info
    master_df = pd.read_csv(f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv", low_memory=False)

    pm_file = get_latest_file("polymarket_prediction_accuracy_all_political*.csv")
    kalshi_file = get_latest_file("kalshi_prediction_accuracy_all_political*.csv")
    if not pm_file or not kalshi_file:
        log("  ⚠ No prediction accuracy files found")
        return

    pm_pred = pd.read_csv(pm_file)
    kalshi_pred = pd.read_csv(kalshi_file, low_memory=False)

    # Filter to 1 day before
    pm_1d = pm_pred[pm_pred['days_before_event'] == 1].copy()
    kalshi_1d = kalshi_pred[kalshi_pred['days_before_event'] == 1].copy()

    # Convert IDs to string for merging
    pm_1d['market_id'] = pm_1d['market_id'].astype(str)
    kalshi_1d['ticker'] = kalshi_1d['ticker'].astype(str)
    master_df['market_id'] = master_df['market_id'].astype(str)

    # Add ticker-derived categories using unified category column
    master_df_cats = add_ticker_category_column(master_df)
    cat_lookup = dict(zip(master_df_cats['market_id'].astype(str), master_df_cats['category']))

    pm_1d['category'] = pm_1d['market_id'].map(
        lambda mid: cat_lookup.get(str(mid), 'MISC')
    )
    kalshi_1d['category'] = kalshi_1d['ticker'].map(
        lambda mid: cat_lookup.get(str(mid), 'MISC')
    )

    n_missing_pm = (pm_1d['category'] == '').sum() + (pm_1d['category'] == 'MISC').sum()
    n_missing_k = (kalshi_1d['category'] == '').sum() + (kalshi_1d['category'] == 'MISC').sum()
    if n_missing_pm > 0 or n_missing_k > 0:
        log(f"  Note: {n_missing_pm} PM and {n_missing_k} Kalshi predictions categorized as MISC")

    # Group by category
    pm_by_cat = pm_1d.groupby('category').agg({
        'brier_score': 'mean',
        'market_id': 'count'
    }).rename(columns={'market_id': 'count'})

    kalshi_by_cat = kalshi_1d.groupby('category').agg({
        'brier_score': 'mean',
        'ticker': 'count'
    }).rename(columns={'ticker': 'count'})

    # Combine
    categories = sorted(set(pm_by_cat.index) | set(kalshi_by_cat.index))

    data = {
        'categories': [],
        'polymarket': {'brier': [], 'count': []},
        'kalshi': {'brier': [], 'count': []}
    }

    MIN_N = 5  # Suppress Brier scores from tiny samples

    for cat in categories:
        # Format category name for display
        clean_cat = format_category_name(cat)

        pm_n = int(pm_by_cat.loc[cat, 'count']) if cat in pm_by_cat.index else 0
        k_n = int(kalshi_by_cat.loc[cat, 'count']) if cat in kalshi_by_cat.index else 0

        data['categories'].append(clean_cat)
        data['polymarket']['brier'].append(
            round(pm_by_cat.loc[cat, 'brier_score'], 4) if cat in pm_by_cat.index and pm_n >= MIN_N else None
        )
        data['polymarket']['count'].append(pm_n)
        data['kalshi']['brier'].append(
            round(kalshi_by_cat.loc[cat, 'brier_score'], 4) if cat in kalshi_by_cat.index and k_n >= MIN_N else None
        )
        data['kalshi']['count'].append(k_n)

    with open(f"{WEB_DATA_DIR}/brier_by_category.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Brier by category saved ({len(categories)} categories)")

def generate_brier_by_election_type():
    """Generate Brier score by election type data."""
    log("Generating Brier by election type...")

    pm_file = get_latest_file("polymarket_prediction_accuracy_all_political*.csv")
    kalshi_file = get_latest_file("kalshi_prediction_accuracy_all_political*.csv")
    if not pm_file or not kalshi_file:
        log("  ⚠ No prediction accuracy files found")
        return

    pm_pred = pd.read_csv(pm_file)
    kalshi_pred = pd.read_csv(kalshi_file, low_memory=False)

    # Filter to 1 day before and rows with election type
    pm_electoral = pm_pred[(pm_pred['days_before_event'] == 1) & (pm_pred['election_type'].notna())].copy()
    kalshi_electoral = kalshi_pred[(kalshi_pred['days_before_event'] == 1) & (kalshi_pred['election_type'].notna())].copy()

    # Group
    pm_by_type = pm_electoral.groupby('election_type').agg({
        'brier_score': 'mean',
        'market_id': 'count'
    }).rename(columns={'market_id': 'count'})

    kalshi_by_type = kalshi_electoral.groupby('election_type').agg({
        'brier_score': 'mean',
        'ticker': 'count'
    }).rename(columns={'ticker': 'count'})

    # Combine
    types = sorted(set(pm_by_type.index) | set(kalshi_by_type.index))

    data = {
        'election_types': [],
        'polymarket': {'brier': [], 'count': []},
        'kalshi': {'brier': [], 'count': []}
    }

    MIN_N = 5  # Suppress Brier scores from tiny samples

    for et in types:
        pm_n = int(pm_by_type.loc[et, 'count']) if et in pm_by_type.index else 0
        k_n = int(kalshi_by_type.loc[et, 'count']) if et in kalshi_by_type.index else 0

        data['election_types'].append(et)
        data['polymarket']['brier'].append(
            round(pm_by_type.loc[et, 'brier_score'], 4) if et in pm_by_type.index and pm_n >= MIN_N else None
        )
        data['polymarket']['count'].append(pm_n)
        data['kalshi']['brier'].append(
            round(kalshi_by_type.loc[et, 'brier_score'], 4) if et in kalshi_by_type.index and k_n >= MIN_N else None
        )
        data['kalshi']['count'].append(k_n)

    with open(f"{WEB_DATA_DIR}/brier_by_election_type.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Brier by election type saved ({len(types)} types)")

def generate_calibration_data():
    """Generate calibration curve data with quantile bins (like paper)."""
    log("Generating calibration data...")

    pm_file = get_latest_file("polymarket_prediction_accuracy_all_political*.csv")
    kalshi_file = get_latest_file("kalshi_prediction_accuracy_all_political*.csv")
    if not pm_file or not kalshi_file:
        log("  ⚠ No prediction accuracy files found")
        return

    pm_pred = pd.read_csv(pm_file)
    kalshi_pred = pd.read_csv(kalshi_file)

    # Filter to 1 day before
    pm_1d = pm_pred[pm_pred['days_before_event'] == 1].copy()
    kalshi_1d = kalshi_pred[kalshi_pred['days_before_event'] == 1].copy()

    # Combine for overall calibration
    combined = pd.concat([pm_1d, kalshi_1d], ignore_index=True)

    if len(combined) < 20:
        log("  Not enough data for calibration")
        return

    # Use quantile bins (equal sample sizes) like the paper
    # Number of bins based on data size (aim for ~160 samples per bin like paper)
    num_bins = len(combined) // 160
    num_bins = max(20, min(num_bins, len(combined)))  # Between 20 and data size

    combined_sorted = combined.sort_values('prediction_price').reset_index(drop=True)
    samples_per_bin = len(combined_sorted) // num_bins
    combined_sorted['bin'] = combined_sorted.index // samples_per_bin
    combined_sorted.loc[combined_sorted['bin'] >= num_bins, 'bin'] = num_bins - 1

    # Calculate bin statistics
    bin_stats = combined_sorted.groupby('bin').agg({
        'prediction_price': 'mean',
        'actual_outcome': ['mean', 'count']
    }).reset_index()
    bin_stats.columns = ['bin', 'predicted', 'actual', 'count']

    # Distribution histogram data (100 bins for smooth curve)
    hist_bins = 100
    pm_hist, _ = np.histogram(pm_1d['prediction_price'].dropna(), bins=hist_bins, range=(0, 1))
    kalshi_hist, _ = np.histogram(kalshi_1d['prediction_price'].dropna(), bins=hist_bins, range=(0, 1))
    combined_hist, _ = np.histogram(combined['prediction_price'].dropna(), bins=hist_bins, range=(0, 1))
    hist_x = [(i + 0.5) / hist_bins for i in range(hist_bins)]

    data = {
        'quantile_bins': {
            'predicted': bin_stats['predicted'].round(4).tolist(),
            'actual': bin_stats['actual'].round(4).tolist(),
            'count': bin_stats['count'].astype(int).tolist()
        },
        'distribution': {
            'x': hist_x,
            'polymarket': pm_hist.tolist(),
            'kalshi': kalshi_hist.tolist(),
            'combined': combined_hist.tolist()
        },
        'total_predictions': len(combined),
        'polymarket_count': len(pm_1d),
        'kalshi_count': len(kalshi_1d)
    }

    with open(f"{WEB_DATA_DIR}/calibration.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Calibration data saved ({num_bins} quantile bins, {len(combined)} total predictions)")

def generate_platform_comparison():
    """Generate platform comparison scatter plot data."""
    log("Generating platform comparison...")

    # Load shared elections data if exists
    shared_file = f"{DATA_DIR}/shared_election_markets_detailed.csv"
    if os.path.exists(shared_file):
        shared = pd.read_csv(shared_file)

        data = {
            'elections': [],
            'polymarket_predictions': [],
            'kalshi_predictions': [],
            'labels': []
        }

        for _, row in shared.iterrows():
            label = f"{int(row['election_year'])} {row['office']} - {row['location']}"
            data['elections'].append(label)
            data['polymarket_predictions'].append(round(row['pm_prediction'], 4))
            data['kalshi_predictions'].append(round(row['kalshi_prediction'], 4))
            data['labels'].append(f"{row['location']} '{str(int(row['election_year']))[2:]}")

        with open(f"{WEB_DATA_DIR}/platform_comparison.json", 'w') as f:
            json.dump(data, f, indent=2, allow_nan=False)

        log(f"  ✓ Platform comparison saved ({len(data['elections'])} elections)")
    else:
        log(f"  ⚠ No shared elections file found")

def generate_brier_convergence():
    """Generate Brier score convergence over time to election - all 4 cohorts."""
    log("Generating Brier convergence...")

    cohort_file = f"{DATA_DIR}/combined_brier_overall_cohorts.csv"
    if not os.path.exists(cohort_file):
        log("  ⚠ No cohort file found")
        return

    df = pd.read_csv(cohort_file)

    # All day columns from far to near
    all_day_cols = ['60d', '30d', '20d', '14d', '12d', '10d', '8d', '7d', '6d', '5d', '4d', '3d', '2d', '1d']

    # All 4 cohorts
    cohorts = ['7d', '14d', '30d', '60d']

    data = {
        'cohorts': {}
    }

    for cohort in cohorts:
        cohort_row = df[df['Cohort'] == cohort]
        if len(cohort_row) == 0:
            continue

        row = cohort_row.iloc[0]
        n = int(row['N']) if pd.notna(row['N']) else 0

        days = []
        scores = []

        for col in all_day_cols:
            if col in row.index and pd.notna(row[col]):
                day_num = int(col.replace('d', ''))
                days.append(day_num)
                scores.append(safe_round(row[col], 4))

        data['cohorts'][cohort] = {
            'n': n,
            'days': days,
            'scores': scores
        }

    with open(f"{WEB_DATA_DIR}/brier_convergence.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Brier convergence saved ({len(data['cohorts'])} cohorts)")

def generate_platform_stats():
    """Generate platform comparison statistics table.

    Computes directly from master CSV so stats are always fresh,
    regardless of whether table_3_platform_comparison.py has run.
    """
    log("Generating platform stats...")

    master_file = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
    if not os.path.exists(master_file):
        log("  ⚠ Master CSV not found, cannot generate platform stats")
        return

    master = pd.read_csv(master_file, low_memory=False)
    master['trading_close_time'] = pd.to_datetime(
        master['trading_close_time'], format='mixed', utc=True, errors='coerce'
    )

    reasonable_cutoff = pd.Timestamp('2030-01-01', tz='UTC')
    metrics = []
    polymarket_vals = []
    kalshi_vals = []

    for platform, vals in [('Polymarket', polymarket_vals), ('Kalshi', kalshi_vals)]:
        plat = master[master['platform'] == platform]

        # Total political markets
        total = len(plat)

        # Resolved markets (is_closed=True OR has winning_outcome)
        resolved = plat[
            (plat['is_closed'] == True) |
            (plat['winning_outcome'].notna())
        ]

        # Electoral markets
        electoral = plat[
            (plat['political_category'].str.startswith('1.', na=False)) |
            (plat['political_category'].str.contains('ELECTORAL', case=False, na=False))
        ]

        # Date range (filter out placeholder dates > 2030)
        dates = plat['trading_close_time'].dropna()
        dates = dates[dates < reasonable_cutoff]
        earliest = dates.min().strftime('%Y-%m-%d') if len(dates) > 0 else 'N/A'
        latest = dates.max().strftime('%Y-%m-%d') if len(dates) > 0 else 'N/A'

        # Election types and categories
        election_types = electoral['election_type'].dropna().nunique()
        categories = plat['political_category'].dropna().nunique()

        # Volume
        volume_col = 'volume_usd' if 'volume_usd' in plat.columns else 'volume'
        total_volume = plat[volume_col].sum() if volume_col in plat.columns else 0
        avg_volume = plat[volume_col].mean() if volume_col in plat.columns else 0

        vals.extend([
            f"{total:,}",
            f"{len(resolved):,}",
            f"{len(electoral):,}",
            f"{total - len(electoral):,}",
            earliest,
            latest,
            str(election_types),
            str(categories),
            f"${total_volume:,.0f}",
            f"${avg_volume:,.0f}",
        ])

    metrics = [
        'Total Political Markets',
        'Resolved Markets',
        'Electoral Markets',
        'Non-Electoral Markets',
        'Earliest Market Close',
        'Latest Market Close',
        'Unique Election Types',
        'Unique Political Categories',
        'Total Volume (USD)',
        'Avg Volume per Market',
    ]

    data = {
        'metrics': metrics,
        'polymarket': polymarket_vals,
        'kalshi': kalshi_vals,
    }

    with open(f"{WEB_DATA_DIR}/platform_stats.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Platform stats saved ({len(metrics)} metrics, computed from master CSV)")

def generate_volume_timeseries():
    """Generate volume time series by category (for line chart).

    Uses the same logic as the archive/paper version:
    - Filters to CLOSED markets only
    - Deduplicates Polymarket by market_id (Yes/No rows have same volume)
    - Top 8 categories by volume
    """
    log("Generating volume time series...")

    # Load master data and add ticker categories
    df_master = pd.read_csv(f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv", low_memory=False)
    df_master = add_ticker_category_column(df_master)

    # ========== POLYMARKET ==========
    df_pm = df_master[df_master['platform'] == 'Polymarket'].copy()
    df_pm['market_id'] = df_pm['market_id'].astype(str)

    # Filter to closed markets only (use is_closed or pm_closed)
    if 'is_closed' in df_pm.columns:
        df_pm['is_closed'] = df_pm['is_closed'].fillna(False)
        df_pm = df_pm[df_pm['is_closed'] == True].copy()
    elif 'pm_closed' in df_pm.columns:
        df_pm['pm_closed'] = df_pm['pm_closed'].fillna(False)
        df_pm = df_pm[df_pm['pm_closed'] == True].copy()

    # Parse trading_close_time
    df_pm['trading_close_time'] = pd.to_datetime(df_pm['trading_close_time'], format='mixed', utc=True, errors='coerce')
    df_pm = df_pm[df_pm['trading_close_time'].notna()].copy()

    # IMPORTANT: Deduplicate by market_id to avoid double-counting (Yes/No rows)
    df_pm_markets = df_pm.drop_duplicates(subset=['market_id'], keep='first')[['market_id', 'category', 'volume_usd', 'trading_close_time']].copy()
    df_pm_markets = df_pm_markets.rename(columns={'volume_usd': 'volume', 'trading_close_time': 'date'})
    df_pm_markets['source'] = 'Polymarket'

    log(f"  Polymarket: {len(df_pm_markets):,} unique closed markets")

    # ========== KALSHI ==========
    df_kalshi = df_master[df_master['platform'] == 'Kalshi'].copy()
    df_kalshi['market_id'] = df_kalshi['market_id'].astype(str)

    # Filter to finalized/closed markets (use k_status or is_closed)
    if 'k_status' in df_kalshi.columns:
        df_kalshi = df_kalshi[df_kalshi['k_status'].isin(['finalized', 'closed'])].copy()
    elif 'is_closed' in df_kalshi.columns:
        df_kalshi = df_kalshi[df_kalshi['is_closed'] == True].copy()

    # Parse close_time (prefer trading_close_time as it's more consistently populated)
    close_col = 'trading_close_time'
    df_kalshi[close_col] = pd.to_datetime(df_kalshi[close_col], format='mixed', errors='coerce', utc=True)
    df_kalshi = df_kalshi[df_kalshi[close_col].notna()].copy()

    df_kalshi_markets = df_kalshi[['market_id', 'category', 'volume_usd', close_col]].copy()
    df_kalshi_markets = df_kalshi_markets.rename(columns={'volume_usd': 'volume', close_col: 'date'})
    df_kalshi_markets['source'] = 'Kalshi'

    log(f"  Kalshi: {len(df_kalshi_markets):,} closed markets")

    # ========== COMBINE ==========
    df_combined = pd.concat([
        df_pm_markets[['market_id', 'category', 'volume', 'date', 'source']],
        df_kalshi_markets[['market_id', 'category', 'volume', 'date', 'source']]
    ], ignore_index=True)

    # Filter out missing data
    df_combined = df_combined[df_combined['date'].notna()].copy()
    df_combined = df_combined[df_combined['volume'].notna()].copy()
    df_combined = df_combined[df_combined['volume'] > 0].copy()

    # Convert date to datetime (handle timezone-aware dates)
    df_combined['date'] = pd.to_datetime(df_combined['date'], utc=True, errors='coerce')
    df_combined = df_combined[df_combined['date'].notna()].copy()

    # Filter to only include dates up to current month (exclude future-dated markets)
    now = pd.Timestamp.now(tz='UTC')
    df_combined = df_combined[df_combined['date'] <= now].copy()

    # Extract year-month
    df_combined['year_month'] = df_combined['date'].dt.to_period('M')

    log(f"  Combined: {len(df_combined):,} markets")

    # ========== AGGREGATE ==========
    # Get ALL categories sorted by total volume
    all_categories = df_combined.groupby('category')['volume'].sum().sort_values(ascending=False).index.tolist()

    # Group by category and month
    volume_by_cat_month = df_combined.groupby(['category', 'year_month'])['volume'].sum().reset_index()
    volume_by_cat_month['date'] = volume_by_cat_month['year_month'].dt.to_timestamp()

    # Pivot to wide format
    pivot = volume_by_cat_month.pivot(index='date', columns='category', values='volume').fillna(0)
    pivot = pivot.sort_index()

    # Build output data - use YYYY-MM for historical, today's date for current month
    months_list = pivot.index.strftime('%Y-%m').tolist()
    # Replace last month with today's full date
    today_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    if months_list:
        months_list[-1] = today_str

    data = {
        'months': months_list,
        'categories': {},
        'defaultCategories': []  # Top 8 for initial display
    }

    # Include ALL categories (sorted by volume), mark top 8 as defaults
    for i, cat in enumerate(all_categories):
        if cat in pivot.columns:
            clean_name = format_category_name(cat)
            # Convert to millions
            values = (pivot[cat] / 1_000_000).round(4).tolist()
            data['categories'][clean_name] = values
            # Top 8 are defaults
            if i < 8:
                data['defaultCategories'].append(clean_name)

    with open(f"{WEB_DATA_DIR}/volume_timeseries.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Volume time series saved ({len(data['months'])} months, {len(data['categories'])} categories)")

def generate_market_distribution():
    """Generate market distribution by category."""
    log("Generating market distribution...")

    # Generate distribution directly from master CSV + ticker categories
    df = pd.read_csv(f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv", low_memory=False)
    df = add_ticker_category_column(df)

    # Group by category and platform
    dist = df.groupby(['category', 'platform']).size().unstack(fill_value=0).reset_index()
    dist.columns = ['category', *[c for c in dist.columns if c != 'category']]

    # Ensure both platform columns exist
    for col in ['Polymarket', 'Kalshi']:
        if col not in dist.columns:
            dist[col] = 0

    dist['Total'] = dist['Polymarket'] + dist['Kalshi']
    dist = dist.sort_values('Total', ascending=False)

    data = {
        'categories': [],
        'polymarket': [],
        'kalshi': [],
        'total': []
    }

    for _, row in dist.iterrows():
        clean_name = format_category_name(row['category'])
        data['categories'].append(clean_name)
        data['polymarket'].append(int(row['Polymarket']))
        data['kalshi'].append(int(row['Kalshi']))
        data['total'].append(int(row['Total']))

    with open(f"{WEB_DATA_DIR}/market_distribution.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Market distribution saved ({len(data['categories'])} categories)")

def generate_election_winner_stats():
    """Generate election winner comparison stats."""
    log("Generating election winner stats...")

    panel_a = f"{DATA_DIR}/election_winner_panel_a.csv"
    panel_b = f"{DATA_DIR}/election_winner_panel_b.csv"
    comparison = f"{DATA_DIR}/election_winner_comparison_stats.csv"

    data = {}

    if os.path.exists(panel_a):
        df_a = pd.read_csv(panel_a)
        data['all_elections'] = {}
        pm_a = df_a[df_a['Platform'] == 'Polymarket']
        k_a = df_a[df_a['Platform'] == 'Kalshi']
        if len(pm_a) > 0:
            data['all_elections']['polymarket'] = {
                'n': int(pm_a['N_Elections'].values[0]),
                'brier': safe_round(pm_a['Mean_Brier'].values[0], 4),
                'accuracy': safe_round(pm_a['Accuracy'].values[0], 4)
            }
        if len(k_a) > 0:
            data['all_elections']['kalshi'] = {
                'n': int(k_a['N_Elections'].values[0]),
                'brier': safe_round(k_a['Mean_Brier'].values[0], 4),
                'accuracy': safe_round(k_a['Accuracy'].values[0], 4)
            }

    if os.path.exists(panel_b):
        df_b = pd.read_csv(panel_b)
        data['shared_elections'] = {}
        pm_b = df_b[df_b['Platform'] == 'Polymarket']
        k_b = df_b[df_b['Platform'] == 'Kalshi']
        if len(pm_b) > 0:
            data['shared_elections']['polymarket'] = {
                'n': int(pm_b['N_Elections'].values[0]),
                'brier': safe_round(pm_b['Mean_Brier'].values[0], 4),
                'accuracy': safe_round(pm_b['Accuracy'].values[0], 4)
            }
        if len(k_b) > 0:
            data['shared_elections']['kalshi'] = {
                'n': int(k_b['N_Elections'].values[0]),
                'brier': safe_round(k_b['Mean_Brier'].values[0], 4),
                'accuracy': safe_round(k_b['Accuracy'].values[0], 4)
            }

    if os.path.exists(comparison):
        df_c = pd.read_csv(comparison)
        if len(df_c) > 0:
            data['head_to_head'] = {
                'n_shared': int(df_c['n_shared'].values[0]),
                'correlation': safe_round(df_c['correlation'].values[0], 4),
                'pm_wins': int(df_c['pm_wins'].values[0]),
                'kalshi_wins': int(df_c['kalshi_wins'].values[0]),
                'ties': int(df_c['ties'].values[0]),
                'p_value': safe_round(df_c['p_value'].values[0], 4)
            }

    # Compute consensus (combined) accuracy for shared elections
    # Average both platforms' winner_prediction per election, then check correctness
    detailed_file = f"{DATA_DIR}/election_winner_panel_a_detailed.csv"
    if os.path.exists(detailed_file) and 'shared_elections' in data:
        df_det = pd.read_csv(detailed_file)
        election_cols = ['country', 'office', 'location', 'election_year', 'is_primary']
        shared_rows = []
        for _, grp in df_det.groupby(election_cols):
            platforms = set(grp['platform'].unique())
            if 'Polymarket' in platforms and 'Kalshi' in platforms:
                pm_rows = grp[grp['platform'] == 'Polymarket']
                k_rows = grp[grp['platform'] == 'Kalshi']
                if len(pm_rows) == 0 or len(k_rows) == 0:
                    continue
                pm = pm_rows.iloc[0]
                k = k_rows.iloc[0]
                avg_pred = (pm['winner_prediction'] + k['winner_prediction']) / 2
                shared_rows.append({
                    'consensus_pred': avg_pred,
                    'correct': avg_pred > 0.5,
                    'brier': (avg_pred - 1.0) ** 2,
                })
        if shared_rows:
            sdf = pd.DataFrame(shared_rows)
            data['shared_elections']['combined'] = {
                'n': len(sdf),
                'brier': safe_round(sdf['brier'].mean(), 4),
                'accuracy': safe_round(sdf['correct'].mean(), 4),
            }

    with open(f"{WEB_DATA_DIR}/election_winner_stats.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Election winner stats saved")

def generate_aggregate_statistics():
    """Generate aggregate statistics by political category (Table 1)."""
    log("Generating aggregate statistics...")

    # Load master data
    df = pd.read_csv(f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv", low_memory=False)

    # Add ticker-derived categories
    df = add_ticker_category_column(df)

    # Ensure volume_usd is numeric
    df['volume_usd'] = pd.to_numeric(df['volume_usd'], errors='coerce').fillna(0)

    # Group by category
    # Use all markets for count/mean/sum, but only markets with volume > 0 for median
    stats = df.groupby('category').agg({
        'market_id': 'count',
        'volume_usd': ['mean', 'sum']
    }).reset_index()
    stats.columns = ['category', 'total_markets', 'avg_volume', 'total_volume']

    # Calculate median excluding zero-volume markets (zero = no trading data, not meaningful for median)
    median_stats = df[df['volume_usd'] > 0].groupby('category')['volume_usd'].median().reset_index()
    median_stats.columns = ['category', 'median_volume']
    stats = stats.merge(median_stats, on='category', how='left')
    stats['median_volume'] = stats['median_volume'].fillna(0)

    # Sort by total markets descending
    stats = stats.sort_values('total_markets', ascending=False)

    # Format the data
    data = {
        'categories': [],
        'total_markets': [],
        'avg_volume_k': [],
        'median_volume_k': [],
        'total_volume_m': []
    }

    for _, row in stats.iterrows():
        clean_name = format_category_name(row['category'])
        data['categories'].append(clean_name)
        data['total_markets'].append(int(row['total_markets']))
        data['avg_volume_k'].append(round(row['avg_volume'] / 1000, 1))  # Convert to $K
        data['median_volume_k'].append(round(row['median_volume'] / 1000, 1))  # Convert to $K
        data['total_volume_m'].append(round(row['total_volume'] / 1_000_000, 1))  # Convert to $M

    with open(f"{WEB_DATA_DIR}/aggregate_statistics.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Aggregate statistics saved ({len(data['categories'])} categories)")

def generate_election_types():
    """Generate election types breakdown (Table 2)."""
    log("Generating election types...")

    types_file = f"{DATA_DIR}/table_2_election_types.csv"
    if not os.path.exists(types_file):
        log("  ⚠ No election types file found")
        return

    df = pd.read_csv(types_file)

    # Sort by total descending
    df = df.sort_values('Total', ascending=False)

    data = {
        'election_types': df['election_type'].tolist(),
        'polymarket': df['Polymarket'].tolist(),
        'kalshi': df['Kalshi'].tolist(),
        'total': df['Total'].tolist()
    }

    with open(f"{WEB_DATA_DIR}/election_types.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Election types saved ({len(data['election_types'])} types)")

def generate_partisan_bias_calibration():
    """Generate partisan bias calibration data from Panel A.

    Uses Panel A elections with winning_party to derive Republican win probability:
    - If Republican won: republican_prob = winner_prediction
    - If Democrat won: republican_prob = 1 - winner_prediction
    """
    log("Generating partisan bias calibration...")

    panel_file = f"{DATA_DIR}/election_winner_panel_a_detailed.csv"
    if not os.path.exists(panel_file):
        log("  ⚠ No Panel A file found")
        return

    panel_a = pd.read_csv(panel_file)

    # Filter to elections with winning_party (R or D)
    panel_a = panel_a[panel_a['winning_party'].isin(['Republican', 'Democrat'])].copy()

    # Derive republican_won
    panel_a['republican_won'] = (panel_a['winning_party'] == 'Republican').astype(int)

    # Calculate republican probability from winner_prediction
    # If Republican won: r_prob = winner_prediction
    # If Democrat won: r_prob = 1 - winner_prediction
    def calc_r_prob(row):
        if pd.isna(row['winner_prediction']):
            return np.nan
        if row['republican_won'] == 1:
            return row['winner_prediction']
        else:
            return 1 - row['winner_prediction']

    panel_a['r_prob'] = panel_a.apply(calc_r_prob, axis=1)

    # Filter to valid rows
    valid = panel_a[panel_a['r_prob'].notna()].copy()

    log(f"  R markets: {len(valid)} (PM={len(valid[valid['platform']=='Polymarket'])}, K={len(valid[valid['platform']=='Kalshi'])})")

    data = {}
    for platform, plat_label in [('Polymarket', 'polymarket'), ('Kalshi', 'kalshi')]:
        plat_df = valid[valid['platform'] == platform].copy()
        if len(plat_df) == 0:
            continue

        # Sort by r_prob and create quantile bins
        plat_df = plat_df.sort_values('r_prob').reset_index(drop=True)
        n_bins = min(10, len(plat_df) // 3)  # At least 3 per bin
        if n_bins < 2:
            n_bins = 2
        samples_per_bin = len(plat_df) // n_bins
        plat_df['bin'] = plat_df.index // samples_per_bin
        plat_df.loc[plat_df['bin'] >= n_bins, 'bin'] = n_bins - 1

        bin_stats = plat_df.groupby('bin').agg(
            predicted=('r_prob', 'mean'),
            actual=('republican_won', 'mean'),
            count=('republican_won', 'count')
        ).reset_index()

        data[plat_label] = {
            'bins': [
                {
                    'predicted': round(row['predicted'], 4),
                    'actual': round(row['actual'], 4),
                    'count': int(row['count'])
                }
                for _, row in bin_stats.iterrows()
            ],
            'n_elections': len(plat_df)
        }

    with open(f"{WEB_DATA_DIR}/partisan_bias_calibration.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Partisan bias calibration saved")

def generate_partisan_bias_regression():
    """Generate partisan bias regression table data (OLS models)."""
    log("Generating partisan bias regression...")

    panel_file = f"{DATA_DIR}/election_winner_panel_a_detailed.csv"
    if not os.path.exists(panel_file):
        log("  ⚠ No panel_a_detailed file found")
        return

    try:
        import statsmodels.api as sm
    except ImportError:
        log("  ⚠ statsmodels not installed, skipping regression")
        return

    df = pd.read_csv(panel_file)

    # Drop rows without a winning party
    df = df[df['winning_party'].isin(['Democrat', 'Republican'])].copy()

    # Derive variables
    df['is_republican'] = (df['winning_party'] == 'Republican').astype(int)
    df['is_polymarket'] = (df['platform'] == 'Polymarket').astype(int)

    # prediction_error: winner_prediction - 1 (since winner_prediction is for the actual winner)
    # Positive = overconfident, Negative = underconfident
    df['prediction_error'] = df['winner_prediction'] - 1.0

    # Drop NaNs in key columns
    df = df.dropna(subset=['prediction_error', 'is_republican', 'is_polymarket'])

    models = []

    # Model 1: prediction_error ~ is_republican
    X1 = sm.add_constant(df[['is_republican']])
    m1 = sm.OLS(df['prediction_error'], X1).fit()
    models.append({
        'name': 'Party Only',
        'variables': _extract_ols_vars(m1, ['Intercept', 'Republican']),
        'r_squared': round(m1.rsquared, 4),
        'n': int(m1.nobs)
    })

    # Model 2: prediction_error ~ is_republican + is_polymarket
    X2 = sm.add_constant(df[['is_republican', 'is_polymarket']])
    m2 = sm.OLS(df['prediction_error'], X2).fit()
    models.append({
        'name': 'Party + Platform',
        'variables': _extract_ols_vars(m2, ['Intercept', 'Republican', 'Polymarket']),
        'r_squared': round(m2.rsquared, 4),
        'n': int(m2.nobs)
    })

    # Model 3: prediction_error ~ is_republican * is_polymarket
    df['rep_x_pm'] = df['is_republican'] * df['is_polymarket']
    X3 = sm.add_constant(df[['is_republican', 'is_polymarket', 'rep_x_pm']])
    m3 = sm.OLS(df['prediction_error'], X3).fit()
    models.append({
        'name': 'Party × Platform',
        'variables': _extract_ols_vars(m3, ['Intercept', 'Republican', 'Polymarket', 'Republican × Polymarket']),
        'r_squared': round(m3.rsquared, 4),
        'n': int(m3.nobs)
    })

    data = {'models': models}

    with open(f"{WEB_DATA_DIR}/partisan_bias_regression.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Partisan bias regression saved ({len(models)} models)")

def _extract_ols_vars(model, names):
    """Extract variable info from a statsmodels OLS result."""
    variables = []
    for i, name in enumerate(names):
        variables.append({
            'name': name,
            'coef': safe_round(model.params.iloc[i], 4),
            'se': safe_round(model.bse.iloc[i], 4),
            'p': safe_round(model.pvalues.iloc[i], 4),
            'ci_low': safe_round(model.conf_int().iloc[i, 0], 4),
            'ci_high': safe_round(model.conf_int().iloc[i, 1], 4)
        })
    return variables


def generate_trader_partisanship_distribution():
    """Generate trader partisanship distribution - two KDEs for R and D bettors."""
    log("Generating trader partisanship distribution...")

    trader_file = f"{DATA_DIR}/panel_a_trader_analysis.csv"
    if not os.path.exists(trader_file):
        log("  No trader analysis file found - skipping")
        return

    try:
        from scipy.stats import gaussian_kde
    except ImportError:
        log("  scipy not available - skipping KDE")
        return

    df = pd.read_csv(trader_file)
    df = df[df['total_volume'] > 0].copy()

    # Republican bettors: traders who bet Republican Yes at least once
    rep_bettors = df[df['volume_for_republican'] > 0].copy()
    rep_bettors = rep_bettors[rep_bettors['pct_volume_for_republican'].notna()]

    # Democrat bettors: traders who bet against Republican at least once
    dem_bettors = df[df['volume_against_republican'] > 0].copy()
    dem_bettors = dem_bettors[dem_bettors['pct_volume_for_republican'].notna()]
    # For Dem bettors, their "partisanship" is 100 - pct_volume_for_republican
    dem_bettors['pct_dem_partisanship'] = 100 - dem_bettors['pct_volume_for_republican']

    if len(rep_bettors) == 0 and len(dem_bettors) == 0:
        log("  No valid traders found")
        return

    x_kde = np.linspace(0, 100, 200)

    # Define trade count buckets (Total instead of "1 trade" since 1-trade wallets are all 100%)
    buckets = [
        ('Total', 1, 100000),
        ('2-5 trades', 2, 5),
        ('6+ trades', 6, 100000),
    ]

    def compute_kde_by_buckets(bettors_df, value_col):
        """Compute KDE for each trade count bucket."""
        result = {}
        for label, min_trades, max_trades in buckets:
            subset = bettors_df[(bettors_df['num_trades'] >= min_trades) &
                                (bettors_df['num_trades'] <= max_trades)]
            if len(subset) > 1:
                values = subset[value_col].values
                # Check if there's variance in the data (KDE fails if all values are the same)
                if np.std(values) > 0.01:
                    try:
                        kde = gaussian_kde(values, bw_method='scott')
                        y_kde = kde(x_kde)
                        result[label] = {
                            'x': x_kde.tolist(),
                            'y': y_kde.tolist(),
                            'mean': round(float(np.mean(values)), 2),
                            'n': len(subset)
                        }
                    except Exception as e:
                        log(f"    Warning: KDE failed for {label}: {e}")
                        # Fallback: just record stats without KDE
                        result[label] = {
                            'x': x_kde.tolist(),
                            'y': [0] * len(x_kde),
                            'mean': round(float(np.mean(values)), 2),
                            'n': len(subset)
                        }
                else:
                    # No variance - use a spike at the mean
                    result[label] = {
                        'x': x_kde.tolist(),
                        'y': [0] * len(x_kde),
                        'mean': round(float(np.mean(values)), 2),
                        'n': len(subset)
                    }
        return result

    # Compute KDEs by trade count for each party
    rep_by_trades = compute_kde_by_buckets(rep_bettors, 'pct_volume_for_republican')
    dem_by_trades = compute_kde_by_buckets(dem_bettors, 'pct_dem_partisanship')

    # Overall stats for each party
    rep_overall_mean = round(float(rep_bettors['pct_volume_for_republican'].mean()), 2) if len(rep_bettors) > 0 else 0
    dem_overall_mean = round(float(dem_bettors['pct_dem_partisanship'].mean()), 2) if len(dem_bettors) > 0 else 0

    # Load Panel A data to get actual election outcomes
    panel_a_file = f"{DATA_DIR}/election_winner_panel_a_detailed.csv"
    election_outcomes = {'dem_pct': 50.0, 'rep_pct': 50.0, 'n_elections': 0}
    if os.path.exists(panel_a_file):
        panel_a = pd.read_csv(panel_a_file)
        pm_elections = panel_a[
            (panel_a['platform'] == 'Polymarket') &
            (panel_a['winning_party'].isin(['Republican', 'Democrat']))
        ]
        n_total = len(pm_elections)
        if n_total > 0:
            n_dem = len(pm_elections[pm_elections['winning_party'] == 'Democrat'])
            n_rep = len(pm_elections[pm_elections['winning_party'] == 'Republican'])
            election_outcomes = {
                'dem_pct': round(n_dem / n_total * 100, 1),
                'rep_pct': round(n_rep / n_total * 100, 1),
                'n_elections': n_total
            }

    data = {
        'republican_bettors': {
            'by_trade_count': rep_by_trades,
            'overall_mean': rep_overall_mean,
            'n': len(rep_bettors)
        },
        'democrat_bettors': {
            'by_trade_count': dem_by_trades,
            'overall_mean': dem_overall_mean,
            'n': len(dem_bettors)
        },
        'election_outcomes': election_outcomes
    }

    with open(f"{WEB_DATA_DIR}/trader_partisanship_distribution.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  Trader partisanship distribution saved (R bettors: {len(rep_bettors)}, D bettors: {len(dem_bettors)})")


def generate_trader_accuracy_distribution():
    """Generate trader accuracy distribution data with KDEs by trade count buckets."""
    log("Generating trader accuracy distribution...")

    trader_file = f"{DATA_DIR}/panel_a_trader_analysis.csv"
    if not os.path.exists(trader_file):
        log("  No trader analysis file found - skipping")
        return

    try:
        from scipy.stats import gaussian_kde
    except ImportError:
        log("  scipy not available - skipping KDE")
        return

    df = pd.read_csv(trader_file)

    # Filter to traders with any volume
    df = df[df['total_volume'] > 0].copy()
    df = df[df['pct_volume_correct'].notna()].copy()

    if len(df) == 0:
        log("  No valid traders found")
        return

    x_kde = np.linspace(0, 100, 200)

    # Define trade count buckets
    buckets = [
        ('1 trade', 1, 1),
        ('2-5 trades', 2, 5),
        ('6+ trades', 6, 100000),
    ]

    kde_data = {}
    for label, min_trades, max_trades in buckets:
        subset = df[(df['num_trades'] >= min_trades) & (df['num_trades'] <= max_trades)]
        if len(subset) > 1:
            values = subset['pct_volume_correct'].values
            if np.std(values) > 0.01:
                try:
                    kde = gaussian_kde(values, bw_method='scott')
                    y_kde = kde(x_kde)
                    kde_data[label] = {
                        'x': x_kde.tolist(),
                        'y': y_kde.tolist(),
                        'mean': safe_round(np.mean(values), 2),
                        'n': len(subset)
                    }
                except Exception:
                    kde_data[label] = {
                        'x': x_kde.tolist(),
                        'y': [0] * len(x_kde),
                        'mean': safe_round(np.mean(values), 2),
                        'n': len(subset)
                    }
            else:
                kde_data[label] = {
                    'x': x_kde.tolist(),
                    'y': [0] * len(x_kde),
                    'mean': safe_round(np.mean(values), 2),
                    'n': len(subset)
                }

    # Overall stats
    all_values = df['pct_volume_correct'].values
    mean_val = float(np.mean(all_values))
    median_val = float(np.median(all_values))

    data = {
        'by_trade_count': kde_data,
        'stats': {
            'mean': round(mean_val, 2),
            'median': round(median_val, 2),
            'n': len(df)
        }
    }

    with open(f"{WEB_DATA_DIR}/trader_accuracy_distribution.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  Trader accuracy distribution saved (n={len(df)} traders, {len(kde_data)} buckets)")


def generate_trader_partisanship_actual_vs_perfect():
    """Generate trader partisanship actual vs counterfactual (perfect) data by party."""
    log("Generating trader partisanship actual vs perfect...")

    trader_file = f"{DATA_DIR}/panel_a_trader_analysis.csv"
    if not os.path.exists(trader_file):
        log("  No trader analysis file found - skipping")
        return

    try:
        from scipy.stats import gaussian_kde
    except ImportError:
        log("  scipy not available - skipping KDE")
        return

    df = pd.read_csv(trader_file)
    df = df[df['total_volume'] > 0].copy()
    df = df[df['pct_volume_for_republican'].notna()].copy()
    df = df[df['cf_pct_volume_for_republican'].notna()].copy()
    # Filter to 2+ trades to avoid one-time traders skewing the distribution
    df = df[df['num_trades'] >= 2].copy()

    if len(df) == 0:
        log("  No valid traders found")
        return

    # Pro-Republican bettors - use party-specific counterfactual
    rep_bettors = df[df['volume_for_republican'] > 0].copy()
    rep_bettors = rep_bettors[rep_bettors['cf_rep_only_pct_for_republican'].notna()].copy()

    # Pro-Democrat bettors - compute their % for Democrat (actual and counterfactual)
    dem_bettors = df[df['volume_against_republican'] > 0].copy()
    dem_bettors = dem_bettors[dem_bettors['cf_dem_only_pct_for_republican'].notna()].copy()
    dem_bettors['pct_dem_actual'] = 100 - dem_bettors['pct_volume_for_republican']
    dem_bettors['pct_dem_cf'] = 100 - dem_bettors['cf_dem_only_pct_for_republican']

    x_kde = np.linspace(0, 100, 200)

    def compute_actual_vs_cf_kde(bettors_df, actual_col, cf_col):
        """Compute KDE for actual vs counterfactual."""
        result = {'actual': {}, 'counterfactual': {}}

        if len(bettors_df) < 2:
            return None

        actual_values = bettors_df[actual_col].values
        cf_values = bettors_df[cf_col].values

        # Actual KDE
        if np.std(actual_values) > 0.01:
            try:
                kde = gaussian_kde(actual_values, bw_method='scott')
                result['actual'] = {
                    'x': x_kde.tolist(),
                    'y': kde(x_kde).tolist(),
                    'mean': safe_round(np.mean(actual_values), 2)
                }
            except Exception:
                result['actual'] = {'x': x_kde.tolist(), 'y': [0]*len(x_kde), 'mean': safe_round(np.mean(actual_values), 2)}
        else:
            result['actual'] = {'x': x_kde.tolist(), 'y': [0]*len(x_kde), 'mean': safe_round(np.mean(actual_values), 2)}

        # Counterfactual KDE
        if np.std(cf_values) > 0.01:
            try:
                kde = gaussian_kde(cf_values, bw_method='scott')
                result['counterfactual'] = {
                    'x': x_kde.tolist(),
                    'y': kde(x_kde).tolist(),
                    'mean': safe_round(np.mean(cf_values), 2)
                }
            except Exception:
                result['counterfactual'] = {'x': x_kde.tolist(), 'y': [0]*len(x_kde), 'mean': safe_round(np.mean(cf_values), 2)}
        else:
            result['counterfactual'] = {'x': x_kde.tolist(), 'y': [0]*len(x_kde), 'mean': safe_round(np.mean(cf_values), 2)}

        result['n'] = len(bettors_df)
        result['shift'] = round(result['actual']['mean'] - result['counterfactual']['mean'], 2)

        return result

    # Compute for Pro-Republican bettors (% for Republican) - use party-specific counterfactual
    rep_data = compute_actual_vs_cf_kde(rep_bettors, 'pct_volume_for_republican', 'cf_rep_only_pct_for_republican')

    # Compute for Pro-Democrat bettors (% for Democrat)
    dem_data = compute_actual_vs_cf_kde(dem_bettors, 'pct_dem_actual', 'pct_dem_cf')

    data = {
        'republican_bettors': rep_data,
        'democrat_bettors': dem_data
    }

    with open(f"{WEB_DATA_DIR}/trader_partisanship_actual_vs_perfect.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  Trader partisanship actual vs perfect saved (R: n={len(rep_bettors)}, D: n={len(dem_bettors)})")


def generate_calibration_by_closeness():
    """Generate calibration by race closeness (margin buckets) using Panel A winner markets."""
    log("Generating calibration by closeness...")

    # Use Panel A detailed (winner markets only)
    panel_file = f"{DATA_DIR}/election_winner_panel_a_detailed.csv"
    if not os.path.exists(panel_file):
        log("  ⚠ No Panel A detailed file found")
        return

    df = pd.read_csv(panel_file)

    # Compute margin (vote shares are already in percentage format in Panel A)
    df['dem_share'] = pd.to_numeric(df['democrat_vote_share'], errors='coerce')
    df['rep_share'] = pd.to_numeric(df['republican_vote_share'], errors='coerce')
    df['margin'] = (df['dem_share'] - df['rep_share']).abs()

    # Drop rows without margin data
    df = df.dropna(subset=['margin', 'brier'])

    # Bucket
    def bucket_margin(m):
        if m < 5:
            return '< 5%'
        elif m < 10:
            return '5-10%'
        elif m < 20:
            return '10-20%'
        else:
            return '> 20%'

    df['bucket'] = df['margin'].apply(bucket_margin)
    bucket_order = ['< 5%', '5-10%', '10-20%', '> 20%']

    buckets = []
    for label in bucket_order:
        b = df[df['bucket'] == label]
        pm_b = b[b['platform'].str.lower() == 'polymarket']
        k_b = b[b['platform'].str.lower() == 'kalshi']
        buckets.append({
            'label': label,
            'pm_brier': safe_round(pm_b['brier'].mean(), 4) if len(pm_b) > 0 else None,
            'k_brier': safe_round(k_b['brier'].mean(), 4) if len(k_b) > 0 else None,
            'pm_n': int(len(pm_b)),
            'k_n': int(len(k_b))
        })

    data = {'buckets': buckets}

    with open(f"{WEB_DATA_DIR}/calibration_by_closeness.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Calibration by closeness saved ({len(buckets)} buckets)")

def generate_prediction_vs_volume():
    """Generate prediction price vs volume scatter data, separated by Yes/No tokens."""
    log("Generating prediction vs volume...")

    master_file = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
    pm_file = get_latest_file("polymarket_prediction_accuracy_all_political*.csv")
    kalshi_file = get_latest_file("kalshi_prediction_accuracy_all_political*.csv")

    if not pm_file or not kalshi_file:
        log("  ⚠ No prediction accuracy files found")
        return

    from scipy import stats as scipy_stats

    master_df = pd.read_csv(master_file, low_memory=False)
    pm_pred = pd.read_csv(pm_file)
    kalshi_pred = pd.read_csv(kalshi_file, low_memory=False)

    # Filter to 1 day before
    pm_1d = pm_pred[pm_pred['days_before_event'] == 1].copy()
    kalshi_1d = kalshi_pred[kalshi_pred['days_before_event'] == 1].copy()

    # Get volume from master
    master_df['market_id'] = master_df['market_id'].astype(str)
    master_df['volume_usd'] = pd.to_numeric(master_df['volume_usd'], errors='coerce')
    vol_cols = master_df[['market_id', 'volume_usd']].drop_duplicates(subset='market_id')

    data = {}

    def process_outcome_subset(subset_df, label):
        """Process a subset (Yes or No) and return stats dict."""
        if len(subset_df) == 0:
            return None

        # Points (sample if too many)
        sample = subset_df if len(subset_df) <= 2000 else subset_df.sample(2000, random_state=42)
        points = [
            {
                'price': round(float(row['prediction_price']), 4),
                'volume': round(float(row['volume_usd']), 2),
                'correct': bool(
                    (row['prediction_price'] > 0.5 and row['actual_outcome'] == 1) or
                    (row['prediction_price'] <= 0.5 and row['actual_outcome'] == 0)
                )
            }
            for _, row in sample.iterrows()
        ]

        # Bins for trend line (20 bins)
        subset_df = subset_df.copy()
        subset_df['price_bin'] = pd.cut(subset_df['prediction_price'], bins=20, labels=False)
        bin_stats = subset_df.groupby('price_bin').agg(
            price_mid=('prediction_price', 'mean'),
            median_volume=('volume_usd', 'median')
        ).dropna().reset_index()

        bins = [
            {
                'price_mid': round(float(row['price_mid']), 4),
                'median_volume': round(float(row['median_volume']), 2)
            }
            for _, row in bin_stats.iterrows()
        ]

        # Correlation
        if len(subset_df) >= 2:
            corr, _ = scipy_stats.pearsonr(subset_df['prediction_price'], subset_df['volume_usd'])
            if np.isnan(corr):
                corr = 0.0
        else:
            corr = 0.0

        return {
            'points': points,
            'bins': bins,
            'correlation': round(float(corr), 4),
            'n': len(subset_df)
        }

    for plat_label, pred_df, id_col in [('polymarket', pm_1d, 'market_id'), ('kalshi', kalshi_1d, 'ticker')]:
        pred_df[id_col] = pred_df[id_col].astype(str)
        merged = pred_df.merge(vol_cols, left_on=id_col, right_on='market_id', how='left')
        merged = merged.dropna(subset=['prediction_price', 'volume_usd', 'actual_outcome'])
        merged = merged[merged['volume_usd'] > 0]

        if len(merged) == 0:
            continue

        # Split by outcome_name (Yes vs No)
        # Normalize outcome names to handle variations
        if 'outcome_name' in merged.columns:
            merged['outcome_normalized'] = merged['outcome_name'].str.strip().str.lower()
            yes_df = merged[merged['outcome_normalized'] == 'yes']
            no_df = merged[merged['outcome_normalized'] == 'no']
        else:
            # Fallback: use all data as "yes"
            yes_df = merged
            no_df = pd.DataFrame()

        yes_stats = process_outcome_subset(yes_df, 'yes')
        no_stats = process_outcome_subset(no_df, 'no')

        data[plat_label] = {
            'yes': yes_stats,
            'no': no_stats
        }

        log(f"  {plat_label}: Yes={len(yes_df)}, No={len(no_df)}")

    with open(f"{WEB_DATA_DIR}/prediction_vs_volume.json", 'w') as f:
        json.dump(data, f, indent=2, allow_nan=False)

    log(f"  ✓ Prediction vs volume saved")

def _resolve_coords(country, location):
    """Resolve (country, location) to (lat, lng) using LOCATION_COORDS.

    Falls back to US state abbreviation lookup for House districts,
    then to country-level coordinates.  Returns None if no match.
    """
    key = (str(country), str(location))
    if key in LOCATION_COORDS:
        return LOCATION_COORDS[key]

    # US House district fallback: "AZ-1" -> "Arizona"
    if country == 'United States':
        m = re.match(r'^([A-Z]{2})-', str(location))
        if m:
            state_name = US_STATE_ABBREVS.get(m.group(1))
            if state_name:
                state_key = ('United States', state_name)
                if state_key in LOCATION_COORDS:
                    return LOCATION_COORDS[state_key]

    # Country-level fallback
    country_key = (str(country), str(country))
    if country_key in LOCATION_COORDS:
        return LOCATION_COORDS[country_key]

    return None

def generate_globe_elections():
    """Generate globe election data for the interactive globe visualization.

    Uses shared election_market_utils to:
    1. Group by (country, office, location, year, is_primary)
    2. Filter with keyword patterns (exclusion + inclusion)
    3. Pick highest volume market per election

    Works for ALL countries.
    """
    from collections import defaultdict
    from election_market_utils import get_winner_markets_by_election

    log("Generating globe elections...")

    # Load master CSV
    master_file = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
    df = pd.read_csv(master_file, low_memory=False)

    # Get winner markets grouped by election
    elections = get_winner_markets_by_election(df)
    log(f"  Elections with winner markets: {len(elections):,}")

    # Build election entries with coordinates
    election_entries = []
    skipped_no_coords = 0

    for election_key, data in elections.items():
        markets = data['markets']
        if not markets:
            continue

        # Parse election key
        parts = election_key.split('|')
        country = parts[0] if len(parts) > 0 else ''
        office = parts[1] if len(parts) > 1 else ''
        location = parts[2] if len(parts) > 2 else ''
        year_str = parts[3] if len(parts) > 3 else ''

        try:
            year_val = int(year_str) if year_str else None
        except ValueError:
            year_val = None

        # Get coordinates
        coords = _resolve_coords(country, location)
        if coords is None:
            skipped_no_coords += 1
            continue
        lat, lng = coords

        # Aggregate market stats
        pm_markets = [m for m in markets if m.get('platform') == 'Polymarket']
        k_markets = [m for m in markets if m.get('platform') == 'Kalshi']
        pm_volume = sum(float(m.get('volume_usd', 0)) for m in pm_markets if pd.notna(m.get('volume_usd')))
        k_volume = sum(float(m.get('volume_usd', 0)) for m in k_markets if pd.notna(m.get('volume_usd')))
        total_volume = pm_volume + k_volume

        # Check if completed
        is_completed = all(m.get('is_closed', False) for m in markets)

        # Get event slugs from highest volume market per platform
        pm_event = None
        k_event = None
        if pm_markets:
            pm_sorted = sorted(pm_markets, key=lambda m: float(m.get('volume_usd', 0)) if pd.notna(m.get('volume_usd')) else 0, reverse=True)
            pm_event = pm_sorted[0].get('pm_event_slug') if pd.notna(pm_sorted[0].get('pm_event_slug')) else None
        if k_markets:
            k_sorted = sorted(k_markets, key=lambda m: float(m.get('volume_usd', 0)) if pd.notna(m.get('volume_usd')) else 0, reverse=True)
            k_event = k_sorted[0].get('k_event_ticker') if pd.notna(k_sorted[0].get('k_event_ticker')) else None

        election_entries.append({
            'country': country,
            'location': location,
            'office': office,
            'year': year_val,
            'lat': lat,
            'lng': lng,
            'markets': len(markets),
            'volume': total_volume,
            'has_pm': 1 if pm_markets else 0,
            'has_k': 1 if k_markets else 0,
            'pm_event': pm_event,
            'k_event': k_event,
            'is_completed': is_completed,
        })

    if skipped_no_coords > 0:
        log(f"  Skipped {skipped_no_coords} elections without coordinates")

    # Aggregate by (lat, lng, is_completed)
    point_groups = defaultdict(list)
    for e in election_entries:
        point_groups[(e['lat'], e['lng'], e['is_completed'])].append(e)

    live_entries = []
    completed_entries = []

    for (lat, lng, is_completed), groups in point_groups.items():
        groups.sort(key=lambda g: g['volume'], reverse=True)
        top = groups[0]

        elections_count = len(groups)
        markets = sum(g['markets'] for g in groups)
        volume = sum(g['volume'] for g in groups)
        has_pm = 1 if any(g['has_pm'] for g in groups) else 0
        has_k = 1 if any(g['has_k'] for g in groups) else 0

        # Build label
        loc_str = top['location'] if top['location'] and top['location'] != top['country'] else top['country']
        if top['year'] is not None:
            label = f"{top['year']} \u2014 {loc_str} \u2014 {top['office']}"
        elif top['office']:
            label = f"{loc_str} \u2014 {top['office']}"
        else:
            label = loc_str
        if elections_count > 1:
            label += f" (+{elections_count - 1} more)"

        # Build search_query
        search_parts = []
        if top['office']:
            search_parts.append(str(top['office']))
        if top['location'] and top['location'] != top['country']:
            search_parts.append(str(top['location']))
        search_parts.append(str(top['country']))
        if top['year'] is not None:
            search_parts.append(str(top['year']))
        search_query = ' '.join(search_parts)

        # Platform links
        k_event = next((g['k_event'] for g in groups if g['k_event']), None)
        pm_event = next((g['pm_event'] for g in groups if g['pm_event']), None)

        entry = {
            'lat': lat,
            'lng': lng,
            'label': label,
            'elections': elections_count,
            'markets': markets,
            'volume': round(volume, 2),
            'search_query': search_query,
            'has_pm': has_pm,
            'has_k': has_k,
        }
        if k_event:
            entry['kalshi_event'] = k_event
        if pm_event:
            entry['pm_event'] = pm_event

        if is_completed:
            completed_entries.append(entry)
        else:
            live_entries.append(entry)

    live_entries.sort(key=lambda e: e['volume'], reverse=True)
    completed_entries.sort(key=lambda e: e['volume'], reverse=True)

    output = {'live': live_entries, 'completed': completed_entries}
    with open(f"{WEB_DATA_DIR}/globe_elections.json", 'w') as f:
        json.dump(output, f, indent=2, allow_nan=False)

    total = len(live_entries) + len(completed_entries)
    log(f"  ✓ Globe elections saved ({len(live_entries)} live, {len(completed_entries)} completed, {total} total)")


def generate_globe_markets():
    """Generate expanded globe market data for ALL categories.

    Reads from active_markets.json (which has location data from location extraction)
    and aggregates markets by (lat, lng, category) for display on the globe.

    Output structure optimized for:
    - Category-based coloring
    - Location aggregation (multiple markets at same point)
    - Filtering by category/region
    """
    from collections import defaultdict

    log("Generating globe markets (all categories)...")

    # Load active markets data (generated by generate_monitor_data.py)
    active_markets_file = f"{WEB_DATA_DIR}/active_markets.json"
    if not os.path.exists(active_markets_file):
        log("  ⚠ active_markets.json not found - run generate_monitor_data.py first")
        return

    with open(active_markets_file) as f:
        data = json.load(f)

    markets = data.get('markets', [])
    log(f"  Loaded {len(markets):,} markets from active_markets.json")

    # Filter to markets with location data
    with_location = [m for m in markets if m.get('lat') is not None and m.get('lng') is not None]
    pct = 100 * len(with_location) / len(markets) if len(markets) > 0 else 0
    log(f"  Markets with location: {len(with_location):,} ({pct:.1f}%)")

    def infer_region_from_coords(lat, lng):
        """Infer region from latitude/longitude when region data is missing."""
        if lat is None or lng is None:
            return 'unknown'
        # Europe: lat 35-72, lng -25 to 40
        if 35 <= lat <= 72 and -25 <= lng <= 40:
            return 'europe'
        # North America: lat 15-72, lng -170 to -50
        if 15 <= lat <= 72 and -170 <= lng <= -50:
            return 'north_america'
        # South America: lat -56 to 15, lng -82 to -34
        if -56 <= lat <= 15 and -82 <= lng <= -34:
            return 'south_america'
        # Middle East: lat 12-42, lng 25-63
        if 12 <= lat <= 42 and 25 <= lng <= 63:
            return 'middle_east'
        # Africa: lat -35 to 37, lng -18 to 52
        if -35 <= lat <= 37 and -18 <= lng <= 52:
            return 'africa'
        # Oceania: lat -50 to 0, lng 110-180
        if -50 <= lat <= 0 and 110 <= lng <= 180:
            return 'oceania'
        # Asia (rest of Eastern Hemisphere)
        if lat >= -10 and (lng > 40 or lng < -130):
            return 'asia'
        return 'unknown'

    # Use shared category colors (handles new ticker-based codes)
    # Also build a reverse map for old-format categories from active_markets.json
    old_to_new_color = {}
    for old_cat, new_cat in OLD_TO_NEW_CATEGORY.items():
        if new_cat in CATEGORY_COLORS:
            old_to_new_color[old_cat] = CATEGORY_COLORS[new_cat]

    # Aggregate by (lat, lng, category)
    point_groups = defaultdict(list)
    for m in with_location:
        lat = round(m['lat'], 2)
        lng = round(m['lng'], 2)
        raw_category = m.get('category', 'MISC')
        # Normalize old-format categories to new codes
        category = OLD_TO_NEW_CATEGORY.get(raw_category, raw_category)
        if category not in CATEGORY_COLORS:
            category = 'MISC'
        m['_category'] = category  # store normalized for later use
        point_groups[(lat, lng, category)].append(m)

    log(f"  Unique location-category points: {len(point_groups):,}")

    # Build aggregated entries
    market_entries = []
    for (lat, lng, category), group in point_groups.items():
        # Sort by volume/price to get representative market
        group.sort(key=lambda m: float(m.get('total_volume', 0) or 0), reverse=True)
        top = group[0]

        # Aggregate stats
        total_volume = sum(float(m.get('total_volume', 0) or 0) for m in group)
        has_pm = any(m.get('has_pm') for m in group)
        has_k = any(m.get('has_k') for m in group)
        market_count = len(group)

        # Get country/location from top market
        country = top.get('country', '')
        location = top.get('location', '')
        region = top.get('region')
        # Fallback to inferring region from coordinates if missing
        if not region or region == 'unknown':
            region = infer_region_from_coords(lat, lng)
        category_display = CATEGORY_DISPLAY_NAMES.get(category, top.get('category_display', 'Other'))

        # Build label
        if market_count == 1:
            label = top.get('label', '')[:80]
        else:
            # Multiple markets - show count and category
            loc_str = location if location and location != country else country
            label = f"{loc_str}: {market_count} {category_display} markets"

        # Get representative price (average or top)
        prices = [m.get('price') or m.get('pm_price') or m.get('k_price') for m in group]
        prices = [p for p in prices if p is not None]
        avg_price = sum(prices) / len(prices) if prices else None

        entry = {
            'lat': lat,
            'lng': lng,
            'label': label,
            'category': category,
            'category_display': category_display,
            'color': CATEGORY_COLORS.get(category, old_to_new_color.get(category, '#6b7280')),
            'country': country,
            'location': location,
            'region': region,
            'markets': market_count,
            'volume': round(total_volume, 2),
            'price': round(avg_price, 3) if avg_price else None,
            'has_pm': 1 if has_pm else 0,
            'has_k': 1 if has_k else 0,
        }

        # Add URLs if single market
        if market_count == 1:
            if top.get('url'):
                entry['url'] = top['url']
            if top.get('pm_url'):
                entry['pm_url'] = top['pm_url']
            if top.get('k_url'):
                entry['k_url'] = top['k_url']

        market_entries.append(entry)

    # Sort by volume (most important markets first)
    market_entries.sort(key=lambda e: e['volume'], reverse=True)

    # Aggregate stats by category
    category_stats = defaultdict(lambda: {'count': 0, 'markets': 0, 'volume': 0})
    for entry in market_entries:
        cat = entry['category_display']
        category_stats[cat]['count'] += 1
        category_stats[cat]['markets'] += entry['markets']
        category_stats[cat]['volume'] += entry['volume']

    # Build output
    output = {
        'generated_at': datetime.now().isoformat(),
        'total_points': len(market_entries),
        'total_markets': len(with_location),
        'category_colors': CATEGORY_COLORS,  # Uses new ticker-based codes
        'category_stats': dict(category_stats),
        'markets': market_entries,
    }

    with open(f"{WEB_DATA_DIR}/globe_markets.json", 'w') as f:
        json.dump(output, f, indent=2, allow_nan=False)

    log(f"  ✓ Globe markets saved ({len(market_entries):,} points from {len(with_location):,} markets)")

    # Log category breakdown
    for cat, stats in sorted(category_stats.items()):
        log(f"    {cat}: {stats['count']} points, {stats['markets']} markets, ${stats['volume']:,.0f} volume")


def update_html_placeholders():
    """Update HTML placeholders with current data values.

    This ensures the static HTML shows accurate numbers even before
    JavaScript loads and animates them.
    """
    log("Updating HTML placeholders...")

    index_path = WEBSITE_DIR / "index.html"
    if not index_path.exists():
        log("  ⚠ index.html not found")
        return

    html = index_path.read_text()

    # Load summary data
    summary_path = f"{WEB_DATA_DIR}/summary.json"
    platform_stats_path = f"{WEB_DATA_DIR}/platform_stats.json"

    updates = []

    # Update total markets
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            summary = json.load(f)

        if 'total_markets' in summary:
            total = f"{summary['total_markets']:,}"
            html = re.sub(
                r'(<span[^>]*id="hero-markets"[^>]*>)[^<]*(</span>)',
                rf'\g<1>{total}\2',
                html
            )
            updates.append(f"hero-markets={total}")

        if 'electoral_markets' in summary:
            electoral = f"{summary['electoral_markets']:,}"
            html = re.sub(
                r'(<span[^>]*id="hero-electoral"[^>]*>)[^<]*(</span>)',
                rf'\g<1>{electoral}\2',
                html
            )
            updates.append(f"hero-electoral={electoral}")

        if 'electoral_countries' in summary:
            countries = str(summary['electoral_countries'])
            html = re.sub(
                r'(<span[^>]*id="hero-countries"[^>]*>)[^<]*(</span>)',
                rf'\g<1>{countries}\2',
                html
            )
            updates.append(f"hero-countries={countries}")

    # Update total volume from platform stats
    if os.path.exists(platform_stats_path):
        with open(platform_stats_path) as f:
            stats = json.load(f)

        try:
            vol_idx = stats['metrics'].index('Total Volume (USD)')
            pm_vol = float(stats['polymarket'][vol_idx].replace('$', '').replace(',', ''))
            k_vol = float(stats['kalshi'][vol_idx].replace('$', '').replace(',', ''))
            total_vol = pm_vol + k_vol
            vol_str = f"${total_vol / 1e9:.1f}B"
            html = re.sub(
                r'(<span[^>]*id="hero-volume"[^>]*>)[^<]*(</span>)',
                rf'\g<1>{vol_str}\2',
                html
            )
            updates.append(f"hero-volume={vol_str}")
        except (ValueError, KeyError, IndexError) as e:
            log(f"  ⚠ Could not update volume: {e}")

    # Write updated HTML
    index_path.write_text(html)
    log(f"  ✓ Updated HTML placeholders: {', '.join(updates)}")


def main():
    log("="*60)
    log("GENERATING WEB DATA")
    log("="*60)

    # Run pre-publish validation
    if AUDIT_AVAILABLE:
        log("Running pre-publish data validation...")
        try:
            validator = DataValidator()
            result = validator.run_all_checks(source="pre_publish")

            if result['status'] == 'OK':
                log("  Validation passed - no issues found")
            else:
                log(f"  Validation completed: {result['summary']['critical']} critical, "
                    f"{result['summary']['error']} errors, {result['summary']['warning']} warnings")

                # Log any critical issues
                for issue in result['issues']:
                    if issue['level'] == 'CRITICAL':
                        log(f"  CRITICAL: {issue['message']}")
        except Exception as e:
            log(f"  Warning: Validation failed with error: {e}")

    generate_summary_stats()
    generate_aggregate_statistics()
    generate_election_types()
    generate_brier_by_category()
    generate_brier_by_election_type()
    generate_calibration_data()
    generate_platform_comparison()
    generate_brier_convergence()
    generate_platform_stats()
    generate_volume_timeseries()
    generate_market_distribution()
    generate_election_winner_stats()
    generate_partisan_bias_calibration()
    generate_partisan_bias_regression()
    generate_trader_partisanship_distribution()
    generate_trader_accuracy_distribution()
    generate_trader_partisanship_actual_vs_perfect()
    generate_calibration_by_closeness()
    generate_prediction_vs_volume()
    generate_globe_elections()

    # Generate monitor data (for Market Monitor section)
    try:
        from generate_monitor_data import generate_monitor_data, generate_monitor_summary
        output = generate_monitor_data()
        if output:
            try:
                generate_monitor_summary(output.get('markets', []))
            except Exception as e:
                log(f"  Warning: Monitor robustness summary failed: {e}")
                import traceback
                traceback.print_exc()
    except Exception as e:
        log(f"  Warning: Monitor data generation failed: {e}")

    # Generate expanded globe markets (all categories, requires active_markets.json)
    try:
        generate_globe_markets()
    except Exception as e:
        log(f"  Warning: Globe markets generation failed: {e}")

    # Update HTML placeholders with current values
    try:
        update_html_placeholders()
    except Exception as e:
        log(f"  Warning: HTML placeholder update failed: {e}")

    log("="*60)
    log("✓ WEB DATA GENERATION COMPLETE")
    log(f"Output directory: {WEB_DATA_DIR}")
    log("="*60)

if __name__ == "__main__":
    main()
