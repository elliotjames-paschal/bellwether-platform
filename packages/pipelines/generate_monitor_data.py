#!/usr/bin/env python3
"""
Generate Market Monitor Data

Creates JSON data for the Market Monitor section.
- Electoral markets: Grouped by election with cross-platform comparison (PM vs Kalshi)
- Non-electoral markets: Individual entries per market/platform

Fetches LIVE prices from native APIs (CLOB + Kalshi) for accurate current pricing.

Usage:
    python generate_monitor_data.py [--skip-prices]

Options:
    --skip-prices   Skip live price fetching, use cached historical prices instead
"""

import argparse
import json
import re
import sys
import time
import requests
import threading
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Rate limiter for native APIs
class RateLimiter:
    def __init__(self, calls_per_second=80):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

rate_limiter = RateLimiter(40)  # 40 req/sec to avoid rate limiting

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, WEBSITE_DIR
from generate_web_data import LOCATION_COORDS  # Granular coords with (country, location) keys

import pandas as pd

# Load world cities database for geocoding
CITIES_FILE = DATA_DIR / "cities.json"
WORLD_CITIES = {}
if CITIES_FILE.exists():
    with open(CITIES_FILE) as f:
        _cities = json.load(f)
        # Build lookup by city name (lowercase) -> list of (lat, lng, country_code, pop)
        for c in _cities:
            key = c['name'].lower()
            if key not in WORLD_CITIES:
                WORLD_CITIES[key] = []
            WORLD_CITIES[key].append((c['lat'], c['lng'], c['cc'], c['pop']))

# Load capital/major city coordinates for countries (more precise than country centroids)
CAPITAL_COORDS_FILE = DATA_DIR / "capital_coords.json"
CAPITAL_COORDS = {}
if CAPITAL_COORDS_FILE.exists():
    with open(CAPITAL_COORDS_FILE) as f:
        CAPITAL_COORDS = json.load(f)

# Paths
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"
MATCH_EXCLUSIONS_FILE = DATA_DIR / "match_exclusions.json"
PRICES_FILE = DATA_DIR / "polymarket_all_political_prices_CORRECTED.json"
KALSHI_PRICES_FILE = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"
KALSHI_DAILY_PRICES_DIR = DATA_DIR / "kalshi_daily_prices"
MATCH_EXCLUSIONS_FILE = DATA_DIR / "match_exclusions.json"
SLUG_MAPPING_FILE = DATA_DIR / "pm_event_slug_mapping.json"
OUTPUT_FILE = WEBSITE_DIR / "data" / "active_markets.json"

# Native API endpoints
PM_CLOB_API = "https://clob.polymarket.com"
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Category display names
CATEGORY_DISPLAY = {
    '1. ELECTORAL': 'Electoral',
    '2. MONETARY_POLICY': 'Monetary Policy',
    '3. LEGISLATIVE': 'Legislative',
    '4. APPOINTMENTS': 'Appointments',
    '5. REGULATORY': 'Regulatory',
    '6. INTERNATIONAL': 'International',
    '7. JUDICIAL': 'Judicial',
    '8. MILITARY_SECURITY': 'Military & Security',
    '9. CRISIS_EMERGENCY': 'Crisis & Emergency',
    '10. GOVERNMENT_OPERATIONS': 'Government Operations',
    '11. PARTY_POLITICS': 'Party Politics',
    '12. STATE_LOCAL': 'State & Local',
    '13. TIMING_EVENTS': 'Timing & Events',
    '14. POLLING_APPROVAL': 'Polling & Approval',
    '15. POLITICAL_SPEECH': 'Political Speech',
}

# Category fallback images (for Kalshi markets without images)
# Using Unsplash free images - curated for each category
CATEGORY_IMAGES = {
    '1. ELECTORAL': 'https://images.unsplash.com/photo-1540910419892-4a36d2c3266c?w=400',  # Voting booth
    '2. MONETARY_POLICY': 'https://images.unsplash.com/photo-1526304640581-d334cdbbf45e?w=400',  # Dollar bills / finance
    '3. LEGISLATIVE': 'https://images.unsplash.com/photo-1541872703-74c5e44368f9?w=400',  # US Capitol dome
    '4. APPOINTMENTS': 'https://images.unsplash.com/photo-1560439514-4e9645039924?w=400',  # Boardroom meeting
    '5. REGULATORY': 'https://images.unsplash.com/photo-1450101499163-c8848c66ca85?w=400',  # Legal documents signing
    '6. INTERNATIONAL': 'https://images.unsplash.com/photo-1526470608268-f674ce90ebd4?w=400',  # World flags
    '7. JUDICIAL': 'https://images.unsplash.com/photo-1589829545856-d10d557cf95f?w=400',  # Lady Justice scales
    '8. MILITARY_SECURITY': 'https://images.unsplash.com/photo-1569974498991-d3c12a504f95?w=400',  # Military soldiers
    '9. CRISIS_EMERGENCY': 'https://images.unsplash.com/photo-1523995462485-3d171b5c8fa9?w=400',  # Emergency response
    '10. GOVERNMENT_OPERATIONS': 'https://images.unsplash.com/photo-1585399000684-d2f72660f092?w=400',  # White House
    '11. PARTY_POLITICS': 'https://images.unsplash.com/photo-1529107386315-e1a2ed48a620?w=400',  # Political rally crowd
    '12. STATE_LOCAL': 'https://images.unsplash.com/photo-1555848962-6e79363ec58f?w=400',  # State capitol building
    '13. TIMING_EVENTS': 'https://images.unsplash.com/photo-1506784983877-45594efa4cbe?w=400',  # Calendar / schedule
    '14. POLLING_APPROVAL': 'https://images.unsplash.com/photo-1551288049-bebda4e38f71?w=400',  # Data analytics charts
    '15. POLITICAL_SPEECH': 'https://images.unsplash.com/photo-1475721027785-f74eccf877e2?w=400',  # Microphone podium
}

# Coordinates for electoral markets (for globe display)
COUNTRY_COORDS = {
    'United States': {'lat': 39, 'lng': -98},
    'Germany': {'lat': 51, 'lng': 10},
    'France': {'lat': 47, 'lng': 2},
    'United Kingdom': {'lat': 54, 'lng': -2},
    'Brazil': {'lat': -15, 'lng': -48},
    'Mexico': {'lat': 23, 'lng': -102},
    'India': {'lat': 21, 'lng': 78},
    'Japan': {'lat': 36, 'lng': 138},
    'Australia': {'lat': -25, 'lng': 134},
    'Canada': {'lat': 56, 'lng': -106},
    'Italy': {'lat': 43, 'lng': 12},
    'Spain': {'lat': 40, 'lng': -4},
    'Poland': {'lat': 52, 'lng': 20},
    'Portugal': {'lat': 39, 'lng': -8},
    'Netherlands': {'lat': 52, 'lng': 5},
    'Belgium': {'lat': 51, 'lng': 4},
    'Austria': {'lat': 47, 'lng': 14},
    'Switzerland': {'lat': 47, 'lng': 8},
    'Sweden': {'lat': 62, 'lng': 18},
    'Norway': {'lat': 62, 'lng': 10},
    'Denmark': {'lat': 56, 'lng': 10},
    'Finland': {'lat': 64, 'lng': 26},
    'Ireland': {'lat': 53, 'lng': -8},
    'Greece': {'lat': 39, 'lng': 22},
    'Czech Republic': {'lat': 50, 'lng': 15},
    'Czechia': {'lat': 50, 'lng': 15},
    'Hungary': {'lat': 47, 'lng': 20},
    'Romania': {'lat': 46, 'lng': 25},
    'Bulgaria': {'lat': 43, 'lng': 25},
    'Ukraine': {'lat': 49, 'lng': 32},
    'Russia': {'lat': 60, 'lng': 100},
    'Turkey': {'lat': 39, 'lng': 35},
    'Israel': {'lat': 31, 'lng': 35},
    'South Africa': {'lat': -29, 'lng': 25},
    'Nigeria': {'lat': 10, 'lng': 8},
    'Egypt': {'lat': 27, 'lng': 30},
    'Kenya': {'lat': 0, 'lng': 38},
    'Argentina': {'lat': -34, 'lng': -64},
    'Chile': {'lat': -33, 'lng': -71},
    'Colombia': {'lat': 4, 'lng': -74},
    'Peru': {'lat': -10, 'lng': -76},
    'Venezuela': {'lat': 8, 'lng': -66},
    'Ecuador': {'lat': -2, 'lng': -78},
    'Costa Rica': {'lat': 10, 'lng': -84},
    'South Korea': {'lat': 36, 'lng': 128},
    'Korea': {'lat': 36, 'lng': 128},
    'Taiwan': {'lat': 24, 'lng': 121},
    'Thailand': {'lat': 15, 'lng': 101},
    'Indonesia': {'lat': -2, 'lng': 118},
    'Philippines': {'lat': 12, 'lng': 122},
    'Vietnam': {'lat': 16, 'lng': 106},
    'Malaysia': {'lat': 4, 'lng': 102},
    'Singapore': {'lat': 1, 'lng': 104},
    'China': {'lat': 35, 'lng': 105},
    'New Zealand': {'lat': -41, 'lng': 174},
}

# Update COUNTRY_COORDS with more precise capital/major city coordinates
# Keep United States at centroid since we have state-level coords, update all others
for country, data in CAPITAL_COORDS.items():
    if country != 'United States':  # Keep US at centroid, use state coords for granularity
        COUNTRY_COORDS[country] = {'lat': data['lat'], 'lng': data['lng']}

# Election type mapping
ELECTION_TYPE_MAP = {
    'President': 'presidential',
    'Vice President': 'presidential',
    'Parliament': 'parliamentary',
    'Prime Minister': 'parliamentary',
    'House': 'house',
    'Senate': 'senate',
    'Governor': 'governor',
    'Lt. Governor': 'governor',
    'Mayor': 'mayoral',
    'City Council': 'mayoral',
    'Regional': 'other',
    'Attorney General': 'other',
    'Other': 'other',
}

# Region mapping
REGION_MAP = {
    # North America
    'United States': 'north_america',
    'Canada': 'north_america',
    'Mexico': 'north_america',
    # South America
    'Brazil': 'south_america',
    'Argentina': 'south_america',
    'Colombia': 'south_america',
    'Chile': 'south_america',
    'Peru': 'south_america',
    'Venezuela': 'south_america',
    'Ecuador': 'south_america',
    'Bolivia': 'south_america',
    'Paraguay': 'south_america',
    'Uruguay': 'south_america',
    'Guyana': 'south_america',
    'Suriname': 'south_america',
    # Europe
    'United Kingdom': 'europe',
    'Germany': 'europe',
    'France': 'europe',
    'Italy': 'europe',
    'Spain': 'europe',
    'Poland': 'europe',
    'Netherlands': 'europe',
    'Belgium': 'europe',
    'Sweden': 'europe',
    'Norway': 'europe',
    'Denmark': 'europe',
    'Finland': 'europe',
    'Austria': 'europe',
    'Switzerland': 'europe',
    'Portugal': 'europe',
    'Greece': 'europe',
    'Czech Republic': 'europe',
    'Czechia': 'europe',
    'Romania': 'europe',
    'Hungary': 'europe',
    'Ireland': 'europe',
    'Ukraine': 'europe',
    'Russia': 'europe',
    'Belarus': 'europe',
    'Slovenia': 'europe',
    'Slovakia': 'europe',
    'Croatia': 'europe',
    'Serbia': 'europe',
    'Bulgaria': 'europe',
    'Lithuania': 'europe',
    'Latvia': 'europe',
    'Estonia': 'europe',
    'Moldova': 'europe',
    'Albania': 'europe',
    'North Macedonia': 'europe',
    'Montenegro': 'europe',
    'Kosovo': 'europe',
    'Bosnia and Herzegovina': 'europe',
    'Iceland': 'europe',
    'Luxembourg': 'europe',
    'Malta': 'europe',
    'Cyprus': 'europe',
    'European Union': 'europe',
    # Asia
    'Japan': 'asia',
    'South Korea': 'asia',
    'North Korea': 'asia',
    'China': 'asia',
    'Taiwan': 'asia',
    'India': 'asia',
    'Pakistan': 'asia',
    'Bangladesh': 'asia',
    'Indonesia': 'asia',
    'Philippines': 'asia',
    'Vietnam': 'asia',
    'Thailand': 'asia',
    'Malaysia': 'asia',
    'Singapore': 'asia',
    'Myanmar': 'asia',
    'Cambodia': 'asia',
    'Laos': 'asia',
    'Nepal': 'asia',
    'Sri Lanka': 'asia',
    'Mongolia': 'asia',
    'Kazakhstan': 'asia',
    'Uzbekistan': 'asia',
    'Afghanistan': 'asia',
    # Middle East
    'Israel': 'middle_east',
    'Turkey': 'middle_east',
    'Iran': 'middle_east',
    'Iraq': 'middle_east',
    'Saudi Arabia': 'middle_east',
    'United Arab Emirates': 'middle_east',
    'Qatar': 'middle_east',
    'Kuwait': 'middle_east',
    'Bahrain': 'middle_east',
    'Oman': 'middle_east',
    'Yemen': 'middle_east',
    'Jordan': 'middle_east',
    'Lebanon': 'middle_east',
    'Syria': 'middle_east',
    'Palestine': 'middle_east',
    'Egypt': 'middle_east',
    # Africa
    'South Africa': 'africa',
    'Nigeria': 'africa',
    'Kenya': 'africa',
    'Ethiopia': 'africa',
    'Ghana': 'africa',
    'Tanzania': 'africa',
    'Uganda': 'africa',
    'Morocco': 'africa',
    'Algeria': 'africa',
    'Tunisia': 'africa',
    'Libya': 'africa',
    'Sudan': 'africa',
    'Democratic Republic of the Congo': 'africa',
    'Senegal': 'africa',
    'Ivory Coast': 'africa',
    'Cameroon': 'africa',
    'Zimbabwe': 'africa',
    'Zambia': 'africa',
    'Rwanda': 'africa',
    'Benin': 'africa',
    # Oceania
    'Australia': 'oceania',
    'New Zealand': 'oceania',
    'Papua New Guinea': 'oceania',
    'Fiji': 'oceania',
}

# =========================================================================
# LOCATION EXTRACTION SYSTEM
# Extracts country/location/coordinates from markets for globe display
# =========================================================================

# US State codes mapping (abbreviation -> full name) and coordinates
US_STATE_CODES = {
    'AL': ('Alabama', 32.36, -86.30),
    'AK': ('Alaska', 64.20, -152.49),
    'AZ': ('Arizona', 34.05, -111.09),
    'AR': ('Arkansas', 34.80, -92.20),
    'CA': ('California', 36.78, -119.42),
    'CO': ('Colorado', 39.55, -105.78),
    'CT': ('Connecticut', 41.60, -72.76),
    'DE': ('Delaware', 38.91, -75.53),
    'DC': ('District of Columbia', 38.90, -77.04),
    'FL': ('Florida', 27.99, -81.76),
    'GA': ('Georgia', 32.68, -83.22),
    'HI': ('Hawaii', 19.90, -155.58),
    'ID': ('Idaho', 44.07, -114.74),
    'IL': ('Illinois', 40.63, -89.40),
    'IN': ('Indiana', 40.27, -86.13),
    'IA': ('Iowa', 41.88, -93.10),
    'KS': ('Kansas', 39.01, -98.48),
    'KY': ('Kentucky', 37.67, -84.67),
    'LA': ('Louisiana', 30.98, -91.96),
    'ME': ('Maine', 45.37, -69.60),
    'MD': ('Maryland', 39.05, -76.64),
    'MA': ('Massachusetts', 42.41, -71.38),
    'MI': ('Michigan', 44.31, -85.60),
    'MN': ('Minnesota', 46.73, -94.69),
    'MS': ('Mississippi', 32.35, -89.40),
    'MO': ('Missouri', 38.46, -92.29),
    'MT': ('Montana', 46.80, -110.36),
    'NE': ('Nebraska', 41.49, -99.90),
    'NV': ('Nevada', 38.80, -116.42),
    'NH': ('New Hampshire', 43.19, -71.57),
    'NJ': ('New Jersey', 40.06, -74.41),
    'NM': ('New Mexico', 34.52, -105.87),
    'NY': ('New York', 42.17, -74.95),
    'NC': ('North Carolina', 35.76, -79.02),
    'ND': ('North Dakota', 47.55, -101.00),
    'OH': ('Ohio', 40.42, -82.91),
    'OK': ('Oklahoma', 35.47, -97.52),
    'OR': ('Oregon', 43.80, -120.55),
    'PA': ('Pennsylvania', 41.20, -77.19),
    'RI': ('Rhode Island', 41.58, -71.48),
    'SC': ('South Carolina', 33.84, -81.16),
    'SD': ('South Dakota', 43.97, -99.90),
    'TN': ('Tennessee', 35.52, -86.58),
    'TX': ('Texas', 31.97, -99.90),
    'UT': ('Utah', 39.32, -111.09),
    'VT': ('Vermont', 44.56, -72.58),
    'VA': ('Virginia', 37.43, -78.66),
    'WA': ('Washington', 47.75, -120.74),
    'WV': ('West Virginia', 38.60, -80.45),
    'WI': ('Wisconsin', 43.78, -88.79),
    'WY': ('Wyoming', 43.08, -107.29),
}

# US Cities with coordinates
US_CITIES = {
    'NYC': ('New York City', 40.71, -74.01),
    'LA': ('Los Angeles', 34.05, -118.24),
    'CHICAGO': ('Chicago', 41.88, -87.63),
    'ATLANTA': ('Atlanta', 33.75, -84.39),
    'BOSTON': ('Boston', 42.36, -71.06),
    'SEATTLE': ('Seattle', 47.61, -122.33),
    'MIAMI': ('Miami', 25.76, -80.19),
    'DETROIT': ('Detroit', 42.33, -83.05),
    'PHILADELPHIA': ('Philadelphia', 39.95, -75.17),
    'PITTSBURGH': ('Pittsburgh', 40.44, -80.00),
    'CLEVELAND': ('Cleveland', 41.50, -81.69),
    'DENVER': ('Denver', 39.74, -104.99),
    'SANFRANCISCO': ('San Francisco', 37.77, -122.42),
    'SF': ('San Francisco', 37.77, -122.42),
    'OAKLAND': ('Oakland', 37.80, -122.27),
    'ALBUQUERQUE': ('Albuquerque', 35.08, -106.65),
    'CHARLOTTE': ('Charlotte', 35.23, -80.84),
    'CINCINNATI': ('Cincinnati', 39.10, -84.51),
    'MINNEAPOLIS': ('Minneapolis', 44.98, -93.27),
    'NEWORLEANS': ('New Orleans', 29.95, -90.07),
    'NOLA': ('New Orleans', 29.95, -90.07),
    'OMAHA': ('Omaha', 41.26, -95.94),
    'JERSEYCITY': ('Jersey City', 40.73, -74.04),
    'BUFFALO': ('Buffalo', 42.89, -78.88),
    'FORTWORTH': ('Fort Worth', 32.76, -97.33),
    'SANANTONIO': ('San Antonio', 29.42, -98.49),
    'BALTIMORE': ('Baltimore', 39.29, -76.61),
}

# Canadian provinces with coordinates
CANADA_PROVINCES = {
    'ON': ('Ontario', 51.25, -85.32),
    'QC': ('Quebec', 52.94, -73.55),
    'BC': ('British Columbia', 53.73, -127.65),
    'AB': ('Alberta', 53.93, -116.58),
    'MB': ('Manitoba', 53.76, -98.81),
    'SK': ('Saskatchewan', 52.94, -106.45),
    'NS': ('Nova Scotia', 44.68, -63.74),
    'NB': ('New Brunswick', 46.50, -66.16),
    'NL': ('Newfoundland and Labrador', 53.14, -57.66),
    'PE': ('Prince Edward Island', 46.25, -63.13),
    'NT': ('Northwest Territories', 64.27, -119.18),
    'YT': ('Yukon', 64.28, -135.00),
    'NU': ('Nunavut', 70.30, -83.11),
}

# Canadian cities
CANADA_CITIES = {
    'TORONTO': ('Toronto', 43.65, -79.38),
    'VANCOUVER': ('Vancouver', 49.28, -123.12),
    'MONTREAL': ('Montreal', 45.50, -73.57),
    'CALGARY': ('Calgary', 51.05, -114.07),
    'OTTAWA': ('Ottawa', 45.42, -75.69),
    'EDMONTON': ('Edmonton', 53.55, -113.49),
    'WINNIPEG': ('Winnipeg', 49.90, -97.14),
    'QUEBEC': ('Quebec City', 46.81, -71.21),
    'HAMILTON': ('Hamilton', 43.26, -79.87),
}

# Country name patterns in tickers/slugs (uppercase prefix -> country info)
COUNTRY_TICKER_PATTERNS = {
    'CANADA': ('Canada', 56, -106),
    'CAN': ('Canada', 56, -106),
    'GERMAN': ('Germany', 51, 10),
    'GER': ('Germany', 51, 10),
    'FRENCH': ('France', 47, 2),
    'FR': ('France', 47, 2),
    'FRANCE': ('France', 47, 2),
    'UK': ('United Kingdom', 54, -2),
    'BRITISH': ('United Kingdom', 54, -2),
    'BRITAIN': ('United Kingdom', 54, -2),
    'ENGLAND': ('United Kingdom', 54, -2),
    'BRAZIL': ('Brazil', -15, -48),
    'BR': ('Brazil', -15, -48),
    'MEXICO': ('Mexico', 23, -102),
    'MEX': ('Mexico', 23, -102),
    'INDIA': ('India', 21, 78),
    'JAPAN': ('Japan', 36, 138),
    'JPN': ('Japan', 36, 138),
    'AUSTRALIA': ('Australia', -25, 134),
    'AUS': ('Australia', -25, 134),
    'ISRAEL': ('Israel', 31, 35),
    'TURKEY': ('Turkey', 39, 35),
    'CHINA': ('China', 35, 105),
    'PRC': ('China', 35, 105),
    'TAIWAN': ('Taiwan', 24, 121),
    'KOREA': ('South Korea', 36, 128),
    'SEOUL': ('South Korea', 36, 128),
    'ARGENTINA': ('Argentina', -34, -64),
    'CHILE': ('Chile', -33, -71),
    'COLOMBIA': ('Colombia', 4, -74),
    'PERU': ('Peru', -10, -76),
    'VENEZUELA': ('Venezuela', 8, -66),
    'ECUADOR': ('Ecuador', -2, -78),
    'BOLIVIA': ('Bolivia', -17, -65),
    'URUGUAY': ('Uruguay', -34.88, -56.18),
    'PARAGUAY': ('Paraguay', -25.26, -57.58),
    'POLAND': ('Poland', 52, 20),
    'ROMANIA': ('Romania', 44, 26),
    'HUNGARY': ('Hungary', 47, 20),
    'CZECH': ('Czech Republic', 50, 15),
    'SLOVAKIA': ('Slovakia', 48, 17),
    'AUSTRIA': ('Austria', 47, 14),
    'SWISS': ('Switzerland', 47, 8),
    'SWITZERLAND': ('Switzerland', 47, 8),
    'SWEDEN': ('Sweden', 62, 18),
    'NORWAY': ('Norway', 62, 10),
    'DENMARK': ('Denmark', 56, 10),
    'FINLAND': ('Finland', 64, 26),
    'IRELAND': ('Ireland', 53, -8),
    'NETHERLANDS': ('Netherlands', 52, 5),
    'DUTCH': ('Netherlands', 52, 5),
    'NL': ('Netherlands', 52, 5),
    'BELGIUM': ('Belgium', 51, 4),
    'ITALY': ('Italy', 43, 12),
    'SPAIN': ('Spain', 40, -4),
    'PORTUGAL': ('Portugal', 39, -8),
    'GREECE': ('Greece', 39, 22),
    'UKRAINE': ('Ukraine', 49, 32),
    'RUSSIA': ('Russia', 60, 100),
    'GEORGIA': ('Georgia', 41.72, 44.79),  # The country, not US state
    'ARMENIA': ('Armenia', 40.18, 44.51),
    'MOLDOVA': ('Moldova', 47, 29),
    'ALBANIA': ('Albania', 41, 20),
    'SERBIA': ('Serbia', 44, 21),
    'BULGARIA': ('Bulgaria', 43, 25),
    'CROATIA': ('Croatia', 46, 16),
    'SLOVENIA': ('Slovenia', 46, 15),
    'LATVIA': ('Latvia', 57, 24),
    'LITHUANIA': ('Lithuania', 55, 24),
    'ESTONIA': ('Estonia', 59, 26),
    'CYPRUS': ('Cyprus', 35, 33),
    'MALTA': ('Malta', 36, 14),
    'ICELAND': ('Iceland', 65, -18),
    'GREENLAND': ('Greenland', 72, -42),
    'NEWZEALAND': ('New Zealand', -41, 174),
    'PHILIPPINES': ('Philippines', 12, 122),
    'THAILAND': ('Thailand', 15, 101),
    'VIETNAM': ('Vietnam', 16, 106),
    'MALAYSIA': ('Malaysia', 4, 102),
    'SINGAPORE': ('Singapore', 1, 104),
    'INDONESIA': ('Indonesia', -2, 118),
    'SOUTHAFRICA': ('South Africa', -29, 25),
    'NIGERIA': ('Nigeria', 10, 8),
    'KENYA': ('Kenya', 0, 38),
    'EGYPT': ('Egypt', 27, 30),
    'MOROCCO': ('Morocco', 32, -5),
    'ALGERIA': ('Algeria', 28, 2),
    'GHANA': ('Ghana', 8, -2),
    'ETHIOPIA': ('Ethiopia', 9, 38),
    'TANZANIA': ('Tanzania', -6, 35),
    'SENEGAL': ('Senegal', 14, -14),
    'ZAMBIA': ('Zambia', -15, 28),
    'ZIMBABWE': ('Zimbabwe', -18, 31),
    'MALAWI': ('Malawi', -14, 34),
    'RWANDA': ('Rwanda', -2, 30),
    'BURUNDI': ('Burundi', -3, 30),
    'UGANDA': ('Uganda', 1, 32),
    'CAMEROON': ('Cameroon', 6, 12),
    'IVORYCOAST': ('Ivory Coast', 8, -5),
    'GAMBIA': ('Gambia', 13, -16),
    'GUINEA': ('Guinea', 10, -10),
    'LEBANON': ('Lebanon', 34, 36),
    'SYRIA': ('Syria', 35, 38),
    'IRAN': ('Iran', 32, 53),
    'IRAQ': ('Iraq', 33, 44),
    'SAUDI': ('Saudi Arabia', 24, 45),
    'UAE': ('United Arab Emirates', 24, 54),
    'QATAR': ('Qatar', 25, 51),
    'PAKISTAN': ('Pakistan', 30, 70),
    'BANGLADESH': ('Bangladesh', 24, 90),
    'SRILANKA': ('Sri Lanka', 7, 81),
    'NEPAL': ('Nepal', 28, 84),
    'MYANMAR': ('Myanmar', 22, 96),
    'MONGOLIA': ('Mongolia', 46, 105),
    'DOMINICANREPUBLIC': ('Dominican Republic', 19, -70),
    'JAMAICA': ('Jamaica', 18, -77),
    'CUBA': ('Cuba', 22, -80),
    'HAITI': ('Haiti', 19, -72),
    'COSTARICA': ('Costa Rica', 10, -84),
    'PANAMA': ('Panama', 9, -80),
    'GUATEMALA': ('Guatemala', 15, -90),
    'HONDURAS': ('Honduras', 15, -87),
    'ELSALVADOR': ('El Salvador', 14, -89),
    'NICARAGUA': ('Nicaragua', 13, -85),
    'GUYANA': ('Guyana', 5, -59),
    'SURINAME': ('Suriname', 4, -56),
    'BENIN': ('Benin', 9.3, 2.3),
    'BENINESE': ('Benin', 9.3, 2.3),
    'TÜRKIYE': ('Turkey', 39, 35),
    'TURKIYE': ('Turkey', 39, 35),
    'ERDOGAN': ('Turkey', 39, 35),
    'ERDOĞAN': ('Turkey', 39, 35),
    'AKHANNOUCH': ('Morocco', 32, -5),
}

# Update COUNTRY_TICKER_PATTERNS with capital coordinates for more precision
for key, (country, lat, lng) in list(COUNTRY_TICKER_PATTERNS.items()):
    if country in CAPITAL_COORDS:
        cap = CAPITAL_COORDS[country]
        COUNTRY_TICKER_PATTERNS[key] = (country, cap['lat'], cap['lng'])

# Kalshi ticker patterns that imply US states
# Pattern: prefix + state code (e.g., GOVPARTYTX, HOUSECA, SENATEFL)
KALSHI_STATE_PREFIXES = [
    'GOVPARTY',      # Governor party by state
    'PRESPARTY',     # Presidential party by state
    'HOUSE',         # House race by state
    'SENATE',        # Senate race by state
    'KXSENATE',      # Senate races
    'KXHOUSE',       # House races
    'KXGOV',         # Governor races
    'KXATTYGE',      # Attorney General
    'KXATTYGE',      # Attorney General (alt)
    'KXSECSTATE',    # Secretary of State
    'KXLTGOV',       # Lt Governor
    'KXMAYOR',       # Mayor (followed by city)
]

# Categories that imply US location if no other location found
US_CENTRIC_CATEGORIES = {
    '2. MONETARY_POLICY',     # Fed, Treasury, etc.
    '3. LEGISLATIVE',         # Congress
    '4. APPOINTMENTS',        # Cabinet, judges
    '7. JUDICIAL',            # SCOTUS, federal courts
    '10. GOVERNMENT_OPERATIONS',  # Federal government
    '14. POLLING_APPROVAL',   # Biden/Trump approval
}

# Question text patterns that imply specific countries
QUESTION_LOCATION_PATTERNS = [
    # US Federal entities - use distinct locations to reduce overlap
    # Federal Reserve / Monetary: Eccles Building area
    (r'\b(Fed|Federal Reserve|FOMC|Jerome Powell)\b', 'United States', 'Federal Reserve', 38.8925, -77.0473),
    (r'\b(Treasury|Janet Yellen|Bessent)\b', 'United States', 'Treasury', 38.8978, -77.0328),
    # Congress: Capitol Hill
    (r'\b(Congress|Senate|House of Representatives|Capitol)\b', 'United States', 'Capitol Hill', 38.8899, -77.0091),
    # Supreme Court
    (r'\b(SCOTUS|Supreme Court|Justice Roberts|Justice Thomas|Justice Alito)\b', 'United States', 'Supreme Court', 38.8906, -77.0044),
    # Executive: White House area
    (r'\b(White House|Oval Office)\b', 'United States', 'White House', 38.8977, -77.0365),
    (r'\b(Biden|Trump|Kamala Harris|JD Vance)\b', 'United States', 'White House', 38.8977, -77.0365),
    (r'\b(Elon Musk.*DOGE|DOGE)\b', 'United States', 'White House', 38.8977, -77.0365),
    (r'\b(Mar-?a-?Lago)\b', 'United States', 'Mar-a-Lago', 26.6777, -80.0367),
    # Executive departments
    (r'\b(DHS|DOJ|Department of Justice)\b', 'United States', 'DOJ', 38.8935, -77.0250),
    (r'\b(DOD|Pentagon|Department of Defense)\b', 'United States', 'Pentagon', 38.8719, -77.0563),
    (r'\b(Department of|Cabinet|Secretary of)\b', 'United States', 'Washington DC', 38.90, -77.03),
    # Media/polling
    (r'\b(538|FiveThirtyEight|Nate Silver|RealClearPolitics|RCP)\b', 'United States', 'Washington DC', 38.91, -77.00),
    # Financial markets
    (r'\b(S&P 500|Dow Jones|NASDAQ|NYSE|Wall Street)\b', 'United States', 'Wall Street', 40.71, -74.01),
    # Regulatory agencies - spread around DC
    (r'\b(SEC )\b', 'United States', 'SEC', 38.8985, -77.0430),
    (r'\b(FTC )\b', 'United States', 'FTC', 38.8932, -77.0440),
    (r'\b(FCC )\b', 'United States', 'FCC', 38.8959, -77.0201),
    (r'\b(EPA )\b', 'United States', 'EPA', 38.8930, -77.0450),
    (r'\b(FDA )\b', 'United States', 'FDA', 39.0318, -77.0796),
    (r'\b(FAA |IRS |FBI |CIA |NSA |ATF )\b', 'United States', 'Washington DC', 38.90, -77.04),

    # Central banks (non-US)
    (r'\b(ECB|European Central Bank)\b', 'European Union', None, 50.11, 8.68),
    (r'\b(Bank of England|BoE)\b', 'United Kingdom', None, 51.51, -0.09),
    (r'\b(Bank of Japan|BoJ)\b', 'Japan', None, 35.68, 139.77),
    (r'\b(Bank of Canada|BoC)\b', 'Canada', None, 45.42, -75.70),
    (r'\b(Reserve Bank of Australia|RBA)\b', 'Australia', None, -33.87, 151.21),
    (r'\b(Reserve Bank of India|RBI)\b', 'India', None, 18.93, 72.83),
    (r'\b(People\'s Bank of China|PBOC)\b', 'China', None, 39.90, 116.40),
    (r'\b(Bundesbank)\b', 'Germany', None, 50.11, 8.68),
    (r'\b(Banque de France)\b', 'France', None, 48.86, 2.34),

    # International leaders
    (r'\b(Starmer|Sunak|Boris Johnson|UK Prime Minister|British PM)\b', 'United Kingdom', None, 51.50, -0.13),
    (r'\b(Macron|French President|Élysée)\b', 'France', None, 48.86, 2.35),
    (r'\b(Scholz|Merkel|German Chancellor|Bundestag)\b', 'Germany', None, 52.52, 13.40),
    (r'\b(Trudeau|Canadian PM|Ottawa)\b', 'Canada', None, 45.42, -75.70),
    (r'\b(Albanese|Australian PM|Canberra)\b', 'Australia', None, -35.28, 149.13),
    (r'\b(Modi|Indian PM|New Delhi)\b', 'India', None, 28.61, 77.21),
    (r'\b(Xi Jinping|Chinese President|Beijing|CCP|Communist Party of China)\b', 'China', None, 39.90, 116.40),
    # Zelensky patterns BEFORE Putin - so Zelensky markets default to Ukraine
    # Bilateral meeting markets (Putin AND Zelensky) -> Ukraine (Zelensky is usually the subject)
    (r'\b(Zelenskyy?|Kyiv|Ukrainian President|Ukraine)\b', 'Ukraine', None, 50.45, 30.52),
    (r'\b(Putin|Kremlin|Russian President)\b', 'Russia', None, 55.75, 37.62),
    (r'\b(Netanyahu|Israeli PM|Knesset|Tel Aviv)\b', 'Israel', None, 31.77, 35.22),
    (r'\b(Erdogan|Turkish President|Ankara)\b', 'Turkey', None, 39.93, 32.85),
    (r'\b(Lula|Bolsonaro|Brazilian President|Brasília)\b', 'Brazil', None, -15.79, -47.88),
    (r'\b(AMLO|Mexican President|Mexico City)\b', 'Mexico', None, 19.43, -99.13),
    (r'\b(Kishida|Japanese PM|Tokyo|Diet of Japan)\b', 'Japan', None, 35.68, 139.69),
    (r'\b(Yoon|South Korean President|Seoul|Blue House)\b', 'South Korea', None, 37.57, 126.98),

    # International organizations
    (r'\b(NATO|North Atlantic Treaty)\b', 'Belgium', 'Brussels', 50.85, 4.35),
    (r'\b(European Union|EU Parliament|Brussels)\b', 'Belgium', 'Brussels', 50.85, 4.35),
    (r'\b(United Nations|UN Security Council)\b', 'United States', 'New York City', 40.75, -73.97),
    (r'\b(IMF|World Bank|WTO)\b', 'United States', 'Washington D.C.', 38.90, -77.04),
    (r'\b(BRICS)\b', 'South Africa', None, -25.75, 28.19),
]


def extract_location_from_kalshi_ticker(ticker):
    """Extract location from Kalshi ticker pattern.

    Returns: (country, location, lat, lng) or None
    """
    if not ticker or not isinstance(ticker, str):
        return None

    ticker_upper = ticker.upper()

    # Check for US state patterns (GOVPARTYTX, SENATEFL, etc.)
    for prefix in KALSHI_STATE_PREFIXES:
        if ticker_upper.startswith(prefix):
            # Extract state code after prefix
            remainder = ticker_upper[len(prefix):]
            # Try 2-letter state code
            state_code = remainder[:2]
            if state_code in US_STATE_CODES:
                name, lat, lng = US_STATE_CODES[state_code]
                return ('United States', name, lat, lng)

    # Check for city-specific patterns (MAYORNYC, MAYORSF, etc.)
    for city_code, (city_name, lat, lng) in US_CITIES.items():
        if city_code in ticker_upper:
            return ('United States', city_name, lat, lng)

    # Check for Canadian patterns
    for prov_code, (prov_name, lat, lng) in CANADA_PROVINCES.items():
        if prov_code in ticker_upper and ('CAN' in ticker_upper or 'CANADA' in ticker_upper):
            return ('Canada', prov_name, lat, lng)

    for city_code, (city_name, lat, lng) in CANADA_CITIES.items():
        if city_code in ticker_upper:
            return ('Canada', city_name, lat, lng)

    # Check for country patterns in ticker
    for pattern, (country, lat, lng) in COUNTRY_TICKER_PATTERNS.items():
        if pattern in ticker_upper:
            return (country, None, lat, lng)

    return None


def parse_congressional_district(text):
    """Parse congressional district code like GA-14, NY-12, CA-52.

    Returns: (state_name, lat, lng) or None
    """
    if not text:
        return None

    # Match patterns like GA-14, NY-12, CA-52, also GA14, NY12
    match = re.search(r'\b([A-Z]{2})[-]?(\d{1,2})\b', str(text).upper())
    if match:
        state_code = match.group(1)
        if state_code in US_STATE_CODES:
            name, lat, lng = US_STATE_CODES[state_code]
            return (name, lat, lng)
    return None


def extract_location_from_slug(slug):
    """Extract location from Polymarket slug pattern.

    Returns: (country, location, lat, lng) or None
    """
    if not slug or not isinstance(slug, str):
        return None

    slug_lower = slug.lower().replace('-', ' ').replace('_', ' ')

    # Check for US state names in slug
    for state_code, (state_name, lat, lng) in US_STATE_CODES.items():
        state_lower = state_name.lower()
        if state_lower in slug_lower:
            return ('United States', state_name, lat, lng)

    # Check for country names in slug
    for pattern, (country, lat, lng) in COUNTRY_TICKER_PATTERNS.items():
        if pattern.lower() in slug_lower:
            return (country, None, lat, lng)

    return None


def lookup_city(text):
    """Look up a city name in the world cities database.

    Returns: (city_name, lat, lng) or None
    """
    # DISABLED: Too slow (40k cities * 4k+ markets = very slow)
    # TODO: Optimize with trie or pre-compiled patterns
    return None

    if not text or not WORLD_CITIES:
        return None

    # Try to find city names in text (check longer names first to avoid partial matches)
    text_lower = text.lower()
    for city_name in sorted(WORLD_CITIES.keys(), key=len, reverse=True):
        # Require word boundaries to avoid matching "lensk" inside "Zelenskyy"
        if len(city_name) > 3 and re.search(r'\b' + re.escape(city_name) + r'\b', text_lower):
            # Get the most populous match for this city name
            matches = WORLD_CITIES[city_name]
            best = max(matches, key=lambda x: x[3])  # x[3] is population
            return (city_name.title(), best[0], best[1])
    return None


def extract_location_from_question(question, category=None):
    """Extract location from question text using patterns.

    Returns: (country, location, lat, lng) or None
    """
    if not question or not isinstance(question, str):
        return None

    # Check question patterns
    for pattern, country, location, lat, lng in QUESTION_LOCATION_PATTERNS:
        if re.search(pattern, question, re.IGNORECASE):
            return (country, location, lat, lng)

    # Check for country names in question
    question_upper = question.upper()
    for pattern, (country, lat, lng) in COUNTRY_TICKER_PATTERNS.items():
        # Only match whole words to avoid false positives
        if re.search(r'\b' + re.escape(pattern) + r'\b', question_upper):
            return (country, None, lat, lng)

    # Check for US state names in question
    for state_code, (state_name, lat, lng) in US_STATE_CODES.items():
        if re.search(r'\b' + re.escape(state_name) + r'\b', question, re.IGNORECASE):
            return ('United States', state_name, lat, lng)

    # Try city lookup as fallback
    city_match = lookup_city(question)
    if city_match:
        city_name, lat, lng = city_match
        return (None, city_name, lat, lng)

    return None


def extract_location(row):
    """Extract location from a market row using priority order.

    Priority:
    1. Existing country/location fields (electoral markets)
    2. Kalshi ticker patterns
    3. Polymarket slug patterns
    4. Question text patterns
    5. Category inference (US-centric categories)

    Returns: dict with country, location, lat, lng (all may be None)
    """
    result = {'country': None, 'location': None, 'lat': None, 'lng': None}

    # Priority 1: Existing fields (electoral markets have these)
    existing_country = row.get('country')
    existing_location = row.get('location')
    if pd.notna(existing_country) and str(existing_country).strip():
        country = str(existing_country).strip()
        result['country'] = country
        location = None
        if pd.notna(existing_location):
            location = str(existing_location).strip()
            result['location'] = location

        # Look up coordinates - try granular (country, location) first, then country-only
        coord_key = (country, location) if location else (country, country)
        if coord_key in LOCATION_COORDS:
            lat, lng = LOCATION_COORDS[coord_key]
            result['lat'] = lat
            result['lng'] = lng
        elif location and country == 'United States':
            # Try parsing congressional district (GA-14, NY-12, etc.)
            district_match = parse_congressional_district(location)
            if district_match:
                state_name, lat, lng = district_match
                result['lat'] = lat
                result['lng'] = lng
            elif (country, country) in LOCATION_COORDS:
                lat, lng = LOCATION_COORDS[(country, country)]
                result['lat'] = lat
                result['lng'] = lng
            elif country in COUNTRY_COORDS:
                result['lat'] = COUNTRY_COORDS[country]['lat']
                result['lng'] = COUNTRY_COORDS[country]['lng']
        elif (country, country) in LOCATION_COORDS:
            # Fall back to country center
            lat, lng = LOCATION_COORDS[(country, country)]
            result['lat'] = lat
            result['lng'] = lng
        elif country in COUNTRY_COORDS:
            # Fall back to old COUNTRY_COORDS dict
            result['lat'] = COUNTRY_COORDS[country]['lat']
            result['lng'] = COUNTRY_COORDS[country]['lng']
        return result

    platform = row.get('platform', '')
    market_id = row.get('market_id', '')
    question = row.get('question', '')
    category = row.get('political_category', '')
    pm_slug = row.get('pm_market_slug', '')
    pm_event_slug = row.get('pm_event_slug', '')

    # Priority 2: Kalshi ticker patterns
    if platform == 'Kalshi' and market_id:
        loc = extract_location_from_kalshi_ticker(str(market_id))
        if loc:
            result['country'], result['location'], result['lat'], result['lng'] = loc
            return result

    # Priority 3: Polymarket slug patterns
    if platform == 'Polymarket':
        for slug in [pm_slug, pm_event_slug]:
            if pd.notna(slug):
                loc = extract_location_from_slug(str(slug))
                if loc:
                    result['country'], result['location'], result['lat'], result['lng'] = loc
                    return result

    # Priority 4: Question text patterns
    loc = extract_location_from_question(str(question) if pd.notna(question) else '', category)
    if loc:
        result['country'], result['location'], result['lat'], result['lng'] = loc
        return result

    # Priority 5: Category inference (US-centric categories)
    if category in US_CENTRIC_CATEGORIES:
        result['country'] = 'United States'
        result['lat'] = 38.90
        result['lng'] = -77.04
        return result

    return result


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_candlestick_prices():
    """Load daily candlestick price history for Polymarket tokens."""
    if PRICES_FILE.exists():
        with open(PRICES_FILE, 'r') as f:
            return json.load(f)
    return {}


def load_kalshi_candlestick_prices():
    """Load daily candlestick price history for Kalshi markets."""
    if KALSHI_PRICES_FILE.exists():
        with open(KALSHI_PRICES_FILE, 'r') as f:
            return json.load(f)
    return {}


def load_slug_mapping():
    """Load PM event_slug mapping from backfill."""
    if SLUG_MAPPING_FILE.exists():
        with open(SLUG_MAPPING_FILE, 'r') as f:
            data = json.load(f)
            return data.get('mapping', {})
    return {}


def fetch_pm_images_from_dome(condition_ids):
    """Load PM image URLs from cached data.

    The cache file contains image URLs keyed by condition_id,
    originally built from market data that includes image URLs.
    """
    images = {}

    # Load from cache (keyed by condition_id)
    cache_file = DATA_DIR / 'pm_image_cache_dome.json'
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                images = json.load(f)
            log(f"  Loaded {len(images):,} PM images from Dome cache")
        except Exception as e:
            log(f"  WARNING: Failed to load PM image cache: {e}")

    if not condition_ids:
        return images

    # Count how many we can match
    unique_ids = list(set(str(cid) for cid in condition_ids if cid and pd.notna(cid)))
    found = sum(1 for cid in unique_ids if cid in images)
    missing = len(unique_ids) - found

    log(f"  PM images: {found:,} found, {missing:,} missing (cache has {len(images):,} total)")

    return images


def _fetch_single_price(platform, identifier):
    """Fetch a single price from native APIs (for parallel execution)."""
    rate_limiter.wait()  # Respect rate limit
    try:
        if platform == 'pm':
            # Polymarket CLOB API
            url = f"{PM_CLOB_API}/price?token_id={identifier}&side=buy"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                price = data.get('price')
                if price is not None:
                    return (platform, identifier, float(price), None)
        else:
            # Kalshi native API
            url = f"{KALSHI_API_BASE}/markets/{identifier}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                market = data.get('market', data)
                price = market.get('last_price')
                if price is not None:
                    # Kalshi returns cents (0-100), convert to decimal
                    return (platform, identifier, float(price) / 100, None)
        return (platform, identifier, None, f"status_{response.status_code}")
    except Exception as e:
        return (platform, identifier, None, str(e)[:50])


def fetch_live_prices(pm_token_ids, kalshi_tickers, max_workers=20):
    """Fetch live prices from native APIs for both platforms in parallel."""
    pm_prices = {}
    kalshi_prices = {}

    # Build combined task list
    tasks = [('pm', tid) for tid in pm_token_ids] + [('kalshi', ticker) for ticker in kalshi_tickers]
    total_tasks = len(tasks)

    log(f"  Fetching live prices for {len(pm_token_ids)} PM + {len(kalshi_tickers)} Kalshi = {total_tasks} markets ({max_workers} workers)...")

    pm_fetched = 0
    kalshi_fetched = 0
    error_counts = {}  # Track error types

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_single_price, platform, identifier): (platform, identifier)
                   for platform, identifier in tasks}
        for future in as_completed(futures):
            platform, identifier, price, error = future.result()
            if price is not None:
                if platform == 'pm':
                    pm_prices[identifier] = price
                    pm_fetched += 1
                else:
                    kalshi_prices[identifier] = price
                    kalshi_fetched += 1
            else:
                error_counts[error] = error_counts.get(error, 0) + 1

            total = pm_fetched + kalshi_fetched + sum(error_counts.values())
            if total % 1000 == 0 and total > 0:
                log(f"    Progress: {pm_fetched} PM + {kalshi_fetched} Kalshi fetched, {sum(error_counts.values())} errors ({total}/{total_tasks})")

    total_errors = sum(error_counts.values())
    log(f"  Prices fetched: {pm_fetched} PM, {kalshi_fetched} Kalshi, {total_errors} errors")
    if error_counts:
        top_errors = sorted(error_counts.items(), key=lambda x: -x[1])[:5]
        log(f"  Top errors: {top_errors}")
    return pm_prices, kalshi_prices


# Legacy wrappers for backward compatibility
def fetch_live_pm_prices(token_ids, max_workers=50):
    """Fetch live prices for Polymarket tokens."""
    pm_prices, _ = fetch_live_prices(token_ids, [], max_workers)
    return pm_prices


def fetch_live_kalshi_prices(market_tickers, max_workers=50):
    """Fetch live prices for Kalshi markets."""
    _, kalshi_prices = fetch_live_prices([], market_tickers, max_workers)
    return kalshi_prices

    log(f"  Kalshi prices fetched: {fetched}, errors: {errors}")
    return live_prices


def load_yesterday_kalshi_prices():
    """Load yesterday's Kalshi price snapshot for 24h change calculation."""
    from datetime import timedelta
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday_file = KALSHI_DAILY_PRICES_DIR / f"kalshi_prices_{yesterday}.json"
    if yesterday_file.exists():
        with open(yesterday_file, 'r') as f:
            return json.load(f)
    return {}


def save_today_kalshi_prices(prices):
    """Save today's Kalshi prices for tomorrow's 24h change calculation."""
    KALSHI_DAILY_PRICES_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    today_file = KALSHI_DAILY_PRICES_DIR / f"kalshi_prices_{today}.json"
    with open(today_file, 'w') as f:
        json.dump(prices, f)
    # Clean up old files (keep last 7 days)
    from datetime import timedelta
    cutoff = datetime.now() - timedelta(days=7)
    for old_file in KALSHI_DAILY_PRICES_DIR.glob("kalshi_prices_*.json"):
        try:
            date_str = old_file.stem.replace("kalshi_prices_", "")
            file_date = datetime.strptime(date_str, '%Y-%m-%d')
            if file_date < cutoff:
                old_file.unlink()
        except (ValueError, OSError):
            pass


def calculate_kalshi_24h_change_from_snapshot(market_ticker, current_price, yesterday_prices):
    """Calculate 24h price change from daily snapshots."""
    if not market_ticker or current_price is None:
        return None
    yesterday_price = yesterday_prices.get(market_ticker)
    if yesterday_price is None:
        return None
    return current_price - yesterday_price


def get_current_price(token_id, candlesticks):
    """Get current price from candlestick data."""
    if not token_id or token_id not in candlesticks:
        return None
    prices = candlesticks[token_id]
    if not prices:
        return None
    sorted_prices = sorted(prices, key=lambda x: x['t'], reverse=True)
    return sorted_prices[0]['p']


def calculate_24h_change(token_id, candlesticks):
    """Calculate 24h price change from candlestick data."""
    if not token_id or token_id not in candlesticks:
        return None
    prices = candlesticks[token_id]
    if len(prices) < 2:
        return None
    sorted_prices = sorted(prices, key=lambda x: x['t'], reverse=True)
    current = sorted_prices[0]['p']
    previous = sorted_prices[1]['p']
    return current - previous


def get_kalshi_current_price(market_ticker, kalshi_candlesticks):
    """Get current price from Kalshi candlestick data."""
    if not market_ticker or market_ticker not in kalshi_candlesticks:
        return None
    prices = kalshi_candlesticks[market_ticker]
    if not prices:
        return None
    # Sort by end_period_ts descending
    sorted_prices = sorted(prices, key=lambda x: x.get('end_period_ts', 0), reverse=True)
    # Price is in cents, convert to 0-1 scale
    close_price = sorted_prices[0].get('price', {}).get('close')
    if close_price is not None:
        return float(close_price) / 100.0
    return None


def get_kalshi_mid_price(candle):
    """Get mid-price from Kalshi candlestick (average of yes_bid and yes_ask close)."""
    yes_bid = candle.get('yes_bid', {}).get('close')
    yes_ask = candle.get('yes_ask', {}).get('close')
    if yes_bid is not None and yes_ask is not None:
        return (float(yes_bid) + float(yes_ask)) / 2.0
    # Fallback to price.close if available
    price_close = candle.get('price', {}).get('close')
    if price_close is not None:
        return float(price_close)
    return None


def calculate_kalshi_24h_change(market_ticker, kalshi_candlesticks):
    """Calculate 24h price change from Kalshi candlestick data."""
    if not market_ticker:
        return None
    # Try both formats: raw ticker and with KX prefix
    ticker_to_use = None
    if market_ticker in kalshi_candlesticks:
        ticker_to_use = market_ticker
    elif f"KX{market_ticker}" in kalshi_candlesticks:
        ticker_to_use = f"KX{market_ticker}"
    if not ticker_to_use:
        return None
    prices = kalshi_candlesticks[ticker_to_use]
    if len(prices) < 2:
        return None
    sorted_prices = sorted(prices, key=lambda x: x.get('end_period_ts', 0), reverse=True)
    current = get_kalshi_mid_price(sorted_prices[0])
    previous = get_kalshi_mid_price(sorted_prices[1])
    if current is not None and previous is not None:
        # Convert from cents to 0-1 scale
        return (current - previous) / 100.0
    return None


def get_kalshi_fallback_price(row):
    """Get Kalshi price from CSV row, preferring bid/ask midpoint over stale last_price.

    The k_last_price can be very stale if there hasn't been a trade recently.
    The bid/ask spread gives a more current market view.
    """
    yes_bid = row.get('k_yes_bid')
    yes_ask = row.get('k_yes_ask')
    if pd.notna(yes_bid) and pd.notna(yes_ask) and float(yes_bid) > 0:
        return (float(yes_bid) + float(yes_ask)) / 200.0  # midpoint, convert from cents to 0-1
    last_price = row.get('k_last_price')
    if pd.notna(last_price):
        return float(last_price) / 100.0
    return None


def build_pm_url(row, slug_mapping):
    """Build Polymarket URL."""
    event_slug = row.get('pm_event_slug')
    if event_slug and pd.notna(event_slug):
        return f"https://polymarket.com/event/{event_slug}"

    if slug_mapping:
        condition_id = row.get('pm_condition_id')
        if condition_id and condition_id in slug_mapping:
            return f"https://polymarket.com/event/{slug_mapping[condition_id]}"

    return None


def build_kalshi_url(row):
    """Build Kalshi URL."""
    event_ticker = row.get('k_event_ticker')
    if event_ticker and pd.notna(event_ticker):
        return f"https://kalshi.com/events/{event_ticker}"
    return None


def build_pm_embed_url(row):
    """Build Polymarket embed iframe URL."""
    market_slug = row.get('pm_market_slug') or row.get('market_id')
    if market_slug and pd.notna(market_slug):
        return f"https://embed.polymarket.com/market.html?market={market_slug}&features=volume,chart,filters&theme=light"
    return None


def _get_end_datetime(row):
    """Get effective end datetime for a market row.

    Priority: scheduled_end_time > k_expiration_time > trading_close_time.
    Returns None if no date field is populated.
    """
    for field in ('scheduled_end_time', 'k_expiration_time', 'trading_close_time'):
        val = row.get(field)
        if pd.notna(val):
            try:
                return pd.to_datetime(val, utc=True)
            except Exception:
                continue
    return None


def _is_contract_expired(row, now, grace_hours=24):
    """True if contract end date + grace period has passed.

    Returns False (not expired) if no end date is available,
    so markets without date fields are never incorrectly hidden.
    """
    end_dt = _get_end_datetime(row)
    if end_dt is None:
        return False
    return now > (end_dt + pd.Timedelta(hours=grace_hours))


def load_match_exclusions() -> set:
    """Load match exclusions as a set of frozenset pairs of market IDs.

    Each exclusion means "these two markets should NOT be grouped together"
    even if they share a BWR ticker. Created by pipeline_apply_human_labels.py
    when users flag matches as "different events".
    """
    if not MATCH_EXCLUSIONS_FILE.exists():
        return set()
    try:
        with open(MATCH_EXCLUSIONS_FILE) as f:
            data = json.load(f)
        exclusions = set()
        for exc in data.get("exclusions", []):
            pair = frozenset([str(exc["market_id_a"]), str(exc["market_id_b"])])
            exclusions.add(pair)
        return exclusions
    except (json.JSONDecodeError, OSError, KeyError):
        return set()


def is_excluded_pair(market_a_id: str, market_b_id: str, exclusions: set) -> bool:
    """Check if two markets are in the exclusion set."""
    return frozenset([market_a_id, market_b_id]) in exclusions


def generate_monitor_data(skip_prices=False):
    """Generate monitor data for all active political markets.

    Uses BWR tickers from tickers_postprocessed.json as the canonical grouping system.
    Same ticker = same event. Cross-platform when both PM and K share a ticker.

    Args:
        skip_prices: If True, skip live price fetching and use cached historical prices
    """
    log("Generating monitor data (ticker-based grouping)...")

    # Load data
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"  Loaded {len(df):,} markets from master CSV")

    # Load tickers
    if not TICKERS_FILE.exists():
        log(f"  ERROR: Tickers file not found: {TICKERS_FILE}")
        log("  Run create_tickers.py + postprocess_tickers.py first")
        return None

    with open(TICKERS_FILE, 'r') as f:
        tickers_data = json.load(f)
    ticker_entries = tickers_data.get('tickers', [])
    log(f"  Loaded {len(ticker_entries):,} ticker entries")

    # Build market_id -> ticker_entry lookup
    mid_to_ticker = {}
    for t in ticker_entries:
        mid = str(t['market_id'])
        mid_to_ticker[mid] = t

    # Load match exclusions (human feedback: "different events")
    match_exclusions = load_match_exclusions()
    log(f"  Loaded {len(match_exclusions):,} match exclusions")

    candlesticks = load_candlestick_prices()
    log(f"  Loaded {len(candlesticks):,} tokens with price history")

    slug_mapping = load_slug_mapping()
    log(f"  Loaded {len(slug_mapping):,} PM slug mappings")

    kalshi_candlesticks = load_kalshi_candlestick_prices()
    log(f"  Loaded {len(kalshi_candlesticks):,} Kalshi markets with price history")

    yesterday_kalshi_prices = load_yesterday_kalshi_prices()
    log(f"  Loaded {len(yesterday_kalshi_prices):,} Kalshi prices from yesterday")

    # Filter to active markets
    # is_closed works for Polymarket; for Kalshi, use k_status since is_closed can be stale
    pm_active = df[(df['platform'] == 'Polymarket') & (df['is_closed'] != True)]
    k_active = df[(df['platform'] == 'Kalshi') & (df['k_status'].isin(['active', 'open']))]
    active = pd.concat([pm_active, k_active]).copy()

    # Filter to valid political categories only (exclude NOT_POLITICAL, PARTISAN_CONTROL, etc.)
    valid_categories = set(CATEGORY_DISPLAY.keys())
    before_filter = len(active)
    active = active[active['political_category'].isin(valid_categories)]
    excluded = before_filter - len(active)
    log(f"  Active markets: {len(active):,} ({excluded:,} excluded for invalid category)")

    # Filter out contracts whose end date has passed (with 24h grace period).
    # This catches stale contracts before resolution checking marks them closed.
    now_utc = pd.Timestamp.now(tz='UTC')
    before_expiry = len(active)
    active = active[~active.apply(lambda row: _is_contract_expired(row, now_utc), axis=1)]
    log(f"  Removed {before_expiry - len(active):,} expired contracts (end date + 24h grace passed)")

    # Build market_id -> row lookup from active markets
    mid_to_row = {}
    for _, row in active.iterrows():
        mid = str(row.get('market_id', '')).split('.')[0]
        if mid:
            mid_to_row[mid] = row

    # Collect all token IDs for live price fetch (single pass over active markets)
    pm_token_ids = set()
    k_market_tickers = set()
    pm_condition_ids = set()

    for _, row in active.iterrows():
        platform = row.get('platform')
        if platform == 'Polymarket':
            token_id = row.get('pm_token_id_yes')
            if pd.notna(token_id):
                pm_token_ids.add(str(token_id).split('.')[0])
            cid = row.get('pm_condition_id')
            if pd.notna(cid):
                pm_condition_ids.add(str(cid))
        elif platform == 'Kalshi':
            market_id = row.get('market_id')
            if pd.notna(market_id):
                k_market_tickers.add(str(market_id))

    # Fetch live prices (or skip if using cached data)
    if skip_prices:
        log("  Skipping live price fetch, using cached historical prices...")
        live_pm_prices = {}
        live_k_prices = {}
    else:
        live_pm_prices, live_k_prices = fetch_live_prices(pm_token_ids, k_market_tickers)

        # Save today's Kalshi prices for tomorrow's 24h change calculation
        save_today_kalshi_prices(live_k_prices)
        log(f"  Saved {len(live_k_prices):,} Kalshi prices for tomorrow")

    # Fetch PM images from cache (keyed by condition_id)
    pm_images = fetch_pm_images_from_dome(pm_condition_ids)

    # =========================================================================
    # GROUP ACTIVE MARKETS BY BWR TICKER
    # =========================================================================
    # For each active market, look up its ticker and group
    ticker_groups = defaultdict(lambda: {'pm_markets': [], 'k_markets': []})
    no_ticker_markets = []  # Markets without tickers -> individual fallback

    for _, row in active.iterrows():
        market_id = str(row.get('market_id', '')).split('.')[0]
        if not market_id:
            continue

        question = row.get('question', '')
        if not question or pd.isna(question):
            continue

        platform = row.get('platform')
        ticker_entry = mid_to_ticker.get(market_id)

        if ticker_entry:
            ticker_str = ticker_entry['ticker']
            # Never group UNKNOWN tickers cross-platform — they are
            # collision buckets from GPT failures, not real matches.
            if 'UNKNOWN' in ticker_str:
                no_ticker_markets.append(row)
            elif platform == 'Polymarket':
                ticker_groups[ticker_str]['pm_markets'].append(row)
            elif platform == 'Kalshi':
                ticker_groups[ticker_str]['k_markets'].append(row)
        else:
            no_ticker_markets.append(row)

    # ---- Apply match exclusions to split mismatched pairs ----
    exclusions = set()
    if MATCH_EXCLUSIONS_FILE.exists():
        try:
            with open(MATCH_EXCLUSIONS_FILE) as f:
                exc_data = json.load(f)
            for exc in exc_data.get("exclusions", []):
                pair = frozenset([str(exc["market_id_a"]), str(exc["market_id_b"])])
                exclusions.add(pair)
        except (json.JSONDecodeError, OSError, KeyError):
            pass

    if exclusions:
        log(f"  Loaded {len(exclusions)} match exclusions")
        new_ticker_groups = {}
        split_count = 0
        for ticker_str, group in ticker_groups.items():
            all_markets = group['pm_markets'] + group['k_markets']
            if len(all_markets) <= 1:
                new_ticker_groups[ticker_str] = group
                continue

            # Check if any exclusion applies to this group
            market_ids = [str(m.get('market_id', '')).split('.')[0] for m in all_markets]
            has_exclusion = False
            for i in range(len(market_ids)):
                for j in range(i + 1, len(market_ids)):
                    if frozenset([market_ids[i], market_ids[j]]) in exclusions:
                        has_exclusion = True
                        break
                if has_exclusion:
                    break

            if not has_exclusion:
                new_ticker_groups[ticker_str] = group
                continue

            # Greedy split: assign each market to first compatible subgroup
            subgroups = []  # list of lists of (market_id, market_row) tuples
            for mid, market in zip(market_ids, all_markets):
                placed = False
                for sg in subgroups:
                    conflict = any(frozenset([mid, existing_mid]) in exclusions for existing_mid, _ in sg)
                    if not conflict:
                        sg.append((mid, market))
                        placed = True
                        break
                if not placed:
                    subgroups.append([(mid, market)])

            for idx, sg in enumerate(subgroups):
                key = ticker_str if idx == 0 else f"{ticker_str}__excl{idx}"
                new_group = {'pm_markets': [], 'k_markets': []}
                for _, market in sg:
                    platform = market.get('platform')
                    if platform == 'Polymarket':
                        new_group['pm_markets'].append(market)
                    elif platform == 'Kalshi':
                        new_group['k_markets'].append(market)
                new_ticker_groups[key] = new_group
            split_count += 1

        if split_count:
            log(f"  Split {split_count} ticker groups due to exclusions")
        ticker_groups = new_ticker_groups

    log(f"  Unique tickers with active markets: {len(ticker_groups):,}")
    log(f"  Markets without tickers: {len(no_ticker_markets):,}")

    # =========================================================================
    # HELPER: Get price/change for a market row
    # =========================================================================
    def get_market_price(row):
        """Get current price and 24h change for a market row."""
        platform = row.get('platform')
        price = None
        price_change_24h = None

        if platform == 'Polymarket':
            token_id = row.get('pm_token_id_yes')
            if pd.notna(token_id):
                token_str = str(token_id).split('.')[0]
                if token_str in live_pm_prices:
                    price = live_pm_prices[token_str]
                else:
                    price = get_current_price(token_str, candlesticks)
                price_change_24h = calculate_24h_change(token_str, candlesticks)
            if price is None and pd.notna(row.get('last_price')):
                price = float(row.get('last_price'))
        else:  # Kalshi
            market_ticker = str(row.get('market_id', ''))
            if market_ticker in live_k_prices:
                price = live_k_prices[market_ticker]
            else:
                fallback = get_kalshi_fallback_price(row)
                if fallback is not None:
                    price = fallback
            price_change_24h = calculate_kalshi_24h_change_from_snapshot(market_ticker, price, yesterday_kalshi_prices)

        return price, price_change_24h

    def get_row_volume(row):
        """Get volume_usd from a row."""
        vol = row.get('volume_usd', 0)
        return float(vol) if pd.notna(vol) else 0

    # =========================================================================
    # BUILD ENTRIES FROM TICKER GROUPS
    # =========================================================================
    all_entries = []
    fully_expired_tickers = 0
    excluded_by_feedback = 0

    for ticker_str, group in ticker_groups.items():
        pm_markets = group['pm_markets']
        k_markets = group['k_markets']

        if not pm_markets and not k_markets:
            continue

        # Filter out expired contracts within the group (safety net)
        pm_markets = [m for m in pm_markets if not _is_contract_expired(m, now_utc)]
        k_markets = [m for m in k_markets if not _is_contract_expired(m, now_utc)]

        if not pm_markets and not k_markets:
            fully_expired_tickers += 1
            continue  # All contracts expired → hide entire event

        # Pick best market per platform by volume
        pm_best = max(pm_markets, key=get_row_volume) if pm_markets else None
        k_best = max(k_markets, key=get_row_volume) if k_markets else None

        # Check match exclusions: if the best PM and K markets are excluded
        # (human flagged as "different events"), split into separate entries
        if pm_best is not None and k_best is not None and match_exclusions:
            pm_id = str(pm_best.get('market_id', '')).split('.')[0]
            k_id = str(k_best.get('market_id', '')).split('.')[0]
            if is_excluded_pair(pm_id, k_id, match_exclusions):
                # Demote to two single-platform entries
                no_ticker_markets.append(pm_best)
                no_ticker_markets.append(k_best)
                excluded_by_feedback += 1
                continue

        has_pm = pm_best is not None
        has_k = k_best is not None
        has_both = has_pm and has_k

        # Get prices
        pm_price, pm_change_24h = get_market_price(pm_best) if has_pm else (None, None)
        k_price, k_change_24h = get_market_price(k_best) if has_k else (None, None)

        # Calculate spread (only when both platforms present)
        spread = abs(pm_price - k_price) if (has_both and pm_price is not None and k_price is not None) else None

        # Volumes
        pm_volume = get_row_volume(pm_best) if has_pm else 0
        k_volume = get_row_volume(k_best) if has_k else 0
        total_volume = pm_volume + k_volume

        # Label: prefer K question (usually cleaner), fall back to PM
        k_question = str(k_best.get('question', '')) if has_k else None
        pm_question = str(pm_best.get('question', '')) if has_pm else None
        label = k_question or pm_question or ticker_str

        # Category from best available market
        ref_row = k_best if has_k else pm_best
        category = ref_row.get('political_category', '15. OTHER')
        if pd.isna(category):
            category = '15. OTHER'

        # Entry type
        entry_type = 'cross_platform' if has_both else 'market'

        # URLs
        pm_url = build_pm_url(pm_best, slug_mapping) if has_pm else None
        k_url = build_kalshi_url(k_best) if has_k else None
        pm_embed_url = build_pm_embed_url(pm_best) if has_pm else None

        # Image: prefer PM image, fallback to category image
        image = None
        if has_pm:
            pm_cid = pm_best.get('pm_condition_id')
            if pd.notna(pm_cid) and str(pm_cid) in pm_images:
                image = pm_images[str(pm_cid)]
        if image is None and has_k:
            image = CATEGORY_IMAGES.get(category, CATEGORY_IMAGES.get('15. OTHER'))

        # Location for globe display
        loc_data = extract_location(ref_row)

        # Extract token IDs for live data lookup
        pm_token = pm_best.get('pm_token_id_yes') if has_pm else None
        pm_token_str = str(pm_token).split('.')[0] if pm_token and pd.notna(pm_token) else None

        # Electoral metadata (preserved for electoral markets)
        country = loc_data['country']
        location_str = loc_data['location']
        office = str(ref_row.get('office', '')).strip() if pd.notna(ref_row.get('office')) else None
        year = None
        election_year = ref_row.get('election_year')
        if pd.notna(election_year):
            try:
                year = int(float(election_year))
            except (ValueError, TypeError):
                pass

        region = REGION_MAP.get(country, 'unknown') if country else None

        # Price change: prefer PM (more liquid), fall back to K
        price_change_24h = pm_change_24h if pm_change_24h is not None else k_change_24h

        entry = {
            'key': ticker_str,
            'ticker': ticker_str,
            'label': label,
            'entry_type': entry_type,
            'category': category,
            'category_display': CATEGORY_DISPLAY.get(category, 'Other'),
            # Prices
            'pm_price': pm_price,
            'k_price': k_price,
            'spread': spread,
            'price_change_24h': price_change_24h,
            # Questions
            'pm_question': pm_question,
            'k_question': k_question,
            # Volume
            'pm_volume': pm_volume,
            'k_volume': k_volume,
            'total_volume': total_volume,
            # Counts
            'pm_markets_count': len(pm_markets),
            'k_markets_count': len(k_markets),
            # URLs
            'pm_url': pm_url,
            'k_url': k_url,
            'pm_embed_url': pm_embed_url,
            # Platform availability
            'has_pm': has_pm,
            'has_k': has_k,
            'has_both': has_both,
            # Market identifiers for live data
            'pm_market_id': str(pm_best.get('market_id')) if has_pm else None,
            'pm_token_id': pm_token_str,
            'k_ticker': str(k_best.get('market_id')) if has_k else None,
            # Globe coords
            'country': country,
            'location': location_str,
            'lat': loc_data['lat'],
            'lng': loc_data['lng'],
            'region': region,
            # Electoral metadata
            'office': office,
            'year': year,
            # Image
            'image': image,
        }

        # For single-platform entries, also set 'price' and 'platform' for backward compat
        if not has_both:
            entry['platform'] = 'Polymarket' if has_pm else 'Kalshi'
            entry['price'] = pm_price if has_pm else k_price
            entry['volume'] = total_volume

        all_entries.append(entry)

    ticker_count = len(all_entries)
    cross_platform_count = sum(1 for e in all_entries if e['entry_type'] == 'cross_platform')
    log(f"  Ticker-grouped entries: {ticker_count:,} ({cross_platform_count} cross-platform, {fully_expired_tickers} tickers fully expired, {excluded_by_feedback} split by human feedback)")

    # =========================================================================
    # PROCESS MARKETS WITHOUT TICKERS (fallback individual entries)
    # =========================================================================
    for row in no_ticker_markets:
        market_id = str(row.get('market_id', '')).split('.')[0]
        if not market_id:
            continue

        question = row.get('question', '')
        if not question or pd.isna(question):
            continue

        platform = row.get('platform')
        category = row.get('political_category', '15. OTHER')
        if pd.isna(category):
            category = '15. OTHER'

        price, price_change_24h = get_market_price(row)
        volume = get_row_volume(row)

        # URL
        if platform == 'Polymarket':
            url = build_pm_url(row, slug_mapping)
            embed_url = build_pm_embed_url(row)
        else:
            url = build_kalshi_url(row)
            embed_url = None

        # Image
        image = None
        if platform == 'Polymarket':
            pm_cid = row.get('pm_condition_id')
            if pd.notna(pm_cid) and str(pm_cid) in pm_images:
                image = pm_images[str(pm_cid)]
        elif platform == 'Kalshi':
            image = CATEGORY_IMAGES.get(category, CATEGORY_IMAGES.get('15. OTHER'))

        # Location for globe display
        loc_data = extract_location(row)

        # Token ID for live data
        pm_token = row.get('pm_token_id_yes')
        pm_token_str = str(pm_token).split('.')[0] if pd.notna(pm_token) else None

        entry = {
            'key': f"{platform.lower()}_{market_id}",
            'label': str(question),
            'entry_type': 'market',
            'platform': platform,
            'category': category,
            'category_display': CATEGORY_DISPLAY.get(category, 'Other'),
            'price': price,
            'price_change_24h': price_change_24h,
            'volume': volume,
            'total_volume': volume,
            'pm_url': url if platform == 'Polymarket' else None,
            'k_url': url if platform == 'Kalshi' else None,
            'embed_url': embed_url,
            'has_pm': platform == 'Polymarket',
            'has_k': platform == 'Kalshi',
            'has_both': False,
            'pm_market_id': str(market_id) if platform == 'Polymarket' else None,
            'pm_token_id': pm_token_str if platform == 'Polymarket' else None,
            'k_ticker': str(market_id) if platform == 'Kalshi' else None,
            'image': image,
            'country': loc_data['country'],
            'location': loc_data['location'],
            'lat': loc_data['lat'],
            'lng': loc_data['lng'],
            'region': REGION_MAP.get(loc_data['country'], 'unknown') if loc_data['country'] else None,
        }
        all_entries.append(entry)

    no_ticker_count = len(all_entries) - ticker_count
    log(f"  No-ticker fallback entries: {no_ticker_count:,}")
    log(f"  Total entries: {len(all_entries):,}")

    # Count location coverage
    has_location = sum(1 for e in all_entries if e.get('lat') is not None)
    log(f"  Location coverage: {has_location:,}/{len(all_entries):,} ({100*has_location/len(all_entries):.1f}%)")

    # Count by category
    category_counts = {}
    for e in all_entries:
        cat = e.get('category_display', 'Other')
        category_counts[cat] = category_counts.get(cat, 0) + 1

    # Build output
    output = {
        'generated_at': datetime.now().isoformat(),
        'counts': {
            'total': len(all_entries),
            'ticker_grouped': ticker_count,
            'cross_platform': cross_platform_count,
            'no_ticker_fallback': no_ticker_count,
        },
        'category_counts': category_counts,
        'markets': all_entries,
        'elections': all_entries,  # Backward compat for globe
    }

    # Save output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f)

    log(f"  Saved to {OUTPUT_FILE}")
    log(f"  Category counts: {category_counts}")

    return output


def fetch_orderbook(platform, token_id):
    """Fetch orderbook from native APIs."""
    rate_limiter.wait()  # Respect rate limit

    try:
        if platform == 'polymarket':
            # PM CLOB API - returns {"bids": [...], "asks": [...]}
            url = f"{PM_CLOB_API}/book?token_id={token_id}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('bids') or data.get('asks'):
                    return data
        else:
            # Kalshi API - returns {"orderbook": {"yes": [...], "no": [...]}}
            url = f"{KALSHI_API_BASE}/markets/{token_id}/orderbook"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('orderbook'):
                    return data
    except Exception:
        pass  # Silent fail for orderbooks

    return None


def compute_cost_to_move_5c(bids, asks):
    """Compute min cost to move price 5¢ in either direction."""
    def cost_up(asks):
        if not asks:
            return None
        start = asks[0]['price']
        target = start + 0.05
        spent = 0
        for ask in asks:
            if ask['price'] >= target:
                return spent
            spent += ask['price'] * ask['size']
        return None

    def cost_down(bids):
        if not bids:
            return None
        start = bids[0]['price']
        target = start - 0.05
        value = 0
        for bid in bids:
            if bid['price'] <= target:
                return value
            value += bid['price'] * bid['size']
        return None

    up = cost_up(asks)
    down = cost_down(bids)
    if up is None and down is None:
        return None
    if up is None:
        return down
    if down is None:
        return up
    return min(up, down)


def _assess_single_market(entry):
    """Assess robustness of a single market (for parallel execution)."""
    pm_token = entry.get('pm_token_id')
    k_ticker = entry.get('k_ticker')
    costs = []

    # Fetch PM orderbook
    if pm_token:
        ob = fetch_orderbook('polymarket', pm_token)
        if ob:
            try:
                bids = [{'price': float(b['price']), 'size': float(b['size'])}
                        for b in ob.get('bids', [])]
                asks = [{'price': float(a['price']), 'size': float(a['size'])}
                        for a in ob.get('asks', [])]
                # Sort: bids descending (highest first), asks ascending (lowest first)
                bids_sorted = sorted(bids, key=lambda x: -x['price'])
                asks_sorted = sorted(asks, key=lambda x: x['price'])
                cost = compute_cost_to_move_5c(bids_sorted, asks_sorted)
                if cost:
                    costs.append(cost)
            except (ValueError, TypeError):
                pass

    # Fetch K orderbook
    if k_ticker:
        ob = fetch_orderbook('kalshi', k_ticker)
        if ob:
            try:
                bids, asks = [], []
                # Kalshi prices are always in cents (1-99), convert to decimal
                for p, q in ob.get('orderbook', {}).get('yes', []) or []:
                    price = p / 100  # Always cents
                    bids.append({'price': price, 'size': q})
                for p, q in ob.get('orderbook', {}).get('no', []) or []:
                    price = 1 - (p / 100)  # No price -> Yes equivalent
                    asks.append({'price': price, 'size': q})
                cost = compute_cost_to_move_5c(
                    sorted(bids, key=lambda x: -x['price']),
                    sorted(asks, key=lambda x: x['price'])
                )
                if cost:
                    costs.append(cost)
            except (ValueError, TypeError):
                pass

    if not costs:
        return None
    return min(costs)


def generate_monitor_summary(elections, max_workers=50):
    """Generate robustness summary for Finding 3 (parallel)."""
    log(f"Generating market robustness summary ({max_workers} workers)...")

    robust_count = 0     # >= $100K
    caution_count = 0    # $10K-$100K
    processed = 0

    # Track robust and caution markets for the reportable tab
    robust_markets = []
    caution_markets = []

    # Filter to entries with a token/ticker AND enough volume to possibly be reportable.
    # Markets with <$10K total volume will never have $10K+ orderbook depth,
    # so skip them to avoid ~20K unnecessary API calls.
    MIN_VOLUME_FOR_REPORTABLE = 10000
    assessable = [e for e in elections
                  if (e.get('pm_token_id') or e.get('k_ticker'))
                  and (e.get('total_volume') or 0) >= MIN_VOLUME_FOR_REPORTABLE]
    total_with_ticker = sum(1 for e in elections if e.get('pm_token_id') or e.get('k_ticker'))
    fragile_count = total_with_ticker - len(assessable)  # Low-volume markets are fragile by definition
    total = total_with_ticker
    log(f"  Assessable markets (volume >= ${MIN_VOLUME_FOR_REPORTABLE:,}): {len(assessable)} "
        f"(skipping {fragile_count} low-volume as fragile)")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_assess_single_market, entry): entry for entry in assessable}
        for future in as_completed(futures):
            entry = futures[future]
            min_cost = future.result()
            processed += 1

            if min_cost is not None:
                if min_cost >= 100000:
                    robust_count += 1
                    # Save only key and cost - JS will look up full data from allMarkets
                    robust_markets.append({
                        'key': entry.get('key'),
                        'cost_to_move_5c': round(min_cost, 2),
                    })
                elif min_cost >= 10000:
                    caution_count += 1
                    caution_markets.append({
                        'key': entry.get('key'),
                        'cost_to_move_5c': round(min_cost, 2),
                    })
                else:
                    fragile_count += 1
            else:
                # No orderbook or insufficient depth = fragile
                fragile_count += 1

            if processed % 500 == 0:
                log(f"    Progress: {processed}/{len(assessable)} markets assessed")

    # Sort by cost descending
    robust_markets.sort(key=lambda x: -x['cost_to_move_5c'])
    caution_markets.sort(key=lambda x: -x['cost_to_move_5c'])

    summary = {
        'total_assessed': total,
        'robust_count': robust_count,
        'caution_count': caution_count,
        'fragile_count': fragile_count,
        'generated_at': datetime.now().isoformat()
    }

    output_path = WEBSITE_DIR / 'data' / 'monitor_summary.json'
    with open(output_path, 'w') as f:
        json.dump(summary, f, indent=2)

    # Save reportable markets (robust + caution) for the Reportable tab
    reportable = {
        'generated_at': datetime.now().isoformat(),
        'robust': robust_markets,
        'caution': caution_markets,
    }
    reportable_path = WEBSITE_DIR / 'data' / 'reportable_markets.json'
    with open(reportable_path, 'w') as f:
        json.dump(reportable, f, indent=2)

    log(f"  Monitor summary: {total} assessed, {robust_count} robust, {caution_count} caution, {fragile_count} fragile")
    log(f"  Saved to {output_path}")
    log(f"  Reportable markets: {len(robust_markets)} robust, {len(caution_markets)} caution")
    log(f"  Saved to {reportable_path}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Market Monitor data")
    parser.add_argument('--skip-prices', action='store_true',
                        help='Skip live price fetching, use cached historical prices')
    parser.add_argument('--skip-robustness', action='store_true',
                        help='Skip market robustness assessment (orderbook fetching)')
    args = parser.parse_args()

    output = generate_monitor_data(skip_prices=args.skip_prices)

    # Generate robustness summary for Finding 3
    if not args.skip_robustness and output:
        generate_monitor_summary(output.get('markets', []))
