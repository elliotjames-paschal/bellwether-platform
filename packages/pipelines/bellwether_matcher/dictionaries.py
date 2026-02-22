"""
Normalization dictionaries for market frame extraction.

Uses external packages where possible:
- pycountry: ISO country codes and subdivisions
- country_converter: Demonym→ISO conversion
- us: US state name/abbreviation lookup

Only hand-maintained dictionaries are those that can't be derived
from external packages (verbs, offices, parties, actors, metrics).
"""
from __future__ import annotations

from functools import lru_cache

import country_converter as coco
import pycountry
import us

# =============================================================================
# FRAME TYPE MAPPINGS (must be hand-maintained - semantic verb→frame routing)
# =============================================================================

FRAME_MAP = {
    # Contest frames (elections, races)
    'win': 'contest',
    'defeat': 'contest',
    'beat': 'contest',
    'run': 'contest',

    # Appointment frames
    'appoint': 'appointment',
    'nominate': 'appointment',
    'confirm': 'appointment',
    'replace': 'appointment',
    'succeed': 'appointment',

    # Policy change frames
    'raise': 'policy_change',
    'lower': 'policy_change',
    'cut': 'policy_change',
    'hike': 'policy_change',
    'increase': 'policy_change',
    'decrease': 'policy_change',

    # Threshold frames
    'reach': 'threshold',
    'hit': 'threshold',
    'exceed': 'threshold',
    'surpass': 'threshold',
    'fall': 'threshold',
    'drop': 'threshold',
    'rise': 'threshold',
    'climb': 'threshold',

    # Legislation frames
    'pass': 'legislation',
    'enact': 'legislation',
    'veto': 'legislation',
    'repeal': 'legislation',

    # Ruling frames
    'rule': 'ruling',
    'overturn': 'ruling',
    'uphold': 'ruling',
    'strike': 'ruling',
    'affirm': 'ruling',

    # Regulatory action frames
    'ban': 'regulatory_action',
    'impose': 'regulatory_action',
    'sanction': 'regulatory_action',
    'restrict': 'regulatory_action',
    'approve': 'regulatory_action',
    'authorize': 'regulatory_action',
    'revoke': 'regulatory_action',
    'suspend': 'regulatory_action',

    # Agreement frames
    'agree': 'agreement',
    'sign': 'agreement',
    'negotiate': 'agreement',
    'ratify': 'agreement',

    # Declaration frames
    'declare': 'declaration',
    'announce': 'declaration',
    'claim': 'declaration',
    'recognize': 'declaration',

    # Metric frames
    'poll': 'metric',
    'rate': 'metric',
    'measure': 'metric',

    # Occurrence frames
    'happen': 'occurrence',
    'occur': 'occurrence',
    'begin': 'occurrence',
    'start': 'occurrence',
    'end': 'occurrence',
    'resign': 'occurrence',
    'die': 'occurrence',
    'leave': 'occurrence',
    'withdraw': 'occurrence',
}

# Past participles for "be + participle" routing
PARTICIPLE_FRAME_MAP = {
    'elected': 'contest',
    'defeated': 'contest',
    'nominated': 'contest',
    'confirmed': 'appointment',
    'appointed': 'appointment',
    'replaced': 'appointment',
    'passed': 'legislation',
    'enacted': 'legislation',
    'signed': 'legislation',
    'vetoed': 'legislation',
    'repealed': 'legislation',
    'overturned': 'ruling',
    'upheld': 'ruling',
    'struck': 'ruling',
    'affirmed': 'ruling',
    'ruled': 'ruling',
    'banned': 'regulatory_action',
    'imposed': 'regulatory_action',
    'sanctioned': 'regulatory_action',
    'approved': 'regulatory_action',
    'authorized': 'regulatory_action',
    'revoked': 'regulatory_action',
    'suspended': 'regulatory_action',
    'ratified': 'agreement',
    'impeached': 'occurrence',
    'indicted': 'occurrence',
    'convicted': 'occurrence',
    'acquitted': 'occurrence',
    'arrested': 'occurrence',
    'fired': 'occurrence',
    'removed': 'occurrence',
}

# Threshold indicators for "be + comparative/preposition" routing
THRESHOLD_INDICATORS = {
    'above', 'below', 'over', 'under', 'less', 'more', 'greater', 'fewer',
    'higher', 'lower', 'at least', 'at most', 'between', 'than',
}

# Nouns that indicate occurrence frame
OCCURRENCE_NOUNS = {
    'shutdown', 'recession', 'default', 'crisis', 'collapse',
    'war', 'invasion', 'attack', 'strike', 'conflict',
    'impeachment', 'resignation', 'election', 'coup',
    'pandemic', 'outbreak', 'emergency', 'disaster',
}

# =============================================================================
# COUNTRY NORMALIZATION (uses pycountry + country_converter + small fallback)
# =============================================================================

# Demonyms that country_converter doesn't handle
# Only ~30 entries vs 420+ in the old COUNTRY_MAP
_DEMONYM_FALLBACK = {
    'american': 'US', 'americans': 'US',
    'british': 'GB',
    'french': 'FR',
    'german': 'DE', 'germans': 'DE',
    'spanish': 'ES',
    'dutch': 'NL',
    'belgian': 'BE',
    'polish': 'PL',
    'chinese': 'CN',
    'korean': 'KR', 'koreans': 'KR',
    'turkish': 'TR',
    'irish': 'IE',
    'norwegian': 'NO',
    'danish': 'DK',
    'finnish': 'FI',
    'greek': 'GR',
    'hungarian': 'HU',
    'filipino': 'PH',
    'thai': 'TH',
    'saudi': 'SA',
    'emirati': 'AE',
    'burmese': 'MM',
    'moroccan': 'MA',
    'ivorian': 'CI',
    'congolese': 'CD',
    'maltese': 'MT',
    'cypriot': 'CY',
    'montenegrin': 'ME',
    'kosovar': 'XK',
    'kiwi': 'NZ',
}

# Special aliases not in pycountry or country_converter
_COUNTRY_ALIASES = {
    'us': 'US', 'usa': 'US', 'u.s.': 'US', 'u.s.a.': 'US', 'america': 'US',
    'uk': 'GB', 'u.k.': 'GB', 'britain': 'GB', 'great britain': 'GB', 'england': 'GB',
    'prc': 'CN',
    'roc': 'TW',
    'dprk': 'KP',
    'drc': 'CD',
    'uae': 'AE',
    'holland': 'NL',
    'burma': 'MM',
    'czechia': 'CZ',
    'ivory coast': 'CI',
    "cote d'ivoire": 'CI',
    'republic of georgia': 'GE',
    'tbilisi': 'GE',
}

# Singleton country converter
_cc = coco.CountryConverter()


@lru_cache(maxsize=1024)
def normalize_country(text: str) -> str | None:
    """
    Normalize country name/demonym to ISO 3166-1 alpha-2 code.

    Uses (in order):
    1. Check if it's a US state (return None - states are not countries)
    2. Manual alias lookup (USA, UK, etc.)
    3. Demonym fallback (French, Dutch, etc. that country_converter misses)
    4. country_converter (handles most demonyms + country names)
    5. pycountry exact match (not fuzzy - too many false positives)
    """
    if not text:
        return None

    text_lower = text.lower().strip()

    # 1. Check if it's a US state - states are NOT countries
    state = us.states.lookup(text.strip())
    if state:
        return None  # Don't treat US states as countries

    # 2. Check manual aliases first (USA, UK, etc.)
    if text_lower in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[text_lower]

    # 3. Check demonym fallback
    if text_lower in _DEMONYM_FALLBACK:
        return _DEMONYM_FALLBACK[text_lower]

    # 4. Try country_converter (handles most demonyms + names)
    # Note: not_found=None returns the original string, so use a sentinel
    result = _cc.convert(text, to='ISO2', not_found='_NOT_FOUND_')
    if result and result != '_NOT_FOUND_' and result != text:
        # Verify it's a valid 2-letter ISO code
        if len(result) == 2 and result.isalpha():
            return result

    # 5. Try pycountry EXACT match only (fuzzy has too many false positives)
    try:
        # Try exact name match
        country = pycountry.countries.get(name=text)
        if country:
            return country.alpha_2
        # Try common name
        country = pycountry.countries.get(common_name=text)
        if country:
            return country.alpha_2
        # Try official name
        country = pycountry.countries.get(official_name=text)
        if country:
            return country.alpha_2
    except (LookupError, KeyError):
        pass

    return None


# =============================================================================
# US STATE NORMALIZATION (uses `us` package)
# =============================================================================

@lru_cache(maxsize=256)
def normalize_us_state(text: str) -> str | None:
    """
    Normalize US state name/abbreviation to 2-letter code.

    Uses `us` package which handles:
    - Full names: "California" → "CA"
    - Abbreviations: "CA" → "CA"
    - Some variants: "N.Y." → "NY"
    """
    if not text:
        return None

    state = us.states.lookup(text.strip())
    if state:
        return state.abbr
    return None


def get_us_state_name(abbr: str) -> str | None:
    """Get full state name from abbreviation."""
    state = us.states.lookup(abbr)
    if state:
        return state.name
    return None


# =============================================================================
# OFFICE MAP (must be hand-maintained - political domain knowledge)
# =============================================================================

OFFICE_MAP = {
    # Executive
    'president': 'PRES', 'presidential': 'PRES', 'presidency': 'PRES', 'potus': 'PRES',
    'vice president': 'VP', 'vice-president': 'VP', 'vp': 'VP', 'veep': 'VP',
    'governor': 'GOV', 'gubernatorial': 'GOV', 'governorship': 'GOV',
    'mayor': 'MAYOR', 'mayoral': 'MAYOR',
    'prime minister': 'PM', 'pm': 'PM', 'premier': 'PM',
    'chancellor': 'CHAN',

    # Legislative
    'senate': 'SEN', 'senator': 'SEN', 'senatorial': 'SEN',
    'house': 'HOUSE', 'house of representatives': 'HOUSE',
    'congressman': 'HOUSE', 'congresswoman': 'HOUSE', 'representative': 'HOUSE',
    'congress': 'CONGRESS', 'congressional': 'CONGRESS',
    'parliament': 'PARL', 'parliamentary': 'PARL', 'mp': 'PARL',
    'assembly': 'ASSEMBLY', 'state assembly': 'ASSEMBLY',

    # Party leadership
    'party leader': 'PARTY_LEADER', 'party chair': 'PARTY_CHAIR',
    'party chairman': 'PARTY_CHAIR', 'dnc chair': 'DNC_CHAIR', 'rnc chair': 'RNC_CHAIR',

    # Judiciary
    'supreme court': 'SCOTUS', 'scotus': 'SCOTUS',
    'chief justice': 'CHIEF_JUSTICE', 'justice': 'JUSTICE',

    # Cabinet
    'secretary of state': 'SEC_STATE', 'secretary of defense': 'SEC_DEF',
    'secretary of treasury': 'SEC_TREAS', 'attorney general': 'AG',

    # Central bank
    'fed chair': 'FED_CHAIR', 'federal reserve chair': 'FED_CHAIR',
    'fed chairman': 'FED_CHAIR', 'ecb president': 'ECB_PRES',
}


# =============================================================================
# PARTY MAP (must be hand-maintained - political domain knowledge)
# =============================================================================

PARTY_MAP = {
    # US parties
    'republican': 'GOP', 'republicans': 'GOP', 'gop': 'GOP',
    'r': 'GOP', '(r)': 'GOP', 'rep': 'GOP',
    'democrat': 'DEM', 'democrats': 'DEM', 'democratic': 'DEM',
    'd': 'DEM', '(d)': 'DEM', 'dem': 'DEM', 'dems': 'DEM',
    'independent': 'IND', 'independents': 'IND', 'ind': 'IND', 'i': 'IND', '(i)': 'IND',
    'libertarian': 'LIB', 'libertarians': 'LIB',
    'green': 'GREEN', 'green party': 'GREEN',

    # UK parties
    'conservative': 'CON', 'conservatives': 'CON', 'tory': 'CON', 'tories': 'CON',
    'labour': 'LAB', 'labor': 'LAB',
    'liberal democrat': 'LIBDEM', 'liberal democrats': 'LIBDEM',
    'lib dem': 'LIBDEM', 'lib dems': 'LIBDEM',
    'snp': 'SNP', 'scottish national party': 'SNP',

    # Canadian parties
    'liberal': 'LIB', 'liberals': 'LIB', 'conservative party': 'CON',
    'ndp': 'NDP', 'new democratic party': 'NDP',
    'bloc': 'BLOC', 'bloc québécois': 'BLOC',

    # German parties (must be before generic terms)
    'christian democratic': 'CDU', 'christian democrats': 'CDU',
    'cdu': 'CDU', 'cdu/csu': 'CDU', 'csu': 'CDU',
    'social democratic': 'SPD', 'social democrats': 'SPD', 'spd': 'SPD',
    'afd': 'AFD', 'alternative for germany': 'AFD',
    'greens': 'GREEN', 'die grünen': 'GREEN', 'grüne': 'GREEN',
    'free democratic': 'FDP', 'fdp': 'FDP',

    # Norwegian parties
    'høyre': 'HOYRE', 'hoyre': 'HOYRE',
    'arbeiderpartiet': 'AP', 'labour party': 'LAB',
    'fremskrittspartiet': 'FRP', 'frp': 'FRP', 'progress party': 'FRP',
    'senterpartiet': 'SP', 'centre party': 'SP',
    'kristelig folkeparti': 'KRF', 'krf': 'KRF',

    # French parties
    'en marche': 'LREM', 'la république en marche': 'LREM', 'renaissance': 'LREM',
    'rassemblement national': 'RN', 'national rally': 'RN', 'front national': 'RN',
    'les républicains': 'LR', 'the republicans': 'LR',

    # Italian parties
    "fratelli d'italia": 'FDI', 'brothers of italy': 'FDI',
    'lega': 'LEGA', 'lega nord': 'LEGA',
    'forza italia': 'FI',
    'partito democratico': 'PD', 'pd': 'PD',
    'movimento 5 stelle': 'M5S', 'five star': 'M5S', 'm5s': 'M5S',

    # Spanish parties
    'partido popular': 'PP', 'pp': 'PP',
    'psoe': 'PSOE', 'partido socialista': 'PSOE',
    'vox': 'VOX', 'podemos': 'POD',

    # Dutch parties
    'vvd': 'VVD', 'pvv': 'PVV', 'partij voor de vrijheid': 'PVV',
    'd66': 'D66', 'cda': 'CDA',

    # Australian parties
    'labor party': 'ALP', 'australian labor': 'ALP', 'alp': 'ALP',
    'liberal party': 'LPA',

    # Generic (must be LAST)
    'left': 'LEFT', 'right': 'RIGHT', 'center': 'CENTER', 'centre': 'CENTER',
}


# =============================================================================
# ACTOR/INSTITUTION MAP (must be hand-maintained)
# =============================================================================

ACTOR_MAP = {
    # Central banks
    'federal reserve': 'FED', 'fed': 'FED', 'fomc': 'FED',
    'ecb': 'ECB', 'european central bank': 'ECB',
    'bank of england': 'BOE', 'boe': 'BOE',
    'bank of japan': 'BOJ', 'boj': 'BOJ',
    "people's bank of china": 'PBOC', 'pboc': 'PBOC',
    'bank of canada': 'BOC', 'boc': 'BOC',
    'reserve bank of australia': 'RBA', 'rba': 'RBA',
    'bank of korea': 'BOK', 'bok': 'BOK',
    'bank of russia': 'CBR', 'cbr': 'CBR',
    'reserve bank of new zealand': 'RBNZ', 'rbnz': 'RBNZ',

    # US Government
    'white house': 'WH',
    'congress': 'CONGRESS', 'senate': 'SENATE', 'house': 'HOUSE',
    'supreme court': 'SCOTUS',
    'doj': 'DOJ', 'department of justice': 'DOJ',
    'sec': 'SEC', 'securities and exchange commission': 'SEC',
    'ftc': 'FTC', 'federal trade commission': 'FTC',
    'fda': 'FDA', 'food and drug administration': 'FDA',
    'epa': 'EPA', 'environmental protection agency': 'EPA',
    'treasury': 'TREAS', 'treasury department': 'TREAS',

    # International
    'un': 'UN', 'united nations': 'UN',
    'nato': 'NATO',
    'eu': 'EU', 'european union': 'EU',
    'imf': 'IMF', 'international monetary fund': 'IMF',
    'world bank': 'WB',
    'who': 'WHO', 'world health organization': 'WHO',
    'wto': 'WTO', 'world trade organization': 'WTO',
}


# =============================================================================
# METRIC MAP (must be hand-maintained)
# =============================================================================

METRIC_MAP = {
    # Economic indicators
    'gdp': 'GDP', 'gross domestic product': 'GDP',
    'inflation': 'INFLATION',
    'cpi': 'CPI', 'consumer price index': 'CPI',
    'unemployment': 'UNEMPLOYMENT', 'unemployment rate': 'UNEMPLOYMENT',
    'jobs': 'JOBS', 'nonfarm payrolls': 'NFP',
    'interest rate': 'RATE', 'interest rates': 'RATE',
    'fed funds rate': 'FFR', 'federal funds rate': 'FFR',

    # Market indicators
    's&p': 'SPX', 's&p 500': 'SPX', 'sp500': 'SPX',
    'dow': 'DJIA', 'dow jones': 'DJIA',
    'nasdaq': 'NDX',
    'bitcoin': 'BTC', 'btc': 'BTC',
    'ethereum': 'ETH', 'eth': 'ETH',

    # Polling
    'approval rating': 'APPROVAL', 'approval': 'APPROVAL',
    'favorability': 'FAVORABILITY',
    'polling': 'POLL', 'polls': 'POLL',
}


# =============================================================================
# OUTCOME TYPE MAP (must be hand-maintained)
# =============================================================================

OUTCOME_TYPE_MAP = {
    'win': 'WIN', 'elected': 'WIN',
    'majority': 'MAJORITY', 'control': 'CONTROL',
    'nominate': 'NOMINATION', 'nominee': 'NOMINATION', 'nomination': 'NOMINATION',
    'run': 'CANDIDACY', 'announce': 'CANDIDACY', 'candidate': 'CANDIDACY',
    'resign': 'RESIGN', 'impeach': 'IMPEACH', 'convict': 'CONVICT',
    'confirm': 'CONFIRM', 'appoint': 'APPOINT', 'remain': 'REMAIN',
}


# =============================================================================
# SCOPE/SUBDIVISION DETECTION (uses `us` package for US states)
# =============================================================================

# Non-US scope keywords (Canadian provinces, UK regions, major cities)
_NON_US_SCOPE = {
    # Canadian provinces
    'ontario': 'ON', 'quebec': 'QC', 'british columbia': 'BC',
    'alberta': 'AB', 'manitoba': 'MB', 'saskatchewan': 'SK',
    'nova scotia': 'NS', 'new brunswick': 'NB',

    # UK regions
    'london': 'LONDON', 'scotland': 'SCOTLAND', 'wales': 'WALES',
    'northern ireland': 'NI',

    # Major US cities (state lookup won't find these)
    'new york city': 'NYC', 'nyc': 'NYC',
    'los angeles': 'LA', 'chicago': 'CHICAGO', 'houston': 'HOUSTON',
    'philadelphia': 'PHILA', 'phoenix': 'PHOENIX',
    'san antonio': 'SAN_ANTONIO', 'san diego': 'SAN_DIEGO',
    'dallas': 'DALLAS', 'san francisco': 'SF', 'austin': 'AUSTIN',
    'seattle': 'SEATTLE', 'denver': 'DENVER', 'boston': 'BOSTON',
    'atlanta': 'ATLANTA', 'miami': 'MIAMI', 'detroit': 'DETROIT',
    'oakland': 'OAKLAND',

    # Special
    'federal': 'FEDERAL', 'national': 'NATIONAL',
}


def detect_scope(text: str) -> tuple[str, str] | None:
    """
    Detect geographic scope from text.
    Returns (scope_code, scope_type) or None.

    Uses `us` package for US states, manual dict for cities/provinces.
    """
    if not text:
        return None

    text_lower = text.lower().strip()

    # Check non-US scope first (cities, Canadian provinces, etc.)
    for keyword, code in _NON_US_SCOPE.items():
        if keyword in text_lower:
            scope_type = 'city' if code in ('NYC', 'LA', 'CHICAGO', 'SF', 'BOSTON', 'ATLANTA', 'MIAMI', 'DETROIT', 'OAKLAND') else 'region'
            return (code, scope_type)

    # Try US state lookup
    # Extract potential state names from text
    for word_count in [2, 1]:  # Try 2-word then 1-word matches
        words = text_lower.split()
        for i in range(len(words) - word_count + 1):
            candidate = ' '.join(words[i:i+word_count])
            state = us.states.lookup(candidate)
            if state:
                return (state.abbr, 'state')

    return None


# =============================================================================
# PATTERN CONSTANTS
# =============================================================================

# House district pattern (e.g., "OH-11", "CA-52", "WA-03")
HOUSE_DISTRICT_PATTERN = r'\b([A-Z]{2})-?(\d{1,2})\b'

# Year extraction patterns
YEAR_PATTERNS = [
    r'\b(20\d{2})\b',  # 2020, 2024, etc.
    r'\b(19\d{2})\b',  # 1990s (rare)
]

# Date patterns
DATE_PATTERNS = [
    r'(?:by|before|on|after)\s+(\w+\s+\d{1,2},?\s+\d{4})',
    r'(\w+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})',
    r'(\d{1,2}/\d{1,2}/\d{4})',
    r'(\d{4}-\d{2}-\d{2})',
]


# =============================================================================
# LEGACY COMPATIBILITY - SCOPE_KEYWORDS (deprecated, use detect_scope instead)
# =============================================================================

# Build SCOPE_KEYWORDS from us package + non-US scope for backwards compatibility
SCOPE_KEYWORDS = []

# Add all US states
for state in us.states.STATES:
    SCOPE_KEYWORDS.append((state.name.lower(), state.abbr))

# Add DC
SCOPE_KEYWORDS.extend([
    ('district of columbia', 'DC'),
    ('d.c.', 'DC'),
    ('dc', 'DC'),
])

# Add non-US scope
for keyword, code in _NON_US_SCOPE.items():
    SCOPE_KEYWORDS.append((keyword, code))


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def normalize_office(text: str) -> str | None:
    """Normalize office name to canonical form."""
    if not text:
        return None
    return OFFICE_MAP.get(text.lower().strip())


def normalize_party(text: str) -> str | None:
    """Normalize party name to canonical form."""
    if not text:
        return None
    return PARTY_MAP.get(text.lower().strip())


def normalize_actor(text: str) -> str | None:
    """Normalize institution/actor name to canonical form."""
    if not text:
        return None
    return ACTOR_MAP.get(text.lower().strip())


def normalize_metric(text: str) -> str | None:
    """Normalize metric name to canonical form."""
    if not text:
        return None
    return METRIC_MAP.get(text.lower().strip())
