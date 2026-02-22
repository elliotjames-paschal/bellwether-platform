"""
Frame extraction using spaCy NLP.

Parses prediction market questions to extract structured semantic frames
containing the key entities, relationships, and context.
"""
from __future__ import annotations

import re
from typing import Any

from unidecode import unidecode

from .dictionaries import (
    FRAME_MAP,
    PARTICIPLE_FRAME_MAP,
    THRESHOLD_INDICATORS,
    OCCURRENCE_NOUNS,
    OFFICE_MAP,
    PARTY_MAP,
    SCOPE_KEYWORDS,
    ACTOR_MAP,
    METRIC_MAP,
    OUTCOME_TYPE_MAP,
    YEAR_PATTERNS,
    DATE_PATTERNS,
    HOUSE_DISTRICT_PATTERN,
    normalize_country,
    normalize_us_state,
)

# Global NLP model (lazy loaded)
_nlp = None


def load_nlp():
    """
    Load spaCy model (singleton pattern for efficiency).

    Uses en_core_web_trf (transformer model) only. No fallback.
    """
    global _nlp
    if _nlp is not None:
        return _nlp

    import spacy

    try:
        _nlp = spacy.load("en_core_web_trf")
        return _nlp
    except OSError:
        raise RuntimeError(
            "spaCy transformer model not found. Install with:\n"
            "  python -m spacy download en_core_web_trf"
        )


def extract_frame(question: str, market_metadata: dict | None = None) -> dict[str, Any]:
    """
    Parse a market question and extract a structured semantic frame.

    Frame type is determined ONLY by spaCy verb parsing, never by metadata.
    Category is passed through as metadata but does not influence extraction.

    Args:
        question: The market question text
        market_metadata: Optional dict with existing metadata (platform, category, etc.)
                        Category is passed through but does not influence frame_type.

    Returns:
        dict with extracted frame fields:
        - frame_type: contest, policy_change, threshold, appointment, etc.
        - candidate: Person name if relevant
        - country: ISO country code
        - office: Canonical office name
        - party: Party affiliation
        - year: Election/event year
        - scope: Geographic scope (state, city, district)
        - outcome_type: WIN, NOMINATION, CANDIDACY, etc.
        - threshold_value: Numeric threshold if applicable
        - actor: Institution/actor if applicable
        - metric: Metric being measured if applicable
        - date_reference: Date mentioned in question
        - raw_question: Original question text
        - extraction_confidence: 0-1 confidence score
        - political_category: Pass-through from metadata (not used in extraction)
    """
    if not question:
        return _empty_frame(question, market_metadata)

    # Normalize question text
    question_clean = _clean_question(question)

    # Load spaCy and parse
    nlp = load_nlp()
    doc = nlp(question_clean)

    # Start with empty frame (includes category pass-through)
    frame = _empty_frame(question, market_metadata)

    # Extract entities from spaCy NER
    frame = _extract_entities(frame, doc)

    # Identify frame type from verb analysis ONLY (not metadata)
    frame = _identify_frame_type(frame, doc)

    # Extract frame-specific fields
    if frame['frame_type'] == 'contest':
        frame = _extract_contest_frame(frame, doc, question_clean)
    elif frame['frame_type'] == 'threshold':
        frame = _extract_threshold_frame(frame, doc, question_clean)
    elif frame['frame_type'] == 'metric':
        frame = _extract_threshold_frame(frame, doc, question_clean)  # Same logic
    elif frame['frame_type'] == 'appointment':
        frame = _extract_appointment_frame(frame, doc, question_clean)
    elif frame['frame_type'] == 'policy_change':
        frame = _extract_policy_frame(frame, doc, question_clean)
    elif frame['frame_type'] == 'legislation':
        frame = _extract_legislation_frame(frame, doc, question_clean)
    elif frame['frame_type'] == 'ruling':
        frame = _extract_ruling_frame(frame, doc, question_clean)
    elif frame['frame_type'] == 'regulatory_action':
        frame = _extract_regulatory_frame(frame, doc, question_clean)
    elif frame['frame_type'] == 'occurrence':
        frame = _extract_occurrence_frame(frame, doc, question_clean)
    elif frame['frame_type'] == 'declaration':
        frame = _extract_declaration_frame(frame, doc, question_clean)

    # Fill nulls from Kalshi ticker (if available) - after spaCy, before close_time
    if market_metadata and market_metadata.get('platform') == 'Kalshi':
        ticker = market_metadata.get('market_id') or market_metadata.get('ticker')
        if ticker:
            frame = _apply_kalshi_ticker_hints(frame, ticker)

    # Extract year from text patterns, with fallback to market close time
    frame = _extract_year(frame, question_clean, market_metadata)

    # Extract date references
    frame = _extract_date_reference(frame, question_clean)

    # Extract scope (state, city, district)
    frame = _extract_scope(frame, question_clean)

    # Calculate confidence (based on extraction quality, not metadata)
    frame['extraction_confidence'] = _calculate_confidence(frame)

    return frame


def _empty_frame(question: str, metadata: dict | None = None) -> dict[str, Any]:
    """Return empty frame structure with category pass-through."""
    return {
        'frame_type': None,
        'candidate': None,
        'country': None,
        'office': None,
        'party': None,
        'year': None,
        'scope': None,
        'scope_type': None,
        'outcome_type': None,
        'threshold_value': None,
        'threshold_direction': None,
        'actor': None,
        'metric': None,
        'date_reference': None,
        'raw_question': question,
        'extraction_confidence': 0.0,
        'extracted_names': [],
        # Pass-through metadata fields (do not influence extraction)
        'political_category': metadata.get('political_category') if metadata else None,
    }


def _clean_question(question: str) -> str:
    """Clean and normalize question text."""
    # Convert to ASCII for consistent matching
    text = unidecode(question)
    # Remove extra whitespace
    text = ' '.join(text.split())
    return text


# Auxiliary verbs that appear at start of yes/no questions
_AUXILIARY_VERBS = {
    'will', 'does', 'do', 'is', 'are', 'was', 'were', 'has', 'have', 'had',
    'can', 'could', 'would', 'should', 'shall', 'may', 'might', 'must',
}


def _strip_leading_auxiliary(name: str) -> str | None:
    """
    Strip leading auxiliary verb from a name extracted at start of yes/no question.

    "Will Trump" -> "Trump"
    "Does Biden" -> "Biden"
    "Trump" -> "Trump" (unchanged)

    Returns None if nothing remains after stripping.
    """
    if not name:
        return None

    parts = name.split()
    if not parts:
        return None

    # Check if first word is an auxiliary verb
    if parts[0].lower() in _AUXILIARY_VERBS:
        # Remove the auxiliary
        remaining = parts[1:]
        if remaining:
            return ' '.join(remaining)
        else:
            return None

    return name


def _extract_entities(frame: dict, doc) -> dict:
    """Extract named entities from spaCy doc."""
    persons = []
    gpes = []  # Geopolitical entities
    orgs = []
    dates = []

    for ent in doc.ents:
        if ent.label_ == 'PERSON':
            # Strip leading auxiliary verbs from yes/no questions
            # "Will Trump" -> "Trump", "Does Biden" -> "Biden"
            clean_name = _strip_leading_auxiliary(ent.text)
            if clean_name:
                persons.append(clean_name)
        elif ent.label_ == 'GPE':
            gpes.append(ent.text)
        elif ent.label_ == 'ORG':
            orgs.append(ent.text)
        elif ent.label_ == 'DATE':
            dates.append(ent.text)

    # Store extracted names
    frame['extracted_names'] = persons

    # If we found exactly one person and no candidate yet, use it
    if len(persons) == 1 and not frame.get('candidate'):
        frame['candidate'] = persons[0]
    elif len(persons) > 1:
        # Multiple persons - likely vs match
        frame['extracted_names'] = persons

    # Try to identify country from GPEs
    # ALSO check if GPE is a US state → infer country=US
    # Skip US cities (they're scope, not country)
    _US_CITIES = {
        'oakland', 'new york city', 'nyc', 'los angeles', 'chicago', 'houston',
        'philadelphia', 'phoenix', 'san antonio', 'san diego', 'dallas',
        'san francisco', 'austin', 'seattle', 'denver', 'boston', 'atlanta',
        'miami', 'detroit', 'minneapolis', 'pittsburgh', 'cleveland',
        'new orleans', 'charlotte', 'albuquerque', 'jersey city',
    }

    for gpe in gpes:
        gpe_lower = gpe.lower()

        # Skip US cities - they're scope, not country
        if gpe_lower in _US_CITIES:
            if not frame.get('scope'):
                # Map to scope code
                city_scope_map = {
                    'oakland': 'OAKLAND', 'new york city': 'NYC', 'nyc': 'NYC',
                    'los angeles': 'LA', 'chicago': 'CHICAGO', 'houston': 'HOUSTON',
                    'philadelphia': 'PHILA', 'phoenix': 'PHOENIX',
                    'san antonio': 'SAN_ANTONIO', 'san diego': 'SAN_DIEGO',
                    'dallas': 'DALLAS', 'san francisco': 'SF', 'austin': 'AUSTIN',
                    'seattle': 'SEATTLE', 'denver': 'DENVER', 'boston': 'BOSTON',
                    'atlanta': 'ATLANTA', 'miami': 'MIAMI', 'detroit': 'DETROIT',
                    'minneapolis': 'MINNEAPOLIS', 'pittsburgh': 'PITTSBURGH',
                    'cleveland': 'CLEVELAND', 'new orleans': 'NEW_ORLEANS',
                    'charlotte': 'CHARLOTTE', 'albuquerque': 'ALBUQUERQUE',
                    'jersey city': 'JERSEY_CITY',
                }
                frame['scope'] = city_scope_map.get(gpe_lower, gpe.upper())
                frame['scope_type'] = 'city'
            if not frame.get('country'):
                frame['country'] = 'US'
            continue

        # Check if it's a US state
        state_code = normalize_us_state(gpe)
        if state_code:
            # US state detected - set country=US and scope=state
            if not frame.get('country'):
                frame['country'] = 'US'
            if not frame.get('scope'):
                frame['scope'] = state_code
                frame['scope_type'] = 'state'
            continue  # Don't try to match as country

        # Try as country (with cleaning)
        if not frame.get('country'):
            country_code = normalize_country(_clean_country_string(gpe))
            if country_code:
                frame['country'] = country_code

    # Check for institutions in ORGs
    for org in orgs:
        actor = ACTOR_MAP.get(org.lower())
        if actor:
            frame['actor'] = actor
            break

    return frame


def _clean_country_string(text: str) -> str:
    """
    Clean a potential country string before normalization.

    Strips leading articles, numbers, and punctuation.
    "the 2024 us" → "us"
    "the French" → "French"
    """
    if not text:
        return text

    # Convert to lowercase for processing
    cleaned = text.strip()

    # Remove leading articles
    for article in ['the ', 'a ', 'an ']:
        if cleaned.lower().startswith(article):
            cleaned = cleaned[len(article):]

    # Remove leading numbers and years (e.g., "2024 us" → "us")
    cleaned = re.sub(r'^\d+\s+', '', cleaned)

    # Remove leading punctuation
    cleaned = re.sub(r'^[^\w]+', '', cleaned)

    # Remove trailing punctuation
    cleaned = re.sub(r'[^\w]+$', '', cleaned)

    return cleaned.strip()


def _identify_frame_type(frame: dict, doc) -> dict:
    """
    Identify frame type from verb analysis.

    Special handling for "be" as ROOT verb - looks at children to determine frame:
    - be + past participle: use participle to determine frame
    - be + comparative/preposition (above, below, less than): threshold
    - be + noun predicate (shutdown, recession, war): occurrence
    """
    # Find root token
    root_token = None
    for token in doc:
        if token.dep_ == 'ROOT':
            root_token = token
            break

    if root_token:
        root_lemma = root_token.lemma_.lower()

        # Special handling for "be" as ROOT
        if root_lemma == 'be':
            frame_type = _resolve_be_frame(root_token, doc)
            if frame_type:
                frame['frame_type'] = frame_type
                return frame

        # Check if ROOT verb is in FRAME_MAP
        if root_lemma in FRAME_MAP:
            frame['frame_type'] = FRAME_MAP[root_lemma]
            return frame

    # Check all verbs (not just ROOT)
    for token in doc:
        if token.pos_ == 'VERB':
            lemma = token.lemma_.lower()
            if lemma in FRAME_MAP:
                frame['frame_type'] = FRAME_MAP[lemma]
                return frame

    # Check for outcome keywords in question text as fallback
    text_lower = frame['raw_question'].lower()
    for keyword, outcome_type in OUTCOME_TYPE_MAP.items():
        if keyword in text_lower:
            frame['outcome_type'] = outcome_type
            # Infer frame type from outcome
            if outcome_type in ('WIN', 'NOMINATION', 'CANDIDACY', 'MAJORITY', 'CONTROL'):
                frame['frame_type'] = 'contest'
            elif outcome_type in ('APPOINT', 'CONFIRM'):
                frame['frame_type'] = 'appointment'
            elif outcome_type in ('RESIGN', 'IMPEACH', 'CONVICT'):
                frame['frame_type'] = 'occurrence'
            break

    # Default to binary_outcome if still unknown
    if not frame.get('frame_type'):
        frame['frame_type'] = 'binary_outcome'

    return frame


def _resolve_be_frame(be_token, doc) -> str | None:
    """
    Resolve frame type when ROOT verb is "be".

    Looks at children of "be" to determine the actual frame:
    1. Past participle child -> use PARTICIPLE_FRAME_MAP
    2. Comparative/preposition (above, below, less, more) -> threshold
    3. Noun predicate (shutdown, recession, war) -> occurrence
    """
    text_lower = doc.text.lower()

    # Collect children and their properties
    children = list(be_token.children)

    for child in children:
        # Case 1: Past participle (e.g., "will be confirmed", "is elected")
        if child.tag_ in ('VBN', 'VBD') or child.pos_ == 'VERB':
            participle = child.lemma_.lower()
            # Also check the text form for irregular participles
            participle_text = child.text.lower()

            if participle in PARTICIPLE_FRAME_MAP:
                return PARTICIPLE_FRAME_MAP[participle]
            if participle_text in PARTICIPLE_FRAME_MAP:
                return PARTICIPLE_FRAME_MAP[participle_text]

        # Case 2: Adjective/adverb comparatives or threshold prepositions
        if child.pos_ in ('ADJ', 'ADV', 'ADP'):
            child_text = child.text.lower()
            if child_text in THRESHOLD_INDICATORS:
                return 'threshold'

        # Case 3: Noun predicate (occurrence nouns)
        if child.pos_ == 'NOUN':
            noun = child.lemma_.lower()
            if noun in OCCURRENCE_NOUNS:
                return 'occurrence'

    # Also check entire text for threshold patterns
    # Handles "less than X", "more than X", "above X%", etc.
    for indicator in THRESHOLD_INDICATORS:
        if indicator in text_lower:
            return 'threshold'

    # Check for occurrence nouns anywhere in sentence
    for noun in OCCURRENCE_NOUNS:
        if noun in text_lower:
            return 'occurrence'

    return None


def _extract_contest_frame(frame: dict, doc, question: str) -> dict:
    """Extract election/contest-specific fields."""
    text_lower = question.lower()

    # Detect outcome type
    if 'win' in text_lower or 'elected' in text_lower:
        frame['outcome_type'] = 'WIN'
    elif 'nomin' in text_lower:
        frame['outcome_type'] = 'NOMINATION'
    elif 'run' in text_lower or 'announce' in text_lower or 'candidacy' in text_lower:
        frame['outcome_type'] = 'CANDIDACY'
    elif 'majority' in text_lower:
        frame['outcome_type'] = 'MAJORITY'
    elif 'control' in text_lower:
        frame['outcome_type'] = 'CONTROL'

    # Check for country + office patterns BEFORE generic office detection
    # e.g., "Jamaica House of Representatives" → JM + PARL
    # Run if office is not set (even if country was set by _extract_entities)
    if not frame.get('office'):
        frame = _detect_country_office_pair(frame, text_lower)

    # Detect office type (if not already set by country+office detection)
    if not frame.get('office'):
        for office_term, office_code in OFFICE_MAP.items():
            if office_term in text_lower:
                frame['office'] = office_code
                break

    # Detect party - try longer names first to avoid false matches
    # e.g., "Christian Democratic" should match before "Democratic"
    if not frame.get('party'):
        # Sort by length descending so longer matches are tried first
        sorted_parties = sorted(PARTY_MAP.items(), key=lambda x: len(x[0]), reverse=True)
        for party_term, party_code in sorted_parties:
            # Use word boundaries to match full party name
            if re.search(rf'\b{re.escape(party_term)}\b', text_lower):
                frame['party'] = party_code
                break

    # Check for primary election
    if 'primary' in text_lower or 'caucus' in text_lower:
        frame['is_primary'] = True

    # Check for House district pattern (US-specific)
    district_match = re.search(HOUSE_DISTRICT_PATTERN, question)
    if district_match:
        frame['scope'] = f"{district_match.group(1)}-{district_match.group(2)}"
        frame['scope_type'] = 'district'
        frame['office'] = 'HOUSE'
        # US House district pattern confirms US
        frame['country'] = 'US'

    # Infer country=US when evidence supports it:
    # 1. SCOTUS (definitionally US)
    # 2. US state in scope (e.g., scope='NC' → country='US')
    # 3. House district pattern (already handled above)
    if not frame.get('country'):
        if frame.get('office') == 'SCOTUS':
            frame['country'] = 'US'
        elif frame.get('scope'):
            # Check if scope is a US state code
            scope = frame.get('scope')
            # Handle district format like "NC-12" or plain state "NC"
            state_part = scope.split('-')[0] if '-' in scope else scope
            if normalize_us_state(state_part):
                frame['country'] = 'US'

    return frame


def _detect_country_office_pair(frame: dict, text_lower: str) -> dict:
    """
    Detect country + office pairs like "Jamaica House of Representatives".

    When a word before an office term matches a country via normalize_country(),
    use that country instead of defaulting to US.
    """
    # Patterns: "[Country] House/Senate/Parliament/etc."
    office_keywords = [
        'house of representatives', 'house of commons', 'parliament',
        'senate', 'congress', 'national assembly', 'bundestag',
        'presidential', 'president', 'prime minister',
    ]

    for keyword in office_keywords:
        if keyword not in text_lower:
            continue

        # Find the keyword position and look for country name before it
        idx = text_lower.find(keyword)
        if idx <= 0:
            continue

        # Get words before the keyword
        prefix = text_lower[:idx].strip()
        words_before = prefix.split()

        if not words_before:
            continue

        # Check last 1-3 words before keyword against country list
        for n in range(min(3, len(words_before)), 0, -1):
            potential_country = ' '.join(words_before[-n:])
            country_code = normalize_country(_clean_country_string(potential_country))
            if country_code:
                frame['country'] = country_code
                # Set appropriate office based on keyword
                if 'house' in keyword or 'parliament' in keyword or 'assembly' in keyword:
                    frame['office'] = 'PARL'
                elif 'senate' in keyword:
                    frame['office'] = 'SEN'
                elif 'president' in keyword:
                    frame['office'] = 'PRES'
                elif 'prime minister' in keyword:
                    frame['office'] = 'PM'
                return frame

    return frame


def _extract_threshold_frame(frame: dict, doc, question: str) -> dict:
    """Extract threshold/metric-specific fields."""
    text_lower = question.lower()

    # Detect metric
    for metric_term, metric_code in METRIC_MAP.items():
        if metric_term in text_lower:
            frame['metric'] = metric_code
            break

    # Extract numeric threshold
    # Look for patterns like "above 4%", "reach 50,000", "hit $100"
    threshold_patterns = [
        r'(?:above|over|exceed|surpass|reach|hit|below|under|fall to)\s*\$?([\d,]+\.?\d*)\s*%?',
        r'([\d,]+\.?\d*)\s*(?:percent|%|or more|or less|or higher|or lower)',
    ]

    for pattern in threshold_patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                value_str = match.group(1).replace(',', '')
                frame['threshold_value'] = float(value_str)
                break
            except ValueError:
                pass

    # Detect direction
    if any(word in text_lower for word in ['above', 'over', 'exceed', 'surpass', 'reach', 'hit', 'rise', 'climb']):
        frame['threshold_direction'] = 'above'
    elif any(word in text_lower for word in ['below', 'under', 'fall', 'drop', 'decline']):
        frame['threshold_direction'] = 'below'

    return frame


def _extract_appointment_frame(frame: dict, doc, question: str) -> dict:
    """Extract appointment-specific fields."""
    text_lower = question.lower()

    # Detect position being filled
    for office_term, office_code in OFFICE_MAP.items():
        if office_term in text_lower:
            frame['office'] = office_code
            break

    # Detect institution
    for actor_term, actor_code in ACTOR_MAP.items():
        if actor_term in text_lower:
            frame['actor'] = actor_code
            break

    return frame


def _extract_policy_frame(frame: dict, doc, question: str) -> dict:
    """Extract policy change-specific fields."""
    text_lower = question.lower()

    # Common policy metrics
    if 'rate' in text_lower or 'interest' in text_lower:
        frame['metric'] = 'RATE'
    elif 'tariff' in text_lower:
        frame['metric'] = 'TARIFF'
    elif 'tax' in text_lower:
        frame['metric'] = 'TAX'

    # Detect direction
    if any(word in text_lower for word in ['raise', 'hike', 'increase']):
        frame['threshold_direction'] = 'increase'
    elif any(word in text_lower for word in ['cut', 'lower', 'decrease', 'reduce']):
        frame['threshold_direction'] = 'decrease'

    # Detect actor (central bank, government, etc.)
    for actor_term, actor_code in ACTOR_MAP.items():
        if actor_term in text_lower:
            frame['actor'] = actor_code
            break

    return frame


def _extract_legislation_frame(frame: dict, doc, question: str) -> dict:
    """Extract legislation-specific fields."""
    text_lower = question.lower()

    # Detect legislative body
    if 'senate' in text_lower:
        frame['actor'] = 'SENATE'
    elif 'house' in text_lower:
        frame['actor'] = 'HOUSE'
    elif 'congress' in text_lower:
        frame['actor'] = 'CONGRESS'

    return frame


def _extract_ruling_frame(frame: dict, doc, question: str) -> dict:
    """Extract ruling/judicial decision-specific fields."""
    text_lower = question.lower()

    # Detect court
    if 'supreme court' in text_lower or 'scotus' in text_lower:
        frame['actor'] = 'SCOTUS'
    elif 'appeals court' in text_lower or 'circuit' in text_lower:
        frame['actor'] = 'APPEALS'
    elif 'district court' in text_lower:
        frame['actor'] = 'DISTRICT'

    # Detect ruling type
    if 'overturn' in text_lower or 'reverse' in text_lower:
        frame['outcome_type'] = 'OVERTURN'
    elif 'uphold' in text_lower or 'affirm' in text_lower:
        frame['outcome_type'] = 'UPHOLD'
    elif 'strike' in text_lower:
        frame['outcome_type'] = 'STRIKE_DOWN'

    # Default country to US for federal courts
    if not frame.get('country') and frame.get('actor') in ('SCOTUS', 'APPEALS', 'DISTRICT'):
        frame['country'] = 'US'

    return frame


def _extract_regulatory_frame(frame: dict, doc, question: str) -> dict:
    """Extract regulatory action-specific fields."""
    text_lower = question.lower()

    # Detect regulatory agency
    if 'fda' in text_lower:
        frame['actor'] = 'FDA'
    elif 'sec' in text_lower:
        frame['actor'] = 'SEC'
    elif 'fcc' in text_lower:
        frame['actor'] = 'FCC'
    elif 'epa' in text_lower:
        frame['actor'] = 'EPA'
    elif 'ftc' in text_lower:
        frame['actor'] = 'FTC'

    # Detect action type
    if 'ban' in text_lower:
        frame['outcome_type'] = 'BAN'
    elif 'approve' in text_lower:
        frame['outcome_type'] = 'APPROVE'
    elif 'sanction' in text_lower:
        frame['outcome_type'] = 'SANCTION'
    elif 'impose' in text_lower or 'tariff' in text_lower:
        frame['outcome_type'] = 'IMPOSE'

    return frame


def _extract_occurrence_frame(frame: dict, doc, question: str) -> dict:
    """Extract occurrence/event-specific fields."""
    text_lower = question.lower()

    # Detect event type
    if 'shutdown' in text_lower:
        frame['outcome_type'] = 'SHUTDOWN'
    elif 'recession' in text_lower:
        frame['outcome_type'] = 'RECESSION'
    elif 'war' in text_lower or 'invasion' in text_lower:
        frame['outcome_type'] = 'CONFLICT'
    elif 'resign' in text_lower:
        frame['outcome_type'] = 'RESIGN'
    elif 'impeach' in text_lower:
        frame['outcome_type'] = 'IMPEACH'
    elif 'default' in text_lower:
        frame['outcome_type'] = 'DEFAULT'

    return frame


def _extract_declaration_frame(frame: dict, doc, question: str) -> dict:
    """Extract declaration/announcement-specific fields."""
    text_lower = question.lower()

    # Detect what is being declared
    if 'candidacy' in text_lower or 'run for' in text_lower:
        frame['outcome_type'] = 'CANDIDACY'
    elif 'emergency' in text_lower:
        frame['outcome_type'] = 'EMERGENCY'
    elif 'victory' in text_lower:
        frame['outcome_type'] = 'VICTORY'

    return frame


def _extract_year(frame: dict, question: str, metadata: dict | None = None) -> dict:
    """
    Extract year from question text with fallback to market close time.

    Priority:
    1. Year pattern in question text (e.g., "2024", "2026")
    2. Fallback to close_time/expiration_time from market metadata
    """
    # Try to extract year from question text first
    for pattern in YEAR_PATTERNS:
        match = re.search(pattern, question)
        if match:
            try:
                year = int(match.group(1))
                # Sanity check: should be reasonable year
                if 1990 <= year <= 2100:
                    frame['year'] = year
                    return frame
            except ValueError:
                pass

    # Fallback: extract year from market close time metadata
    if metadata and not frame.get('year'):
        year = _extract_year_from_close_time(metadata)
        if year:
            frame['year'] = year

    return frame


def _extract_year_from_close_time(metadata: dict) -> int | None:
    """
    Extract year from market close/expiration time fields.

    Checks: trading_close_time, scheduled_end_time, k_expiration_time, k_close_time
    """
    # Fields that might contain close/expiration time
    time_fields = [
        'trading_close_time',
        'scheduled_end_time',
        'k_expiration_time',
        'k_close_time',
    ]

    for field in time_fields:
        value = metadata.get(field)
        if not value:
            continue

        try:
            # Handle various date formats
            value_str = str(value)

            # ISO format: "2024-11-05T..." or "2024-11-05 ..."
            if len(value_str) >= 4:
                year_match = re.match(r'^(\d{4})', value_str)
                if year_match:
                    year = int(year_match.group(1))
                    if 1990 <= year <= 2100:
                        return year
        except (ValueError, TypeError):
            continue

    return None


def _extract_date_reference(frame: dict, question: str) -> dict:
    """Extract date reference from question text."""
    for pattern in DATE_PATTERNS:
        match = re.search(pattern, question, re.IGNORECASE)
        if match:
            frame['date_reference'] = match.group(1)
            break

    return frame


def _extract_scope(frame: dict, question: str) -> dict:
    """
    Extract geographic scope from question text.

    When a US state is detected, also infers country=US.
    This is evidence-based inference - the state name IS the country signal.
    """
    text_lower = question.lower()

    # Check for House district first (implies US)
    district_match = re.search(HOUSE_DISTRICT_PATTERN, question)
    if district_match:
        frame['scope'] = f"{district_match.group(1)}-{district_match.group(2)}"
        frame['scope_type'] = 'district'
        if not frame.get('country'):
            frame['country'] = 'US'
        return frame

    # Check for US states using the `us` package
    # This handles full names, abbreviations, and variants
    # Skip common English words that happen to be state abbreviations
    # Only skip words that are BOTH common English AND state codes
    _SKIP_STATE_ABBREVS = {'in', 'or', 'me', 'oh', 'ok', 'hi', 'la', 'co', 'al', 'pa', 'ma', 'de', 'nd', 'md'}
    words = text_lower.split()
    for i in range(len(words)):
        # Try 2-word state names first (New York, North Carolina, etc.)
        if i < len(words) - 1:
            two_word = f"{words[i]} {words[i+1]}"
            state_code = normalize_us_state(two_word)
            if state_code:
                frame['scope'] = state_code
                frame['scope_type'] = 'state'
                if not frame.get('country'):
                    frame['country'] = 'US'
                return frame
        # Try single-word state names - but skip common words
        word = words[i]
        if word in _SKIP_STATE_ABBREVS:
            continue  # Don't match "in", "or", "me", etc. as states
        state_code = normalize_us_state(word)
        if state_code:
            frame['scope'] = state_code
            frame['scope_type'] = 'state'
            if not frame.get('country'):
                frame['country'] = 'US'
            return frame

    # Check non-US scope keywords (cities, Canadian provinces, etc.)
    for keyword, code in SCOPE_KEYWORDS:
        if keyword in text_lower:
            # Skip US states (already handled above)
            if normalize_us_state(keyword):
                continue
            frame['scope'] = code
            # Determine scope type
            if code in ('NYC', 'LA', 'CHICAGO', 'SF', 'BOSTON', 'ATLANTA', 'MIAMI',
                       'DETROIT', 'OAKLAND', 'SEATTLE', 'DENVER', 'HOUSTON', 'DALLAS',
                       'PHILA', 'PHOENIX', 'SAN_ANTONIO', 'SAN_DIEGO', 'AUSTIN'):
                frame['scope_type'] = 'city'
                # Major US cities also imply country=US
                if not frame.get('country'):
                    frame['country'] = 'US'
            else:
                frame['scope_type'] = 'region'
            return frame

    return frame


def _calculate_confidence(frame: dict) -> float:
    """Calculate extraction confidence score (0-1)."""
    score = 0.0
    max_score = 0.0

    # Frame type identified
    max_score += 1.0
    if frame.get('frame_type') and frame['frame_type'] != 'binary_outcome':
        score += 1.0
    elif frame.get('frame_type') == 'binary_outcome':
        score += 0.3  # Partial credit for fallback

    # For contest frames, check key fields
    if frame.get('frame_type') == 'contest':
        max_score += 4.0
        if frame.get('country'):
            score += 1.0
        if frame.get('office'):
            score += 1.0
        if frame.get('year'):
            score += 1.0
        if frame.get('candidate') or frame.get('party'):
            score += 1.0

    # For threshold frames
    elif frame.get('frame_type') == 'threshold':
        max_score += 3.0
        if frame.get('metric'):
            score += 1.0
        if frame.get('threshold_value') is not None:
            score += 1.0
        if frame.get('threshold_direction'):
            score += 1.0

    # For appointment frames
    elif frame.get('frame_type') == 'appointment':
        max_score += 3.0
        if frame.get('candidate'):
            score += 1.0
        if frame.get('office') or frame.get('actor'):
            score += 1.0
        if frame.get('year'):
            score += 1.0

    # Bonus for date reference
    max_score += 0.5
    if frame.get('date_reference') or frame.get('year'):
        score += 0.5

    if max_score == 0:
        return 0.0

    return round(score / max_score, 3)


def extract_candidate_name(name: str) -> tuple[str, str] | None:
    """
    Parse a full name into (first_name, last_name).

    Returns None if parsing fails.
    """
    if not name:
        return None

    # Clean and normalize
    name = unidecode(name.strip())

    # Remove common titles
    titles = ['Mr.', 'Mrs.', 'Ms.', 'Dr.', 'Sen.', 'Rep.', 'Gov.', 'President', 'Vice President']
    for title in titles:
        name = re.sub(rf'^{re.escape(title)}\s+', '', name, flags=re.IGNORECASE)

    parts = name.split()
    if len(parts) == 0:
        return None
    elif len(parts) == 1:
        return ('', parts[0])
    else:
        # Last word is surname, rest is first name
        return (' '.join(parts[:-1]), parts[-1])


# Kalshi ticker country hints: prefix -> ISO country code
_TICKER_COUNTRY_HINTS = {
    'IRELAND': 'IE',
    'FINLAND': 'FI',
    'THEGAMBIA': 'GM',
    'GAMBIA': 'GM',
    'COLOMBIA': 'CO',
    'BRAZIL': 'BR',
    'MEXICO': 'MX',
    'CANADA': 'CA',
    'UK': 'GB',
    'GERMANY': 'DE',
    'FRANCE': 'FR',
    'ITALY': 'IT',
    'SPAIN': 'ES',
    'NETHERLANDS': 'NL',
    'POLAND': 'PL',
    'SWEDEN': 'SE',
    'NORWAY': 'NO',
    'DENMARK': 'DK',
    'AUSTRALIA': 'AU',
    'JAPAN': 'JP',
    'INDIA': 'IN',
    'ISRAEL': 'IL',
    'TURKEY': 'TR',
    'SOUTHAFRICA': 'ZA',
    'ARGENTINA': 'AR',
    'CHILE': 'CL',
    'PERU': 'PE',
    'ECUADOR': 'EC',
    'VENEZUELA': 'VE',
    'PHILIPPINES': 'PH',
    'INDONESIA': 'ID',
    'THAILAND': 'TH',
    'MALAYSIA': 'MY',
    'SINGAPORE': 'SG',
    'TAIWAN': 'TW',
    'SOUTHKOREA': 'KR',
    'KOREA': 'KR',
}

# Kalshi ticker office hints: keyword -> office code
_TICKER_OFFICE_HINTS = {
    'PRES': 'PRES',
    'PRESIDENT': 'PRES',
    'PARLI': 'PARL',
    'PARLIAMENT': 'PARL',
    'GOV': 'GOV',
    'GOVERNOR': 'GOV',
    'SENATE': 'SEN',
    'SEN': 'SEN',
    'HOUSE': 'HOUSE',
    'ATTYGEN': 'AG',
    'AG': 'AG',
    'MAYOR': 'MAYOR',
    'PM': 'PM',
}

# US state codes for ticker parsing
_US_STATE_CODES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC',
}


def _parse_kalshi_ticker(ticker: str) -> dict:
    """
    Parse a Kalshi ticker to extract structured hints.

    Kalshi tickers encode useful data:
    - KXCOLOMBIAPRES-26-ICAS → country=CO, office=PRES, year=2026, candidate_id=ICAS
    - SENATEMD-28-R → office=SEN, scope=MD, year=2028, party=GOP
    - HOUSEPA10-24-R → office=HOUSE, scope=PA-10, year=2024, party=GOP
    - KXFINLANDPARLI-27-LEFT → country=FI, office=PARL, year=2027, party=LEFT

    Returns dict with extracted hints (may be partial).
    """
    if not ticker:
        return {}

    hints = {}
    ticker_upper = ticker.upper()

    # Remove common prefixes
    clean = ticker_upper
    if clean.startswith('KX'):
        clean = clean[2:]

    # Split by dashes
    parts = clean.split('-')
    if not parts:
        return {}

    prefix = parts[0]

    # Extract year from parts (look for 4-digit or 2-digit year)
    for part in parts[1:]:
        # 4-digit year like "2024"
        if re.match(r'^(19|20|21)\d{2}$', part):
            hints['year'] = int(part)
            break
        # Pure 2-digit year like "26" or "28"
        if re.match(r'^(\d{2})$', part):
            year_2digit = int(part)
            # Assume 20xx for years 20-99, 2100+ for 00-19
            if year_2digit >= 20:
                hints['year'] = 2000 + year_2digit
            else:
                hints['year'] = 2100 + year_2digit
            break
        # Year with month like "26AUG" or "25NOV05"
        year_match = re.match(r'^(\d{2})[A-Z]{3}', part)
        if year_match:
            year_2digit = int(year_match.group(1))
            if year_2digit >= 20:
                hints['year'] = 2000 + year_2digit
            else:
                hints['year'] = 2100 + year_2digit
            break

    # Extract party from last part
    last_part = parts[-1] if len(parts) > 1 else ''
    if last_part == 'R':
        hints['party'] = 'GOP'
    elif last_part == 'D':
        hints['party'] = 'DEM'
    elif last_part == 'LEFT':
        hints['party'] = 'LEFT'
    elif last_part == 'RIGHT':
        hints['party'] = 'RIGHT'

    # Extract country from prefix
    for country_key, country_code in _TICKER_COUNTRY_HINTS.items():
        if country_key in prefix:
            hints['country'] = country_code
            break

    # Extract office from prefix
    for office_key, office_code in _TICKER_OFFICE_HINTS.items():
        if office_key in prefix:
            hints['office'] = office_code
            break

    # Extract US state from prefix (e.g., SENATEMD, HOUSEPA10, GOVNH)
    # Look for state code pattern after office keywords
    for state in _US_STATE_CODES:
        # Match state code after office keyword, with optional district number
        # Patterns: SENATEMD, HOUSEPA10, GOVNH, ATTYGENMA
        state_pattern = rf'(?:SENATE|HOUSE|GOV|ATTYGEN)({state})(\d*)'
        state_match = re.search(state_pattern, prefix)
        if state_match:
            hints['scope'] = state_match.group(1)
            hints['country'] = 'US'
            # Check for district number
            if state_match.group(2):
                hints['scope'] = f"{state_match.group(1)}-{state_match.group(2)}"
            break

    return hints


def _apply_kalshi_ticker_hints(frame: dict, ticker: str) -> dict:
    """
    Apply hints from Kalshi ticker to fill null fields in frame.

    Only fills fields that are currently null - never overrides spaCy extraction.
    Priority: question text (spaCy) > ticker > close_time
    """
    hints = _parse_kalshi_ticker(ticker)

    # Only fill nulls - never override
    if hints.get('year') and not frame.get('year'):
        frame['year'] = hints['year']

    if hints.get('country') and not frame.get('country'):
        frame['country'] = hints['country']

    if hints.get('office') and not frame.get('office'):
        frame['office'] = hints['office']

    if hints.get('scope') and not frame.get('scope'):
        frame['scope'] = hints['scope']

    if hints.get('party') and not frame.get('party'):
        frame['party'] = hints['party']

    return frame
