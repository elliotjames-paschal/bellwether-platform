#!/usr/bin/env python3
"""
Generate civic elections data from Google Civic Information API.

This script fetches upcoming US elections and contest data, then saves
it as static JSON for the website timeline and modal enrichment.

Usage:
    python scripts/generate_civic_elections.py

Requires:
    GOOGLE_CIVIC_API_KEY environment variable or .env file
"""

import os
import json
import re
from datetime import datetime, date
from pathlib import Path
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Google Civic API configuration
API_KEY = os.getenv('GOOGLE_CIVIC_API_KEY')
BASE_URL = 'https://www.googleapis.com/civicinfo/v2'

# State capital addresses for voterInfo queries
STATE_CAPITALS = {
    'AL': '600 Dexter Ave, Montgomery, AL 36130',
    'AK': '120 4th St, Juneau, AK 99801',
    'AZ': '1700 W Washington St, Phoenix, AZ 85007',
    'AR': '500 Woodlane St, Little Rock, AR 72201',
    'CA': '1315 10th St, Sacramento, CA 95814',
    'CO': '200 E Colfax Ave, Denver, CO 80203',
    'CT': '210 Capitol Ave, Hartford, CT 06106',
    'DE': '411 Legislative Ave, Dover, DE 19901',
    'FL': '400 S Monroe St, Tallahassee, FL 32399',
    'GA': '206 Washington St SW, Atlanta, GA 30334',
    'HI': '415 S Beretania St, Honolulu, HI 96813',
    'ID': '700 W Jefferson St, Boise, ID 83702',
    'IL': '401 S 2nd St, Springfield, IL 62701',
    'IN': '200 W Washington St, Indianapolis, IN 46204',
    'IA': '1007 E Grand Ave, Des Moines, IA 50319',
    'KS': '300 SW 10th Ave, Topeka, KS 66612',
    'KY': '700 Capitol Ave, Frankfort, KY 40601',
    'LA': '900 N 3rd St, Baton Rouge, LA 70802',
    'ME': '210 State St, Augusta, ME 04330',
    'MD': '100 State Cir, Annapolis, MD 21401',
    'MA': '24 Beacon St, Boston, MA 02133',
    'MI': '100 N Capitol Ave, Lansing, MI 48933',
    'MN': '75 Rev Dr Martin Luther King Jr Blvd, St Paul, MN 55155',
    'MS': '400 High St, Jackson, MS 39201',
    'MO': '201 W Capitol Ave, Jefferson City, MO 65101',
    'MT': '1301 E 6th Ave, Helena, MT 59601',
    'NE': '1445 K St, Lincoln, NE 68509',
    'NV': '101 N Carson St, Carson City, NV 89701',
    'NH': '107 N Main St, Concord, NH 03301',
    'NJ': '125 W State St, Trenton, NJ 08608',
    'NM': '490 Old Santa Fe Trail, Santa Fe, NM 87501',
    'NY': '138 State St, Albany, NY 12207',
    'NC': '1 E Edenton St, Raleigh, NC 27601',
    'ND': '600 E Boulevard Ave, Bismarck, ND 58505',
    'OH': '77 S High St, Columbus, OH 43215',
    'OK': '2300 N Lincoln Blvd, Oklahoma City, OK 73105',
    'OR': '900 Court St NE, Salem, OR 97301',
    'PA': '501 N 3rd St, Harrisburg, PA 17120',
    'RI': '82 Smith St, Providence, RI 02903',
    'SC': '1100 Gervais St, Columbia, SC 29201',
    'SD': '500 E Capitol Ave, Pierre, SD 57501',
    'TN': '600 Dr M L K Jr Blvd, Nashville, TN 37243',
    'TX': '1100 Congress Ave, Austin, TX 78701',
    'UT': '350 N State St, Salt Lake City, UT 84103',
    'VT': '115 State St, Montpelier, VT 05633',
    'VA': '1000 Bank St, Richmond, VA 23219',
    'WA': '416 Sid Snyder Ave SW, Olympia, WA 98501',
    'WV': '1900 Kanawha Blvd E, Charleston, WV 25305',
    'WI': '2 E Main St, Madison, WI 53703',
    'WY': '200 W 24th St, Cheyenne, WY 82001',
    'DC': '1350 Pennsylvania Ave NW, Washington, DC 20004',
}

# State name to abbreviation mapping
STATE_ABBREVS = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
    'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
    'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
    'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
    'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
}


def get_elections():
    """Fetch list of upcoming elections from Google Civic API."""
    if not API_KEY or API_KEY == 'your_api_key_here':
        print("Warning: GOOGLE_CIVIC_API_KEY not set, using test data")
        return get_test_elections()

    url = f"{BASE_URL}/elections"
    params = {'key': API_KEY}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        elections = data.get('elections', [])
        print(f"Fetched {len(elections)} elections from API")
        return elections
    except requests.RequestException as e:
        print(f"Error fetching elections: {e}")
        return get_test_elections()


def get_test_elections():
    """Return test election data for development."""
    return [
        {
            'id': '2000',
            'name': 'VIP Test Election',
            'electionDay': '2026-06-10',
            'ocdDivisionId': 'ocd-division/country:us/state:ca'
        },
        {
            'id': '9001',
            'name': 'California Special Election',
            'electionDay': '2026-03-15',
            'ocdDivisionId': 'ocd-division/country:us/state:ca'
        },
        {
            'id': '9002',
            'name': 'Texas Primary Election',
            'electionDay': '2026-03-01',
            'ocdDivisionId': 'ocd-division/country:us/state:tx'
        },
        {
            'id': '9003',
            'name': 'US General Election 2026',
            'electionDay': '2026-11-03',
            'ocdDivisionId': 'ocd-division/country:us'
        },
        {
            'id': '9004',
            'name': 'Georgia Runoff Election',
            'electionDay': '2026-12-06',
            'ocdDivisionId': 'ocd-division/country:us/state:ga'
        }
    ]


def parse_state_from_ocd(ocd_id):
    """Extract state abbreviation from OCD Division ID."""
    if not ocd_id:
        return None

    # Pattern: ocd-division/country:us/state:XX
    match = re.search(r'/state:(\w{2})', ocd_id)
    if match:
        return match.group(1).upper()

    return None


def parse_state_from_name(election_name):
    """Extract state abbreviation from election name."""
    if not election_name:
        return None

    name_lower = election_name.lower()

    # Check for state names
    for state_name, abbrev in STATE_ABBREVS.items():
        if state_name in name_lower:
            return abbrev

    # Check for state abbreviations (e.g., "CA Primary")
    words = name_lower.split()
    for word in words:
        upper_word = word.upper()
        if upper_word in STATE_CAPITALS:
            return upper_word

    return None


def get_voter_info(election_id, state):
    """Fetch voter info for a specific election and state."""
    if not API_KEY or API_KEY == 'your_api_key_here':
        return get_test_voter_info(election_id, state)

    address = STATE_CAPITALS.get(state)
    if not address:
        print(f"  No capital address for state: {state}")
        return None

    url = f"{BASE_URL}/voterinfo"
    params = {
        'key': API_KEY,
        'address': address,
        'electionId': election_id
    }

    try:
        response = requests.get(url, params=params, timeout=30)

        if response.status_code == 400:
            # No voter info available for this election/state
            return None

        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        print(f"  Error fetching voter info for {state}: {e}")
        return None


def get_test_voter_info(election_id, state):
    """Return test voter info data for development."""
    test_contests = {
        ('2000', 'CA'): {
            'election': {'id': '2000', 'name': 'VIP Test Election', 'electionDay': '2026-06-10'},
            'contests': [
                {
                    'type': 'General',
                    'office': 'US House - District 35',
                    'district': {'name': 'California\'s 35th congressional district'},
                    'candidates': [
                        {'name': 'Jane Smith', 'party': 'Republican Party', 'photoUrl': ''},
                        {'name': 'John Doe', 'party': 'Democratic Party', 'photoUrl': ''}
                    ]
                }
            ]
        },
        ('9001', 'CA'): {
            'election': {'id': '9001', 'name': 'California Special Election', 'electionDay': '2026-03-15'},
            'contests': [
                {
                    'type': 'General',
                    'office': 'US Senate',
                    'district': {'name': 'California'},
                    'candidates': [
                        {'name': 'Adam Schiff', 'party': 'Democratic Party', 'photoUrl': ''},
                        {'name': 'Steve Garvey', 'party': 'Republican Party', 'photoUrl': ''}
                    ]
                }
            ]
        },
        ('9002', 'TX'): {
            'election': {'id': '9002', 'name': 'Texas Primary Election', 'electionDay': '2026-03-01'},
            'contests': [
                {
                    'type': 'Primary',
                    'office': 'Governor',
                    'district': {'name': 'Texas'},
                    'candidates': [
                        {'name': 'Greg Abbott', 'party': 'Republican Party', 'photoUrl': ''},
                        {'name': 'Beto O\'Rourke', 'party': 'Democratic Party', 'photoUrl': ''}
                    ]
                }
            ]
        },
        ('9004', 'GA'): {
            'election': {'id': '9004', 'name': 'Georgia Runoff Election', 'electionDay': '2026-12-06'},
            'contests': [
                {
                    'type': 'Runoff',
                    'office': 'US Senate',
                    'district': {'name': 'Georgia'},
                    'candidates': [
                        {'name': 'Raphael Warnock', 'party': 'Democratic Party', 'photoUrl': ''},
                        {'name': 'Herschel Walker', 'party': 'Republican Party', 'photoUrl': ''}
                    ]
                }
            ]
        }
    }

    key = (election_id, state)
    return test_contests.get(key, None)


def parse_contests(voter_info):
    """Parse contests from voter info response."""
    if not voter_info:
        return []

    contests = []
    raw_contests = voter_info.get('contests', [])

    for contest in raw_contests:
        # Only include electoral contests (not referendums)
        if contest.get('type') in ['General', 'Primary', 'Runoff']:
            parsed = {
                'office': contest.get('office', 'Unknown Office'),
                'type': contest.get('type', 'General'),
                'district': contest.get('district', {}).get('name', ''),
                'candidates': []
            }

            for candidate in contest.get('candidates', []):
                parsed['candidates'].append({
                    'name': candidate.get('name', 'Unknown'),
                    'party': normalize_party(candidate.get('party', '')),
                    'photoUrl': candidate.get('photoUrl', '')
                })

            if parsed['candidates']:
                contests.append(parsed)

    return contests


def normalize_party(party_str):
    """Normalize party names to standard format."""
    if not party_str:
        return 'Unknown'

    party_lower = party_str.lower()

    if 'republican' in party_lower:
        return 'Republican'
    if 'democrat' in party_lower:
        return 'Democratic'
    if 'libertarian' in party_lower:
        return 'Libertarian'
    if 'green' in party_lower:
        return 'Green'
    if 'independent' in party_lower:
        return 'Independent'
    if 'nonpartisan' in party_lower:
        return 'Nonpartisan'

    return party_str


def load_active_markets():
    """Load active markets to match with elections."""
    markets_path = Path(__file__).parent.parent / 'website' / 'data' / 'active_markets.json'

    try:
        with open(markets_path, 'r') as f:
            data = json.load(f)
            return data.get('markets', [])
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not load active markets: {e}")
        return []


def match_markets_to_election(election, state, markets):
    """Find markets that match a given election."""
    matched = []

    election_year = None
    if election.get('electionDay'):
        try:
            election_year = datetime.strptime(election['electionDay'], '%Y-%m-%d').year
        except ValueError:
            pass

    for market in markets:
        # Only consider US Electoral markets
        category = market.get('category_display', '')
        if category != 'US Electoral':
            continue

        label = (market.get('label') or '').lower()
        question = (market.get('pm_question') or market.get('k_question') or '').lower()

        # Check if market matches this state
        state_match = False
        if state:
            state_lower = state.lower()
            state_name = [k for k, v in STATE_ABBREVS.items() if v == state]
            state_name = state_name[0] if state_name else ''

            if state_lower in label or state_lower in question:
                state_match = True
            if state_name and (state_name in label or state_name in question):
                state_match = True

        # Check if market matches election year
        year_match = False
        if election_year:
            if str(election_year) in label or str(election_year) in question:
                year_match = True

        # For national elections, don't require state match
        election_name = (election.get('name') or '').lower()
        is_national = 'general election' in election_name or 'presidential' in election_name

        if is_national:
            if year_match:
                matched.append(market['key'])
        elif state_match and year_match:
            matched.append(market['key'])

    return matched


def days_until(date_str):
    """Calculate days until a given date."""
    try:
        election_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        today = date.today()
        delta = (election_date - today).days
        return delta
    except ValueError:
        return None


def generate_civic_data():
    """Main function to generate civic elections data."""
    print("Generating civic elections data...")

    # Fetch elections
    elections = get_elections()

    # Load markets for matching
    markets = load_active_markets()
    print(f"Loaded {len(markets)} active markets for matching")

    processed_elections = []

    for election in elections:
        election_id = election.get('id')
        election_name = election.get('name', 'Unknown Election')
        election_day = election.get('electionDay', '')
        ocd_id = election.get('ocdDivisionId', '')

        print(f"\nProcessing: {election_name}")

        # Skip test elections
        if election_id == '2000' or 'test' in election_name.lower() or 'vip' in election_name.lower():
            print(f"  Skipping (test election)")
            continue

        # Skip past elections
        days = days_until(election_day)
        if days is not None and days < 0:
            print(f"  Skipping (past election)")
            continue

        # Parse state from OCD ID or name
        state = parse_state_from_ocd(ocd_id) or parse_state_from_name(election_name)
        print(f"  State: {state or 'National/Unknown'}")

        # Fetch voter info for contests
        contests = []
        if state:
            voter_info = get_voter_info(election_id, state)
            contests = parse_contests(voter_info)
            print(f"  Contests: {len(contests)}")

        # Match to markets
        matched_markets = match_markets_to_election(election, state, markets)
        print(f"  Matched markets: {len(matched_markets)}")

        processed_elections.append({
            'id': election_id,
            'name': election_name,
            'electionDay': election_day,
            'daysUntil': days,
            'state': state,
            'contests': contests,
            'matchedMarketCount': len(matched_markets),
            'matchedMarkets': matched_markets[:10]  # Limit to 10 for display
        })

    # Sort by date
    processed_elections.sort(key=lambda e: e.get('electionDay') or '9999-99-99')

    # Build output
    output = {
        'generated_at': datetime.now().isoformat(),
        'elections': processed_elections
    }

    # Write to file
    output_path = Path(__file__).parent.parent / 'website' / 'data' / 'civic_elections.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    # Also write to docs/data/ for GitHub Pages deployment
    docs_path = Path(__file__).parent.parent.parent / 'docs' / 'data' / 'civic_elections.json'
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    with open(docs_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Generated {output_path}")
    print(f"  Also copied to {docs_path}")
    print(f"  {len(processed_elections)} elections with data")

    return output


if __name__ == '__main__':
    generate_civic_data()
