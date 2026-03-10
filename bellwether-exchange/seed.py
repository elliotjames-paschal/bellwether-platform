"""Seed database with initial Blueprint markets."""

from datetime import datetime, timezone

from sqlalchemy import select

from db import async_session
from lmsr import b_from_subsidy
from models import Market

INITIAL_MARKETS = [
    {
        "ticker": "BWR-DEM-CONTROL-SENATE-CERTIFIED-ANY-2026",
        "title": "Will Democrats control the Senate after the 2026 midterms?",
        "category": "ELECTORAL",
        "subsidy": 1000.0,
        "resolution_source": "Associated Press race calls",
        "resolution_criteria": "Resolves YES if the Democratic Party holds a majority of seats in the U.S. Senate following the certification of the 2026 midterm election results.",
        "expiration": "2027-01-15T00:00:00Z",
    },
    {
        "ticker": "BWR-US-SHUTDOWN-FEDERAL-ANY-OCCURS-2025_Q1",
        "title": "Will the U.S. federal government shut down in Q1 2025?",
        "category": "GOVERNMENT_OPERATIONS",
        "subsidy": 1000.0,
        "resolution_source": "Office of Management and Budget",
        "resolution_criteria": "Resolves YES if any federal government shutdown lasting 24+ hours occurs between January 1 and March 31, 2025.",
        "expiration": "2025-04-01T00:00:00Z",
    },
    {
        "ticker": "BWR-US-DEBTCEILING-RESOLUTION-SIGNED_INTO_LAW-ANY-2025",
        "title": "Will the U.S. debt ceiling be resolved before default in 2025?",
        "category": "LEGISLATIVE",
        "subsidy": 1000.0,
        "resolution_source": "U.S. Treasury Department",
        "resolution_criteria": "Resolves YES if legislation suspending or raising the debt ceiling is signed into law before the U.S. Treasury exhausts extraordinary measures.",
        "expiration": "2025-12-31T00:00:00Z",
    },
    {
        "ticker": "BWR-US-AI-LEGISLATION-COMPREHENSIVE-SIGNED_INTO_LAW-ANY-2025",
        "title": "Will comprehensive federal AI legislation be signed into law in 2025?",
        "category": "REGULATORY",
        "subsidy": 1000.0,
        "resolution_source": "Congress.gov",
        "resolution_criteria": "Resolves YES if a bill establishing a comprehensive federal regulatory framework for AI systems is signed into law by December 31, 2025.",
        "expiration": "2025-12-31T00:00:00Z",
    },
    {
        "ticker": "BWR-US-VENEZUELA-MILITARY-ACTION-ANY-2025",
        "title": "Will the U.S. conduct direct military action against Venezuela in 2025?",
        "category": "MILITARY_SECURITY",
        "subsidy": 1000.0,
        "resolution_source": "U.S. Department of Defense official statements",
        "resolution_criteria": "Resolves YES if U.S. armed forces conduct any kinetic military operation on Venezuelan soil or territorial waters, confirmed by DoD.",
        "expiration": "2025-12-31T00:00:00Z",
    },
]


async def seed_markets():
    """Seed markets if the table is empty. Idempotent."""
    async with async_session() as session:
        result = await session.execute(select(Market).limit(1))
        if result.scalars().first() is not None:
            return

        for m in INITIAL_MARKETS:
            b = b_from_subsidy(m["subsidy"])
            market = Market(
                ticker=m["ticker"],
                title=m["title"],
                description=m.get("description", ""),
                category=m["category"],
                b=b,
                subsidy=m["subsidy"],
                q_yes=0.0,
                q_no=0.0,
                status="active",
                resolution_source=m["resolution_source"],
                resolution_criteria=m["resolution_criteria"],
                expiration=datetime.fromisoformat(m["expiration"].replace("Z", "+00:00")),
            )
            session.add(market)

        await session.commit()
        print(f"Seeded {len(INITIAL_MARKETS)} markets")
