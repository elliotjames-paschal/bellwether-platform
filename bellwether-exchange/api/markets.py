"""Market endpoints."""

import os
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db
from lmsr import b_from_subsidy, lmsr_price_yes, lmsr_price_no
from models import Market, Position, Agent, PnLSnapshot, Resolution
from schemas import MarketCreate, MarketResolve, MarketResponse

router = APIRouter()

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "bellwether-admin-secret")


def _market_to_response(m: Market) -> MarketResponse:
    return MarketResponse(
        ticker=m.ticker,
        title=m.title,
        description=m.description,
        category=m.category,
        subsidy=m.subsidy,
        yes_price=lmsr_price_yes(m.q_yes, m.q_no, m.b),
        no_price=lmsr_price_no(m.q_yes, m.q_no, m.b),
        q_yes=m.q_yes,
        q_no=m.q_no,
        status=m.status,
        outcome=m.outcome,
        resolution_source=m.resolution_source,
        resolution_criteria=m.resolution_criteria,
        expiration=m.expiration,
        resolved_at=m.resolved_at,
        created_at=m.created_at,
    )


@router.get("", response_model=list[MarketResponse])
async def list_markets(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Market).where(Market.status == "active"))
    return [_market_to_response(m) for m in result.scalars().all()]


@router.get("/{ticker}", response_model=MarketResponse)
async def get_market(ticker: str, db: AsyncSession = Depends(get_db)):
    market = await db.get(Market, ticker)
    if not market:
        raise HTTPException(404, "Market not found")
    return _market_to_response(market)


@router.post("", response_model=MarketResponse, status_code=201)
async def create_market(
    body: MarketCreate,
    db: AsyncSession = Depends(get_db),
    x_admin_key: str = Header(alias="X-Admin-Key"),
):
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(403, "Invalid admin key")

    existing = await db.get(Market, body.ticker)
    if existing:
        raise HTTPException(409, "Market already exists")

    b = b_from_subsidy(body.subsidy)
    market = Market(
        ticker=body.ticker,
        title=body.title,
        description=body.description,
        category=body.category,
        b=b,
        subsidy=body.subsidy,
        q_yes=0.0,
        q_no=0.0,
        status="active",
        resolution_source=body.resolution_source,
        resolution_criteria=body.resolution_criteria,
        expiration=body.expiration,
    )
    db.add(market)
    await db.commit()
    await db.refresh(market)
    return _market_to_response(market)


@router.post("/{ticker}/resolve", status_code=200)
async def resolve_market(
    ticker: str,
    body: MarketResolve,
    db: AsyncSession = Depends(get_db),
    x_admin_key: str = Header(alias="X-Admin-Key"),
):
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(403, "Invalid admin key")

    if body.outcome not in ("yes", "no"):
        raise HTTPException(400, "Outcome must be 'yes' or 'no'")

    market = await db.get(Market, ticker)
    if not market:
        raise HTTPException(404, "Market not found")
    if market.status != "active":
        raise HTTPException(400, f"Market is already {market.status}")

    now = datetime.now(timezone.utc)
    market.status = "resolved"
    market.outcome = body.outcome
    market.resolved_at = now

    payout_yes = 1.0 if body.outcome == "yes" else 0.0
    payout_no = 1.0 if body.outcome == "no" else 0.0

    # Settle all positions
    result = await db.execute(
        select(Position).where(Position.market_ticker == ticker)
    )
    positions = result.scalars().all()
    settled_agents = []

    for pos in positions:
        payout = pos.yes_shares * payout_yes + pos.no_shares * payout_no
        if payout > 0:
            agent = await db.get(Agent, pos.agent_id)
            if agent:
                agent.cash_balance += payout
                settled_agents.append(agent)

    # PnL snapshots for affected agents
    for agent in settled_agents:
        equity = await _compute_equity(agent.id, db)
        snapshot = PnLSnapshot(
            id=_uuid(),
            agent_id=agent.id,
            cash=agent.cash_balance,
            equity=equity,
            total=agent.cash_balance + equity,
            timestamp=now,
        )
        db.add(snapshot)

    resolution = Resolution(
        market_ticker=ticker,
        outcome=body.outcome,
        payout_per_yes_share=payout_yes,
        payout_per_no_share=payout_no,
        resolved_at=now,
    )
    db.add(resolution)
    await db.commit()

    return {
        "ticker": ticker,
        "outcome": body.outcome,
        "positions_settled": len(positions),
        "resolved_at": now.isoformat(),
    }


async def _compute_equity(agent_id: str, db: AsyncSession) -> float:
    """Mark-to-market equity across all active positions."""
    result = await db.execute(
        select(Position).where(Position.agent_id == agent_id)
    )
    positions = result.scalars().all()
    equity = 0.0
    for pos in positions:
        market = await db.get(Market, pos.market_ticker)
        if market and market.status == "active":
            yes_price = lmsr_price_yes(market.q_yes, market.q_no, market.b)
            no_price = lmsr_price_no(market.q_yes, market.q_no, market.b)
            equity += pos.yes_shares * yes_price + pos.no_shares * no_price
    return equity


def _uuid() -> str:
    import uuid
    return str(uuid.uuid4())
