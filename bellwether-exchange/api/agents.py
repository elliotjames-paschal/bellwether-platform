"""Agent endpoints."""

import secrets
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db
from lmsr import lmsr_price_yes, lmsr_price_no
from models import Agent, Market, Order, Position, PnLSnapshot
from schemas import AgentCreate, AgentResponse, PositionResponse, PnLResponse

router = APIRouter()


@router.post("", response_model=AgentResponse, status_code=201)
async def register_agent(body: AgentCreate, db: AsyncSession = Depends(get_db)):
    agent = Agent(
        id=str(uuid.uuid4()),
        name=body.name,
        api_key=secrets.token_urlsafe(32),
        framework=body.framework,
        cash_balance=10000.0,
    )
    db.add(agent)
    await db.commit()
    await db.refresh(agent)
    return AgentResponse.model_validate(agent)


@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(agent_id: str, db: AsyncSession = Depends(get_db)):
    agent = await db.get(Agent, agent_id)
    if not agent:
        raise HTTPException(404, "Agent not found")
    return AgentResponse.model_validate(agent)


@router.get("/{agent_id}/positions", response_model=list[PositionResponse])
async def get_positions(agent_id: str, db: AsyncSession = Depends(get_db)):
    agent = await db.get(Agent, agent_id)
    if not agent:
        raise HTTPException(404, "Agent not found")

    result = await db.execute(
        select(Position).where(Position.agent_id == agent_id)
    )
    positions = result.scalars().all()

    response = []
    for pos in positions:
        market = await db.get(Market, pos.market_ticker)
        if not market:
            continue

        yes_price = lmsr_price_yes(market.q_yes, market.q_no, market.b)
        no_price = lmsr_price_no(market.q_yes, market.q_no, market.b)
        mtm = pos.yes_shares * yes_price + pos.no_shares * no_price

        # Compute cost basis from orders
        order_result = await db.execute(
            select(Order).where(
                Order.agent_id == agent_id,
                Order.market_ticker == pos.market_ticker,
            )
        )
        orders = order_result.scalars().all()
        cost_basis = sum(o.cost for o in orders)

        response.append(
            PositionResponse(
                market_ticker=pos.market_ticker,
                yes_shares=pos.yes_shares,
                no_shares=pos.no_shares,
                current_yes_price=round(yes_price, 6),
                mark_to_market=round(mtm, 2),
                cost_basis=round(cost_basis, 2),
                unrealized_pnl=round(mtm - cost_basis, 2),
            )
        )
    return response


@router.get("/{agent_id}/pnl", response_model=list[PnLResponse])
async def get_pnl(agent_id: str, db: AsyncSession = Depends(get_db)):
    agent = await db.get(Agent, agent_id)
    if not agent:
        raise HTTPException(404, "Agent not found")

    result = await db.execute(
        select(PnLSnapshot)
        .where(PnLSnapshot.agent_id == agent_id)
        .order_by(PnLSnapshot.timestamp)
    )
    return [PnLResponse.model_validate(s) for s in result.scalars().all()]
