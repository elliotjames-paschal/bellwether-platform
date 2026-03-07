"""Order endpoints."""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db
from lmsr import lmsr_cost_to_buy, lmsr_price_yes, lmsr_price_no
from models import Agent, Market, Order, Position, PnLSnapshot
from schemas import OrderCreate, OrderResponse

router = APIRouter()


async def _get_agent_by_key(api_key: str, db: AsyncSession) -> Agent:
    result = await db.execute(select(Agent).where(Agent.api_key == api_key))
    agent = result.scalars().first()
    if not agent:
        raise HTTPException(401, "Invalid API key")
    return agent


@router.post("", response_model=OrderResponse, status_code=201)
async def create_order(
    body: OrderCreate,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(alias="X-API-Key"),
):
    if body.side not in ("yes", "no"):
        raise HTTPException(400, "Side must be 'yes' or 'no'")
    if body.shares <= 0:
        raise HTTPException(400, "Shares must be positive")

    agent = await _get_agent_by_key(x_api_key, db)

    market = await db.get(Market, body.market_ticker)
    if not market:
        raise HTTPException(404, "Market not found")
    if market.status != "active":
        raise HTTPException(400, f"Market is {market.status}, cannot trade")

    # Compute cost
    cost = lmsr_cost_to_buy(body.side, body.shares, market.q_yes, market.q_no, market.b)
    fill_price = cost / body.shares

    if agent.cash_balance < cost:
        raise HTTPException(
            400,
            f"Insufficient cash. Need ${cost:.2f}, have ${agent.cash_balance:.2f}",
        )

    # Execute trade
    agent.cash_balance -= cost

    if body.side == "yes":
        market.q_yes += body.shares
    else:
        market.q_no += body.shares

    # Upsert position
    result = await db.execute(
        select(Position).where(
            Position.agent_id == agent.id,
            Position.market_ticker == body.market_ticker,
        )
    )
    position = result.scalars().first()
    if position:
        if body.side == "yes":
            position.yes_shares += body.shares
        else:
            position.no_shares += body.shares
    else:
        position = Position(
            id=str(uuid.uuid4()),
            agent_id=agent.id,
            market_ticker=body.market_ticker,
            yes_shares=body.shares if body.side == "yes" else 0.0,
            no_shares=body.shares if body.side == "no" else 0.0,
        )
        db.add(position)

    # Record order
    order = Order(
        id=str(uuid.uuid4()),
        agent_id=agent.id,
        market_ticker=body.market_ticker,
        side=body.side,
        shares=body.shares,
        price=fill_price,
        cost=cost,
    )
    db.add(order)

    # PnL snapshot
    equity = await _compute_equity(agent.id, db)
    snapshot = PnLSnapshot(
        id=str(uuid.uuid4()),
        agent_id=agent.id,
        cash=agent.cash_balance,
        equity=equity,
        total=agent.cash_balance + equity,
        timestamp=datetime.now(timezone.utc),
    )
    db.add(snapshot)

    await db.commit()

    new_yes_price = lmsr_price_yes(market.q_yes, market.q_no, market.b)

    return OrderResponse(
        order_id=order.id,
        market_ticker=body.market_ticker,
        side=body.side,
        shares=body.shares,
        fill_price=round(fill_price, 6),
        cost=round(cost, 2),
        new_yes_price=round(new_yes_price, 6),
        cash_remaining=round(agent.cash_balance, 2),
    )


@router.get("")
async def list_orders(
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(alias="X-API-Key"),
):
    agent = await _get_agent_by_key(x_api_key, db)
    result = await db.execute(
        select(Order)
        .where(Order.agent_id == agent.id)
        .order_by(Order.created_at.desc())
    )
    orders = result.scalars().all()
    return [
        {
            "order_id": o.id,
            "market_ticker": o.market_ticker,
            "side": o.side,
            "shares": o.shares,
            "fill_price": round(o.price, 6),
            "cost": round(o.cost, 2),
            "created_at": o.created_at.isoformat(),
        }
        for o in orders
    ]


async def _compute_equity(agent_id: str, db: AsyncSession) -> float:
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
