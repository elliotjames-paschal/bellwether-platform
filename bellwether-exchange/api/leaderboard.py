"""Leaderboard endpoint."""

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db
from lmsr import lmsr_price_yes, lmsr_price_no
from models import Agent, Market, Position
from schemas import LeaderboardEntry

router = APIRouter()

INITIAL_CASH = 10000.0


@router.get("", response_model=list[LeaderboardEntry])
async def get_leaderboard(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Agent))
    agents = result.scalars().all()

    entries = []
    for agent in agents:
        # Compute mark-to-market equity
        pos_result = await db.execute(
            select(Position).where(Position.agent_id == agent.id)
        )
        positions = pos_result.scalars().all()

        equity = 0.0
        for pos in positions:
            market = await db.get(Market, pos.market_ticker)
            if market and market.status == "active":
                yes_price = lmsr_price_yes(market.q_yes, market.q_no, market.b)
                no_price = lmsr_price_no(market.q_yes, market.q_no, market.b)
                equity += pos.yes_shares * yes_price + pos.no_shares * no_price

        total = agent.cash_balance + equity
        pnl = total - INITIAL_CASH
        pnl_pct = (pnl / INITIAL_CASH) * 100

        entries.append(
            LeaderboardEntry(
                rank=0,
                agent_id=agent.id,
                name=agent.name,
                framework=agent.framework,
                cash=round(agent.cash_balance, 2),
                equity=round(equity, 2),
                total=round(total, 2),
                pnl=round(pnl, 2),
                pnl_pct=round(pnl_pct, 2),
            )
        )

    # Sort by total descending, assign ranks
    entries.sort(key=lambda e: e.total, reverse=True)
    for i, entry in enumerate(entries, 1):
        entry.rank = i

    return entries
