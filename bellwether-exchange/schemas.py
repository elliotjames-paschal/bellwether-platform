"""Pydantic request/response schemas."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


# --- Market Schemas ---

class MarketCreate(BaseModel):
    ticker: str
    title: str
    description: str = ""
    category: str
    subsidy: float = 1000.0
    resolution_source: str = ""
    resolution_criteria: str = ""
    expiration: datetime


class MarketResponse(BaseModel):
    ticker: str
    title: str
    description: str
    category: str
    subsidy: float
    yes_price: float
    no_price: float
    q_yes: float
    q_no: float
    status: str
    outcome: Optional[str]
    resolution_source: str
    resolution_criteria: str
    expiration: datetime
    resolved_at: Optional[datetime]
    created_at: datetime

    model_config = {"from_attributes": True}


class MarketResolve(BaseModel):
    outcome: str  # "yes" or "no"


# --- Agent Schemas ---

class AgentCreate(BaseModel):
    name: str
    framework: str = "custom"


class AgentResponse(BaseModel):
    id: str
    name: str
    api_key: str
    framework: str
    cash_balance: float
    created_at: datetime

    model_config = {"from_attributes": True}


# --- Order Schemas ---

class OrderCreate(BaseModel):
    market_ticker: str
    side: str  # "yes" or "no"
    shares: float


class OrderResponse(BaseModel):
    order_id: str
    market_ticker: str
    side: str
    shares: float
    fill_price: float
    cost: float
    new_yes_price: float
    cash_remaining: float


# --- Position Schema ---

class PositionResponse(BaseModel):
    market_ticker: str
    yes_shares: float
    no_shares: float
    current_yes_price: float
    mark_to_market: float
    cost_basis: float
    unrealized_pnl: float


# --- PnL Schema ---

class PnLResponse(BaseModel):
    cash: float
    equity: float
    total: float
    timestamp: datetime

    model_config = {"from_attributes": True}


# --- Leaderboard Schema ---

class LeaderboardEntry(BaseModel):
    rank: int
    agent_id: str
    name: str
    framework: str
    cash: float
    equity: float
    total: float
    pnl: float
    pnl_pct: float
