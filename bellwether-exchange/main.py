"""Bellwether Exchange — FastAPI entrypoint."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db import init_db
from seed import seed_markets
from api import markets, orders, agents, leaderboard

app = FastAPI(
    title="Bellwether Exchange",
    description="Agent-native prediction market exchange. Paper trading on Blueprint markets.",
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(markets.router, prefix="/markets", tags=["markets"])
app.include_router(orders.router, prefix="/orders", tags=["orders"])
app.include_router(agents.router, prefix="/agents", tags=["agents"])
app.include_router(leaderboard.router, prefix="/leaderboard", tags=["leaderboard"])


@app.on_event("startup")
async def startup():
    await init_db()
    await seed_markets()


@app.get("/")
def root():
    return {
        "service": "Bellwether Exchange",
        "version": "3.0.0",
        "docs": "/docs",
    }
