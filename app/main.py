from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

from app.providers.intelligence import build_market_intelligence
from app.providers.official_news import get_official_news

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    String,
    Text,
    create_engine,
    delete,
    inspect,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
CFTC_ENDPOINT = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"

database_url = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'cot.db'}")
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql+psycopg://", 1)
elif database_url.startswith("postgresql://"):
    database_url = database_url.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(database_url, pool_pre_ping=True)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("institutional-dashboard-v6-rebuilt")

# Exact CFTC contract codes are preferred. Name aliases are retained only as a fallback.
# These codes are stable CFTC market identifiers for the selected legacy futures-only contracts.
MARKET_DEFINITIONS: dict[str, dict[str, Any]] = {
    "USD": {"code": "098662", "aliases": ["U.S. DOLLAR INDEX", "US DOLLAR INDEX"]},
    "EUR": {"code": "099741", "aliases": ["EURO FX"]},
    "GBP": {"code": "096742", "aliases": ["BRITISH POUND"]},
    "JPY": {"code": "097741", "aliases": ["JAPANESE YEN"]},
    "CHF": {"code": "092741", "aliases": ["SWISS FRANC"]},
    "CAD": {"code": "090741", "aliases": ["CANADIAN DOLLAR"]},
    "AUD": {"code": "232741", "aliases": ["AUSTRALIAN DOLLAR"]},
    "NZD": {"code": "112741", "aliases": ["NEW ZEALAND DOLLAR"]},
    "GOLD": {"code": "088691", "aliases": ["GOLD"]},
    "SILVER": {"code": "084691", "aliases": ["SILVER"]},
    "PLATINUM": {"code": "076651", "aliases": ["PLATINUM"]},
    "PALLADIUM": {"code": "075651", "aliases": ["PALLADIUM"]},
}

CURRENCY_MARKETS = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"]
METAL_MARKETS = ["GOLD", "SILVER", "PLATINUM", "PALLADIUM"]

BBC_FEEDS = {
    "BBC Business": "https://feeds.bbci.co.uk/news/business/rss.xml",
}

# Free-tier Global Markets uses liquid US-listed ETFs as transparent proxies.
# This avoids claiming that a delayed ETF price is the official cash index level.
FREE_MARKET_PROXIES: dict[str, dict[str, str]] = {
    "UK": {"symbol": "EWU", "name": "United Kingdom", "benchmark": "FTSE / broad UK equities"},
    "US_LARGE": {"symbol": "SPY", "name": "US Large Cap", "benchmark": "S&P 500"},
    "US_TECH": {"symbol": "QQQ", "name": "US Technology", "benchmark": "Nasdaq 100"},
    "US_SMALL": {"symbol": "IWM", "name": "US Small Cap", "benchmark": "Russell 2000"},
    "EUROPE": {"symbol": "VGK", "name": "Europe", "benchmark": "Broad developed Europe"},
    "JAPAN": {"symbol": "EWJ", "name": "Japan", "benchmark": "Japanese equities / Nikkei context"},
    "CHINA": {"symbol": "FXI", "name": "China Large Cap", "benchmark": "Large Chinese equities"},
    "AUSTRALIA": {"symbol": "EWA", "name": "Australia", "benchmark": "Australian equities / ASX context"},
}

# Automatic seasonality is calculated from monthly closing prices.
# The free Twelve Data plan is respected by splitting the universe into
# two groups of no more than eight symbols.


# Synthetic USD basket used when an official DXY time series is unavailable.
# The weights match the six-currency ICE USDX basket. The series is rebased to
# 100 at its first common observation and is clearly labelled as synthetic.
SYNTHETIC_DXY_COMPONENTS: dict[str, dict[str, Any]] = {
    "EUR": {"symbol": "EUR/USD", "stooq": "eurusd", "exponent": -0.576},
    "JPY": {"symbol": "USD/JPY", "stooq": "usdjpy", "exponent": 0.136},
    "GBP": {"symbol": "GBP/USD", "stooq": "gbpusd", "exponent": -0.119},
    "CAD": {"symbol": "USD/CAD", "stooq": "usdcad", "exponent": 0.091},
    "SEK": {"symbol": "USD/SEK", "stooq": "usdsek", "exponent": 0.042},
    "CHF": {"symbol": "USD/CHF", "stooq": "usdchf", "exponent": 0.036},
}
SYNTHETIC_DXY_CACHE_TTL = timedelta(hours=8)
_synthetic_dxy_cache: dict[str, Any] = {}
_synthetic_dxy_cache_time: datetime | None = None

COT_PRICE_OVERLAY_SYMBOLS: dict[str, dict[str, Any]] = {
    "USD": {"symbols": ["DXY", "DXY:ICE", "UUP"], "label": "US Dollar Index", "fallback_label": "UUP Dollar Index ETF proxy"},
    "AUD": {"symbols": ["6A1!", "6A1!:CME", "AUD/USD"], "label": "Australian Dollar futures", "fallback_label": "AUD/USD spot fallback"},
    "EUR": {"symbols": ["6E1!", "6E1!:CME", "EUR/USD"], "label": "Euro FX futures", "fallback_label": "EUR/USD spot fallback"},
    "GBP": {"symbols": ["6B1!", "6B1!:CME", "GBP/USD"], "label": "British Pound futures", "fallback_label": "GBP/USD spot fallback"},
    "CAD": {"symbols": ["6C1!", "6C1!:CME", "USD/CAD"], "label": "Canadian Dollar futures", "fallback_label": "USD/CAD spot fallback", "invert_fallback": True},
    "JPY": {"symbols": ["6J1!", "6J1!:CME", "USD/JPY"], "label": "Japanese Yen futures", "fallback_label": "USD/JPY spot fallback", "invert_fallback": True},
    "CHF": {"symbols": ["6S1!", "6S1!:CME", "USD/CHF"], "label": "Swiss Franc futures", "fallback_label": "USD/CHF spot fallback", "invert_fallback": True},
    "NZD": {"symbols": ["6N1!", "6N1!:CME", "NZD/USD"], "label": "New Zealand Dollar futures", "fallback_label": "NZD/USD spot fallback"},
    "GOLD": {"symbols": ["GC1!", "GC1!:COMEX", "XAU/USD"], "label": "Gold futures", "fallback_label": "Gold spot fallback"},
    "SILVER": {"symbols": ["SI1!", "SI1!:COMEX", "XAG/USD"], "label": "Silver futures", "fallback_label": "Silver spot fallback"},
}

AUTO_SEASONALITY_GROUPS: dict[str, dict[str, dict[str, Any]]] = {
    "core": {
        "USD": {"symbol": "UUP", "invert": False, "label": "US Dollar proxy"},
        "EUR": {"symbol": "EUR/USD", "invert": False, "label": "Euro vs US Dollar"},
        "GBP": {"symbol": "GBP/USD", "invert": False, "label": "British Pound vs US Dollar"},
        "JPY": {"symbol": "USD/JPY", "invert": True, "label": "Japanese Yen vs US Dollar"},
        "CHF": {"symbol": "USD/CHF", "invert": True, "label": "Swiss Franc vs US Dollar"},
        "CAD": {"symbol": "USD/CAD", "invert": True, "label": "Canadian Dollar vs US Dollar"},
        "AUD": {"symbol": "AUD/USD", "invert": False, "label": "Australian Dollar vs US Dollar"},
        "NZD": {"symbol": "NZD/USD", "invert": False, "label": "New Zealand Dollar vs US Dollar"},
    },
    "assets": {
        "GOLD": {"symbol": "XAU/USD", "invert": False, "label": "Gold"},
        "SILVER": {"symbol": "XAG/USD", "invert": False, "label": "Silver"},
        "SP500": {"symbol": "SPY", "invert": False, "label": "S&P 500 ETF proxy"},
        "FTSE100": {"symbol": "EWU", "invert": False, "label": "UK equity ETF proxy"},
        "NASDAQ100": {"symbol": "QQQ", "invert": False, "label": "Nasdaq 100 ETF proxy"},
        "JAPAN": {"symbol": "EWJ", "invert": False, "label": "Japan equity ETF proxy"},
        "CHINA": {"symbol": "FXI", "invert": False, "label": "China large-cap ETF proxy"},
        "AUSTRALIA_EQ": {"symbol": "EWA", "invert": False, "label": "Australia equity ETF proxy"},
    },
    "dxy": {
        "DXY": {
            "symbols": ["DXY", "DXY:ICE", "UUP"],
            "invert": False,
            "label": "US Dollar Index",
            "fallback_label": "UUP US Dollar Index ETF proxy",
        },
    },
}


COUNTRY_RULES = {
    "USD": ("United States", ["dollar", "fed", "federal reserve", "us economy", "treasury", "wall street"]),
    "GBP": ("United Kingdom", ["sterling", "bank of england", "uk economy", "britain", "ftse"]),
    "EUR": ("Euro area", ["euro", "ecb", "eurozone", "european central bank"]),
    "JPY": ("Japan", ["yen", "bank of japan", "japan economy", "nikkei"]),
    "CHF": ("Switzerland", ["swiss franc", "snb", "switzerland"]),
    "CAD": ("Canada", ["canadian dollar", "bank of canada", "canada economy"]),
    "AUD": ("Australia", ["australian dollar", "rba", "australia economy"]),
    "NZD": ("New Zealand", ["new zealand dollar", "rbnz", "new zealand economy"]),
    "GOLD": ("Global", ["gold", "bullion", "precious metals"]),
    "SILVER": ("Global", ["silver"]),
}

HIGH_IMPACT_TERMS = [
    "interest rate", "rate decision", "inflation", "cpi", "employment", "jobs",
    "payroll", "gdp", "central bank", "federal reserve", "bank of england",
    "ecb", "bank of japan", "recession", "war", "tariff", "sanction",
]
MEDIUM_IMPACT_TERMS = [
    "retail sales", "manufacturing", "services", "pmi", "housing", "trade",
    "budget", "oil", "commodity", "bond", "yield",
]


class Base(DeclarativeBase):
    pass


# IMPORTANT:
# Keep the existing V5 table and its `currency` column so the live database remains compatible.
# Metals are stored in a separate V6 table.
class CotPosition(Base):
    __tablename__ = "cot_positions"

    currency: Mapped[str] = mapped_column(String(20), primary_key=True)
    report_date: Mapped[date] = mapped_column(Date, primary_key=True)
    market_name: Mapped[str] = mapped_column(String(255))
    commercial_long: Mapped[int] = mapped_column(BigInteger)
    commercial_short: Mapped[int] = mapped_column(BigInteger)
    commercial_net: Mapped[int] = mapped_column(BigInteger)
    noncommercial_long: Mapped[int] = mapped_column(BigInteger)
    noncommercial_short: Mapped[int] = mapped_column(BigInteger)
    noncommercial_net: Mapped[int] = mapped_column(BigInteger)
    open_interest: Mapped[int] = mapped_column(BigInteger, default=0)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MetalCotPosition(Base):
    __tablename__ = "metal_cot_positions"

    market: Mapped[str] = mapped_column(String(20), primary_key=True)
    report_date: Mapped[date] = mapped_column(Date, primary_key=True)
    market_name: Mapped[str] = mapped_column(String(255))
    commercial_long: Mapped[int] = mapped_column(BigInteger)
    commercial_short: Mapped[int] = mapped_column(BigInteger)
    commercial_net: Mapped[int] = mapped_column(BigInteger)
    noncommercial_long: Mapped[int] = mapped_column(BigInteger)
    noncommercial_short: Mapped[int] = mapped_column(BigInteger)
    noncommercial_net: Mapped[int] = mapped_column(BigInteger)
    open_interest: Mapped[int] = mapped_column(BigInteger, default=0)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class AppStatus(Base):
    __tablename__ = "app_status"

    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    value: Mapped[str] = mapped_column(Text)


class SeasonalityValue(Base):
    __tablename__ = "seasonality_values"

    market: Mapped[str] = mapped_column(String(40), primary_key=True)
    month_number: Mapped[int] = mapped_column(primary_key=True)
    tendency: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SeasonalityProfile(Base):
    __tablename__ = "seasonality_profiles"

    market: Mapped[str] = mapped_column(String(40), primary_key=True)
    month_number: Mapped[int] = mapped_column(primary_key=True)
    average_5y: Mapped[float] = mapped_column(Float, default=0.0)
    average_10y: Mapped[float] = mapped_column(Float, default=0.0)
    average_20y: Mapped[float] = mapped_column(Float, default=0.0)
    win_rate: Mapped[float] = mapped_column(Float, default=50.0)
    sample_years: Mapped[int] = mapped_column(default=0)
    notes: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SeasonalityBacktest(Base):
    __tablename__ = "seasonality_backtests"

    market: Mapped[str] = mapped_column(String(40), primary_key=True)
    month_number: Mapped[int] = mapped_column(primary_key=True)
    median_return: Mapped[float] = mapped_column(Float, default=0.0)
    volatility: Mapped[float] = mapped_column(Float, default=0.0)
    best_return: Mapped[float] = mapped_column(Float, default=0.0)
    worst_return: Mapped[float] = mapped_column(Float, default=0.0)
    max_drawdown: Mapped[float] = mapped_column(Float, default=0.0)
    positive_years: Mapped[int] = mapped_column(default=0)
    negative_years: Mapped[int] = mapped_column(default=0)
    reliability_grade: Mapped[str] = mapped_column(String(10), default="N/A")
    current_year_return: Mapped[float | None] = mapped_column(Float, nullable=True)
    divergence: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class EconomicEvent(Base):
    __tablename__ = "economic_events"

    event_id: Mapped[str] = mapped_column(String(120), primary_key=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    country: Mapped[str] = mapped_column(String(80))
    currency: Mapped[str] = mapped_column(String(20))
    event_name: Mapped[str] = mapped_column(String(255))
    impact: Mapped[str] = mapped_column(String(20))
    actual: Mapped[str | None] = mapped_column(String(80), nullable=True)
    forecast: Mapped[str | None] = mapped_column(String(80), nullable=True)
    previous: Mapped[str | None] = mapped_column(String(80), nullable=True)
    source: Mapped[str] = mapped_column(String(120), default="Manual")
    effects_json: Mapped[str] = mapped_column(Text, default="[]")
    is_manual: Mapped[bool] = mapped_column(Boolean, default=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))



class TechnicalSetup(Base):
    __tablename__ = "technical_setups"

    pair: Mapped[str] = mapped_column(String(20), primary_key=True)
    direction: Mapped[str] = mapped_column(String(10), default="Long")
    htf_direction: Mapped[str] = mapped_column(String(12), default="Neutral")
    mtf_choch: Mapped[bool] = mapped_column(Boolean, default=False)
    ltf_choch: Mapped[bool] = mapped_column(Boolean, default=False)
    bos_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    pullback_ready: Mapped[bool] = mapped_column(Boolean, default=False)
    invalidated: Mapped[bool] = mapped_column(Boolean, default=False)
    notes: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

def initialise_database() -> None:
    # This is intentionally additive only. It preserves the V5 cot_positions table.
    Base.metadata.create_all(engine)


def to_int(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    return 0 if value in (None, "") else int(float(value))


async def download_cftc_dataset() -> list[dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=190)).date().isoformat()
    params = {
        "$select": (
            "market_and_exchange_names,report_date_as_yyyy_mm_dd,"
            "cftc_contract_market_code,commodity_name,"
            "comm_positions_long_all,comm_positions_short_all,"
            "noncomm_positions_long_all,noncomm_positions_short_all,open_interest_all"
        ),
        "$where": f"report_date_as_yyyy_mm_dd >= '{cutoff}T00:00:00.000'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": "50000",
    }
    async with httpx.AsyncClient(
        headers={"User-Agent": "InstitutionalMacroDashboard/6.1"},
        timeout=90,
        follow_redirects=True,
    ) as client:
        response = await client.get(CFTC_ENDPOINT, params=params)
        response.raise_for_status()
        return response.json()


def select_market_rows(
    dataset: list[dict[str, Any]], definition: dict[str, Any]
) -> list[dict[str, Any]]:
    code = str(definition["code"])
    aliases = [alias.upper() for alias in definition["aliases"]]

    exact = [
        row for row in dataset
        if str(row.get("cftc_contract_market_code", "")).strip() == code
    ]
    candidates = exact
    if not candidates:
        candidates = []
        for row in dataset:
            market = str(row.get("market_and_exchange_names", "")).upper()
            commodity = str(row.get("commodity_name", "")).upper()
            if any(alias in market or alias in commodity for alias in aliases):
                candidates.append(row)

    if not candidates:
        return []

    unique: dict[str, dict[str, Any]] = {}
    for row in candidates:
        report_date = str(row["report_date_as_yyyy_mm_dd"])[:10]
        unique.setdefault(report_date, row)
    return [unique[key] for key in sorted(unique, reverse=True)[:10]]


refresh_lock = asyncio.Lock()


def upsert_currency_rows(
    session: Session,
    market_code: str,
    rows: list[dict[str, Any]],
    fetched_at: datetime,
) -> None:
    retained_dates: list[date] = []
    for row in rows:
        report_date = date.fromisoformat(str(row["report_date_as_yyyy_mm_dd"])[:10])
        retained_dates.append(report_date)
        commercial_long = to_int(row, "comm_positions_long_all")
        commercial_short = to_int(row, "comm_positions_short_all")
        noncommercial_long = to_int(row, "noncomm_positions_long_all")
        noncommercial_short = to_int(row, "noncomm_positions_short_all")

        position = session.get(
            CotPosition,
            {"currency": market_code, "report_date": report_date},
        )
        if position is None:
            position = CotPosition(
                currency=market_code,
                report_date=report_date,
                market_name="",
                commercial_long=0,
                commercial_short=0,
                commercial_net=0,
                noncommercial_long=0,
                noncommercial_short=0,
                noncommercial_net=0,
                open_interest=0,
                fetched_at=fetched_at,
            )
            session.add(position)

        position.market_name = str(row.get("market_and_exchange_names", ""))
        position.commercial_long = commercial_long
        position.commercial_short = commercial_short
        position.commercial_net = commercial_long - commercial_short
        position.noncommercial_long = noncommercial_long
        position.noncommercial_short = noncommercial_short
        position.noncommercial_net = noncommercial_long - noncommercial_short
        position.open_interest = to_int(row, "open_interest_all")
        position.fetched_at = fetched_at

    if retained_dates:
        session.execute(
            delete(CotPosition).where(
                CotPosition.currency == market_code,
                CotPosition.report_date.not_in(retained_dates),
            )
        )


def upsert_metal_rows(
    session: Session,
    market_code: str,
    rows: list[dict[str, Any]],
    fetched_at: datetime,
) -> None:
    retained_dates: list[date] = []
    for row in rows:
        report_date = date.fromisoformat(str(row["report_date_as_yyyy_mm_dd"])[:10])
        retained_dates.append(report_date)
        commercial_long = to_int(row, "comm_positions_long_all")
        commercial_short = to_int(row, "comm_positions_short_all")
        noncommercial_long = to_int(row, "noncomm_positions_long_all")
        noncommercial_short = to_int(row, "noncomm_positions_short_all")

        position = session.get(
            MetalCotPosition,
            {"market": market_code, "report_date": report_date},
        )
        if position is None:
            position = MetalCotPosition(
                market=market_code,
                report_date=report_date,
                market_name="",
                commercial_long=0,
                commercial_short=0,
                commercial_net=0,
                noncommercial_long=0,
                noncommercial_short=0,
                noncommercial_net=0,
                open_interest=0,
                fetched_at=fetched_at,
            )
            session.add(position)

        position.market_name = str(row.get("market_and_exchange_names", ""))
        position.commercial_long = commercial_long
        position.commercial_short = commercial_short
        position.commercial_net = commercial_long - commercial_short
        position.noncommercial_long = noncommercial_long
        position.noncommercial_short = noncommercial_short
        position.noncommercial_net = noncommercial_long - noncommercial_short
        position.open_interest = to_int(row, "open_interest_all")
        position.fetched_at = fetched_at

    if retained_dates:
        session.execute(
            delete(MetalCotPosition).where(
                MetalCotPosition.market == market_code,
                MetalCotPosition.report_date.not_in(retained_dates),
            )
        )


async def refresh_cot() -> dict[str, Any]:
    if refresh_lock.locked():
        return {"status": "already_running"}

    async with refresh_lock:
        fetched_at = datetime.now(timezone.utc)
        dataset = await download_cftc_dataset()
        result: dict[str, Any] = {"updated": [], "failed": []}

        with Session(engine) as session:
            for market_code, definition in MARKET_DEFINITIONS.items():
                try:
                    rows = select_market_rows(dataset, definition)
                    if not rows:
                        raise RuntimeError(f"No matching CFTC contract found for {market_code}")
                    if market_code in CURRENCY_MARKETS:
                        upsert_currency_rows(session, market_code, rows, fetched_at)
                    else:
                        upsert_metal_rows(session, market_code, rows, fetched_at)
                    result["updated"].append(market_code)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Refresh failed for %s", market_code)
                    result["failed"].append({"market": market_code, "error": str(exc)})

            status_value = json.dumps({
                "time": fetched_at.isoformat(),
                "updated": result["updated"],
                "failed": result["failed"],
            })
            status = session.get(AppStatus, "last_refresh")
            if status is None:
                session.add(AppStatus(key="last_refresh", value=status_value))
            else:
                status.value = status_value
            session.commit()

        return result


def serialise_position(row: Any) -> dict[str, Any]:
    return {
        "report_date": row.report_date.isoformat(),
        "market_name": row.market_name,
        "commercial_long": row.commercial_long,
        "commercial_short": row.commercial_short,
        "commercial_net": row.commercial_net,
        "noncommercial_long": row.noncommercial_long,
        "noncommercial_short": row.noncommercial_short,
        "noncommercial_net": row.noncommercial_net,
        "open_interest": row.open_interest,
    }


def build_payload() -> dict[str, Any]:
    with Session(engine) as session:
        currencies = {}
        for market in CURRENCY_MARKETS:
            rows = session.scalars(
                select(CotPosition)
                .where(CotPosition.currency == market)
                .order_by(CotPosition.report_date.asc())
            ).all()
            currencies[market] = [serialise_position(row) for row in rows[-10:]]

        metals = {}
        for market in METAL_MARKETS:
            rows = session.scalars(
                select(MetalCotPosition)
                .where(MetalCotPosition.market == market)
                .order_by(MetalCotPosition.report_date.asc())
            ).all()
            metals[market] = [serialise_position(row) for row in rows[-10:]]

        status = session.get(AppStatus, "last_refresh")

    parsed_status = None
    if status:
        try:
            parsed_status = json.loads(status.value)
        except json.JSONDecodeError:
            parsed_status = {"raw": status.value}

    return {
        "source": "CFTC Legacy Futures Only",
        "dataset_id": "6dca-aqww",
        "non_reportables_included": False,
        "last_refresh": parsed_status,
        "currencies": currencies,
        "metals": metals,
    }


def is_stale() -> bool:
    with Session(engine) as session:
        latest_currency = session.scalar(
            select(CotPosition.fetched_at).order_by(CotPosition.fetched_at.desc())
        )
        latest_metal = session.scalar(
            select(MetalCotPosition.fetched_at).order_by(MetalCotPosition.fetched_at.desc())
        )
    latest = max(
        [x for x in [latest_currency, latest_metal] if x is not None],
        default=None,
    )
    if latest is None:
        return True
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - latest > timedelta(days=6)


def clean_html(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value or "").strip()


def classify_news(title: str, description: str) -> dict[str, Any]:
    haystack = f"{title} {description}".lower()
    affected = []
    countries = []
    for symbol, (country, keywords) in COUNTRY_RULES.items():
        if any(keyword in haystack for keyword in keywords):
            affected.append(symbol)
            if country not in countries:
                countries.append(country)

    impact = "Low"
    if any(term in haystack for term in HIGH_IMPACT_TERMS):
        impact = "High"
    elif any(term in haystack for term in MEDIUM_IMPACT_TERMS):
        impact = "Medium"

    effects = []
    if "inflation" in haystack or "cpi" in haystack:
        effects = ["Currencies", "Bond yields", "Precious metals", "Equity indices"]
    elif "interest rate" in haystack or "central bank" in haystack:
        effects = ["Currencies", "Government bonds", "Equity indices", "Gold"]
    elif "oil" in haystack:
        effects = ["CAD", "Energy equities", "Inflation expectations"]
    elif "war" in haystack or "sanction" in haystack:
        effects = ["Risk sentiment", "Gold", "Oil", "Safe-haven currencies"]
    elif affected:
        effects = ["Currencies", "Related national equity index", "Government bonds"]
    else:
        effects = ["Global risk sentiment"]

    return {
        "impact": impact,
        "affected_markets": affected or ["GLOBAL"],
        "countries": countries or ["Global"],
        "effects": effects,
    }


async def fetch_bbc_feed(source: str, url: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        response = await client.get(
            url,
            headers={"User-Agent": "InstitutionalMacroDashboard/6.1"},
        )
        response.raise_for_status()

    root = ET.fromstring(response.content)
    items = []
    for item in root.findall(".//item")[:20]:
        title = clean_html(item.findtext("title", default=""))
        summary = clean_html(item.findtext("description", default=""))
        link = item.findtext("link", default="")
        published_raw = item.findtext("pubDate", default="")
        try:
            published = parsedate_to_datetime(published_raw).astimezone(timezone.utc).isoformat()
        except Exception:  # noqa: BLE001
            published = published_raw

        items.append({
            "source": source,
            "title": title,
            "summary": summary,
            "url": link,
            "published": published,
            **classify_news(title, summary),
        })
    return items


async def bbc_news() -> dict[str, Any]:
    results = await asyncio.gather(
        *[fetch_bbc_feed(source, url) for source, url in BBC_FEEDS.items()],
        return_exceptions=True,
    )

    items = []
    errors = []
    for source, result in zip(BBC_FEEDS, results):
        if isinstance(result, Exception):
            errors.append({"source": source, "error": str(result)})
        else:
            items.extend(result)

    unique: dict[str, dict[str, Any]] = {}
    for item in items:
        unique.setdefault(item["title"], item)
    sorted_items = sorted(unique.values(), key=lambda row: row["published"], reverse=True)
    return {
        "items": sorted_items[:40],
        "errors": errors,
        "sources": list(BBC_FEEDS),
        "analysis_note": "Impact and affected-market labels are keyword classification, not final AI judgement.",
    }


async def trading_economics_calendar() -> dict[str, Any]:
    api_key = os.getenv("TRADING_ECONOMICS_API_KEY")
    if not api_key:
        return {
            "configured": False,
            "events": [],
            "message": "Add TRADING_ECONOMICS_API_KEY in Render.",
        }

    start = datetime.now(timezone.utc).date().isoformat()
    end = (datetime.now(timezone.utc).date() + timedelta(days=14)).isoformat()
    url = f"https://api.tradingeconomics.com/calendar/country/All/{start}/{end}"
    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.get(url, params={"c": api_key})
        response.raise_for_status()
        raw = response.json()

    events = []
    for row in raw[:300]:
        importance_number = int(row.get("Importance") or 1)
        impact = {1: "Low", 2: "Medium", 3: "High"}.get(importance_number, "Low")
        events.append({
            "date": row.get("Date"),
            "country": row.get("Country") or "Unknown",
            "event": row.get("Event") or row.get("Category") or "Economic event",
            "impact": impact,
            "actual": row.get("Actual"),
            "forecast": row.get("Forecast"),
            "previous": row.get("Previous"),
            "source": row.get("Source"),
            "category": row.get("Category"),
        })
    return {"configured": True, "events": events}


async def fred_macro() -> dict[str, Any]:
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        return {
            "configured": False,
            "series": {},
            "summaries": {},
            "message": "Add FRED_API_KEY in Render.",
        }

    series_ids = {
        "US_2Y": {"id": "DGS2", "name": "US 2-Year Treasury", "unit": "%"},
        "US_5Y": {"id": "DGS5", "name": "US 5-Year Treasury", "unit": "%"},
        "US_10Y": {"id": "DGS10", "name": "US 10-Year Treasury", "unit": "%"},
        "US_30Y": {"id": "DGS30", "name": "US 30-Year Treasury", "unit": "%"},
        "FED_FUNDS": {"id": "DFF", "name": "Effective Federal Funds Rate", "unit": "%"},
        "REAL_10Y": {"id": "DFII10", "name": "US 10-Year Real Yield", "unit": "%"},
        "BREAKEVEN_10Y": {"id": "T10YIE", "name": "US 10-Year Breakeven Inflation", "unit": "%"},
    }

    output: dict[str, list[dict[str, Any]]] = {}
    summaries: dict[str, dict[str, Any]] = {}

    async with httpx.AsyncClient(timeout=45) as client:
        for label, definition in series_ids.items():
            response = await client.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id": definition["id"],
                    "api_key": api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 180,
                },
            )
            response.raise_for_status()
            rows = [
                {
                    "date": row["date"],
                    "value": None if row["value"] == "." else float(row["value"]),
                }
                for row in response.json().get("observations", [])
            ]
            valid = [row for row in rows if row["value"] is not None]
            output[label] = list(reversed(valid[:120]))

            latest = valid[0]["value"] if valid else None
            previous = valid[1]["value"] if len(valid) > 1 else None
            week_ago = valid[min(5, len(valid) - 1)]["value"] if valid else None
            month_ago = valid[min(21, len(valid) - 1)]["value"] if valid else None

            summaries[label] = {
                "name": definition["name"],
                "series_id": definition["id"],
                "unit": definition["unit"],
                "latest_date": valid[0]["date"] if valid else None,
                "latest": latest,
                "daily_change": None if latest is None or previous is None else round(latest - previous, 3),
                "weekly_change": None if latest is None or week_ago is None else round(latest - week_ago, 3),
                "monthly_change": None if latest is None or month_ago is None else round(latest - month_ago, 3),
            }

    two_year = summaries.get("US_2Y", {}).get("latest")
    ten_year = summaries.get("US_10Y", {}).get("latest")
    thirty_year = summaries.get("US_30Y", {}).get("latest")
    real_ten = summaries.get("REAL_10Y", {}).get("latest")
    breakeven = summaries.get("BREAKEVEN_10Y", {}).get("latest")

    curve_2s10s = None if two_year is None or ten_year is None else round(ten_year - two_year, 3)
    curve_10s30s = None if ten_year is None or thirty_year is None else round(thirty_year - ten_year, 3)

    if curve_2s10s is None:
        curve_state = "Unavailable"
    elif curve_2s10s < -0.05:
        curve_state = "Inverted"
    elif curve_2s10s > 0.25:
        curve_state = "Upward sloping"
    else:
        curve_state = "Flat"

    interpretation = []
    if curve_2s10s is not None:
        interpretation.append(
            f"2s10s spread is {curve_2s10s:+.3f} percentage points ({curve_state.lower()})."
        )
    if real_ten is not None:
        interpretation.append(
            f"10-year real yield is {real_ten:.3f}%, an important input for USD and precious metals."
        )
    if breakeven is not None:
        interpretation.append(
            f"10-year breakeven inflation is {breakeven:.3f}%."
        )

    return {
        "configured": True,
        "provider": "FRED",
        "series": output,
        "summaries": summaries,
        "yield_curve": {
            "curve_2s10s": curve_2s10s,
            "curve_10s30s": curve_10s30s,
            "state": curve_state,
        },
        "interpretation": interpretation,
    }


def read_seasonality() -> dict[str, list[float]]:
    output: dict[str, list[float]] = {}
    with Session(engine) as session:
        rows = session.scalars(
            select(SeasonalityValue).order_by(
                SeasonalityValue.market.asc(),
                SeasonalityValue.month_number.asc(),
            )
        ).all()
    for row in rows:
        output.setdefault(row.market, [0.0] * 12)
        if 1 <= row.month_number <= 12:
            output[row.market][row.month_number - 1] = row.tendency
    return output


def save_seasonality(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("markets", [])
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        for row in rows:
            market = str(row.get("market", "")).upper().strip()
            values = row.get("values", [])
            if not market or len(values) != 12:
                continue
            for month_number, tendency in enumerate(values, start=1):
                existing = session.get(
                    SeasonalityValue,
                    {"market": market, "month_number": month_number},
                )
                if existing is None:
                    existing = SeasonalityValue(
                        market=market,
                        month_number=month_number,
                        tendency=float(tendency or 0),
                        updated_at=now,
                    )
                    session.add(existing)
                else:
                    existing.tendency = float(tendency or 0)
                    existing.updated_at = now
        session.commit()
    return {"saved": True, "markets": len(rows)}



_market_cache: dict[str, Any] | None = None
_market_cache_time: datetime | None = None
_market_cache_lock = asyncio.Lock()
MARKET_CACHE_TTL = timedelta(minutes=15)


def _change_pct(values: list[dict[str, Any]], periods_back: int) -> float | None:
    if len(values) <= periods_back:
        return None
    latest = float(values[0]["close"])
    previous = float(values[periods_back]["close"])
    if previous == 0:
        return None
    return round((latest / previous - 1) * 100, 2)


async def twelve_data_free_markets() -> dict[str, Any]:
    global _market_cache, _market_cache_time

    now = datetime.now(timezone.utc)
    if (
        _market_cache is not None
        and _market_cache_time is not None
        and now - _market_cache_time < MARKET_CACHE_TTL
    ):
        return _market_cache

    api_key = os.getenv("TWELVE_DATA_API_KEY")
    if not api_key:
        return {
            "configured": False,
            "provider": "Twelve Data",
            "markets": [],
            "message": "Add a free TWELVE_DATA_API_KEY in Render to activate Global Markets.",
            "proxy_note": "Major regions are represented by liquid US-listed ETFs, not official cash-index levels.",
        }

    symbols = ",".join(item["symbol"] for item in FREE_MARKET_PROXIES.values())
    params = {
        "symbol": symbols,
        "interval": "1day",
        "outputsize": 260,
        "apikey": api_key,
        "format": "JSON",
    }

    async with _market_cache_lock:
        now = datetime.now(timezone.utc)
        if (
            _market_cache is not None
            and _market_cache_time is not None
            and now - _market_cache_time < MARKET_CACHE_TTL
        ):
            return _market_cache

        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(
                "https://api.twelvedata.com/time_series",
                params=params,
            )

        if response.status_code == 429:
            if _market_cache is not None:
                cached = dict(_market_cache)
                cached["stale"] = True
                cached["message"] = (
                    "Twelve Data rate limit reached. Showing the most recent cached market data."
                )
                return cached
            return {
                "configured": True,
                "provider": "Twelve Data free tier",
                "markets": [],
                "rate_limited": True,
                "message": (
                    "Twelve Data free-plan rate limit reached. "
                    "Wait one minute and refresh the page."
                ),
                "proxy_note": "Free plan supports 8 API credits per minute.",
            }

        response.raise_for_status()
        raw = response.json()

    # A multi-symbol response is keyed by symbol. A single error response has status=error.
    if raw.get("status") == "error":
        return {
            "configured": True,
            "provider": "Twelve Data",
            "markets": [],
            "error": raw.get("message", "Twelve Data returned an error."),
            "proxy_note": "Major regions are represented by liquid US-listed ETFs.",
        }

    by_symbol = {definition["symbol"]: (key, definition) for key, definition in FREE_MARKET_PROXIES.items()}
    markets = []
    errors = []

    for symbol, (market_id, definition) in by_symbol.items():
        payload = raw.get(symbol)
        if not payload:
            errors.append({"symbol": symbol, "error": "No response returned"})
            continue
        if payload.get("status") == "error":
            errors.append({"symbol": symbol, "error": payload.get("message", "Unavailable")})
            continue

        values = payload.get("values", [])
        if not values:
            errors.append({"symbol": symbol, "error": "No price history"})
            continue

        latest = values[0]
        latest_close = float(latest["close"])
        daily = _change_pct(values, 1)
        weekly = _change_pct(values, min(5, len(values) - 1))
        monthly = _change_pct(values, min(21, len(values) - 1))
        six_month = _change_pct(values, min(126, len(values) - 1))
        yearly = _change_pct(values, min(252, len(values) - 1))

        chart = [
            {"date": row["datetime"][:10], "close": float(row["close"])}
            for row in reversed(values[:60])
        ]

        markets.append({
            "id": market_id,
            "symbol": symbol,
            "name": definition["name"],
            "benchmark": definition["benchmark"],
            "date": latest["datetime"][:10],
            "close": latest_close,
            "currency": payload.get("meta", {}).get("currency", "USD"),
            "daily_change_pct": daily,
            "weekly_change_pct": weekly,
            "monthly_change_pct": monthly,
            "six_month_change_pct": six_month,
            "yearly_change_pct": yearly,
            "trend": "Bullish" if (monthly or 0) > 1 else "Bearish" if (monthly or 0) < -1 else "Neutral",
            "chart": chart,
        })

    # A transparent risk score based only on markets successfully returned.
    risk_values = [row["weekly_change_pct"] for row in markets if row["weekly_change_pct"] is not None]
    average_weekly = round(sum(risk_values) / len(risk_values), 2) if risk_values else None
    risk_regime = (
        "Risk-on" if average_weekly is not None and average_weekly > 0.5
        else "Risk-off" if average_weekly is not None and average_weekly < -0.5
        else "Mixed"
    )

    result = {
        "configured": True,
        "provider": "Twelve Data free tier",
        "markets": markets,
        "errors": errors,
        "risk_regime": risk_regime,
        "average_weekly_change_pct": average_weekly,
        "proxy_note": (
            "These are liquid US-listed ETF proxies used for free macro context. "
            "They are not official index levels and can differ because of currency, fees and trading hours."
        ),
        "cache_minutes": 15,
    }
    _market_cache = result
    _market_cache_time = datetime.now(timezone.utc)
    return result



def normalise_event_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        event_time = value
    else:
        event_time = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    return event_time.astimezone(timezone.utc)


def event_effects(event_name: str, currency: str) -> list[str]:
    haystack = event_name.lower()
    effects = [currency] if currency else []
    if any(term in haystack for term in ["rate", "central bank", "monetary policy"]):
        effects += ["Government bonds", "Equities", "Gold"]
    elif any(term in haystack for term in ["inflation", "cpi", "ppi", "pce"]):
        effects += ["Bond yields", "Policy expectations", "Precious metals"]
    elif any(term in haystack for term in ["employment", "payroll", "unemployment", "wages"]):
        effects += ["Bond yields", "Equities"]
    elif any(term in haystack for term in ["gdp", "pmi", "retail sales", "growth"]):
        effects += ["Equities", "Government bonds"]
    else:
        effects += ["Related national equity index", "Government bonds"]
    return list(dict.fromkeys(effects))


def read_manual_events(days_back: int = 2, days_forward: int = 45) -> list[dict[str, Any]]:
    start = datetime.now(timezone.utc) - timedelta(days=days_back)
    end = datetime.now(timezone.utc) + timedelta(days=days_forward)
    with Session(engine) as session:
        rows = session.scalars(
            select(EconomicEvent)
            .where(EconomicEvent.event_time >= start, EconomicEvent.event_time <= end)
            .order_by(EconomicEvent.event_time.asc())
        ).all()

    output = []
    for row in rows:
        try:
            effects = json.loads(row.effects_json)
        except json.JSONDecodeError:
            effects = []
        output.append({
            "event_id": row.event_id,
            "date": row.event_time.isoformat(),
            "country": row.country,
            "currency": row.currency,
            "event": row.event_name,
            "impact": row.impact,
            "actual": row.actual,
            "forecast": row.forecast,
            "previous": row.previous,
            "source": row.source,
            "effects": effects,
            "is_manual": row.is_manual,
        })
    return output


def save_manual_events(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("events", [])
    now = datetime.now(timezone.utc)
    saved = 0
    with Session(engine) as session:
        for row in rows:
            try:
                event_time = normalise_event_time(row.get("date"))
            except Exception:
                continue
            event_name = str(row.get("event", "")).strip()
            country = str(row.get("country", "")).strip() or "Unknown"
            currency = str(row.get("currency", "")).strip().upper() or "GLOBAL"
            if not event_name:
                continue
            event_id = str(row.get("event_id") or f"manual-{event_time.isoformat()}-{country}-{event_name}")[:120]
            effects = row.get("effects") or event_effects(event_name, currency)

            existing = session.get(EconomicEvent, event_id)
            if existing is None:
                existing = EconomicEvent(
                    event_id=event_id,
                    event_time=event_time,
                    country=country,
                    currency=currency,
                    event_name=event_name,
                    impact=str(row.get("impact", "Medium")),
                    actual=row.get("actual"),
                    forecast=row.get("forecast"),
                    previous=row.get("previous"),
                    source=str(row.get("source", "Manual")),
                    effects_json=json.dumps(effects),
                    is_manual=True,
                    updated_at=now,
                )
                session.add(existing)
            else:
                existing.event_time = event_time
                existing.country = country
                existing.currency = currency
                existing.event_name = event_name
                existing.impact = str(row.get("impact", "Medium"))
                existing.actual = row.get("actual")
                existing.forecast = row.get("forecast")
                existing.previous = row.get("previous")
                existing.source = str(row.get("source", "Manual"))
                existing.effects_json = json.dumps(effects)
                existing.updated_at = now
            saved += 1
        session.commit()
    return {"saved": saved}


def delete_manual_event(event_id: str) -> dict[str, Any]:
    with Session(engine) as session:
        event = session.get(EconomicEvent, event_id)
        if event is None:
            return {"deleted": False}
        session.delete(event)
        session.commit()
    return {"deleted": True}


async def combined_calendar() -> dict[str, Any]:
    manual = read_manual_events()
    provider = await trading_economics_calendar()
    provider_events = provider.get("events", []) if provider.get("configured") else []

    combined: dict[str, dict[str, Any]] = {}
    for event in provider_events + manual:
        key = str(event.get("event_id") or f"{event.get('date')}-{event.get('country')}-{event.get('event')}")
        if "effects" not in event:
            event["effects"] = event_effects(
                str(event.get("event", "")),
                str(event.get("currency") or event.get("country") or "GLOBAL"),
            )
        combined[key] = event

    events = sorted(combined.values(), key=lambda row: row.get("date") or "")
    return {
        "configured": True,
        "provider_connected": bool(provider.get("configured")),
        "manual_events": len(manual),
        "events": events,
        "message": (
            "Trading Economics calendar connected."
            if provider.get("configured")
            else "Using cloud-saved manual calendar entries. Trading Economics remains optional."
        ),
    }


def latest_market_brief(
    cot_payload: dict[str, Any],
    macro_payload: dict[str, Any],
    markets_payload: dict[str, Any],
    news_payload: dict[str, Any],
    calendar_payload: dict[str, Any],
) -> dict[str, Any]:
    currencies = cot_payload.get("currencies", {})
    relationships = {}
    for market, rows in currencies.items():
        if len(rows) < 2:
            continue
        first, last = rows[0], rows[-1]
        commercial = last["commercial_net"] - first["commercial_net"]
        noncommercial = last["noncommercial_net"] - first["noncommercial_net"]
        score = noncommercial - commercial
        if commercial < 0 and noncommercial > 0:
            label = "Bullish relationship"
        elif commercial > 0 and noncommercial < 0:
            label = "Bearish relationship"
        else:
            label = "Mixed relationship"
        relationships[market] = {
            "score": score,
            "label": label,
            "commercial_change": commercial,
            "noncommercial_change": noncommercial,
        }

    ranked = sorted(relationships.items(), key=lambda row: row[1]["score"], reverse=True)
    strongest = ranked[0] if ranked else None
    weakest = ranked[-1] if ranked else None

    market_regime = markets_payload.get("risk_regime") or "Unavailable"
    average_weekly = markets_payload.get("average_weekly_change_pct")

    summaries = macro_payload.get("summaries", {}) if macro_payload.get("configured") else {}
    us10y = summaries.get("US_10Y", {})
    real10y = summaries.get("REAL_10Y", {})

    news_data = news_payload.get("data", news_payload)
    today_summary = news_data.get("today_summary", {})
    high_news = today_summary.get("high_importance", 0)

    now = datetime.now(timezone.utc)
    upcoming = [
        event for event in calendar_payload.get("events", [])
        if event.get("date") and normalise_event_time(event["date"]) >= now
    ]
    upcoming.sort(key=lambda row: row["date"])
    next_high = next((event for event in upcoming if event.get("impact") == "High"), upcoming[0] if upcoming else None)

    evidence = []
    if strongest:
        evidence.append(f"Strongest COT relationship: {strongest[0]} — {strongest[1]['label']}.")
    if weakest:
        evidence.append(f"Weakest COT relationship: {weakest[0]} — {weakest[1]['label']}.")
    if market_regime != "Unavailable":
        evidence.append(
            f"Global equity proxy regime: {market_regime}"
            + (f" ({average_weekly:+.2f}% average weekly move)." if average_weekly is not None else ".")
        )
    if us10y.get("latest") is not None:
        evidence.append(
            f"US 10-year yield: {us10y['latest']:.3f}% with weekly change {us10y.get('weekly_change', 0):+.3f} pp."
        )
    if real10y.get("latest") is not None:
        evidence.append(
            f"US 10-year real yield: {real10y['latest']:.3f}% with weekly change {real10y.get('weekly_change', 0):+.3f} pp."
        )
    evidence.append(f"High-importance financial stories in the latest window: {high_news}.")
    if next_high:
        evidence.append(
            f"Next major calendar event: {next_high.get('event')} — {next_high.get('country')} at {next_high.get('date')}."
        )

    preferred_pair = None
    if strongest and weakest and strongest[0] != weakest[0]:
        preferred_pair = f"{strongest[0]}/{weakest[0]}"

    return {
        "generated_at": now.isoformat(),
        "market_regime": market_regime,
        "strongest_relationship": strongest,
        "weakest_relationship": weakest,
        "preferred_pair_for_review": preferred_pair,
        "next_high_impact_event": next_high,
        "high_importance_news": high_news,
        "us_10y": us10y,
        "real_10y": real10y,
        "evidence": evidence,
        "trade_confirmation": "Not confirmed — wait for TradingView HTF/MTF/LTF alignment.",
    }



def normalise_pair(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z]", "", value or "").upper()
    if len(cleaned) == 6:
        return f"{cleaned[:3]}/{cleaned[3:]}"
    if "/" in (value or ""):
        parts = [part.strip().upper() for part in value.split("/", 1)]
        if len(parts) == 2 and all(len(part) == 3 for part in parts):
            return f"{parts[0]}/{parts[1]}"
    raise ValueError("Pair must contain two three-letter currencies, for example GBP/JPY.")


def technical_setup_state(row: dict[str, Any]) -> dict[str, Any]:
    direction = str(row.get("direction", "Long")).title()
    htf_direction = str(row.get("htf_direction", "Neutral")).title()
    expected_htf = "Bullish" if direction == "Long" else "Bearish"
    htf_aligned = htf_direction == expected_htf

    checks = {
        "HTF aligned": htf_aligned,
        "MTF CHoCH": bool(row.get("mtf_choch")),
        "LTF CHoCH": bool(row.get("ltf_choch")),
        "BOS confirmed": bool(row.get("bos_confirmed")),
        "Pullback ready": bool(row.get("pullback_ready")),
    }
    completed = sum(1 for value in checks.values() if value)
    technical_score = completed * 20

    if bool(row.get("invalidated")):
        status = "Invalidated"
    elif completed == 5:
        status = "Ready"
    elif htf_aligned and completed >= 2:
        status = "Watch"
    else:
        status = "Not Ready"

    waiting_for = [label for label, value in checks.items() if not value]
    return {
        "status": status,
        "technical_score": technical_score,
        "htf_aligned": htf_aligned,
        "checks": checks,
        "waiting_for": waiting_for,
    }


def serialise_technical_setup(row: TechnicalSetup) -> dict[str, Any]:
    payload = {
        "pair": row.pair,
        "direction": row.direction,
        "htf_direction": row.htf_direction,
        "mtf_choch": row.mtf_choch,
        "ltf_choch": row.ltf_choch,
        "bos_confirmed": row.bos_confirmed,
        "pullback_ready": row.pullback_ready,
        "invalidated": row.invalidated,
        "notes": row.notes,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
    return {**payload, **technical_setup_state(payload)}


def read_technical_setups() -> list[dict[str, Any]]:
    with Session(engine) as session:
        rows = session.scalars(
            select(TechnicalSetup).order_by(TechnicalSetup.updated_at.desc())
        ).all()
    return [serialise_technical_setup(row) for row in rows]


def save_technical_setup(payload: dict[str, Any]) -> dict[str, Any]:
    pair = normalise_pair(str(payload.get("pair", "")))
    direction = str(payload.get("direction", "Long")).title()
    if direction not in {"Long", "Short"}:
        raise ValueError("Direction must be Long or Short.")

    htf_direction = str(payload.get("htf_direction", "Neutral")).title()
    if htf_direction not in {"Bullish", "Bearish", "Neutral"}:
        raise ValueError("HTF direction must be Bullish, Bearish or Neutral.")

    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        setup = session.get(TechnicalSetup, pair)
        if setup is None:
            setup = TechnicalSetup(
                pair=pair,
                direction=direction,
                htf_direction=htf_direction,
                mtf_choch=bool(payload.get("mtf_choch")),
                ltf_choch=bool(payload.get("ltf_choch")),
                bos_confirmed=bool(payload.get("bos_confirmed")),
                pullback_ready=bool(payload.get("pullback_ready")),
                invalidated=bool(payload.get("invalidated")),
                notes=str(payload.get("notes", "")),
                updated_at=now,
            )
            session.add(setup)
        else:
            setup.direction = direction
            setup.htf_direction = htf_direction
            setup.mtf_choch = bool(payload.get("mtf_choch"))
            setup.ltf_choch = bool(payload.get("ltf_choch"))
            setup.bos_confirmed = bool(payload.get("bos_confirmed"))
            setup.pullback_ready = bool(payload.get("pullback_ready"))
            setup.invalidated = bool(payload.get("invalidated"))
            setup.notes = str(payload.get("notes", ""))
            setup.updated_at = now
        session.commit()
        session.refresh(setup)
        return serialise_technical_setup(setup)


def delete_technical_setup(pair: str) -> dict[str, Any]:
    normalised = normalise_pair(pair)
    with Session(engine) as session:
        setup = session.get(TechnicalSetup, normalised)
        if setup is None:
            return {"deleted": False}
        session.delete(setup)
        session.commit()
    return {"deleted": True}



def default_profile_values() -> dict[str, Any]:
    return {
        "average_5y": 0.0,
        "average_10y": 0.0,
        "average_20y": 0.0,
        "win_rate": 50.0,
        "sample_years": 0,
        "notes": "",
    }


def read_seasonality_profiles() -> dict[str, list[dict[str, Any]]]:
    profiles: dict[str, list[dict[str, Any]]] = {}

    with Session(engine) as session:
        rows = session.scalars(
            select(SeasonalityProfile).order_by(
                SeasonalityProfile.market.asc(),
                SeasonalityProfile.month_number.asc(),
            )
        ).all()

        legacy_rows = session.scalars(
            select(SeasonalityValue).order_by(
                SeasonalityValue.market.asc(),
                SeasonalityValue.month_number.asc(),
            )
        ).all()

    for row in rows:
        profiles.setdefault(
            row.market,
            [
                {"month_number": month, **default_profile_values()}
                for month in range(1, 13)
            ],
        )
        profiles[row.market][row.month_number - 1] = {
            "month_number": row.month_number,
            "average_5y": row.average_5y,
            "average_10y": row.average_10y,
            "average_20y": row.average_20y,
            "win_rate": row.win_rate,
            "sample_years": row.sample_years,
            "notes": row.notes,
        }

    for row in legacy_rows:
        profiles.setdefault(
            row.market,
            [
                {"month_number": month, **default_profile_values()}
                for month in range(1, 13)
            ],
        )
        current = profiles[row.market][row.month_number - 1]
        if (
            current["average_5y"] == 0
            and current["average_10y"] == 0
            and current["average_20y"] == 0
        ):
            current["average_10y"] = row.tendency

    return profiles


def save_seasonality_profiles(payload: dict[str, Any]) -> dict[str, Any]:
    markets = payload.get("markets", [])
    now = datetime.now(timezone.utc)
    saved = 0

    with Session(engine) as session:
        for market_row in markets:
            market = str(market_row.get("market", "")).upper().strip()
            months = market_row.get("months", [])
            if not market:
                continue

            for index, month_row in enumerate(months[:12], start=1):
                month_number = int(month_row.get("month_number") or index)
                if not 1 <= month_number <= 12:
                    continue

                profile = session.get(
                    SeasonalityProfile,
                    {"market": market, "month_number": month_number},
                )
                if profile is None:
                    profile = SeasonalityProfile(
                        market=market,
                        month_number=month_number,
                        average_5y=0.0,
                        average_10y=0.0,
                        average_20y=0.0,
                        win_rate=50.0,
                        sample_years=0,
                        notes="",
                        updated_at=now,
                    )
                    session.add(profile)

                profile.average_5y = float(month_row.get("average_5y") or 0)
                profile.average_10y = float(month_row.get("average_10y") or 0)
                profile.average_20y = float(month_row.get("average_20y") or 0)
                profile.win_rate = max(
                    0.0,
                    min(100.0, float(month_row.get("win_rate") or 0)),
                )
                profile.sample_years = max(
                    0,
                    int(float(month_row.get("sample_years") or 0)),
                )
                profile.notes = str(month_row.get("notes") or "")
                profile.updated_at = now
                saved += 1

        session.commit()

    return {"saved": saved, "markets": len(markets)}


def seasonality_horizon_summary(
    market: str,
    metric: str = "average_10y",
) -> dict[str, Any]:
    profiles = read_seasonality_profiles()
    months = profiles.get(market.upper(), [])
    if not months:
        return {
            "market": market.upper(),
            "available": False,
            "metric": metric,
        }

    current_month = datetime.now(timezone.utc).month
    current = months[current_month - 1]
    current_value = float(current.get(metric) or 0)
    next_six = [
        float(months[(current_month - 1 + offset) % 12].get(metric) or 0)
        for offset in range(6)
    ]
    full_year = [float(row.get(metric) or 0) for row in months]

    win_rate = float(current.get("win_rate") or 0)
    sample_years = int(current.get("sample_years") or 0)
    reliability = round(
        max(
            0,
            min(
                100,
                (abs(win_rate - 50) * 1.5)
                + min(30, sample_years * 1.5),
            ),
        )
    )

    return {
        "market": market.upper(),
        "available": True,
        "metric": metric,
        "current_month": current_month,
        "current_value": round(current_value, 3),
        "six_month_value": round(sum(next_six) / 6, 3),
        "yearly_value": round(sum(full_year) / 12, 3),
        "win_rate": round(win_rate, 1),
        "sample_years": sample_years,
        "reliability": reliability,
        "months": months,
        "methodology": (
            "Daily and weekly views use the current monthly seasonal backdrop. "
            "Six-month and yearly views average the corresponding monthly profile."
        ),
    }



_auto_seasonality_lock = asyncio.Lock()


def _average(values: list[float]) -> float:
    return round(sum(values) / len(values), 4) if values else 0.0


def _monthly_returns_from_values(
    values: list[dict[str, Any]],
    invert: bool,
) -> dict[int, list[tuple[int, float]]]:
    parsed: list[tuple[date, float]] = []
    for row in values:
        raw_date = str(row.get("datetime", ""))[:10]
        raw_close = row.get("close")
        if not raw_date or raw_close in (None, ""):
            continue
        try:
            dt = date.fromisoformat(raw_date)
            close = float(raw_close)
            if close <= 0:
                continue
            adjusted = 1.0 / close if invert else close
            parsed.append((dt, adjusted))
        except (TypeError, ValueError):
            continue

    parsed.sort(key=lambda item: item[0])
    returns: dict[int, list[tuple[int, float]]] = {month: [] for month in range(1, 13)}
    for index in range(1, len(parsed)):
        current_date, current_close = parsed[index]
        _, previous_close = parsed[index - 1]
        if previous_close == 0:
            continue
        monthly_return = (current_close / previous_close - 1.0) * 100.0
        returns[current_date.month].append((current_date.year, monthly_return))
    return returns


def _window_values(
    observations: list[tuple[int, float]],
    years: int,
) -> list[float]:
    if not observations:
        return []
    latest_year = max(year for year, _ in observations)
    cutoff = latest_year - years + 1
    return [value for year, value in observations if year >= cutoff]


def _upsert_auto_seasonality(
    market: str,
    returns_by_month: dict[int, list[tuple[int, float]]],
    source_label: str,
    symbol: str,
) -> None:
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        for month_number in range(1, 13):
            observations = returns_by_month.get(month_number, [])
            values_5 = _window_values(observations, 5)
            values_10 = _window_values(observations, 10)
            values_20 = _window_values(observations, 20)
            values_for_win_rate = values_20 or values_10 or values_5

            profile = session.get(
                SeasonalityProfile,
                {"market": market, "month_number": month_number},
            )
            if profile is None:
                profile = SeasonalityProfile(
                    market=market,
                    month_number=month_number,
                    average_5y=0.0,
                    average_10y=0.0,
                    average_20y=0.0,
                    win_rate=50.0,
                    sample_years=0,
                    notes="",
                    updated_at=now,
                )
                session.add(profile)

            profile.average_5y = _average(values_5)
            profile.average_10y = _average(values_10)
            profile.average_20y = _average(values_20)
            profile.win_rate = (
                round(
                    sum(1 for value in values_for_win_rate if value > 0)
                    / len(values_for_win_rate)
                    * 100,
                    1,
                )
                if values_for_win_rate
                else 50.0
            )
            profile.sample_years = len(values_for_win_rate)
            profile.notes = (
                f"Automatic: {source_label} ({symbol}); monthly close-to-close returns. "
                f"Updated {now.date().isoformat()}."
            )
            profile.updated_at = now

        session.commit()


async def _fetch_monthly_history(
    client: httpx.AsyncClient,
    symbol: str,
    api_key: str,
) -> dict[str, Any]:
    response = await client.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": symbol,
            "interval": "1month",
            "outputsize": 300,
            "apikey": api_key,
            "format": "JSON",
            "order": "ASC",
        },
    )
    if response.status_code == 429:
        return {
            "status": "error",
            "code": 429,
            "message": "Twelve Data free-plan rate limit reached.",
        }
    response.raise_for_status()
    return response.json()


async def refresh_auto_seasonality(group: str) -> dict[str, Any]:
    if group not in AUTO_SEASONALITY_GROUPS:
        return {
            "status": "error",
            "message": f"Unknown group: {group}",
            "allowed_groups": list(AUTO_SEASONALITY_GROUPS),
        }

    api_key = os.getenv("TWELVE_DATA_API_KEY")
    if not api_key:
        return {
            "status": "not_configured",
            "message": "Add TWELVE_DATA_API_KEY in Render.",
            "group": group,
        }

    if _auto_seasonality_lock.locked():
        return {
            "status": "already_running",
            "message": "Another automatic seasonality refresh is already running.",
            "group": group,
        }

    definitions = AUTO_SEASONALITY_GROUPS[group]
    results = []
    async with _auto_seasonality_lock:
        async with httpx.AsyncClient(timeout=75, follow_redirects=True) as client:
            for market, definition in definitions.items():
                candidate_symbols = definition.get("symbols") or [definition["symbol"]]
                selected_symbol = None
                selected_payload = None
                attempt_errors = []

                try:
                    for symbol in candidate_symbols:
                        payload = await _fetch_monthly_history(client, symbol, api_key)
                        if payload.get("status") == "error":
                            attempt_errors.append(
                                {
                                    "symbol": symbol,
                                    "message": payload.get("message", "Provider error"),
                                }
                            )
                            # A 429 means the provider allowance has been reached;
                            # do not waste further attempts.
                            if payload.get("code") == 429:
                                break
                            continue

                        values = payload.get("values", [])
                        if len(values) < 24:
                            attempt_errors.append(
                                {
                                    "symbol": symbol,
                                    "message": f"Only {len(values)} monthly observations",
                                }
                            )
                            continue

                        selected_symbol = symbol
                        selected_payload = payload
                        break

                    if selected_payload is None or selected_symbol is None:
                        results.append(
                            {
                                "market": market,
                                "status": "error",
                                "attempts": attempt_errors,
                                "message": "No supported Dollar Index symbol returned sufficient history.",
                            }
                        )
                        continue

                    values = selected_payload.get("values", [])
                    returns_by_month = _monthly_returns_from_values(
                        values,
                        bool(definition.get("invert")),
                    )

                    source_label = definition["label"]
                    is_fallback = selected_symbol == "UUP"
                    if is_fallback:
                        source_label = definition.get(
                            "fallback_label",
                            "UUP US Dollar Index ETF proxy",
                        )

                    _upsert_auto_seasonality(
                        market=market,
                        returns_by_month=returns_by_month,
                        source_label=source_label,
                        symbol=selected_symbol,
                    )
                    current_year_by_month = {
                        month_number: _current_year_month_return(
                            values,
                            bool(definition.get("invert")),
                            month_number,
                        )
                        for month_number in range(1, 13)
                    }
                    _upsert_backtest_metrics(
                        market=market,
                        returns_by_month=returns_by_month,
                        current_year_by_month=current_year_by_month,
                    )
                    results.append(
                        {
                            "market": market,
                            "symbol": selected_symbol,
                            "status": "updated",
                            "observations": len(values),
                            "official_index_symbol": not is_fallback,
                            "fallback_used": is_fallback,
                            "attempts": attempt_errors,
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Automatic seasonality failed for %s", market)
                    results.append(
                        {
                            "market": market,
                            "status": "error",
                            "message": str(exc),
                            "attempts": attempt_errors,
                        }
                    )

    completed_at = datetime.now(timezone.utc)
    with Session(engine) as session:
        status = session.get(AppStatus, f"auto_seasonality_{group}")
        value = json.dumps(
            {
                "group": group,
                "completed_at": completed_at.isoformat(),
                "results": results,
            }
        )
        if status is None:
            session.add(
                AppStatus(
                    key=f"auto_seasonality_{group}",
                    value=value,
                )
            )
        else:
            status.value = value
        session.commit()

    updated = sum(1 for row in results if row["status"] == "updated")
    return {
        "status": "complete",
        "group": group,
        "updated": updated,
        "total": len(results),
        "results": results,
        "completed_at": completed_at.isoformat(),
    }


def auto_seasonality_status() -> dict[str, Any]:
    groups = {}
    with Session(engine) as session:
        for group in AUTO_SEASONALITY_GROUPS:
            row = session.get(AppStatus, f"auto_seasonality_{group}")
            if row is None:
                groups[group] = None
            else:
                try:
                    groups[group] = json.loads(row.value)
                except json.JSONDecodeError:
                    groups[group] = {"raw": row.value}

    return {
        "configured": bool(os.getenv("TWELVE_DATA_API_KEY")),
        "groups": groups,
        "universe": AUTO_SEASONALITY_GROUPS,
        "schedule": {
            "core": "First day of each month at 03:10 UTC",
            "assets": "First day of each month at 03:20 UTC",
            "dxy": "First day of each month at 03:30 UTC",
        },
        "methodology": (
            "Monthly close-to-close returns are grouped by calendar month. "
            "The 5-, 10- and 20-year averages use the latest available years. "
            "Win rate uses the 20-year window when available."
        ),
    }



def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[midpoint], 4)
    return round((ordered[midpoint - 1] + ordered[midpoint]) / 2, 4)


def _std_dev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return round(variance ** 0.5, 4)


def _max_drawdown_from_monthly_returns(values: list[float]) -> float:
    if not values:
        return 0.0
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for value in values:
        equity *= 1.0 + value / 100.0
        peak = max(peak, equity)
        drawdown = (equity / peak - 1.0) * 100.0
        worst = min(worst, drawdown)
    return round(worst, 4)


def _reliability_grade(
    win_rate: float,
    sample_years: int,
    volatility: float,
) -> str:
    score = 0
    score += min(40, abs(win_rate - 50) * 1.6)
    score += min(35, sample_years * 1.75)
    score += max(0, 25 - min(25, volatility * 4))
    if score >= 85:
        return "A+"
    if score >= 75:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    return "D"


def _current_year_month_return(
    values: list[dict[str, Any]],
    invert: bool,
    month_number: int,
) -> float | None:
    parsed: list[tuple[date, float]] = []
    for row in values:
        raw_date = str(row.get("datetime", ""))[:10]
        raw_close = row.get("close")
        if not raw_date or raw_close in (None, ""):
            continue
        try:
            dt = date.fromisoformat(raw_date)
            close = float(raw_close)
            if close <= 0:
                continue
            adjusted = 1.0 / close if invert else close
            parsed.append((dt, adjusted))
        except (TypeError, ValueError):
            continue

    parsed.sort(key=lambda item: item[0])
    current_year = datetime.now(timezone.utc).year
    for index in range(1, len(parsed)):
        current_date, current_close = parsed[index]
        _, previous_close = parsed[index - 1]
        if (
            current_date.year == current_year
            and current_date.month == month_number
            and previous_close != 0
        ):
            return round((current_close / previous_close - 1.0) * 100.0, 4)
    return None


def _upsert_backtest_metrics(
    market: str,
    returns_by_month: dict[int, list[tuple[int, float]]],
    current_year_by_month: dict[int, float | None],
) -> None:
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        for month_number in range(1, 13):
            observations = returns_by_month.get(month_number, [])
            values = [value for _, value in observations]
            win_rate = (
                sum(1 for value in values if value > 0) / len(values) * 100.0
                if values
                else 50.0
            )
            volatility = _std_dev(values)
            sample_years = len(values)
            median_return = _median(values)
            current_year_return = current_year_by_month.get(month_number)
            divergence = (
                round(current_year_return - median_return, 4)
                if current_year_return is not None
                else None
            )

            record = session.get(
                SeasonalityBacktest,
                {"market": market, "month_number": month_number},
            )
            if record is None:
                record = SeasonalityBacktest(
                    market=market,
                    month_number=month_number,
                    median_return=0.0,
                    volatility=0.0,
                    best_return=0.0,
                    worst_return=0.0,
                    max_drawdown=0.0,
                    positive_years=0,
                    negative_years=0,
                    reliability_grade="N/A",
                    current_year_return=None,
                    divergence=None,
                    updated_at=now,
                )
                session.add(record)

            record.median_return = median_return
            record.volatility = volatility
            record.best_return = round(max(values), 4) if values else 0.0
            record.worst_return = round(min(values), 4) if values else 0.0
            record.max_drawdown = _max_drawdown_from_monthly_returns(values)
            record.positive_years = sum(1 for value in values if value > 0)
            record.negative_years = sum(1 for value in values if value <= 0)
            record.reliability_grade = _reliability_grade(
                win_rate=win_rate,
                sample_years=sample_years,
                volatility=volatility,
            )
            record.current_year_return = current_year_return
            record.divergence = divergence
            record.updated_at = now

        session.commit()


def read_backtest_metrics() -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    with Session(engine) as session:
        rows = session.scalars(
            select(SeasonalityBacktest).order_by(
                SeasonalityBacktest.market.asc(),
                SeasonalityBacktest.month_number.asc(),
            )
        ).all()

    for row in rows:
        output.setdefault(row.market, [])
        output[row.market].append(
            {
                "month_number": row.month_number,
                "median_return": row.median_return,
                "volatility": row.volatility,
                "best_return": row.best_return,
                "worst_return": row.worst_return,
                "max_drawdown": row.max_drawdown,
                "positive_years": row.positive_years,
                "negative_years": row.negative_years,
                "reliability_grade": row.reliability_grade,
                "current_year_return": row.current_year_return,
                "divergence": row.divergence,
            }
        )
    return output


def strongest_seasonal_windows(
    market: str,
    metric: str = "average_10y",
) -> list[dict[str, Any]]:
    profiles = read_seasonality_profiles()
    months = profiles.get(market.upper(), [])
    if len(months) != 12:
        return []

    windows = []
    for start_index in range(12):
        for length in (2, 3):
            values = [
                float(months[(start_index + offset) % 12].get(metric) or 0)
                for offset in range(length)
            ]
            average_return = sum(values) / length
            windows.append(
                {
                    "start_month": start_index + 1,
                    "end_month": ((start_index + length - 1) % 12) + 1,
                    "length_months": length,
                    "average_return": round(average_return, 4),
                }
            )

    windows.sort(key=lambda row: row["average_return"], reverse=True)
    return windows[:6]


def direct_pair_seasonality(
    base: str,
    quote: str,
    metric: str = "average_10y",
) -> dict[str, Any]:
    profiles = read_seasonality_profiles()
    pair_key = f"{base.upper()}{quote.upper()}"
    direct = profiles.get(pair_key)
    source = "direct"

    if direct is None:
        base_rows = profiles.get(base.upper())
        quote_rows = profiles.get(quote.upper())
        if not base_rows or not quote_rows:
            return {
                "available": False,
                "pair": f"{base.upper()}/{quote.upper()}",
            }
        direct = []
        for month_number in range(1, 13):
            base_value = float(base_rows[month_number - 1].get(metric) or 0)
            quote_value = float(quote_rows[month_number - 1].get(metric) or 0)
            direct.append(
                {
                    "month_number": month_number,
                    metric: round(base_value - quote_value, 4),
                }
            )
        source = "derived_from_currency_profiles"

    return {
        "available": True,
        "pair": f"{base.upper()}/{quote.upper()}",
        "source": source,
        "metric": metric,
        "months": direct,
    }



_cot_overlay_cache: dict[str, dict[str, Any]] = {}
_cot_overlay_cache_time: dict[str, datetime] = {}
_cot_overlay_lock = asyncio.Lock()
COT_OVERLAY_TTL = timedelta(hours=6)


def _prepare_overlay(values: list[dict[str, Any]], invert: bool = False) -> list[dict[str, Any]]:
    output = []
    for row in values:
        try:
            o, h, l, c = [float(row[key]) for key in ("open", "high", "low", "close")]
            if min(o, h, l, c) <= 0:
                continue
            if invert:
                o, c, h, l = 1/o, 1/c, 1/l, 1/h
            output.append({
                "date": str(row.get("datetime", ""))[:10],
                "open": o, "high": h, "low": l, "close": c,
            })
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            continue
    return sorted(output, key=lambda item: item["date"])


async def cot_price_overlay(market: str) -> dict[str, Any]:

    market = market.upper()
    
    if market == "USD":
        synthetic = await synthetic_dxy_history()
        if synthetic.get("available") and synthetic.get("candles"):
            return {
                "configured": True,
                "market": "USD",
                "symbol": synthetic["symbol"],
                "source_label": synthetic["source_label"],
                "fallback_used": True,
                "synthetic": True,
                "candles": synthetic["candles"],
                "providers": synthetic.get("providers", {}),
                "weights": synthetic.get("weights", {}),
                "message": synthetic["message"],
            }
    
        uup = await uup_weekly_fallback()
        if uup.get("available") and uup.get("candles"):
            return {
                "configured": True,
                "market": "USD",
                "symbol": uup["symbol"],
                "source_label": uup["source_label"],
                "fallback_used": True,
                "synthetic": False,
                "candles": uup["candles"],
                "message": uup["message"],
            }
    definition = COT_PRICE_OVERLAY_SYMBOLS.get(market)
    if not definition:
        return {"configured": False, "candles": [], "message": "No overlay mapping exists."}

    now = datetime.now(timezone.utc)
    cached = _cot_overlay_cache.get(market)
    cached_at = _cot_overlay_cache_time.get(market)
    if cached and cached_at and now - cached_at < COT_OVERLAY_TTL:
        return cached

    key = os.getenv("TWELVE_DATA_API_KEY")
    if not key:
        return {"configured": False, "candles": [], "message": "TWELVE_DATA_API_KEY is not configured."}

    async with _cot_overlay_lock:
        attempts = []
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            for index, symbol in enumerate(definition["symbols"]):
                try:
                    response = await client.get(
                        "https://api.twelvedata.com/time_series",
                        params={
                            "symbol": symbol,
                            "interval": "1week",
                            "outputsize": 260,
                            "apikey": key,
                            "format": "JSON",
                            "order": "ASC",
                        },
                    )
                    if response.status_code == 429:
                        return {"configured": True, "candles": [], "message": "Twelve Data rate limit reached."}
                    response.raise_for_status()
                    payload = response.json()
                    if payload.get("status") == "error":
                        attempts.append({"symbol": symbol, "message": payload.get("message", "Provider error")})
                        continue
                    values = payload.get("values", [])
                    fallback = index == len(definition["symbols"]) - 1
                    candles = _prepare_overlay(values, bool(fallback and definition.get("invert_fallback")))
                    if len(candles) < 20:
                        attempts.append({"symbol": symbol, "message": "Insufficient weekly OHLC history"})
                        continue
                    result = {
                        "configured": True,
                        "market": market,
                        "symbol": symbol,
                        "source_label": definition.get("fallback_label") if fallback else definition["label"],
                        "fallback_used": fallback,
                        "candles": candles,
                        "attempts": attempts,
                    }
                    _cot_overlay_cache[market] = result
                    _cot_overlay_cache_time[market] = datetime.now(timezone.utc)
                    return result
                except Exception as exc:
                    attempts.append({"symbol": symbol, "message": str(exc)})

    return {"configured": True, "candles": [], "attempts": attempts, "message": "No supported price series was returned."}



_trading212_cache: dict[str, Any] = {}
_trading212_cache_time: datetime | None = None
_trading212_lock = asyncio.Lock()
TRADING212_CACHE_TTL = timedelta(minutes=2)


def trading212_configuration() -> dict[str, Any]:
    environment = os.getenv("TRADING212_ENVIRONMENT", "live").strip().lower()
    if environment not in {"live", "demo"}:
        environment = "live"
    return {
        "api_key": os.getenv("TRADING212_API_KEY"),
        "api_secret": os.getenv("TRADING212_API_SECRET"),
        "environment": environment,
        "base_url": (
            "https://demo.trading212.com/api/v0"
            if environment == "demo"
            else "https://live.trading212.com/api/v0"
        ),
    }


async def _t212_get(
    client: httpx.AsyncClient,
    path: str,
    config: dict[str, Any],
) -> Any:
    try:
        response = await client.get(
            f"{config['base_url']}{path}",
            auth=(config["api_key"], config["api_secret"]),
            headers={"Accept": "application/json"},
        )
    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=504,
            detail={"code": "trading212_timeout", "message": "Trading 212 timed out."},
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail={"code": "trading212_network_error", "message": "The dashboard could not reach Trading 212."},
        ) from exc

    errors = {
        401: {
            "code": "trading212_unauthorised",
            "message": "Trading 212 rejected the API key or secret. Check the matching key pair and live/demo environment.",
        },
        403: {
            "code": "trading212_permission_denied",
            "message": "The API key does not have permission to read this account or its positions.",
        },
        429: {
            "code": "trading212_rate_limited",
            "message": "Trading 212 rate limit reached. Wait briefly and try again.",
        },
    }
    if response.status_code in errors:
        raise HTTPException(status_code=response.status_code, detail=errors[response.status_code])
    if response.status_code >= 500:
        raise HTTPException(
            status_code=502,
            detail={"code": "trading212_upstream_error", "message": "Trading 212 returned a server error."},
        )
    if not response.is_success:
        raise HTTPException(
            status_code=response.status_code,
            detail={
                "code": "trading212_request_failed",
                "message": f"Trading 212 returned HTTP {response.status_code}.",
            },
        )
    try:
        return response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail={"code": "trading212_invalid_response", "message": "Trading 212 returned invalid JSON."},
        ) from exc


async def _t212_dividends(
    client: httpx.AsyncClient,
    config: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    try:
        response = await client.get(
            f"{config['base_url']}/equity/history/dividends",
            params={"limit": 50},
            auth=(config["api_key"], config["api_secret"]),
            headers={"Accept": "application/json"},
        )
    except httpx.RequestError:
        return [], {"code": "dividend_history_unavailable", "message": "Dividend history could not be reached."}

    if response.status_code in {401, 403}:
        return [], {"code": "dividend_permission_missing", "message": "Dividend history permission is not enabled."}
    if response.status_code == 429:
        return [], {"code": "dividend_rate_limited", "message": "Dividend history is temporarily rate limited."}
    if not response.is_success:
        return [], {"code": "dividend_history_error", "message": f"Dividend history returned HTTP {response.status_code}."}
    try:
        return response.json().get("items", []), None
    except ValueError:
        return [], {"code": "dividend_invalid_response", "message": "Dividend history returned invalid JSON."}


def _t212_position(row: dict[str, Any]) -> dict[str, Any]:
    instrument = row.get("instrument") or {}
    wallet = row.get("walletImpact") or {}
    quantity = float(row.get("quantity") or 0)
    average_price = float(row.get("averagePricePaid") or 0)
    current_price = float(row.get("currentPrice") or 0)

    current_value = wallet.get("currentValue")
    if current_value is None:
        current_value = wallet.get("totalValue")
    if current_value is None:
        current_value = quantity * current_price

    invested = wallet.get("investedValue")
    if invested is None:
        invested = wallet.get("totalCost")
    if invested is None:
        invested = quantity * average_price

    profit = wallet.get("unrealizedProfitLoss")
    if profit is None:
        profit = wallet.get("result")
    if profit is None:
        profit = float(current_value or 0) - float(invested or 0)

    current_value = float(current_value or 0)
    invested = float(invested or 0)
    profit = float(profit or 0)

    return {
        "ticker": instrument.get("ticker") or row.get("ticker") or "Unknown",
        "name": instrument.get("name") or instrument.get("shortName") or instrument.get("ticker") or "Unknown instrument",
        "currency": instrument.get("currencyCode") or instrument.get("currency"),
        "quantity": quantity,
        "average_price": average_price,
        "current_price": current_price,
        "current_value": round(current_value, 2),
        "invested_value": round(invested, 2),
        "profit_loss": round(profit, 2),
        "profit_loss_pct": round(profit / invested * 100 if invested else 0, 2),
    }



def _first_number(*values: Any, default: float = 0.0) -> float:
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.replace(",", "").strip())
            except ValueError:
                continue
        if isinstance(value, dict):
            for key in (
                "total",
                "available",
                "free",
                "value",
                "amount",
                "cash",
                "result",
                "current",
                "currentValue",
                "totalValue",
            ):
                if key in value:
                    nested = _first_number(value.get(key), default=None)
                    if nested is not None:
                        return nested
    return default


def _first_text(*values: Any, default: str = "") -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for key in ("code", "currency", "currencyCode", "primaryCurrency"):
                nested = value.get(key)
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
    return default


async def trading212_portfolio(force: bool = False) -> dict[str, Any]:
    global _trading212_cache, _trading212_cache_time

    config = trading212_configuration()
    if not config["api_key"] or not config["api_secret"]:
        return {
            "configured": False,
            "connected": False,
            "environment": config["environment"],
            "message": "Add TRADING212_API_KEY and TRADING212_API_SECRET in Render.",
            "summary": {},
            "positions": [],
            "allocation": [],
            "dividends": [],
        }

    now = datetime.now(timezone.utc)
    if (
        not force
        and _trading212_cache
        and _trading212_cache_time
        and now - _trading212_cache_time < TRADING212_CACHE_TTL
    ):
        return _trading212_cache

    async with _trading212_lock:
        async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
            summary_raw, positions_raw = await asyncio.gather(
                _t212_get(client, "/equity/account/summary", config),
                _t212_get(client, "/equity/positions", config),
            )
            dividends, dividend_warning = await _t212_dividends(client, config)

        positions = [_t212_position(row) for row in positions_raw]
        positions.sort(key=lambda row: row["current_value"], reverse=True)

        positions_value = sum(row["current_value"] for row in positions)
        invested = sum(row["invested_value"] for row in positions)
        profit = sum(row["profit_loss"] for row in positions)
        cash_payload = summary_raw.get("cash")
        cash = _first_number(
            cash_payload,
            summary_raw.get("freeFunds"),
            summary_raw.get("availableCash"),
            summary_raw.get("cashAvailable"),
            default=0.0,
        )

        total_value = _first_number(
            summary_raw.get("totalValue"),
            summary_raw.get("accountValue"),
            summary_raw.get("portfolioValue"),
            summary_raw.get("total"),
            default=cash + positions_value,
        )

        currency = _first_text(
            summary_raw.get("currency"),
            summary_raw.get("currencyCode"),
            summary_raw.get("primaryCurrency"),
            cash_payload,
            default="GBP",
        )

        allocation = [
            {
                "ticker": row["ticker"],
                "name": row["name"],
                "value": row["current_value"],
                "weight_pct": round(row["current_value"] / positions_value * 100, 2)
                if positions_value else 0,
            }
            for row in positions
        ]

        result = {
            "configured": True,
            "connected": True,
            "read_only": True,
            "environment": config["environment"],
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "currency": currency,
                "cash": round(cash, 2),
                "positions_value": round(positions_value, 2),
                "total_value": round(total_value, 2),
                "invested_value": round(invested, 2),
                "unrealised_profit_loss": round(profit, 2),
                "unrealised_profit_loss_pct": round(profit / invested * 100 if invested else 0, 2),
                "position_count": len(positions),
            },
            "positions": positions,
            "allocation": allocation,
            "winners": sorted(positions, key=lambda row: row["profit_loss_pct"], reverse=True)[:5],
            "losers": sorted(positions, key=lambda row: row["profit_loss_pct"])[:5],
            "dividends": dividends[:20],
            "diagnostics": {
                "environment": config["environment"],
                "account_summary": "ok",
                "positions": "ok",
                "dividends": "warning" if dividend_warning else "ok",
                "summary_shape": {
                    "cash_type": type(summary_raw.get("cash")).__name__,
                    "top_level_keys": sorted(summary_raw.keys()),
                },
                "warnings": [dividend_warning] if dividend_warning else [],
            },
            "message": "Connected using the official Trading 212 read-only API.",
        }
        _trading212_cache = result
        _trading212_cache_time = datetime.now(timezone.utc)
        return result



MARKET_CACHE_TTL = timedelta(minutes=20)
DAILY_CACHE_TTL = timedelta(hours=8)

_market_engine_cache: dict[str, dict[str, Any]] = {}
_market_engine_cache_time: dict[str, datetime] = {}
_market_engine_lock = asyncio.Lock()

STOOQ_FALLBACK_SYMBOLS: dict[str, str] = {
    "EWU": "ewu.us", "SPY": "spy.us", "QQQ": "qqq.us", "DIA": "dia.us",
    "IWM": "iwm.us", "EWG": "ewg.us", "EWQ": "ewq.us", "VGK": "vgk.us",
    "EWJ": "ewj.us", "EWH": "ewh.us", "FXI": "fxi.us", "EWA": "ewa.us",
    "EWC": "ewc.us", "INDA": "inda.us", "EWZ": "ewz.us", "UUP": "uup.us",
    "GLD": "gld.us", "SLV": "slv.us",
}


def _engine_cache_get(key: str, ttl: timedelta) -> dict[str, Any] | None:
    timestamp = _market_engine_cache_time.get(key)
    if timestamp and datetime.now(timezone.utc) - timestamp < ttl:
        return _market_engine_cache.get(key)
    return None


def _engine_cache_set(key: str, value: dict[str, Any]) -> dict[str, Any]:
    _market_engine_cache[key] = value
    _market_engine_cache_time[key] = datetime.now(timezone.utc)
    return value


async def _stooq_series(client: httpx.AsyncClient, symbol: str) -> list[dict[str, Any]]:
    stooq_symbol = STOOQ_FALLBACK_SYMBOLS.get(symbol)
    if not stooq_symbol:
        return []
    response = await client.get(
        "https://stooq.com/q/d/l/",
        params={
            "s": stooq_symbol,
            "i": "d",
            "d1": (date.today() - timedelta(days=800)).strftime("%Y%m%d"),
            "d2": date.today().strftime("%Y%m%d"),
        },
        headers={"User-Agent": "InstitutionalTerminal/9.0"},
    )
    response.raise_for_status()
    lines = response.text.strip().splitlines()
    if len(lines) < 3:
        return []
    rows = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < 5:
            continue
        try:
            rows.append({
                "datetime": parts[0],
                "open": float(parts[1]),
                "high": float(parts[2]),
                "low": float(parts[3]),
                "close": float(parts[4]),
                "volume": float(parts[5]) if len(parts) > 5 and parts[5] else 0,
            })
        except (TypeError, ValueError):
            continue
    return rows


async def resilient_daily_series(symbol: str, outputsize: int = 260) -> dict[str, Any]:
    cache_key = f"daily:{symbol}:{outputsize}"
    cached = _engine_cache_get(cache_key, DAILY_CACHE_TTL)
    if cached:
        return {**cached, "cache": "hit"}

    attempts = []
    api_key = os.getenv("TWELVE_DATA_API_KEY")
    if api_key:
        try:
            async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
                response = await client.get(
                    "https://api.twelvedata.com/time_series",
                    params={
                        "symbol": symbol, "interval": "1day",
                        "outputsize": outputsize, "apikey": api_key,
                        "format": "JSON", "order": "ASC",
                    },
                )
                if response.status_code != 429:
                    response.raise_for_status()
                    payload = response.json()
                    values = payload.get("values", [])
                    if values:
                        return _engine_cache_set(cache_key, {
                            "provider": "Twelve Data", "symbol": symbol,
                            "values": values, "fallback_used": False,
                            "attempts": attempts,
                        })
                    attempts.append({"provider": "Twelve Data", "message": payload.get("message", "No data")})
                else:
                    attempts.append({"provider": "Twelve Data", "message": "Rate limit reached"})
        except Exception as exc:
            attempts.append({"provider": "Twelve Data", "message": str(exc)})
    else:
        attempts.append({"provider": "Twelve Data", "message": "API key missing"})

    try:
        async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
            values = await _stooq_series(client, symbol)
        if values:
            return _engine_cache_set(cache_key, {
                "provider": "Stooq",
                "symbol": STOOQ_FALLBACK_SYMBOLS.get(symbol, symbol),
                "requested_symbol": symbol,
                "values": values[-outputsize:],
                "fallback_used": True,
                "attempts": attempts,
            })
        attempts.append({"provider": "Stooq", "message": "No fallback data"})
    except Exception as exc:
        attempts.append({"provider": "Stooq", "message": str(exc)})

    stale = _market_engine_cache.get(cache_key)
    if stale:
        return {**stale, "stale": True, "cache": "stale", "attempts": attempts}
    return {"provider": None, "symbol": symbol, "values": [], "attempts": attempts, "message": "All providers failed"}


async def resilient_global_markets() -> dict[str, Any]:
    cache_key = "global_markets_v9"
    cached = _engine_cache_get(cache_key, MARKET_CACHE_TTL)
    if cached:
        return {**cached, "cache": "hit"}

    rows = []
    providers: dict[str, int] = {}
    for market_name, definition in FREE_MARKET_PROXIES.items():
        symbol = definition["symbol"]
        series = await resilient_daily_series(symbol, 260)
        values = series.get("values", [])
        closes = []
        for item in values:
            try:
                closes.append({"date": str(item.get("datetime", ""))[:10], "close": float(item["close"])})
            except (KeyError, TypeError, ValueError):
                continue
        if len(closes) < 2:
            rows.append({
                "market": market_name, "symbol": symbol, "available": False,
                "provider": series.get("provider"), "message": series.get("message", "Unavailable"),
            })
            continue

        latest = closes[-1]["close"]
        weekly_base = closes[-6]["close"] if len(closes) >= 6 else closes[0]["close"]
        monthly_base = closes[-22]["close"] if len(closes) >= 22 else closes[0]["close"]
        provider = series.get("provider") or "Unknown"
        providers[provider] = providers.get(provider, 0) + 1
        rows.append({
            "market": market_name,
            "symbol": symbol,
            "label": definition.get("label", market_name),
            "available": True,
            "latest": round(latest, 4),
            "weekly_change_pct": round((latest / weekly_base - 1) * 100 if weekly_base else 0, 2),
            "monthly_change_pct": round((latest / monthly_base - 1) * 100 if monthly_base else 0, 2),
            "provider": provider,
            "fallback_used": bool(series.get("fallback_used")),
            "history": closes[-90:],
        })

    available = [r for r in rows if r.get("available")]
    avg = sum(r["weekly_change_pct"] for r in available) / len(available) if available else None
    regime = "Unavailable" if avg is None else "Risk-on" if avg > .35 else "Risk-off" if avg < -.35 else "Mixed"
    return _engine_cache_set(cache_key, {
        "configured": True,
        "markets": rows,
        "risk_regime": regime,
        "average_weekly_change_pct": round(avg, 2) if avg is not None else None,
        "provider_counts": providers,
        "cache_minutes": 20,
        "message": "Twelve Data primary with automatic Stooq fallback.",
    })


async def refresh_resilient_market_cache() -> dict[str, Any]:
    _market_engine_cache.pop("global_markets_v9", None)
    _market_engine_cache_time.pop("global_markets_v9", None)
    result = await resilient_global_markets()
    return {
        "status": "complete",
        "markets": len(result.get("markets", [])),
        "providers": result.get("provider_counts", {}),
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
    }


def market_engine_status() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "twelve_data_configured": bool(os.getenv("TWELVE_DATA_API_KEY")),
        "providers": [
            {"name": "Twelve Data", "role": "Primary market prices and specialist symbols"},
            {"name": "Stooq", "role": "Automatic free fallback for supported ETFs and proxies"},
            {"name": "FRED", "role": "Macroeconomic data and bond yields"},
            {"name": "CFTC", "role": "Weekly COT positioning"},
        ],
        "cache_policy": {
            "global_markets": "20 minutes",
            "daily_history": "8 hours",
            "news": "10 minutes",
            "macro": "12 hours",
            "cot": "weekly",
            "seasonality": "monthly",
            "trading_212": "2 minutes",
        },
        "cached_items": [
            {
                "key": key,
                "updated_at": timestamp.isoformat(),
                "age_seconds": round((now - timestamp).total_seconds()),
            }
            for key, timestamp in sorted(_market_engine_cache_time.items())
        ],
    }



async def _stooq_fx_daily(
    client: httpx.AsyncClient,
    stooq_symbol: str,
    days: int = 900,
) -> list[dict[str, Any]]:
    response = await client.get(
        "https://stooq.com/q/d/l/",
        params={
            "s": stooq_symbol,
            "i": "d",
            "d1": (date.today() - timedelta(days=days)).strftime("%Y%m%d"),
            "d2": date.today().strftime("%Y%m%d"),
        },
        headers={"User-Agent": "InstitutionalTerminal/9.3"},
    )
    response.raise_for_status()
    lines = response.text.strip().splitlines()
    if len(lines) < 20:
        return []

    rows: list[dict[str, Any]] = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) < 5:
            continue
        try:
            rows.append(
                {
                    "date": parts[0],
                    "open": float(parts[1]),
                    "high": float(parts[2]),
                    "low": float(parts[3]),
                    "close": float(parts[4]),
                }
            )
        except (TypeError, ValueError):
            continue
    return rows


async def _twelve_fx_daily(
    client: httpx.AsyncClient,
    symbol: str,
    api_key: str,
) -> list[dict[str, Any]]:
    response = await client.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": symbol,
            "interval": "1day",
            "outputsize": 700,
            "apikey": api_key,
            "format": "JSON",
            "order": "ASC",
        },
    )
    if response.status_code == 429:
        return []
    response.raise_for_status()
    payload = response.json()
    values = payload.get("values", [])
    rows = []
    for row in values:
        try:
            rows.append(
                {
                    "date": str(row.get("datetime", ""))[:10],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return rows


async def _component_fx_history(
    client: httpx.AsyncClient,
    definition: dict[str, Any],
    api_key: str | None,
) -> tuple[list[dict[str, Any]], str]:
    # Stooq is attempted first for the synthetic basket because it is free and
    # avoids consuming Twelve Data credits for six component series.
    try:
        rows = await _stooq_fx_daily(client, definition["stooq"])
        if len(rows) >= 100:
            return rows, "Stooq"
    except Exception:
        pass

    if api_key:
        try:
            rows = await _twelve_fx_daily(client, definition["symbol"], api_key)
            if len(rows) >= 100:
                return rows, "Twelve Data"
        except Exception:
            pass

    return [], "Unavailable"


def _fx_rows_by_date(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["date"]: row for row in rows if row.get("date")}


def _geometric_index_value(
    component_rows: dict[str, dict[str, Any]],
    field: str,
) -> float:
    value = 1.0
    for component, definition in SYNTHETIC_DXY_COMPONENTS.items():
        rate = float(component_rows[component][field])
        if rate <= 0:
            raise ValueError("Synthetic DXY component rate must be positive.")
        value *= rate ** float(definition["exponent"])
    return value


def _daily_to_weekly_ohlc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    weekly: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in rows:
        try:
            dt = date.fromisoformat(row["date"])
        except (TypeError, ValueError):
            continue
        iso = dt.isocalendar()
        weekly.setdefault((iso.year, iso.week), []).append(row)

    output = []
    for key in sorted(weekly):
        group = sorted(weekly[key], key=lambda row: row["date"])
        output.append(
            {
                "date": group[-1]["date"],
                "open": group[0]["open"],
                "high": max(row["high"] for row in group),
                "low": min(row["low"] for row in group),
                "close": group[-1]["close"],
            }
        )
    return output


async def synthetic_dxy_history(force: bool = False) -> dict[str, Any]:
    global _synthetic_dxy_cache, _synthetic_dxy_cache_time

    now = datetime.now(timezone.utc)
    if (
        not force
        and _synthetic_dxy_cache
        and _synthetic_dxy_cache_time
        and now - _synthetic_dxy_cache_time < SYNTHETIC_DXY_CACHE_TTL
    ):
        return {**_synthetic_dxy_cache, "cache": "hit"}

    api_key = os.getenv("TWELVE_DATA_API_KEY")
    component_data: dict[str, dict[str, dict[str, Any]]] = {}
    providers: dict[str, str] = {}
    missing = []

    async with httpx.AsyncClient(timeout=50, follow_redirects=True) as client:
        for component, definition in SYNTHETIC_DXY_COMPONENTS.items():
            rows, provider = await _component_fx_history(client, definition, api_key)
            providers[component] = provider
            if len(rows) < 100:
                missing.append(component)
            component_data[component] = _fx_rows_by_date(rows)

    if missing:
        return {
            "available": False,
            "source_label": "Synthetic USD basket",
            "missing_components": missing,
            "providers": providers,
            "candles": [],
            "message": "Synthetic DXY could not be calculated because one or more FX components were unavailable.",
        }

    common_dates = set.intersection(
        *(set(component_data[component].keys()) for component in SYNTHETIC_DXY_COMPONENTS)
    )
    ordered_dates = sorted(common_dates)
    if len(ordered_dates) < 100:
        return {
            "available": False,
            "source_label": "Synthetic USD basket",
            "missing_components": [],
            "providers": providers,
            "candles": [],
            "message": "The six FX components did not have enough overlapping dates.",
        }

    raw_rows = []
    for dt in ordered_dates:
        selected = {
            component: component_data[component][dt]
            for component in SYNTHETIC_DXY_COMPONENTS
        }
        try:
            open_value = _geometric_index_value(selected, "open")
            close_value = _geometric_index_value(selected, "close")

            # Geometric synthetic high/low are approximations based on the
            # component daily extremes. This is suitable for background context,
            # not for execution or official settlement comparison.
            high_value = _geometric_index_value(
                {
                    component: {
                        **row,
                        "high": (
                            row["low"]
                            if SYNTHETIC_DXY_COMPONENTS[component]["exponent"] < 0
                            else row["high"]
                        ),
                    }
                    for component, row in selected.items()
                },
                "high",
            )
            low_value = _geometric_index_value(
                {
                    component: {
                        **row,
                        "low": (
                            row["high"]
                            if SYNTHETIC_DXY_COMPONENTS[component]["exponent"] < 0
                            else row["low"]
                        ),
                    }
                    for component, row in selected.items()
                },
                "low",
            )
            raw_rows.append(
                {
                    "date": dt,
                    "open": open_value,
                    "high": max(high_value, open_value, close_value),
                    "low": min(low_value, open_value, close_value),
                    "close": close_value,
                }
            )
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            continue

    if len(raw_rows) < 100:
        return {
            "available": False,
            "source_label": "Synthetic USD basket",
            "providers": providers,
            "candles": [],
            "message": "Not enough valid synthetic observations were produced.",
        }

    base = raw_rows[0]["close"]
    rebased = []
    for row in raw_rows:
        rebased.append(
            {
                "date": row["date"],
                "open": round(row["open"] / base * 100, 4),
                "high": round(row["high"] / base * 100, 4),
                "low": round(row["low"] / base * 100, 4),
                "close": round(row["close"] / base * 100, 4),
            }
        )

    weekly = _daily_to_weekly_ohlc(rebased)[-156:]
    result = {
        "available": True,
        "market": "USD",
        "symbol": "SYNTHETIC_DXY",
        "source_label": "Synthetic USD basket",
        "official_index": False,
        "rebased": True,
        "base_value": 100,
        "providers": providers,
        "weights": {
            "EUR": 57.6,
            "JPY": 13.6,
            "GBP": 11.9,
            "CAD": 9.1,
            "SEK": 4.2,
            "CHF": 3.6,
        },
        "candles": weekly,
        "daily": rebased[-700:],
        "message": (
            "Synthetic six-currency USD basket, rebased to 100. "
            "Designed for trend context and not presented as the official ICE DXY level."
        ),
    }
    _synthetic_dxy_cache = result
    _synthetic_dxy_cache_time = datetime.now(timezone.utc)
    return result


async def uup_weekly_fallback() -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=45, follow_redirects=True) as client:
            rows = await _stooq_daily_series(client, "UUP")
        if len(rows) < 100:
            return {"available": False, "candles": []}
        prepared = [
            {
                "date": str(row.get("datetime", ""))[:10],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
            for row in rows
        ]
        return {
            "available": True,
            "market": "USD",
            "symbol": "UUP",
            "source_label": "UUP Dollar Index ETF proxy",
            "official_index": False,
            "fallback_used": True,
            "candles": _daily_to_weekly_ohlc(prepared)[-156:],
            "message": "UUP ETF proxy used because official and synthetic DXY series were unavailable.",
        }
    except Exception:
        return {"available": False, "candles": []}



# ---------------------------------------------------------------------------
# Version 10: 28-pair Currency Strength Matrix
# ---------------------------------------------------------------------------

CURRENCY_MATRIX_CURRENCIES = ["EUR", "GBP", "AUD", "NZD", "USD", "CAD", "CHF", "JPY"]

CURRENCY_MATRIX_PAIRS: list[tuple[str, str]] = [
    ("EUR", "GBP"), ("EUR", "AUD"), ("EUR", "NZD"), ("EUR", "USD"),
    ("EUR", "CAD"), ("EUR", "CHF"), ("EUR", "JPY"),
    ("GBP", "AUD"), ("GBP", "NZD"), ("GBP", "USD"), ("GBP", "CAD"),
    ("GBP", "CHF"), ("GBP", "JPY"),
    ("AUD", "NZD"), ("AUD", "USD"), ("AUD", "CAD"), ("AUD", "CHF"),
    ("AUD", "JPY"),
    ("NZD", "USD"), ("NZD", "CAD"), ("NZD", "CHF"), ("NZD", "JPY"),
    ("USD", "CAD"), ("USD", "CHF"), ("USD", "JPY"),
    ("CAD", "CHF"), ("CAD", "JPY"),
    ("CHF", "JPY"),
]

STOOQ_MATRIX_SYMBOLS: dict[str, str] = {
    "EURGBP": "eurgbp", "EURAUD": "euraud", "EURNZD": "eurnzd",
    "EURUSD": "eurusd", "EURCAD": "eurcad", "EURCHF": "eurchf",
    "EURJPY": "eurjpy", "GBPAUD": "gbpaud", "GBPNZD": "gbpnzd",
    "GBPUSD": "gbpusd", "GBPCAD": "gbpcad", "GBPCHF": "gbpchf",
    "GBPJPY": "gbpjpy", "AUDNZD": "audnzd", "AUDUSD": "audusd",
    "AUDCAD": "audcad", "AUDCHF": "audchf", "AUDJPY": "audjpy",
    "NZDUSD": "nzdusd", "NZDCAD": "nzdcad", "NZDCHF": "nzdchf",
    "NZDJPY": "nzdjpy", "USDCAD": "usdcad", "USDCHF": "usdchf",
    "USDJPY": "usdjpy", "CADCHF": "cadchf", "CADJPY": "cadjpy",
    "CHFJPY": "chfjpy",
}

_currency_matrix_cache: dict[str, Any] = {}
_currency_matrix_updated_at: datetime | None = None
_currency_matrix_lock = asyncio.Lock()
CURRENCY_MATRIX_CACHE_TTL = timedelta(hours=6)


def _matrix_pair_name(base: str, quote: str) -> str:
    return f"{base}{quote}"


async def _matrix_daily_rows(
    client: httpx.AsyncClient,
    pair: str,
) -> tuple[list[dict[str, Any]], str]:
    stooq_symbol = STOOQ_MATRIX_SYMBOLS[pair]
    try:
        response = await client.get(
            "https://stooq.com/q/d/l/",
            params={
                "s": stooq_symbol,
                "i": "d",
                "d1": (date.today() - timedelta(days=150)).strftime("%Y%m%d"),
                "d2": date.today().strftime("%Y%m%d"),
            },
            headers={"User-Agent": "InstitutionalTerminal/10.0"},
        )
        response.raise_for_status()
        lines = response.text.strip().splitlines()
        rows = []
        for line in lines[1:]:
            parts = line.split(",")
            if len(parts) < 5:
                continue
            try:
                rows.append(
                    {
                        "date": parts[0],
                        "open": float(parts[1]),
                        "high": float(parts[2]),
                        "low": float(parts[3]),
                        "close": float(parts[4]),
                    }
                )
            except (TypeError, ValueError):
                continue
        if len(rows) >= 30:
            return rows, "Stooq"
    except Exception:
        pass

    api_key = os.getenv("TWELVE_DATA_API_KEY")
    if not api_key:
        return [], "Unavailable"

    try:
        response = await client.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": f"{pair[:3]}/{pair[3:]}",
                "interval": "1day",
                "outputsize": 120,
                "apikey": api_key,
                "format": "JSON",
                "order": "ASC",
            },
        )
        if response.status_code == 429:
            return [], "Unavailable"
        response.raise_for_status()
        payload = response.json()
        rows = []
        for row in payload.get("values", []):
            try:
                rows.append(
                    {
                        "date": str(row.get("datetime", ""))[:10],
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                    }
                )
            except (KeyError, TypeError, ValueError):
                continue
        return rows, "Twelve Data" if len(rows) >= 30 else "Unavailable"
    except Exception:
        return [], "Unavailable"


def _completed_daily_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned = []
    today = date.today().isoformat()
    for row in rows:
        try:
            current = {
                "date": str(row["date"])[:10],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
            if min(current["open"], current["high"], current["low"], current["close"]) <= 0:
                continue
            cleaned.append(current)
        except (KeyError, TypeError, ValueError):
            continue

    cleaned.sort(key=lambda row: row["date"])
    if cleaned and cleaned[-1]["date"] == today:
        cleaned = cleaned[:-1]
    return cleaned[-65:]


def _confirmed_swings(
    rows: list[dict[str, Any]],
    left: int = 2,
    right: int = 2,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    highs: list[dict[str, Any]] = []
    lows: list[dict[str, Any]] = []

    for index in range(left, len(rows) - right):
        window = rows[index - left:index + right + 1]
        high = rows[index]["high"]
        low = rows[index]["low"]

        if high == max(row["high"] for row in window):
            if not highs or high != highs[-1]["price"]:
                highs.append(
                    {"date": rows[index]["date"], "price": high, "index": index}
                )

        if low == min(row["low"] for row in window):
            if not lows or low != lows[-1]["price"]:
                lows.append(
                    {"date": rows[index]["date"], "price": low, "index": index}
                )

    return highs, lows


def _score_pair_structure(rows: list[dict[str, Any]]) -> dict[str, Any]:
    highs, lows = _confirmed_swings(rows)
    if len(highs) < 2 or len(lows) < 2:
        return {
            "pair_score": 0,
            "pattern": "Insufficient data",
            "trend": "No trend",
            "reason": "Two confirmed highs and two confirmed lows are required.",
            "recent_highs": highs[-2:],
            "recent_lows": lows[-2:],
        }

    previous_high, latest_high = highs[-2], highs[-1]
    previous_low, latest_low = lows[-2], lows[-1]

    higher_high = latest_high["price"] > previous_high["price"]
    lower_high = latest_high["price"] < previous_high["price"]
    higher_low = latest_low["price"] > previous_low["price"]
    lower_low = latest_low["price"] < previous_low["price"]

    if higher_high and higher_low:
        pair_score = 1
        pattern = "A"
        trend = "Uptrend"
        reason = "Higher highs and higher lows."
    elif lower_high and lower_low:
        pair_score = -1
        pattern = "B"
        trend = "Downtrend"
        reason = "Lower highs and lower lows."
    elif higher_high and lower_low:
        pair_score = 0
        pattern = "C"
        trend = "No trend"
        reason = "Higher highs but lower lows."
    elif lower_high and higher_low:
        pair_score = 0
        pattern = "D"
        trend = "No trend"
        reason = "Lower highs but higher lows."
    else:
        pair_score = 0
        pattern = "No agreement"
        trend = "No trend"
        reason = "The latest confirmed highs and lows do not form an agreed cycle."

    return {
        "pair_score": pair_score,
        "pattern": pattern,
        "trend": trend,
        "reason": reason,
        "higher_high": higher_high,
        "lower_high": lower_high,
        "higher_low": higher_low,
        "lower_low": lower_low,
        "recent_highs": [previous_high, latest_high],
        "recent_lows": [previous_low, latest_low],
        "last_completed_candle": rows[-1]["date"] if rows else None,
    }


def _pair_trade_direction(
    pair: str,
    base_score: int,
    quote_score: int,
) -> str:
    if base_score > quote_score:
        return f"Long {pair}"
    if base_score < quote_score:
        return f"Short {pair}"
    return "Neutral"


async def refresh_currency_matrix() -> dict[str, Any]:
    global _currency_matrix_cache, _currency_matrix_updated_at

    async with _currency_matrix_lock:
        currency_scores = {currency: 0 for currency in CURRENCY_MATRIX_CURRENCIES}
        pair_rows = []

        async with httpx.AsyncClient(timeout=50, follow_redirects=True) as client:
            for base, quote in CURRENCY_MATRIX_PAIRS:
                pair = _matrix_pair_name(base, quote)
                raw_rows, provider = await _matrix_daily_rows(client, pair)
                rows = _completed_daily_rows(raw_rows)
                structure = _score_pair_structure(rows)
                pair_score = int(structure["pair_score"])

                # Pair uptrend: base +1, quote -1.
                # Pair downtrend: base -1, quote +1.
                currency_scores[base] += pair_score
                currency_scores[quote] -= pair_score

                pair_rows.append(
                    {
                        "pair": pair,
                        "base": base,
                        "quote": quote,
                        "base_award": pair_score,
                        "quote_award": -pair_score,
                        "provider": provider,
                        "days": len(rows),
                        "available": len(rows) >= 20,
                        **structure,
                    }
                )

        ranking = sorted(
            [
                {
                    "currency": currency,
                    "score": score,
                    "classification": (
                        "Strong" if score >= 4
                        else "Bullish" if score >= 1
                        else "Weak" if score <= -4
                        else "Bearish" if score <= -1
                        else "Neutral"
                    ),
                }
                for currency, score in currency_scores.items()
            ],
            key=lambda row: (row["score"], row["currency"]),
            reverse=True,
        )

        score_map = {row["currency"]: row["score"] for row in ranking}
        opportunities = []
        for base, quote in CURRENCY_MATRIX_PAIRS:
            pair = _matrix_pair_name(base, quote)
            base_score = score_map[base]
            quote_score = score_map[quote]
            difference = base_score - quote_score
            opportunities.append(
                {
                    "pair": pair,
                    "base": base,
                    "quote": quote,
                    "base_score": base_score,
                    "quote_score": quote_score,
                    "difference": difference,
                    "strength_gap": abs(difference),
                    "direction": _pair_trade_direction(pair, base_score, quote_score),
                }
            )

        opportunities.sort(
            key=lambda row: (row["strength_gap"], row["pair"]),
            reverse=True,
        )

        payload = {
            "methodology": {
                "history": "Approximately two months of completed daily candles",
                "swing_confirmation": "Two candles either side",
                "pattern_A": "Higher highs and higher lows: base +1, quote -1",
                "pattern_B": "Lower highs and lower lows: base -1, quote +1",
                "pattern_C": "Higher highs and lower lows: both 0",
                "pattern_D": "Lower highs and higher lows: both 0",
                "score_range": "-7 to +7",
                "pair_count": 28,
            },
            "ranking": ranking,
            "pairs": pair_rows,
            "opportunities": opportunities,
            "strongest": ranking[0] if ranking else None,
            "weakest": ranking[-1] if ranking else None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "automatic": True,
            "schedule": "Recalculated every six hours from completed daily candles.",
        }

        _currency_matrix_cache = payload
        _currency_matrix_updated_at = datetime.now(timezone.utc)
        return payload


async def currency_matrix_data() -> dict[str, Any]:
    if (
        _currency_matrix_cache
        and _currency_matrix_updated_at
        and datetime.now(timezone.utc) - _currency_matrix_updated_at
        < CURRENCY_MATRIX_CACHE_TTL
    ):
        return _currency_matrix_cache
    return await refresh_currency_matrix()


scheduler = AsyncIOScheduler(timezone="UTC")


@asynccontextmanager
async def lifespan(_: FastAPI):
    initialise_database()
    scheduler.add_job(
        refresh_cot,
        "cron",
        day_of_week="fri",
        hour=22,
        minute=30,
        id="weekly-cot-refresh",
        replace_existing=True,
    )
    scheduler.add_job(
        refresh_resilient_market_cache,
        "interval",
        minutes=20,
        id="resilient-market-cache-refresh",
        replace_existing=True,
    )
    scheduler.add_job(
        refresh_currency_matrix,
        "interval",
        hours=6,
        id="currency-strength-matrix-refresh",
        replace_existing=True,
    )
    scheduler.add_job(
        refresh_auto_seasonality,
        "cron",
        day=1,
        hour=3,
        minute=10,
        args=["core"],
        id="monthly-auto-seasonality-core",
        replace_existing=True,
    )
    scheduler.add_job(
        refresh_auto_seasonality,
        "cron",
        day=1,
        hour=3,
        minute=20,
        args=["assets"],
        id="monthly-auto-seasonality-assets",
        replace_existing=True,
    )
    scheduler.add_job(
        refresh_auto_seasonality,
        "cron",
        day=1,
        hour=3,
        minute=30,
        args=["dxy"],
        id="monthly-auto-seasonality-dxy",
        replace_existing=True,
    )
    scheduler.start()
    if is_stale():
        asyncio.create_task(refresh_cot())
    asyncio.create_task(refresh_currency_matrix())
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Institutional Market Intelligence Terminal v10 Pair Structure Currency Matrix", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
async def health(background_tasks: BackgroundTasks) -> dict[str, Any]:
    if is_stale():
        background_tasks.add_task(refresh_cot)
    current = build_payload()
    return {
        "status": "ok" if any(current["currencies"].values()) else "warming_up",
        "currencies_available": sum(bool(rows) for rows in current["currencies"].values()),
        "metals_available": sum(bool(rows) for rows in current["metals"].values()),
        "providers": {
            "cftc": True,
            "bbc": True,
            "fred": bool(os.getenv("FRED_API_KEY")),
            "trading_economics": bool(os.getenv("TRADING_ECONOMICS_API_KEY")),
            "twelve_data": bool(os.getenv("TWELVE_DATA_API_KEY")),
        },
        "last_refresh": current["last_refresh"],
    }


@app.get("/api/cot")
async def cot(background_tasks: BackgroundTasks) -> dict[str, Any]:
    if is_stale():
        background_tasks.add_task(refresh_cot)
    current = build_payload()
    if not any(current["currencies"].values()):
        raise HTTPException(status_code=503, detail="Initial CFTC download is still running.")
    return current


@app.post("/api/cot/refresh")
async def manual_refresh() -> dict[str, Any]:
    return await refresh_cot()


@app.get("/api/news")
async def news() -> dict[str, Any]:
    result = await get_official_news()
    return result.as_dict()



@app.get("/api/calendar")
async def calendar() -> dict[str, Any]:
    return await combined_calendar()


@app.post("/api/calendar")
async def save_calendar(payload: dict[str, Any]) -> dict[str, Any]:
    return save_manual_events(payload)


@app.delete("/api/calendar/{event_id}")
async def delete_calendar(event_id: str) -> dict[str, Any]:
    return delete_manual_event(event_id)


@app.get("/api/macro")
async def macro() -> dict[str, Any]:
    return await fred_macro()


@app.get("/api/seasonality")
async def get_seasonality() -> dict[str, Any]:
    return {"markets": read_seasonality()}


@app.post("/api/seasonality")
async def post_seasonality(payload: dict[str, Any]) -> dict[str, Any]:
    return save_seasonality(payload)


@app.get("/api/seasonality/v2")
async def get_seasonality_v2() -> dict[str, Any]:
    return {
        "markets": read_seasonality_profiles(),
        "methodology": (
            "Profiles store monthly 5-year, 10-year and 20-year average returns, "
            "win rates and sample sizes. Short-horizon views use the current "
            "monthly backdrop rather than pretending daily history is available."
        ),
    }


@app.post("/api/seasonality/v2")
async def post_seasonality_v2(payload: dict[str, Any]) -> dict[str, Any]:
    return save_seasonality_profiles(payload)


@app.get("/api/seasonality/v2/{market}")
async def get_market_seasonality(
    market: str,
    metric: str = "average_10y",
) -> dict[str, Any]:
    allowed = {"average_5y", "average_10y", "average_20y"}
    if metric not in allowed:
        raise HTTPException(status_code=400, detail="Unsupported seasonality metric.")
    return seasonality_horizon_summary(market, metric)


@app.get("/api/seasonality/auto/status")
async def get_auto_seasonality_status() -> dict[str, Any]:
    return auto_seasonality_status()


@app.post("/api/seasonality/auto/refresh/{group}")
async def run_auto_seasonality_refresh(group: str) -> dict[str, Any]:
    result = await refresh_auto_seasonality(group)
    if result.get("status") == "error":
        raise HTTPException(status_code=400, detail=result.get("message"))
    return result


@app.get("/api/seasonality/backtest")
async def get_seasonality_backtest() -> dict[str, Any]:
    return {
        "markets": read_backtest_metrics(),
        "methodology": (
            "Backtesting uses monthly close-to-close historical returns. "
            "Median, sample volatility, best/worst outcomes, win counts, "
            "drawdown proxy, current-year comparison and reliability grade "
            "are calculated separately from the average seasonal profile."
        ),
    }


@app.get("/api/seasonality/windows/{market}")
async def get_seasonal_windows(
    market: str,
    metric: str = "average_10y",
) -> dict[str, Any]:
    return {
        "market": market.upper(),
        "metric": metric,
        "windows": strongest_seasonal_windows(market, metric),
    }


@app.get("/api/seasonality/pair/{base}/{quote}")
async def get_pair_seasonality(
    base: str,
    quote: str,
    metric: str = "average_10y",
) -> dict[str, Any]:
    return direct_pair_seasonality(base, quote, metric)


@app.get("/api/cot/price-overlay/{market}")
async def get_cot_price_overlay(market: str) -> dict[str, Any]:
    return await cot_price_overlay(market)



@app.get("/api/dxy/synthetic")
async def get_synthetic_dxy(force: bool = False) -> dict[str, Any]:
    return await synthetic_dxy_history(force=force)


@app.get("/api/currency-strength-matrix")
async def get_currency_strength_matrix() -> dict[str, Any]:
    return await currency_matrix_data()


@app.post("/api/currency-strength-matrix/refresh")
async def refresh_currency_strength_matrix() -> dict[str, Any]:
    return await refresh_currency_matrix()


@app.get("/api/markets")
async def markets() -> dict[str, Any]:
    return await resilient_global_markets()


@app.get("/api/intelligence")
async def intelligence() -> dict[str, Any]:
    cot_payload = build_payload()
    try:
        markets_payload = await twelve_data_free_markets()
    except Exception as exc:  # noqa: BLE001
        markets_payload = {"configured": False, "markets": [], "error": str(exc)}
    news_result = await get_official_news()
    return build_market_intelligence(
        cot_payload=cot_payload,
        markets_payload=markets_payload,
        news_payload=news_result.data,
    )


@app.get("/api/brief")
async def daily_brief() -> dict[str, Any]:
    cot_payload = build_payload()
    macro_payload = await fred_macro()
    try:
        markets_payload = await twelve_data_free_markets()
    except Exception as exc:  # noqa: BLE001
        markets_payload = {"configured": False, "markets": [], "risk_regime": "Unavailable", "error": str(exc)}
    news_payload = (await get_official_news()).as_dict()
    calendar_payload = await combined_calendar()
    return latest_market_brief(
        cot_payload=cot_payload,
        macro_payload=macro_payload,
        markets_payload=markets_payload,
        news_payload=news_payload,
        calendar_payload=calendar_payload,
    )


@app.get("/api/setups")
async def technical_setups() -> dict[str, Any]:
    return {"setups": read_technical_setups()}


@app.post("/api/setups")
async def upsert_technical_setup(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        return save_technical_setup(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/setups/{pair:path}")
async def remove_technical_setup(pair: str) -> dict[str, Any]:
    try:
        return delete_technical_setup(pair)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/portfolio/trading212")
async def get_trading212_portfolio(force: bool = False):
    try:
        return await trading212_portfolio(force=force)
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else {
            "code": "trading212_http_error",
            "message": str(exc.detail),
        }
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "configured": True,
                "connected": False,
                "environment": trading212_configuration()["environment"],
                "read_only": True,
                "error": detail,
                "message": detail.get("message", "Trading 212 connection failed."),
                "summary": {},
                "positions": [],
                "allocation": [],
                "dividends": [],
                "diagnostics": {
                    "environment": trading212_configuration()["environment"],
                    "account_summary": "failed",
                    "positions": "failed",
                    "dividends": "not checked",
                    "warnings": [],
                },
            },
        )
    except Exception as exc:
        logger.exception("Unexpected Trading 212 portfolio error")
        return JSONResponse(
            status_code=500,
            content={
                "configured": True,
                "connected": False,
                "environment": trading212_configuration()["environment"],
                "read_only": True,
                "error": {
                    "code": "trading212_internal_error",
                    "message": "The dashboard encountered an internal Trading 212 error.",
                },
                "message": "The dashboard encountered an internal Trading 212 error.",
                "summary": {},
                "positions": [],
                "allocation": [],
                "dividends": [],
                "diagnostics": {
                    "environment": trading212_configuration()["environment"],
                    "account_summary": "failed",
                    "positions": "failed",
                    "dividends": "not checked",
                    "warnings": [],
                },
            },
        )


@app.get("/api/market-engine/status")
async def get_market_engine_status() -> dict[str, Any]:
    return market_engine_status()


@app.post("/api/market-engine/refresh")
async def refresh_market_engine() -> dict[str, Any]:
    return await refresh_resilient_market_cache()




@app.get("/api/portfolio/trading212/status")
async def get_trading212_status() -> dict[str, Any]:
    config = trading212_configuration()
    return {
        "configured": bool(config.get("api_key") and config.get("api_secret")),
        "environment": config["environment"],
        "read_only": True,
        "checks": {
            "api_key_present": bool(config.get("api_key")),
            "api_secret_present": bool(config.get("api_secret")),
            "environment_valid": config["environment"] in {"live", "demo"},
        },
    }


@app.get("/api/providers")
async def providers() -> dict[str, Any]:
    return {
        "cftc": {"configured": True, "description": "Live Legacy Futures Only COT"},
        "bbc": {"configured": True, "description": "BBC Business, World and UK RSS"},
        "fred": {
            "configured": bool(os.getenv("FRED_API_KEY")),
            "required_variable": "FRED_API_KEY",
        },
        "twelve_data": {
            "configured": bool(os.getenv("TWELVE_DATA_API_KEY")),
            "required_variable": "TWELVE_DATA_API_KEY",
            "description": "Free-tier ETF proxies for Global Markets",
        },
        "trading_economics": {
            "configured": bool(os.getenv("TRADING_ECONOMICS_API_KEY")),
            "required_variable": "TRADING_ECONOMICS_API_KEY",
            "description": "Optional future upgrade for official index and calendar data",
        },
    }
