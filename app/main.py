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
from fastapi.responses import FileResponse
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
    scheduler.start()
    if is_stale():
        asyncio.create_task(refresh_cot())
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Institutional Market Intelligence Terminal v8 Seasonality and Alignment", lifespan=lifespan)
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


@app.get("/api/markets")
async def markets() -> dict[str, Any]:
    return await twelve_data_free_markets()


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
