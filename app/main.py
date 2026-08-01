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
            "message": "Add FRED_API_KEY in Render.",
        }

    series_ids = {
        "US_2Y": "DGS2",
        "US_10Y": "DGS10",
        "US_30Y": "DGS30",
        "FED_FUNDS": "DFF",
        "REAL_10Y": "DFII10",
    }
    output = {}
    async with httpx.AsyncClient(timeout=45) as client:
        for label, series_id in series_ids.items():
            response = await client.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id": series_id,
                    "api_key": api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 30,
                },
            )
            response.raise_for_status()
            output[label] = [
                {
                    "date": row["date"],
                    "value": None if row["value"] == "." else float(row["value"]),
                }
                for row in response.json().get("observations", [])
            ]
    return {"configured": True, "series": output}


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


app = FastAPI(title="Institutional Market Intelligence Terminal v7.1 Markets Hotfix", lifespan=lifespan)
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
    return await trading_economics_calendar()


@app.get("/api/macro")
async def macro() -> dict[str, Any]:
    return await fred_macro()


@app.get("/api/seasonality")
async def get_seasonality() -> dict[str, Any]:
    return {"markets": read_seasonality()}


@app.post("/api/seasonality")
async def post_seasonality(payload: dict[str, Any]) -> dict[str, Any]:
    return save_seasonality(payload)


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
