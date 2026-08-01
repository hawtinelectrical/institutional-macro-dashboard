from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import BigInteger, Date, DateTime, String, create_engine, delete, select
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
logger = logging.getLogger("institutional-dashboard")

MARKETS: dict[str, list[str]] = {
    "USD": ["U.S. DOLLAR INDEX", "US DOLLAR INDEX", "DOLLAR INDEX"],
    "EUR": ["EURO FX"],
    "GBP": ["BRITISH POUND"],
    "JPY": ["JAPANESE YEN"],
    "CHF": ["SWISS FRANC"],
    "CAD": ["CANADIAN DOLLAR"],
    "AUD": ["AUSTRALIAN DOLLAR"],
    "NZD": ["NEW ZEALAND DOLLAR"],
}


class Base(DeclarativeBase):
    pass


class CotPosition(Base):
    __tablename__ = "cot_positions"

    currency: Mapped[str] = mapped_column(String(3), primary_key=True)
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
    value: Mapped[str] = mapped_column(String(1000))


def to_int(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    return 0 if value in (None, "") else int(float(value))


def initialise_database() -> None:
    Base.metadata.create_all(engine)


async def download_dataset() -> list[dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=180)).date().isoformat()
    params = {
        "$select": (
            "market_and_exchange_names,report_date_as_yyyy_mm_dd,"
            "commodity_name,comm_positions_long_all,comm_positions_short_all,"
            "noncomm_positions_long_all,noncomm_positions_short_all,open_interest_all"
        ),
        "$where": f"report_date_as_yyyy_mm_dd >= '{cutoff}T00:00:00.000'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": "50000",
    }
    async with httpx.AsyncClient(
        headers={"User-Agent": "InstitutionalMacroDashboard/3.0"},
        timeout=90,
    ) as client:
        response = await client.get(CFTC_ENDPOINT, params=params)
        response.raise_for_status()
        return response.json()


def select_market_rows(
    dataset: list[dict[str, Any]], aliases: list[str]
) -> list[dict[str, Any]]:
    aliases_upper = [alias.upper() for alias in aliases]
    matches = []
    for row in dataset:
        market = str(row.get("market_and_exchange_names", "")).upper()
        commodity = str(row.get("commodity_name", "")).upper()
        if any(alias in market or alias in commodity for alias in aliases_upper):
            matches.append(row)

    if not matches:
        return []

    counts: dict[str, int] = {}
    for row in matches:
        name = str(row.get("market_and_exchange_names", ""))
        counts[name] = counts.get(name, 0) + 1

    selected_name = max(counts, key=counts.get)
    unique: dict[str, dict[str, Any]] = {}
    for row in matches:
        if row.get("market_and_exchange_names") != selected_name:
            continue
        report_date = str(row["report_date_as_yyyy_mm_dd"])[:10]
        unique.setdefault(report_date, row)

    return [unique[key] for key in sorted(unique, reverse=True)[:10]]


refresh_lock = asyncio.Lock()


async def refresh_cot() -> dict[str, Any]:
    if refresh_lock.locked():
        return {"status": "already_running"}

    async with refresh_lock:
        fetched_at = datetime.now(timezone.utc)
        dataset = await download_dataset()
        result: dict[str, Any] = {"updated": [], "failed": []}

        with Session(engine) as session:
            for currency, aliases in MARKETS.items():
                try:
                    rows = select_market_rows(dataset, aliases)
                    if not rows:
                        raise RuntimeError(f"No matching CFTC contract found for {currency}")

                    retained_dates: list[date] = []
                    for row in rows:
                        report_date = date.fromisoformat(
                            str(row["report_date_as_yyyy_mm_dd"])[:10]
                        )
                        retained_dates.append(report_date)

                        commercial_long = to_int(row, "comm_positions_long_all")
                        commercial_short = to_int(row, "comm_positions_short_all")
                        noncommercial_long = to_int(row, "noncomm_positions_long_all")
                        noncommercial_short = to_int(row, "noncomm_positions_short_all")

                        position = session.get(
                            CotPosition,
                            {"currency": currency, "report_date": report_date},
                        )
                        if position is None:
                            position = CotPosition(
                                currency=currency,
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

                        position.market_name = str(
                            row.get("market_and_exchange_names", "")
                        )
                        position.commercial_long = commercial_long
                        position.commercial_short = commercial_short
                        position.commercial_net = commercial_long - commercial_short
                        position.noncommercial_long = noncommercial_long
                        position.noncommercial_short = noncommercial_short
                        position.noncommercial_net = (
                            noncommercial_long - noncommercial_short
                        )
                        position.open_interest = to_int(row, "open_interest_all")
                        position.fetched_at = fetched_at

                    session.execute(
                        delete(CotPosition).where(
                            CotPosition.currency == currency,
                            CotPosition.report_date.not_in(retained_dates),
                        )
                    )
                    result["updated"].append(currency)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Refresh failed for %s", currency)
                    result["failed"].append(
                        {"currency": currency, "error": str(exc)}
                    )

            status_value = (
                f"{fetched_at.isoformat()} | "
                f"updated={','.join(result['updated'])} | "
                f"failed={len(result['failed'])}"
            )
            status = session.get(AppStatus, "last_refresh")
            if status is None:
                session.add(AppStatus(key="last_refresh", value=status_value))
            else:
                status.value = status_value

            session.commit()

        return result


def build_payload() -> dict[str, Any]:
    currencies: dict[str, list[dict[str, Any]]] = {}
    with Session(engine) as session:
        for currency in MARKETS:
            rows = session.scalars(
                select(CotPosition)
                .where(CotPosition.currency == currency)
                .order_by(CotPosition.report_date.asc())
            ).all()

            currencies[currency] = [
                {
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
                for row in rows[-10:]
            ]

        status = session.get(AppStatus, "last_refresh")

    return {
        "source": "CFTC Legacy Futures Only",
        "dataset_id": "6dca-aqww",
        "non_reportables_included": False,
        "last_refresh": status.value if status else None,
        "currencies": currencies,
    }


def is_stale() -> bool:
    with Session(engine) as session:
        latest = session.scalar(
            select(CotPosition.fetched_at).order_by(CotPosition.fetched_at.desc())
        )
    if latest is None:
        return True
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - latest > timedelta(days=6)


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


app = FastAPI(title="Institutional Macro Dashboard Cloud v5", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
async def health(background_tasks: BackgroundTasks) -> dict[str, Any]:
    if is_stale():
        background_tasks.add_task(refresh_cot)

    current = build_payload()
    available = sum(bool(rows) for rows in current["currencies"].values())
    return {
        "status": "ok" if available else "warming_up",
        "currencies_available": available,
        "last_refresh": current["last_refresh"],
    }


@app.get("/api/cot")
async def cot(background_tasks: BackgroundTasks) -> dict[str, Any]:
    if is_stale():
        background_tasks.add_task(refresh_cot)

    current = build_payload()
    if not any(current["currencies"].values()):
        raise HTTPException(
            status_code=503,
            detail="The initial CFTC download is still running. Retry shortly.",
        )
    return current


@app.post("/api/cot/refresh")
async def manual_refresh() -> dict[str, Any]:
    return await refresh_cot()
