from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from .base import ProviderResult


OFFICIAL_FEEDS: dict[str, str] = {
    "BBC Business": "https://feeds.bbci.co.uk/news/business/rss.xml",
    "ECB Press Releases": "https://www.ecb.europa.eu/rss/press.html",
    "ECB Speeches": "https://www.ecb.europa.eu/rss/key.html",
    "Federal Reserve Press Releases": "https://www.federalreserve.gov/feeds/press_all.xml",
    "Bank of England News": "https://www.bankofengland.co.uk/rss/news",
}

FINANCIAL_TERMS = [
    "interest rate", "inflation", "cpi", "employment", "payroll", "gdp",
    "central bank", "federal reserve", "bank of england", "ecb", "bond",
    "yield", "currency", "sterling", "dollar", "euro", "yen", "gold",
    "silver", "oil", "stock market", "equities", "recession", "tariff",
    "sanction", "trade", "bank", "financial stability", "monetary policy",
]


def clean_html(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value or "").strip()


def is_financial(title: str, summary: str, source: str) -> bool:
    if source != "BBC Business":
        return True
    haystack = f"{title} {summary}".lower()
    return any(term in haystack for term in FINANCIAL_TERMS)


def classify(title: str, summary: str) -> dict[str, Any]:
    haystack = f"{title} {summary}".lower()
    categories = []
    affected = []
    countries = []

    rules = [
        ("Central Banks", ["interest rate", "monetary policy", "federal reserve", "bank of england", "ecb"]),
        ("Inflation", ["inflation", "cpi", "prices"]),
        ("Employment", ["employment", "jobs", "payroll", "unemployment", "wages"]),
        ("Growth", ["gdp", "growth", "recession", "pmi"]),
        ("Bonds", ["bond", "yield", "treasury", "gilt"]),
        ("Currencies", ["currency", "dollar", "sterling", "euro", "yen", "franc"]),
        ("Precious Metals", ["gold", "silver", "platinum", "palladium"]),
        ("Equities", ["stock", "shares", "equities", "ftse", "s&p", "nasdaq", "dow"]),
        ("Commodities", ["oil", "gas", "copper", "commodity"]),
        ("Trade / Geopolitics", ["tariff", "sanction", "war", "trade"]),
    ]
    for category, terms in rules:
        if any(term in haystack for term in terms):
            categories.append(category)

    market_rules = [
        ("USD", "United States", ["dollar", "federal reserve", "fed", "treasury", "us economy"]),
        ("GBP", "United Kingdom", ["sterling", "bank of england", "uk economy", "britain", "ftse"]),
        ("EUR", "Euro area", ["euro", "ecb", "eurozone"]),
        ("JPY", "Japan", ["yen", "bank of japan", "japan"]),
        ("GOLD", "Global", ["gold", "bullion"]),
        ("SILVER", "Global", ["silver"]),
    ]
    for symbol, country, terms in market_rules:
        if any(term in haystack for term in terms):
            affected.append(symbol)
            if country not in countries:
                countries.append(country)

    impact = "Low"
    if any(term in haystack for term in [
        "rate decision", "interest rate", "inflation", "cpi", "payroll",
        "employment", "gdp", "financial stability", "war", "sanction",
    ]):
        impact = "High"
    elif categories:
        impact = "Medium"

    return {
        "categories": categories or ["General Markets"],
        "affected_markets": affected or ["GLOBAL"],
        "countries": countries or ["Global"],
        "impact": impact,
    }


async def fetch_feed(source: str, url: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        response = await client.get(url, headers={"User-Agent": "InstitutionalTerminal/7.0"})
        response.raise_for_status()

    root = ET.fromstring(response.content)
    rows = []
    for item in root.findall(".//item")[:30]:
        title = clean_html(item.findtext("title", default=""))
        summary = clean_html(item.findtext("description", default=""))
        if not is_financial(title, summary, source):
            continue
        published_raw = item.findtext("pubDate", default="")
        try:
            published = parsedate_to_datetime(published_raw).isoformat()
        except Exception:
            published = published_raw
        rows.append({
            "source": source,
            "title": title,
            "summary": summary,
            "url": item.findtext("link", default=""),
            "published": published,
            **classify(title, summary),
        })
    return rows


async def get_official_news() -> ProviderResult:
    results = await asyncio.gather(
        *[fetch_feed(source, url) for source, url in OFFICIAL_FEEDS.items()],
        return_exceptions=True,
    )
    items: list[dict[str, Any]] = []
    errors = []
    for source, result in zip(OFFICIAL_FEEDS, results):
        if isinstance(result, Exception):
            errors.append({"source": source, "error": str(result)})
        else:
            items.extend(result)

    deduped = {}
    for item in items:
        deduped.setdefault(item["title"], item)

    ordered = sorted(deduped.values(), key=lambda row: row.get("published", ""), reverse=True)
    return ProviderResult(
        provider="Official and BBC financial feeds",
        configured=True,
        live=bool(ordered),
        data={"items": ordered[:80], "source_errors": errors},
        message="BBC Business is financially filtered; official central-bank feeds are always retained.",
    )
