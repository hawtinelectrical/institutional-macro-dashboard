from __future__ import annotations

import asyncio
import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from .base import ProviderResult


# Public feeds that can be consumed without a paid news licence.
# Each feed fails independently so one unavailable source cannot break the full page.
OFFICIAL_FEEDS: dict[str, dict[str, str]] = {
    "BBC Business": {
        "url": "https://feeds.bbci.co.uk/news/business/rss.xml",
        "tier": "Major financial news",
        "country": "Global",
    },
    "Federal Reserve": {
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "tier": "Official central bank",
        "country": "United States",
    },
    "ECB Press Releases": {
        "url": "https://www.ecb.europa.eu/rss/press.html",
        "tier": "Official central bank",
        "country": "Euro area",
    },
    "ECB Speeches": {
        "url": "https://www.ecb.europa.eu/rss/key.html",
        "tier": "Official central bank",
        "country": "Euro area",
    },
    "Bank of England": {
        "url": "https://www.bankofengland.co.uk/rss/news",
        "tier": "Official central bank",
        "country": "United Kingdom",
    },
    "BLS Latest Releases": {
        "url": "https://www.bls.gov/feed/bls_latest.rss",
        "tier": "Official statistics",
        "country": "United States",
    },
}

FINANCIAL_TERMS = [
    "interest rate", "inflation", "cpi", "employment", "payroll", "gdp",
    "central bank", "federal reserve", "bank of england", "ecb", "bond",
    "yield", "currency", "sterling", "dollar", "euro", "yen", "gold",
    "silver", "oil", "stock market", "equities", "recession", "tariff",
    "sanction", "trade", "bank", "financial stability", "monetary policy",
    "unemployment", "wages", "producer price", "consumer price", "jobs",
    "retail sales", "productivity", "job openings",
]


def clean_html(value: str) -> str:
    value = html.unescape(value or "")
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def first_text(node: ET.Element, names: list[str]) -> str:
    for name in names:
        found = node.find(name)
        if found is not None and found.text:
            return found.text.strip()
    return ""


def entry_link(node: ET.Element) -> str:
    direct = first_text(node, ["link"])
    if direct:
        return direct
    for link_node in node.findall("{*}link"):
        href = link_node.attrib.get("href")
        if href:
            return href
    return ""


def parse_date(value: str) -> str:
    if not value:
        return ""
    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return value


def is_financial(title: str, summary: str, source: str) -> bool:
    # Official central-bank and statistics releases are retained.
    if source != "BBC Business":
        return True
    haystack = f"{title} {summary}".lower()
    return any(term in haystack for term in FINANCIAL_TERMS)


def classify(title: str, summary: str, source: str, source_country: str) -> dict[str, Any]:
    haystack = f"{title} {summary}".lower()
    categories: list[str] = []
    affected: list[str] = []
    countries: list[str] = []

    rules = [
        ("Central Banks", ["interest rate", "rate decision", "monetary policy", "federal reserve", "bank of england", "ecb", "fomc"]),
        ("Inflation", ["inflation", "consumer price", "producer price", "cpi", "ppi", "prices"]),
        ("Employment", ["employment", "jobs", "payroll", "unemployment", "wages", "job openings"]),
        ("Growth", ["gdp", "growth", "recession", "pmi", "productivity", "retail sales"]),
        ("Bonds", ["bond", "yield", "treasury", "gilt", "yield curve"]),
        ("Currencies", ["currency", "dollar", "sterling", "euro", "yen", "franc"]),
        ("Precious Metals", ["gold", "silver", "platinum", "palladium", "bullion"]),
        ("Equities", ["stock", "shares", "equities", "ftse", "s&p", "nasdaq", "dow"]),
        ("Commodities", ["oil", "gas", "copper", "commodity"]),
        ("Trade / Geopolitics", ["tariff", "sanction", "war", "trade"]),
        ("Banking / Stability", ["bank", "financial stability", "stress test", "liquidity"]),
    ]
    for category, terms in rules:
        if any(term in haystack for term in terms):
            categories.append(category)

    market_rules = [
        ("USD", "United States", ["dollar", "federal reserve", "fed", "treasury", "us economy", "bls"]),
        ("GBP", "United Kingdom", ["sterling", "bank of england", "uk economy", "britain", "ftse"]),
        ("EUR", "Euro area", ["euro", "ecb", "eurozone"]),
        ("JPY", "Japan", ["yen", "bank of japan", "japan"]),
        ("CHF", "Switzerland", ["swiss franc", "snb", "switzerland"]),
        ("CAD", "Canada", ["canadian dollar", "bank of canada", "canada"]),
        ("AUD", "Australia", ["australian dollar", "rba", "australia"]),
        ("NZD", "New Zealand", ["new zealand dollar", "rbnz", "new zealand"]),
        ("GOLD", "Global", ["gold", "bullion"]),
        ("SILVER", "Global", ["silver"]),
    ]
    for symbol, country, terms in market_rules:
        if any(term in haystack for term in terms):
            affected.append(symbol)
            if country not in countries:
                countries.append(country)

    if source_country != "Global" and source_country not in countries:
        countries.append(source_country)

    critical_terms = [
        "rate decision", "fomc statement", "monetary policy decision",
        "consumer price index", "producer price index", "employment situation",
        "nonfarm payroll", "gross domestic product", "financial stability",
    ]
    high_terms = [
        "interest rate", "inflation", "cpi", "ppi", "payroll", "employment",
        "gdp", "unemployment", "financial stability", "war", "sanction",
    ]
    medium_terms = [
        "retail sales", "productivity", "job openings", "bond", "yield",
        "speech", "testimony", "trade", "bank",
    ]

    if any(term in haystack for term in critical_terms):
        importance = "Critical"
        importance_score = 100
    elif any(term in haystack for term in high_terms):
        importance = "High"
        importance_score = 80
    elif any(term in haystack for term in medium_terms) or categories:
        importance = "Medium"
        importance_score = 55
    else:
        importance = "Low"
        importance_score = 25

    likely_effects: list[str] = []
    if "Central Banks" in categories:
        likely_effects.extend(["Currencies", "Government bonds", "Equities", "Gold"])
    if "Inflation" in categories:
        likely_effects.extend(["Policy expectations", "Bond yields", "Currencies", "Precious metals"])
    if "Employment" in categories:
        likely_effects.extend(["Currencies", "Bond yields", "Equities"])
    if "Trade / Geopolitics" in categories:
        likely_effects.extend(["Risk sentiment", "Safe havens", "Oil", "Equities"])
    if "Precious Metals" in categories:
        likely_effects.extend(["Gold", "Silver", "USD", "Real yields"])
    if not likely_effects:
        likely_effects.append("General market sentiment")

    return {
        "categories": categories or ["General Markets"],
        "affected_markets": affected or ["GLOBAL"],
        "countries": countries or ["Global"],
        "impact": importance if importance != "Critical" else "High",
        "importance": importance,
        "importance_score": importance_score,
        "effects": list(dict.fromkeys(likely_effects)),
    }


def parse_feed(content: bytes) -> list[ET.Element]:
    root = ET.fromstring(content)
    rss_items = root.findall(".//item")
    if rss_items:
        return rss_items
    return root.findall(".//{*}entry")


async def fetch_feed(source: str, config: dict[str, str]) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        response = await client.get(
            config["url"],
            headers={"User-Agent": "InstitutionalTerminal/7.3"},
        )
        response.raise_for_status()

    rows = []
    for node in parse_feed(response.content)[:35]:
        title = clean_html(first_text(node, ["title", "{*}title"]))
        summary = clean_html(first_text(node, ["description", "summary", "content", "{*}summary", "{*}content"]))
        if not title or not is_financial(title, summary, source):
            continue

        published_raw = first_text(
            node,
            ["pubDate", "published", "updated", "{*}published", "{*}updated"],
        )
        rows.append({
            "source": source,
            "source_tier": config["tier"],
            "source_country": config["country"],
            "title": title,
            "summary": summary,
            "url": entry_link(node),
            "published": parse_date(published_raw),
            **classify(title, summary, source, config["country"]),
        })
    return rows


def normalised_title(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()


def build_today_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=30)
    recent = []
    for item in items:
        try:
            published = datetime.fromisoformat(item["published"].replace("Z", "+00:00"))
            if published.tzinfo is None:
                published = published.replace(tzinfo=timezone.utc)
            if published >= cutoff:
                recent.append(item)
        except Exception:
            continue

    category_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    market_counts: dict[str, int] = {}
    for item in recent:
        source_counts[item["source"]] = source_counts.get(item["source"], 0) + 1
        for category in item.get("categories", []):
            category_counts[category] = category_counts.get(category, 0) + 1
        for market in item.get("affected_markets", []):
            market_counts[market] = market_counts.get(market, 0) + 1

    top_categories = sorted(category_counts.items(), key=lambda row: row[1], reverse=True)[:4]
    top_markets = sorted(market_counts.items(), key=lambda row: row[1], reverse=True)[:5]
    high_items = [
        item for item in recent
        if item.get("importance") in {"Critical", "High"}
    ]
    high_items.sort(key=lambda row: row.get("importance_score", 0), reverse=True)

    bullets = []
    if high_items:
        bullets.append(f"{len(high_items)} critical or high-importance financial developments were detected.")
    if top_categories:
        bullets.append("Leading themes: " + ", ".join(name for name, _ in top_categories) + ".")
    if top_markets:
        bullets.append("Most-mentioned markets: " + ", ".join(name for name, _ in top_markets) + ".")
    if not bullets:
        bullets.append("No major new financially classified developments were detected in the latest feed window.")

    return {
        "window_hours": 30,
        "stories": len(recent),
        "high_importance": len(high_items),
        "top_categories": [{"name": name, "count": count} for name, count in top_categories],
        "top_markets": [{"name": name, "count": count} for name, count in top_markets],
        "headline_items": high_items[:6],
        "bullets": bullets,
    }


async def get_official_news() -> ProviderResult:
    results = await asyncio.gather(
        *[fetch_feed(source, config) for source, config in OFFICIAL_FEEDS.items()],
        return_exceptions=True,
    )

    items: list[dict[str, Any]] = []
    source_status = []
    for (source, config), result in zip(OFFICIAL_FEEDS.items(), results):
        if isinstance(result, Exception):
            source_status.append({
                "source": source,
                "tier": config["tier"],
                "live": False,
                "stories": 0,
                "error": str(result),
            })
        else:
            items.extend(result)
            source_status.append({
                "source": source,
                "tier": config["tier"],
                "live": True,
                "stories": len(result),
                "error": None,
            })

    deduped: dict[str, dict[str, Any]] = {}
    for item in items:
        key = normalised_title(item["title"])
        existing = deduped.get(key)
        if existing is None or item.get("importance_score", 0) > existing.get("importance_score", 0):
            deduped[key] = item

    ordered = sorted(
        deduped.values(),
        key=lambda row: (
            row.get("importance_score", 0),
            row.get("published", ""),
        ),
        reverse=True,
    )

    return ProviderResult(
        provider="Financial and official news intelligence",
        configured=True,
        live=bool(ordered),
        data={
            "items": ordered[:140],
            "source_status": source_status,
            "today_summary": build_today_summary(ordered),
        },
        message=(
            "Includes BBC Business, the Federal Reserve, ECB, Bank of England "
            "and BLS official economic releases. Reuters is not included without a licensed feed."
        ),
    )
