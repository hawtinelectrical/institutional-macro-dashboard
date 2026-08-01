from __future__ import annotations

from typing import Any


def cot_relationship(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if len(rows) < 2:
        return {
            "bias": "Unavailable",
            "relationship_score": 0,
            "commercial_change": 0,
            "noncommercial_change": 0,
            "evidence": ["Insufficient COT history"],
        }

    first, latest = rows[0], rows[-1]
    commercial = latest["commercial_net"] - first["commercial_net"]
    noncommercial = latest["noncommercial_net"] - first["noncommercial_net"]

    if commercial < 0 and noncommercial > 0:
        bias = "Bullish relationship"
    elif commercial > 0 and noncommercial < 0:
        bias = "Bearish relationship"
    else:
        bias = "Mixed relationship"

    denominator = max(
        abs(commercial) + abs(noncommercial),
        int(latest.get("open_interest") or 0) * 0.02,
        1,
    )
    score = round(max(-100, min(100, (noncommercial - commercial) / denominator * 100)))

    return {
        "bias": bias,
        "relationship_score": score,
        "commercial_change": commercial,
        "noncommercial_change": noncommercial,
        "evidence": [
            f"Commercial 10-week change: {commercial:+,}",
            f"Non-commercial 10-week change: {noncommercial:+,}",
        ],
    }


def market_regime(markets: dict[str, Any]) -> dict[str, Any]:
    rows = markets.get("markets", []) if isinstance(markets, dict) else []
    weekly = [row.get("weekly_change_pct") for row in rows if row.get("weekly_change_pct") is not None]
    if not weekly:
        return {"regime": "Unavailable", "score": 0, "evidence": ["No live market proxy data"]}

    average = sum(weekly) / len(weekly)
    regime = "Risk-on" if average > 0.5 else "Risk-off" if average < -0.5 else "Mixed"
    return {
        "regime": regime,
        "score": round(max(-100, min(100, average * 20))),
        "evidence": [f"Average weekly equity proxy move: {average:+.2f}%"],
    }


def build_market_intelligence(
    cot_payload: dict[str, Any],
    markets_payload: dict[str, Any],
    news_payload: dict[str, Any],
) -> dict[str, Any]:
    currencies = cot_payload.get("currencies", {})
    cot = {symbol: cot_relationship(rows) for symbol, rows in currencies.items()}
    regime = market_regime(markets_payload)

    stories = news_payload.get("items", [])
    news_by_market: dict[str, dict[str, int]] = {}
    for story in stories:
        for market in story.get("affected_markets", ["GLOBAL"]):
            bucket = news_by_market.setdefault(market, {"High": 0, "Medium": 0, "Low": 0})
            impact = story.get("impact", "Low")
            bucket[impact] = bucket.get(impact, 0) + 1

    return {
        "cot_relationships": cot,
        "market_regime": regime,
        "news_counts": news_by_market,
        "methodology": {
            "relationship": "Commercial selling plus Non-commercial buying is bullish context; the reverse is bearish context.",
            "trade_confirmation": "No trade is confirmed without price and TradingView HTF/MTF/LTF structure alignment.",
            "missing_data": "Unavailable evidence is omitted and lowers confidence; it is never replaced with invented values.",
        },
    }
