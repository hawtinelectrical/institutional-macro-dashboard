# Version 8.5 — COT Price Candle Overlay

Weekly price candles now appear behind the COT Commercial and Non-commercial
lines with low adjustable opacity and no visible price scale.

Preferred mappings:
USD DXY, AUD 6A1!, EUR 6E1!, GBP 6B1!, CAD 6C1!, JPY 6J1!,
CHF 6S1!, NZD 6N1!, Gold GC1!, Silver SI1!.

The dashboard tries continuous futures first and uses a labelled spot or ETF
fallback only when necessary.

Controls:
- Price candles on/off
- Opacity 5%–45%
- Six-hour cache

Replace app/main.py, app/static/index.html and README.md.
Commit: Version 8.5 COT candle overlay
