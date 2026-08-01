# Institutional Market Intelligence Terminal — Cloud v6

## Live without extra keys

- CFTC Legacy Futures-Only COT data
- Commercial and Non-commercial positions
- 8 major currencies
- Gold, silver, platinum and palladium COT
- Commercial / Non-commercial on-off controls
- net-position and weekly buying/selling line graphs
- relationship interpretation engine
- BBC Business, World and UK RSS feeds
- news impact, country, market and effects classification
- editable browser-saved seasonality heatmap
- daily, weekly, monthly, 6-month and yearly outlook framework

## Optional provider keys

Add these in Render → Environment:

- `FRED_API_KEY`
  - US 2Y, 10Y, 30Y yields
  - Fed funds
  - US 10Y real yield

- `TRADING_ECONOMICS_API_KEY`
  - economic calendar
  - global stock indices
  - precious-metal prices
  - wider market feeds, subject to subscription permissions

## Important interpretation

The V6 relationship engine follows this requested model:

- Commercial selling + Non-commercial buying = bullish relationship
- Commercial buying + Non-commercial selling = bearish relationship

This is directional context, not standalone entry timing. Price and the TradingView structure model should confirm entries.

## Update live site

Replace in GitHub:

- `app/main.py`
- `app/static/index.html`
- `requirements.txt`
- `README.md`

Commit. Render auto-deploys.
