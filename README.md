# Institutional Market Intelligence Terminal — Version 6.2 Free Global Markets

## What this patch adds

- Free Global Markets connection through Twelve Data
- No paid Trading Economics subscription required
- UK, US, Europe, Japan, Hong Kong, China, Australia, Canada, India and Brazil
- Daily, weekly, monthly, six-month and yearly percentage changes
- 60-trading-day charts
- Transparent risk-on / risk-off summary
- BBC Business-only news feed

## Important proxy disclosure

The free build uses liquid US-listed ETFs as regional market proxies:

- EWU — United Kingdom
- SPY — S&P 500
- QQQ — Nasdaq 100
- DIA — Dow
- IWM — Russell 2000
- EWG — Germany
- EWQ — France
- VGK — Europe
- EWJ — Japan
- EWH — Hong Kong
- FXI — China large cap
- EWA — Australia
- EWC — Canada
- INDA — India
- EWZ — Brazil

These are not official cash-index levels. They can differ because of currency effects, fund fees and trading hours. The dashboard labels them clearly.

## One free account needed

Create a free Twelve Data account and add the key privately in Render:

`TWELVE_DATA_API_KEY`

The free plan currently advertises 800 API credits per day. This dashboard makes one batch request, while each symbol still consumes one credit.

## Update the live site

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Commit with:

`Version 6.2 free global markets`
