# Version 8.1 — Automatic Online Seasonality

## What this adds

The dashboard now calculates seasonality from historical monthly prices rather
than requiring every heatmap cell to be entered manually.

For each configured market it calculates:

- 5-year average monthly return
- 10-year average monthly return
- 20-year average monthly return
- Monthly win rate
- Available sample years
- Automatic source note and refresh date

The results are stored in PostgreSQL and immediately feed:

- seasonal heatmaps
- seasonal curves
- currency intelligence
- pair rankings
- Institutional Alignment

## Free-plan protection

Twelve Data's free allowance is protected by two groups of eight symbols:

### Core currencies

- USD proxy
- EUR
- GBP
- JPY
- CHF
- CAD
- AUD
- NZD

### Assets

- Gold
- Silver
- S&P 500 proxy
- FTSE 100 / UK equities proxy
- Nasdaq 100 proxy
- Japan equity proxy
- China equity proxy
- Australia equity proxy

The scheduler runs:

- Core: first day of each month at 03:10 UTC
- Assets: first day of each month at 03:20 UTC

The page also provides separate manual refresh buttons. Do not run both groups
within the same minute on the free plan.

## Methodology

Monthly close-to-close returns are grouped by calendar month. Inverted forex
quotes such as USD/JPY are converted to the currency's own perspective before
returns are calculated.

ETF-based regional data is clearly labelled as a proxy and is not presented as
an official cash-index value.

## Update GitHub

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep the existing `app/providers/` folder.

Commit with:

`Version 8.1 automatic seasonality`
