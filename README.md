# Version 8.2 — Dollar Index and Automatic Seasonality

## Dedicated Dollar Index module

The automatic seasonality engine now has a separate Dollar Index refresh group.

It tries the provider symbols in this order:

1. `DXY`
2. `DXY:ICE`
3. `UUP` as a transparent ETF fallback

The selected source is stored in the seasonality notes and displayed on the
dashboard. UUP is never presented as the official DXY index.

## USD integration

When a DXY profile with real historical observations exists, the dashboard
uses DXY as the preferred seasonal input for USD scoring. The older UUP-based
USD profile remains available as a fallback.

## Calculations

For DXY, the dashboard calculates:

- 5-year monthly average return
- 10-year monthly average return
- 20-year monthly average return
- monthly win rate
- sample years
- reliability
- seasonal curve
- current-month, six-month and yearly tendency

## Refresh schedule

- Currencies: first day of each month at 03:10 UTC
- Assets: first day of each month at 03:20 UTC
- Dollar Index: first day of each month at 03:30 UTC

Leave at least one minute between manual refresh buttons on the free plan.

## Update GitHub

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep the existing `app/providers/` folder.

Commit with:

`Version 8.2 Dollar Index`
