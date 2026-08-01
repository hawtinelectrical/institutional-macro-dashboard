# Version 7.4 — Economic Calendar and Daily Market Brief

## Economic Calendar

Works immediately without a paid provider:

- Add dated events manually
- Cloud storage in PostgreSQL
- Country and currency
- High, Medium or Low impact
- Actual, forecast and previous
- Automatic affected-market labels
- Delete manual events
- Visible across devices

If `TRADING_ECONOMICS_API_KEY` is added later, provider events merge automatically with manual entries.

## Daily Market Brief

Combines:

- COT Commercial / Non-commercial relationships
- FRED US yields and real yields
- Free Global Markets regime
- Financial news importance
- Economic calendar risk

Displays:

- Market regime
- Strongest and weakest COT relationships
- Preferred currency pair for technical review
- Next high-impact event
- Evidence summary
- Explicit “not confirmed” status until TradingView HTF/MTF/LTF alignment

## Update GitHub

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep `app/providers/` unchanged.

Commit with:

`Version 7.4 calendar and daily brief`
