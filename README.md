# Institutional Market Intelligence Terminal — Version 6 Rebuilt

This rebuild is safe to deploy over the existing Version 5 PostgreSQL database.

## Migration safety

- Keeps the existing `cot_positions.currency` schema used by Version 5.
- Adds a separate `metal_cot_positions` table.
- Adds a separate `seasonality_values` table.
- Uses additive table creation only; it does not rename or drop the live COT table.

## Live without extra keys

- CFTC Legacy Futures-Only COT
- 8 major currencies
- Gold, silver, platinum and palladium COT
- Commercial and Non-commercial ON/OFF controls
- Net-position chart
- Weekly buying/selling chart
- Relationship interpretation
- BBC Business, World and UK RSS
- Cloud-stored seasonality
- Daily, weekly, monthly, 6-month and yearly outlook framework

## Safer interpretation

The dashboard distinguishes:

1. **Trend relationship**
   - Commercial selling + Non-commercial buying = bullish relationship
   - Commercial buying + Non-commercial selling = bearish relationship

2. **Reversal warning**
   - Opposing Commercial activity may become relevant at extremes.

3. **Trade confirmation**
   - Not confirmed until price and the TradingView HTF/MTF/LTF structure agree.

## Optional Render environment variables

- `FRED_API_KEY`
- `TRADING_ECONOMICS_API_KEY`

## Update the live site

Replace:

- `app/main.py`
- `app/static/index.html`
- `requirements.txt`
- `README.md`

Commit to GitHub and allow Render to redeploy.
