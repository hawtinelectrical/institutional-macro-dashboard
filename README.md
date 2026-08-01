# Institutional Macro Dashboard — Cloud v4

Version 4 adds:

- dashboard navigation
- institutional COT-flow currency ranking
- strongest-versus-weakest pair opportunities
- pair comparison matrix
- selected-currency 10-week trend chart
- money-flow summary cards
- original detailed 10-week COT table
- Bonds & Rates placeholder page for the next live module

## Update the live site

Replace these files in GitHub:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Commit the changes. Render auto-deploys the update.

## Strength model

The v4 ranking follows the selected interpretation:

- rising Commercial net positioning adds bullish flow
- rising Non-commercial net positioning is treated inversely
- raw flow = 10-week Commercial change minus 10-week Non-commercial change
- raw flow is scaled from 0 to 100 across the eight currencies

This is a transparent context model, not a guaranteed price forecast.
