# Version 9.1 — Trading 212 Stability and 104-Week COT

## Trading 212 fixes

- Prevents `Unexpected token 'I' ... is not valid JSON`
- Every portfolio error now returns structured JSON
- Shows exact diagnostics for:
  - incorrect key or secret
  - live/demo mismatch
  - missing read permissions
  - rate limits
  - timeout
  - network errors
  - invalid API responses
- Dividend-history errors no longer break holdings and account data
- Adds a visible connection-diagnostics panel
- No credentials are displayed or sent to the browser

## COT chart

- Displays the most recent 104 weeks
- Commercial and Non-commercial lines use the same 104-week range
- Background DXY/futures candles use that same two-year range
- 104 weeks is the default fixed view
- Candle opacity and visibility controls remain available

## Replace in GitHub

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep `app/providers/`.

Commit:

`Version 9.1 Trading 212 stability and 104 week COT`
