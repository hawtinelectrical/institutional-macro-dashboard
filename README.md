# Version 8.6 — My Portfolio

Adds a read-only Trading 212 portfolio page using the official Public API.

## Features

- Total account value
- Cash balance
- Position value
- Unrealised profit/loss
- Holdings and allocation
- Winners and losers
- Recent dividends
- Manual refresh
- Two-minute server cache

## Render variables

```text
TRADING212_API_KEY=<read-only key>
TRADING212_API_SECRET=<secret>
TRADING212_ENVIRONMENT=live
```

Use `demo` for paper trading.

## Security

The build calls only account summary, positions and dividend-history endpoints.
There are no order-placement, amendment or cancellation routes.

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep `app/providers/`.

Commit: `Version 8.6 My Portfolio`
