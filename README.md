# Version 9.2 — Trading 212 Cash Parser Fix

## Fault identified

Trading 212 returned the account `cash` field as a nested JSON object. The
previous parser attempted to call `float()` on that object, causing:

```text
TypeError: float() argument must be a string or a real number, not 'dict'
```

## Fix

- Safely reads numbers from nested Trading 212 response objects
- Supports `total`, `available`, `free`, `value`, `amount`, `cash`,
  `currentValue` and `totalValue`
- Safely reads nested account currency fields
- Falls back to calculated portfolio value when the API omits a total
- Keeps the structured error diagnostics from Version 9.1
- Keeps the 104-week COT chart and background-candle view

## Update GitHub

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep `app/providers/`.

Commit:

`Version 9.2 Trading 212 cash parser fix`
