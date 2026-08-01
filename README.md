# Version 8.4 — Seasonality Backtesting and Current-Year Comparison

## Added

### Backtesting statistics

- Median monthly return
- Historical volatility
- Best historical outcome
- Worst historical outcome
- Positive and negative year counts
- Drawdown proxy
- Reliability grade from A+ to D

### Current-year comparison

- Current-year monthly return where available
- Divergence from historical median
- Current year overlaid against the 10-year seasonal pattern
- Current month remains highlighted

### Seasonal windows

- Strongest two-month windows
- Strongest three-month windows
- Ranked historical average return

### Direct pair seasonality

- Uses a direct pair profile when one exists
- Otherwise derives base-minus-quote currency seasonality
- Current month highlighted
- Pair seasonal bias displayed separately from the macro score

## Methodology

Backtesting uses monthly close-to-close historical returns. Median and
volatility are calculated separately from the mean seasonal profile.

The drawdown value is a proxy calculated from the sequence of historical
monthly observations for that calendar month; it is not an intramonth price
drawdown.

Current-year comparison is only displayed when the provider has returned a
completed monthly observation.

## Update GitHub

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep the existing `app/providers/` folder.

Commit with:

`Version 8.4 seasonality backtesting`
