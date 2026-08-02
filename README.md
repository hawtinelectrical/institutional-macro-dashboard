# Version 10 — Pair-Structure Currency Strength Matrix

## Correct scoring method

The Currency Strength Matrix is now calculated from all 28 daily currency-pair
charts, not from individual currency futures.

Each pair is analysed from its two most recent confirmed highs and two most
recent confirmed lows.

### Pattern A

Higher highs and higher lows:

- Base currency: +1
- Quote currency: -1

### Pattern B

Lower highs and lower lows:

- Base currency: -1
- Quote currency: +1

### Pattern C

Higher highs and lower lows:

- Base currency: 0
- Quote currency: 0

### Pattern D

Lower highs and higher lows:

- Base currency: 0
- Quote currency: 0

Every currency is represented in seven pairs, so its total score ranges from
-7 to +7.

## Automatic updates

- Uses completed daily candles only
- Approximately two months of daily history are analysed
- Two candles on either side confirm a swing
- Recalculated at server startup
- Recalculated automatically every six hours
- Results are cached server-side
- Browser reloads do not trigger 28 new provider calls

## Data hierarchy

- Stooq is used first for the 28 ordinary FX pairs
- Twelve Data is a fallback only
- This reduces Twelve Data credit usage substantially

## Navigation

The sidebar now groups these pages under **COT Data**:

- COT Positioning
- Currency Intelligence
- Currency Strength Matrix
- Pair Rankings
- Seasonality

## Pair opportunities

The system ranks the 28 tradable pairs by the absolute difference between the
base and quote currency totals.

The result is a directional shortlist, not an automatic entry signal.

## Existing features retained

- Exactly 104 COT weeks
- Exactly 104 matching COT background candles
- Synthetic DXY
- Trading 212 parser fixes and diagnostics
- Seasonality backtesting
- Resilient market-data caching

## Update GitHub

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep:

- `app/providers/`

Commit:

`Version 10 pair structure currency matrix`
