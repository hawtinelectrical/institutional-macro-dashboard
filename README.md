# Version 8 — Seasonality and Institutional Alignment

## Seasonality Engine

- Monthly heatmaps
- 5-year average return
- 10-year average return
- 20-year average return
- Win rate
- Sample years
- Reliability score
- Cloud-stored profiles
- Cell editor
- Seasonal curves
- Current-month leaders
- Six-month and yearly averages
- Seasonality incorporated into currency and pair scoring

Daily and weekly displays use the current monthly seasonal backdrop. They are
not presented as separate historical daily or weekly studies unless that data
is later supplied.

## Institutional Alignment Engine

Combines:

- Commercial / Non-commercial COT relationship
- Seasonality and reliability
- Financial news
- Economic calendar
- FRED rates evidence
- Global risk regime
- Saved TradingView technical setup

Outputs:

- Currency alignment ranking
- Pair alignment ranking
- Macro score
- Technical score
- Combined alignment
- Conflicting evidence
- Macro Only / Watch / Trade Ready / Invalidated status

## Update GitHub

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep the existing `app/providers/` folder.

Commit with:

`Version 8 seasonality and alignment`
