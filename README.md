# Version 8.3 — Current Month Highlight

## Added to the seasonality heatmap

- Current month column highlighted from header to bottom
- Yellow border around the complete current-month column
- “CURRENT MONTH” label above the active month
- Subtle glow without hiding positive or negative heatmap colours
- Current-month legend key

## Added to the seasonal curve

- Shaded vertical current-month band
- Yellow vertical reference line
- Current month label above the chart
- Highlighted 5Y, 10Y and 20Y curve markers
- Marker rings for easier chart scanning
- Current-month caption below the chart

The highlighted month is calculated automatically from the browser date, so it
moves to the next column when the calendar month changes.

## Update GitHub

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep the existing `app/providers/` folder.

Commit with:

`Version 8.3 current month highlight`
