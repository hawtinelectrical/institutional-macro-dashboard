# Version 7.6 — TradingView Setup Centre

## Added

- Cloud-saved pair watchlist
- Long or Short direction
- HTF Bullish / Bearish / Neutral
- MTF CHoCH confirmation
- LTF CHoCH confirmation
- BOS confirmation
- Pullback-ready confirmation
- Manual invalidation
- Setup notes
- Automatic technical score
- Not Ready / Watch / Ready / Invalidated status
- Macro-versus-technical alignment
- 40% macro and 60% technical combined score
- “Waiting for” checklist
- Load the current top-ranked pair into the setup form

## Status rules

- **Ready:** HTF aligned, MTF CHoCH, LTF CHoCH, BOS and pullback are all complete.
- **Watch:** HTF is aligned and at least one further confirmation is present.
- **Not Ready:** insufficient alignment.
- **Invalidated:** manually cancelled.

This remains a decision-support tool. Ready does not replace price review, risk management or event checks.

## Update GitHub

Replace:

- `app/main.py`
- `app/static/index.html`
- `README.md`

Keep the existing `app/providers/` folder.

Commit with:

`Version 7.6 TradingView setup centre`
