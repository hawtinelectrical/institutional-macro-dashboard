# Version 7.3 — News Intelligence

## Live news sources

- BBC Business — financially filtered
- Federal Reserve press releases
- ECB press releases
- ECB speeches
- Bank of England news
- U.S. Bureau of Labor Statistics latest releases

## Added

- RSS and Atom feed support
- Independent source-failure handling
- Source health dashboard
- Critical / High / Medium / Low importance
- Source, category and market filters
- Countries, affected markets and likely effects
- “What Changed Today?” summary
- High-importance headline shortlist
- Duplicate-title removal
- Up to 140 financially classified stories

## Reuters

Reuters is not included because a reliable public RSS/API feed with suitable reuse rights is not available for this implementation. It can be added later through a licensed provider.

## Update GitHub

Replace:

- `app/providers/official_news.py`
- `app/main.py`
- `app/static/index.html`
- `README.md`

Commit with:

`Version 7.3 news intelligence`
