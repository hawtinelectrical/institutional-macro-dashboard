# Version 7.1 Global Markets Hotfix

## Fixed

- Reduces the free Twelve Data request from 15 symbols to 8.
- Matches the free plan's 8 API credits per minute.
- Caches Global Markets data for 15 minutes.
- Prevents repeated page loads and `/api/intelligence` calls from consuming more credits.
- Handles HTTP 429 without crashing the dashboard.
- Uses cached data when available.
- Shows a clear wait-and-refresh message when no cache exists.

## Security

The API key appeared in a shared Render log. Revoke that key in Twelve Data and create a new one before continuing.

## Update GitHub

Replace:

- `app/main.py`
- `README.md`

Commit with:

`Version 7.1 markets rate-limit hotfix`
