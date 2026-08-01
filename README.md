# Institutional Macro Dashboard — Cloud v3

Upload the **contents of this folder** directly to the root of a new GitHub repository.

The repository root must show:

- `app/`
- `.python-version`
- `.gitignore`
- `README.md`
- `render.yaml`
- `requirements.txt`

Do not upload the outer folder itself.

## Deploy on Render

1. Create a new private GitHub repository.
2. Upload the six root items listed above.
3. In Render, select **New → Blueprint**.
4. Select the repository.
5. Confirm Render detects `render.yaml`.
6. Apply the Blueprint.

Render creates:

- `institutional-macro-dashboard`
- `institutional-macro-db`

The first CFTC refresh runs automatically after deployment.
