# Peloton Data Pipeline (DEPRECATED)

> **This repo is no longer live.** The canonical pipeline is `scottieb3/peloton-motherduck`.
> The scheduled workflow has been disabled. Do not run the pipeline or workflow here —
> it shares the Peloton refresh-token chain, and any run will break the canonical repo's secret.

A robust data pipeline that fetches your Peloton workout history and syncs it to a MotherDuck database.

## Features

- **Auto-Authentication**: Automatically manages OAuth token lifecycle (refreshing when expired).
- **Incremental Sync**: Queries the destination database to only fetch new workouts.
- **Data Enrichment**: Fetches detailed metrics and ride metadata for each workout.
- **GitHub Actions Support**: Includes a workflow for daily automated runs.

## Setup

### Local Development

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configuration**:
    - Ensure `peloton_tokens.json` is present in the root directory (generated via initial login).
    - Copy `.env.example` to `.env` and fill in your `MOTHERDUCK_TOKEN`.

    > **Note**: Running locally rotates the Peloton refresh token, which invalidates the token stored in GitHub Secrets. After a local run, update the GitHub secret with your refreshed token:
    > ```bash
    > gh secret set PELOTON_TOKENS_JSON --repo <owner>/<repo> < peloton_tokens.json
    > ```

3.  **Run**:
    ```bash
    python peloton_pipeline.py
    ```

### GitHub Actions Deployment

1.  **Repository Secrets**:
    Go to your GitHub repository settings -> Secrets and variables -> Actions, and add:
    - `MOTHERDUCK_TOKEN`: Your MotherDuck service token.
    - `PELOTON_TOKENS_JSON`: The **content** of your local `peloton_tokens.json` file.
    - `GH_PAT`: A GitHub Personal Access Token (classic) with `repo` scope. This is required so the workflow can update `PELOTON_TOKENS_JSON` after each run to persist rotated refresh tokens.

2.  **Workflow**:
    The pipeline is configured to run daily at 6:00 AM UTC via `.github/workflows/peloton_pipeline.yml`. After each run, it automatically updates the `PELOTON_TOKENS_JSON` secret with the refreshed tokens so the next run has a valid refresh token.

## Token Recovery (invalid_grant / "Unknown or invalid refresh token")

If the refresh-token chain breaks (e.g., the Action fails for several consecutive days, or tokens were rotated elsewhere), refresh can no longer recover on its own. Re-seed it:

1.  Ensure `.env` contains `PELOTON_USERNAME` and `PELOTON_PASSWORD`.
2.  Mint fresh tokens (headless login, no browser needed):
    ```bash
    python peloton_login.py
    ```
3.  Update the GitHub secret with the new file:
    ```bash
    gh secret set PELOTON_TOKENS_JSON --repo <owner>/<repo> < peloton_tokens.json
    ```
    (Or paste the file contents into the secret via repo Settings → Secrets → Actions.)
4.  Trigger the workflow manually (Actions → Peloton Data Pipeline → Run workflow) to verify.
