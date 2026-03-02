# Deploying LHP Web App to Databricks Apps

This guide covers deploying the Lakehouse Plumber (LHP) web application — FastAPI backend + React 19 frontend + OpenCode AI sidecar — as a **single Databricks App** using DABs (Databricks Asset Bundles).

## Architecture

```
Databricks App (single process)
  start.py → reads $DATABRICKS_APP_PORT → uvicorn
    FastAPI
      /api/*   → 15 API routers (existing backend)
      /*       → SPAStaticFiles(dist/) for React SPA (custom catch-all)
    OpenCode subprocesses (per-user, on localhost ports)
      auth: fresh OAuth token injected at spawn via databricks-sdk
      max lifetime: 50 min (reaped before 1h token expiry)
```

**Key design decisions:**
- React SPA is pre-built and served as static files by FastAPI (no separate web server)
- Custom `SPAStaticFiles` class provides catch-all routing for React Router (Starlette's `StaticFiles(html=True)` does NOT support SPA fallback — see Lessons Learned #8)
- OpenCode (Node.js AI assistant) is spawned per-user as a subprocess
- OAuth tokens are obtained via `databricks-sdk` (zero-config inside Databricks Apps)
- No PATs or secrets in config — the app's service principal handles auth

## Prerequisites

1. **Databricks CLI** with DABs support:
   ```bash
   brew install databricks/tap/databricks
   # or: pip install databricks-cli
   ```

2. **Node.js** (v18+) for building the frontend:
   ```bash
   node --version  # Should be >= 18
   ```

3. **Python** (3.11+) with `build` package:
   ```bash
   pip install build
   ```

4. **Databricks workspace** with:
   - Databricks CLI configured with a named profile (`databricks configure --profile <name>`)
   - (Optional) A Model Serving endpoint for AI chat — must be created separately, NOT declared as a DABs resource dependency (see Lessons Learned #4)

## What Was Created / Modified

### Modified Files (6)

| File | Change |
|------|--------|
| `src/lhp/api/config.py` | Added `static_dir: Optional[str]` setting (env: `LHP_STATIC_DIR`) |
| `src/lhp/api/app.py` | Custom `SPAStaticFiles` mount after all API routers (SPA catch-all fallback) |
| `src/lhp/api/auth.py` | Uses correct Databricks proxy headers: `X-Forwarded-Email`, `X-Forwarded-Preferred-Username`, `X-Forwarded-User` |
| `src/lhp/api/services/opencode_manager.py` | OAuth token helper, `created_at` tracking, env injection for Databricks Apps, `node_modules/.bin/` binary fallback, max-lifetime reaping (50 min) |
| `src/lhp/api/services/ai_config.py` | `DATABRICKS_HOST` fallback for deriving `ANTHROPIC_BASE_URL` |
| `pyproject.toml` | Added `deploy` optional dep group (`databricks-sdk>=0.38.0`) |

### New Files (7)

| File | Purpose |
|------|---------|
| `deploy/start.py` | Entry point — reads `$DATABRICKS_APP_PORT`, starts uvicorn |
| `deploy/app.yaml` | Databricks App config (env vars, start command) |
| `deploy/package.json` | Node.js dependency for OpenCode (`opencode-ai`, not `opencode` — see Lessons Learned #3) |
| `deploy/databricks.yml` | DABs bundle config (targets: dev, prod) with `sync.include` for gitignored build artifacts |
| `deploy/resources/lhp_app.yml` | DABs app resource definition (no serving endpoint dependency) |
| `scripts/build_deploy.sh` | Build + package script (frontend, wheel, static copy, timestamped requirements.txt) |
| `.gitignore` (updated) | Excludes `deploy/static/`, `deploy/*.whl`, `deploy/node_modules/` |

## Build & Deploy Steps

### 1. Bump the version (required for code changes)

The Databricks Apps runtime skips `pip install` if the wheel version hasn't changed. **Always bump the version** before deploying code changes:

```bash
# In pyproject.toml, bump version e.g.:
# version = "0.7.7.dev1"  →  "0.7.7.dev2"
```

Use `.devN` suffixes for iteration (e.g., `0.7.7.dev1`, `0.7.7.dev2`). See Lessons Learned #7.

### 2. Build the deployment package

```bash
./scripts/build_deploy.sh
```

This does:
1. `cd web_app && npm ci && npm run build` → builds React into `web_app/dist/`
2. `python -m build --wheel` → builds Python wheel into `deploy/`
3. Copies `web_app/dist/` → `deploy/static/`
4. Generates `deploy/requirements.txt` pointing to the local wheel with `[api,deploy]` extras (includes build timestamp to trigger re-install)

**Partial rebuild flags:**
```bash
./scripts/build_deploy.sh --skip-frontend  # Skip npm build
./scripts/build_deploy.sh --skip-wheel     # Skip Python wheel build
```

### 3. Validate the bundle

```bash
cd deploy
databricks bundle validate -t dev
```

### 4. Deploy to Databricks (two steps)

```bash
# Step 1: Upload files + create/update app resource
cd deploy
databricks bundle deploy -t dev

# Step 2: Trigger the app to install deps + start
databricks apps deploy lhp-web-app \
  --source-code-path /Workspace/Users/<your-email>/.bundle/lhp-web-app/dev/files \
  --profile <your-profile>
```

> **Important:** `bundle deploy` and `apps deploy` are separate operations. `bundle deploy` only uploads files — you must also run `apps deploy` to trigger the app restart. See Lessons Learned #6.

### 5. Check status

```bash
databricks apps get lhp-web-app --profile <your-profile>
```

### 6. View logs if something fails

```bash
databricks apps logs lhp-web-app --tail-lines 100 --profile <your-profile>
```

## Configuration

### Environment Variables (set in `deploy/app.yaml`)

| Variable | Default | Description |
|----------|---------|-------------|
| `LHP_DEV_MODE` | `false` | Must be `false` for production |
| `LHP_AI_ENABLED` | `true` | Enables OpenCode AI assistant |
| `LHP_AI_MODEL` | `anthropic/databricks-claude-sonnet-4-6` | AI model identifier (`provider/model-slug`). Auto-added to allowed list. |
| `LHP_STATIC_DIR` | `./static` | Path to pre-built React files |
| `LHP_SOURCE_REPO` | (git URL) | Repo cloned per-user for workspaces |
| `ANTHROPIC_API_KEY` | `unused` | Placeholder (actual auth via OAuth) |
| `OPENCODE_DISABLE_CLAUDE_CODE_SKILLS` | `1` | Disable Claude Code's built-in skills |
| `OPENCODE_DISABLE_CLAUDE_CODE` | `1` | Prevent Claude Code CLI interference |

### Automatic (injected by Databricks Apps runtime)

| Variable | Description |
|----------|-------------|
| `DATABRICKS_APP_PORT` | Port the app must listen on |
| `DATABRICKS_HOST` | Workspace URL (e.g. `https://myworkspace.azuredatabricks.net`) |
| `DATABRICKS_CLIENT_ID` | Service principal client ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal secret |

### HTTP Headers (injected by Databricks Apps reverse proxy)

These headers are injected by the Databricks Apps reverse proxy for every authenticated request. The app uses them in `src/lhp/api/auth.py` to identify the user:

| Header | Description | Maps to |
|--------|-------------|---------|
| `X-Forwarded-Email` | User email from IdP | `UserContext.email` |
| `X-Forwarded-Preferred-Username` | Username from IdP | `UserContext.username` |
| `X-Forwarded-User` | User identifier from IdP | `UserContext.user_id` |
| `X-Forwarded-Host` | Original host/domain | (not used by LHP) |
| `X-Real-Ip` | Client IP address | (not used by LHP) |
| `X-Request-Id` | Request UUID | (not used by LHP) |

> **Important:** There is NO `X-Forwarded-User-Id` header. The user ID comes from `X-Forwarded-User`. See [Azure Databricks docs](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/http-headers) and Lessons Learned #10.

### Customizing the source repo

Edit `deploy/app.yaml` to change `LHP_SOURCE_REPO` to your own LHP project repo:
```yaml
  - name: LHP_SOURCE_REPO
    value: "https://github.com/your-org/your-lhp-project.git"
```

Or use the DABs variable:
```yaml
  - name: LHP_SOURCE_REPO
    value: "${var.source_repo}"
```

## How OAuth Works

When running inside Databricks Apps (`DATABRICKS_CLIENT_ID` is present in the environment):

1. **On each OpenCode spawn**, `_get_databricks_oauth_token()` is called
2. It uses `databricks-sdk`'s `WorkspaceClient()` (zero-config — auto-detects credentials)
3. The fresh token is injected as `ANTHROPIC_AUTH_TOKEN` into the subprocess env
4. `ANTHROPIC_BASE_URL` is auto-derived from `DATABRICKS_HOST` as `{host}/serving-endpoints/anthropic`
5. Processes are reaped after **50 minutes** (safety margin before 1h OAuth token expiry)

When **not** inside Databricks Apps (local dev/prod simulation):
- `DATABRICKS_CLIENT_ID` is absent → OAuth block is skipped entirely
- Uses whatever `ANTHROPIC_AUTH_TOKEN` / `ANTHROPIC_BASE_URL` are in the environment
- No max-lifetime reaping (only idle timeout applies)

## Backward Compatibility

| Scenario | Impact |
|----------|--------|
| `lhp serve` (dev mode) | `static_dir=None` → no static mount. No OAuth. **No change.** |
| `prod_services.sh` (local sim) | Uses env vars directly. No OAuth. **No change.** |
| `pytest tests/` | `databricks-sdk` not in test deps → `ImportError` caught. **No change.** |

## Troubleshooting

### App shows blank page
- Check `LHP_STATIC_DIR` points to a directory containing `index.html`
- Verify `deploy/static/` was populated by the build script

### SPA deep routes return 404 JSON
- Ensure `app.py` uses `SPAStaticFiles` (not plain `StaticFiles`) for the catch-all mount
- `StaticFiles(html=True)` does NOT serve `index.html` for deep routes like `/pipelines`

### All authenticated endpoints return 401
- Check that you're accessing the app through the Databricks workspace proxy (not direct URL)
- Verify `auth.py` reads the correct headers: `X-Forwarded-Email`, `X-Forwarded-Preferred-Username`, `X-Forwarded-User` (NOT `X-Forwarded-User-Id`)
- Direct URL access (curl, Playwright) will always 401 — the proxy injects headers only for authenticated Databricks users

### Wheel updates not taking effect
- You MUST bump the version in `pyproject.toml` before each deploy
- Use `.devN` suffixes: `0.7.7.dev1`, `0.7.7.dev2`, etc.
- `--force-reinstall` in requirements.txt does NOT work (runtime rejects it)

### AI chat doesn't work
- Check the Model Serving endpoint exists and is running
- Check app logs for OAuth token errors
- Ensure the app's service principal has `CAN_QUERY` on the endpoint

### OpenCode binary not found
- Verify `deploy/package.json` includes `opencode-ai` (NOT `opencode`)
- Check that `npm install` ran successfully at deploy time
- Look for `./node_modules/.bin/opencode` in the app's working directory

### Deploy blocked by previous failure
- Wait 2-5 minutes for compute to become ACTIVE, then retry
- Check status: `databricks apps get lhp-web-app --profile <profile>`

## Quick Deploy Checklist

```bash
# 1. Bump version in pyproject.toml (required for code changes)
# 2. Build (from repo root)
./scripts/build_deploy.sh

# 3. Validate
cd deploy
databricks bundle validate -t dev

# 4. Deploy bundle (uploads files + manages app resource)
databricks bundle deploy -t dev

# 5. Deploy app (triggers install + start)
databricks apps deploy lhp-web-app \
  --source-code-path /Workspace/Users/mehdi.modarressi@databricks.com/.bundle/lhp-web-app/dev/files \
  --profile field-eng

# 6. Check status
databricks apps get lhp-web-app --profile field-eng

# 7. View logs if something fails
databricks apps logs lhp-web-app --tail-lines 100 --profile field-eng
```

## Current Working Configuration

| File | Key Setting |
|------|-------------|
| `databricks.yml` | `workspace.profile: field-eng`, `sync.include: ["static/**", "*.whl"]` |
| `resources/lhp_app.yml` | No serving endpoint dependency (removed) |
| `package.json` | `"opencode-ai": "^1.2.15"` |
| `app.yaml` | `LHP_SOURCE_REPO: https://github.com/Mmodarre/acme_supermarkets_lhp_sample.git` |

**App URL:** `https://lhp-web-app-984752964297111.11.azure.databricksapps.com`

## Production Deployment

For production, change the target:
```bash
cd deploy
databricks bundle deploy -t prod
```

The `prod` target uses `mode: production` which enables additional safeguards in DABs.
