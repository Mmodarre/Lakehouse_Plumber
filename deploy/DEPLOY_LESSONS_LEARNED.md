# Deployment Lessons Learned

Issues encountered during the first deployment of LHP to Databricks Apps, and how to avoid them next time.

---

## 1. DABs respects `.gitignore` — build artifacts won't upload

**Problem:** The build script produces `deploy/*.whl` and `deploy/static/` but `.gitignore` excludes them. When `databricks bundle deploy` runs, it skips these files entirely. The app then fails at startup because `requirements.txt` references a wheel that was never uploaded.

**Symptom:**
```
WARNING: Requirement './lakehouse_plumber-0.7.6-py3-none-any.whl[api,deploy]' looks like a filename, but the file does not exist
ERROR: Could not install packages due to an OSError: [Errno 2] No such file or directory
```

**Fix:** Add `sync.include` in `databricks.yml` to force-include gitignored build artifacts:
```yaml
sync:
  include:
    - "static/**"
    - "*.whl"
```

**Prevention:** Any time a build step produces files that are gitignored (wheels, compiled assets, bundled JS), add them to `sync.include` in the bundle config.

---

## 2. Databricks CLI requires an explicit profile or DEFAULT

**Problem:** `databricks.yml` had no workspace/profile configured. The CLI tried to use the `DEFAULT` profile from `~/.databrickscfg`, which was empty.

**Symptom:**
```
Error: resolve: /Users/.../.databrickscfg has no DEFAULT profile configured
```

**Fix:** Add the profile to the target in `databricks.yml`:
```yaml
targets:
  dev:
    mode: development
    default: true
    workspace:
      profile: field-eng
```

**Prevention:** Always specify `workspace.profile` (or `workspace.host`) in each DABs target. Don't rely on `DEFAULT` profile existing.

---

## 3. npm package name was wrong (`opencode` vs `opencode-ai`)

**Problem:** `deploy/package.json` listed `"opencode": "^1.2.14"` but the actual npm registry package is `opencode-ai`. The `opencode` package does not exist on npm.

**Symptom:**
```
npm error code E404
npm error 404 Not Found - GET https://registry.npmjs.org/opencode - Not found
npm error 404  'opencode@^1.2.14' is not in this registry.
```

**Fix:** Change `package.json` to use the correct package name:
```json
{
  "dependencies": {
    "opencode-ai": "^1.2.15"
  }
}
```

Note: The binary is still called `opencode` (check with `npm view opencode-ai bin`), so backend code doesn't need changes.

**Prevention:** Always verify npm package names with `npm view <name>` before adding them to `package.json`. The CLI binary name and the npm package name are often different.

---

## 4. Serving endpoint resource dependency blocks deployment

**Problem:** `resources/lhp_app.yml` declared a dependency on a serving endpoint named `anthropic`. If this endpoint doesn't exist in the target workspace, `databricks bundle deploy` fails at the Terraform apply step.

**Symptom:**
```
Error: failed to create app
  Endpoint with name 'anthropic' does not exist.
```

**Fix:** Either:
- Remove the `resources` block from `lhp_app.yml` if the endpoint isn't needed for deployment
- Ensure the endpoint exists before deploying
- Make the endpoint name configurable via a DABs variable

**Prevention:** Serving endpoint references in DABs are hard dependencies — the endpoint must exist at deploy time. For optional features (like AI chat), consider making the endpoint reference conditional or documenting it as a prerequisite that must be created first.

---

## 5. Failed deployments can block new ones

**Problem:** After a deployment fails, the app can enter a state where new deploys are rejected with "Cannot deploy app as there is an active deployment in progress", even though the active deployment shows as CANCELLED or FAILED.

**Symptom:**
```
Error: Cannot deploy app lhp-web-app as there is an active deployment in progress.
```

**Fix:** Wait for the compute status to become `ACTIVE` (can take 2-5 minutes after a failed deploy), then retry. Check with:
```bash
databricks apps get lhp-web-app --profile field-eng -o json | python3 -c "
import json, sys
data = json.load(sys.stdin)
print('Compute:', data['compute_status']['state'])
print('Deploy:', data['active_deployment']['status']['state'])
"
```

**Prevention:** Be patient between deploy attempts. Poll the app status before retrying rather than immediately re-running the deploy command.

---

## 6. `databricks bundle deploy` vs `databricks apps deploy` are different steps

**Problem:** `databricks bundle deploy` uploads files to the workspace and creates/updates the app resource via Terraform, but it does **not** trigger the app to pick up the new source code. A separate `databricks apps deploy` (or starting the app) is needed.

**Flow:**
```
databricks bundle deploy -t dev          # Uploads files + creates app resource
databricks apps deploy lhp-web-app \     # Triggers the app to install deps + start
  --source-code-path /Workspace/.../.bundle/lhp-web-app/dev/files
```

**Prevention:** After `bundle deploy`, always run `apps deploy` with the correct `--source-code-path` pointing to the bundle's files location. The path follows the pattern:
```
/Workspace/Users/<email>/.bundle/<bundle-name>/<target>/files
```

---

## 7. Same-version wheel updates are silently skipped

**Problem:** When you rebuild the wheel without bumping the version, the Databricks Apps runtime detects that `requirements.txt` hasn't changed (or the version is the same) and skips `pip install`. Your code changes are never applied.

**Symptom:**
```
lakehouse-plumber is already installed with the same version as the provided wheel. Use --force-reinstall to force an installation of the wheel.
```

**Fix:** The `build_deploy.sh` script now adds a `# build: <timestamp>` comment to `requirements.txt`. This changes the file content each build, triggering the runtime to re-run pip. However, pip itself still skips same-version wheels. For code changes to take effect, you **must bump the version** in `pyproject.toml` (e.g., `0.7.7.dev1`).

**Note:** `--force-reinstall` in requirements.txt does NOT work — the Databricks Apps runtime rejects it as an invalid requirement.

**Prevention:** Always bump the version when deploying code changes. Use `.devN` suffixes for iteration (e.g., `0.7.7.dev1`, `0.7.7.dev2`).

---

## 8. `StaticFiles(html=True)` does NOT provide SPA fallback

**Problem:** Starlette's `StaticFiles(html=True)` serves `index.html` for `/` but returns 404 for deep routes like `/pipelines`. It only resolves `index.html` for directory paths, not as a catch-all.

**Symptom:** Root `/` loads the React SPA but any client-side route like `/pipelines` returns `{"detail":"Not Found"}`.

**Fix:** Replace `StaticFiles` with a custom `SPAStaticFiles` subclass that catches file-not-found exceptions and returns `index.html`:
```python
class SPAStaticFiles(StaticFiles):
    async def get_response(self, path, scope):
        try:
            return await super().get_response(path, scope)
        except Exception:
            return FileResponse(Path(self.directory) / "index.html", media_type="text/html")
```

**Prevention:** Any SPA deployment needs explicit catch-all routing. Never rely on `html=True` alone for React/Vue/Angular apps.

---

## 9. Accessing the app URL directly gives 401 on authenticated endpoints

**Problem:** Databricks Apps rely on the workspace proxy to inject OAuth tokens. When accessing the app URL directly (e.g., via Playwright or curl), there are no auth headers, so authenticated endpoints return 401.

**What works without auth:** `/api/health`, `/api/version`, `/api/ai/status`, `/api/docs`, `/api/openapi.json`, static files, SPA routes.

**What requires Databricks OAuth:** `/api/workspace`, `/api/project/*`, `/api/pipelines/*`, and all data-modifying endpoints.

**For testing:** Use Playwright to verify the SPA loads, static files serve, and unauthenticated endpoints respond. For full integration testing, access the app through the Databricks workspace UI.

---

## 10. Auth headers used wrong Databricks proxy header names

**Problem:** The `src/lhp/api/auth.py` code expected `X-Forwarded-User-Id` for the user identifier, but Databricks Apps does NOT inject that header. The actual headers injected by the Databricks Apps reverse proxy are:

| Header | Description |
|--------|-------------|
| `X-Forwarded-Email` | User email from IdP |
| `X-Forwarded-Preferred-Username` | Username from IdP |
| `X-Forwarded-User` | **User identifier** from IdP |

The original code had:
```python
email = request.headers.get("X-Forwarded-Email")       # ✅ correct
username = request.headers.get("X-Forwarded-User")      # ❌ this is actually user ID
user_id = request.headers.get("X-Forwarded-User-Id")    # ❌ doesn't exist!
```

Since the code checked `all([email, username, user_id])` and `user_id` was always `None`, auth **always failed** — even through the Databricks proxy.

**Symptom:** Every authenticated endpoint returns 401, even when accessed through the Databricks workspace UI (where the proxy should inject valid headers).

**Fix:** Map to the correct Databricks proxy headers:
```python
email = request.headers.get("X-Forwarded-Email")
username = request.headers.get("X-Forwarded-Preferred-Username")
user_id = request.headers.get("X-Forwarded-User")
```

Also updated: `tests/api/test_auth.py`, `tests/api/conftest.py`, `web_app/vite.config.ts`.

**Prevention:** Always verify header names against the [official Databricks docs](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/http-headers). The header names are NOT obvious — `X-Forwarded-User` is the **user ID**, not the username.

---

## Quick Deploy Checklist

```bash
# 1. Build (from repo root)
./scripts/build_deploy.sh

# 2. Validate
cd deploy
databricks bundle validate -t dev

# 3. Deploy bundle (uploads files + manages app resource)
databricks bundle deploy -t dev

# 4. Deploy app (triggers install + start)
databricks apps deploy lhp-web-app \
  --source-code-path /Workspace/Users/mehdi.modarressi@databricks.com/.bundle/lhp-web-app/dev/files \
  --profile field-eng

# 5. Check status
databricks apps get lhp-web-app --profile field-eng

# 6. View logs if something fails
databricks apps logs lhp-web-app --tail-lines 100 --profile field-eng
```

---

## Current Working Configuration

| File | Key Setting |
|------|-------------|
| `databricks.yml` | `workspace.profile: field-eng`, `sync.include: ["static/**", "*.whl"]` |
| `resources/lhp_app.yml` | No serving endpoint dependency (removed) |
| `package.json` | `"opencode-ai": "^1.2.15"` |
| `app.yaml` | `LHP_SOURCE_REPO: https://github.com/Mmodarre/acme_supermarkets_lhp_sample.git` |

**App URL:** `https://lhp-web-app-984752964297111.11.azure.databricksapps.com`
