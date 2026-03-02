# LHP Docker — Architecture Summary

## Overview

The Docker setup packages the entire LHP web application — React frontend, FastAPI
backend, and OpenCode AI assistant — into a single container. The developer's LHP
project is mounted as a read-only volume at runtime.

## Container Layout

```
Docker container
├── /app/
│   ├── static/           ← Pre-built React SPA (from build stage)
│   ├── src/lhp/          ← Installed Python package
│   └── entrypoint.sh     ← Startup validation script
├── /project/             ← Developer's LHP project (volume mount, read-only)
├── /usr/bin/opencode     ← OpenCode CLI (globally installed via npm)
└── /tmp/
    └── lhp-opencode-*/   ← OpenCode runtime config + skills (ephemeral)
```

## Multi-Stage Build

The `Dockerfile` uses two stages to keep the final image lean:

### Stage 1: `frontend-builder` (node:20-alpine)

Builds the React SPA. Package files are copied first for layer caching, then the
full source is copied and built with `npm run build`. Output is a static `dist/`
directory.

### Stage 2: `runtime` (python:3.12-slim)

The production image:

1. **System dependencies** — `git` (GitPython), `curl` (healthcheck), `ca-certificates`
2. **Node.js 20** — Required because OpenCode is a Node.js CLI tool. The backend's
   `opencode_manager.py` uses `shutil.which("opencode")` to find the binary.
3. **OpenCode** — `npm install -g opencode-ai@1.2.15`. The npm package name is
   `opencode-ai` but the binary is `opencode`. Installed globally so it's on PATH
   regardless of working directory.
4. **Python package** — Installed with `.[api,ai]` extras. The `[deploy]` extra is
   skipped (saves ~50MB) since `databricks-sdk` isn't needed in dev mode — its import
   is guarded with try/except in `opencode_manager.py`.
5. **Static files** — Copied from stage 1 into `/app/static/`.

## Runtime Architecture

```
Developer machine                     Docker container (port 8000)
────────────────                      ──────────────────────────────
  LHP project dir ── volume (:ro) ──→ /project/
  .env file ─────── env_file ──────→ environment variables
                                      │
                                      entrypoint.sh (validates /project/lhp.yaml)
                                      └── uvicorn (PID 1)
                                           ├── /api/*  → FastAPI backend
                                           ├── /*      → SPAStaticFiles (React SPA)
                                           └── spawns: opencode serve --port 4096
                                                       (CWD=/project)
```

A single `uvicorn` process serves both the API and the pre-built React SPA via
FastAPI's `SPAStaticFiles` mount. The SPA catch-all returns `index.html` for any
path that doesn't match a static file, enabling React Router's client-side routing.

OpenCode runs as a subprocess managed by `OpenCodeProcessPool` inside the same
container. This is why it's a single-container design — splitting OpenCode into a
separate container would break the process lifecycle management.

## Key Design Decisions

### Single container (not multi-service)

OpenCode is spawned and managed as a child process by `OpenCodeProcessPool`. It's
not a standalone service — the backend controls its lifecycle (start, health checks,
shutdown). A separate container would require reimplementing this management.

### Read-write volume mount

The project is mounted read-write so that workspace operations (git status, file
editing via the UI, AI-assisted changes) can write to the project directory. This
mirrors how a developer would work locally.

### No `[deploy]` extras

The `databricks-sdk` package (~50MB) is only needed for Databricks Apps deployment
(OAuth token refresh). In dev mode, `ANTHROPIC_AUTH_TOKEN` is passed directly. The
import is guarded in `opencode_manager.py` so the code runs fine without it.

### Global OpenCode install

`npm install -g opencode-ai` puts the binary at `/usr/bin/opencode`, ensuring
`shutil.which("opencode")` finds it regardless of the current working directory.
A local `node_modules/.bin/` install wouldn't work since CWD is the mounted project.

## Entrypoint

`docker/entrypoint.sh` runs before uvicorn and:

1. **Validates** that `/project/lhp.yaml` exists — exits with a clear error if missing
2. **Warns** (non-fatal) if AI is enabled but `ANTHROPIC_BASE_URL` is unset
3. **Logs** startup configuration (project root, dev mode, AI status)
4. **Execs** the CMD (`uvicorn`) so it becomes PID 1 with proper signal handling

## Environment Variables

Variables are layered from three sources (later overrides earlier):

| Source | Variables |
|--------|-----------|
| `Dockerfile` ENV defaults | `LHP_STATIC_DIR`, `LHP_PROJECT_ROOT`, `LHP_DEV_MODE`, `LHP_AI_ENABLED`, `LHP_OPENCODE_PORT`, `LHP_LOG_LEVEL` |
| `.env` file (via `env_file`) | `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_API_KEY`, `LHP_PORT`, `LHP_LOG_LEVEL` |
| `docker-compose.yml` environment | `LHP_PROJECT_ROOT=/project`, `LHP_DEV_MODE=true`, `LHP_AI_ENABLED=true`, `LHP_STATIC_DIR=/app/static` (hardcoded, always wins) |

## Health Check

The container includes a Docker `HEALTHCHECK` that polls `/api/health` every 30
seconds. The 15-second start period accounts for OpenCode subprocess startup time.

## Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Multi-stage image build |
| `docker-compose.yml` | Service definition, volume mount, env wiring |
| `docker/entrypoint.sh` | Pre-start validation and logging |
| `.dockerignore` | Excludes `.git/`, `node_modules/`, tests, secrets from build context |
| `.env.example` | Documented template for developer configuration |
| `scripts/docker_dev.sh` | Convenience wrapper with pre-flight checks |
