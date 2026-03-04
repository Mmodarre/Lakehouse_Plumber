# =============================================================================
# LHP Web App — Docker image for local development
#
# Multi-stage build:
#   Stage 1: Build React frontend (node:22-alpine)
#   Stage 2: Python runtime with pre-built SPA + OpenCode binary
#
# Usage:
#   docker compose up --build
#   open http://localhost:8000
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: Build React frontend
# ---------------------------------------------------------------------------
FROM node:22-alpine AS frontend-builder

WORKDIR /build

# Copy package files first for layer caching
COPY web_app/package.json web_app/package-lock.json ./
RUN npm ci

# Copy source and build
COPY web_app/ ./
RUN npm run build

# Output: /build/dist/ with production React SPA

# ---------------------------------------------------------------------------
# Stage 2: Python runtime
# ---------------------------------------------------------------------------
FROM python:3.12.12-slim AS runtime

# System dependencies: git (for GitPython), curl (for healthcheck),
# ca-certificates (for HTTPS), gnupg (for NodeSource GPG key)
RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        curl \
        ca-certificates \
        gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 22 LTS (required for OpenCode binary)
# OpenCode is a Node.js CLI tool — opencode_manager.py uses shutil.which("opencode")
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install OpenCode globally
# Note: npm package is "opencode-ai", binary is "opencode" (see deploy/DEPLOY_LESSONS_LEARNED.md #3)
RUN npm install -g opencode-ai@1.2.15

# Patch vulnerable transitive npm dependencies (9 HIGH + 2 LOW CVEs)
# Creates a temp package.json with overrides in the global node_modules dir
# to force-resolve patched versions, then cleans up
RUN GLOBAL_NM="$(npm root -g)" \
    && echo '{"private":true,"overrides":{"tar":">=7.5.8","minimatch":">=9.0.7","glob":">=10.5.0","cross-spawn":">=7.0.5","brace-expansion":">=2.0.2","diff":">=5.2.2"}}' > "$GLOBAL_NM/package.json" \
    && cd "$GLOBAL_NM" && npm install --no-audit --no-fund \
    && rm -f "$GLOBAL_NM/package.json" "$GLOBAL_NM/package-lock.json"

# Set up working directory
WORKDIR /app

# Install Python package with API + AI extras
# Copy pyproject.toml and src/ for pip install
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir ".[api,ai]" \
    && pip install --no-cache-dir --upgrade pip

# Copy pre-built React SPA from stage 1
COPY --from=frontend-builder /build/dist /app/static

# Copy entrypoint script
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Environment defaults for dev mode
ENV LHP_STATIC_DIR=/app/static \
    LHP_PROJECT_ROOT=/project \
    LHP_DEV_MODE=true \
    LHP_AI_ENABLED=true \
    LHP_OPENCODE_PORT=4096 \
    LHP_LOG_LEVEL=INFO

# Mount point for developer's LHP project
VOLUME /project

EXPOSE 8000

# Health check — start period allows time for OpenCode subprocess startup
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["uvicorn", "lhp.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
