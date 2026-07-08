import { defineConfig } from '@playwright/test'
import { EMPTY_PORT, FIXTURE_PORT } from './e2e/helpers/servers'

// Browser E2E suite for the LHP web IDE.
//
// Prerequisite: the SPA must be built into src/lhp/webapp/static/ first —
// run `bash scripts/build_webapp.sh` from the repo root (serve.mjs fails
// fast with that instruction if the assets are missing). The Python side
// needs the webapp extra installed (`pip install -e ".[dev,webapp]"`);
// serve.mjs uses `python3` (override with LHP_PYTHON).
//
// Two real uvicorn servers are launched (see e2e/scripts/serve.mjs):
//   :8123 — a temp COPY of tests/e2e/fixtures/testing_project (never the
//           original), used by every spec except the init wizard;
//   :8124 — an empty temp dir, so the app boots in `no_project` state for
//           the init-wizard spec.
//
// Specs run serially (workers: 1) because they mutate shared server state —
// files on disk, run history, the empty project. Spec files are numbered to
// make the (alphabetical) execution order explicit.
export default defineConfig({
  testDir: './e2e',
  // *.e2e.ts (NOT *.spec.ts): vitest's default include pattern would
  // otherwise pick these files up and fail `npm run test`.
  testMatch: '**/*.e2e.ts',
  outputDir: './e2e/.results',
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  // No retries: several specs mutate server state (saved files, run history,
  // the init wizard's one-shot project scaffold), so a mid-spec retry would
  // observe a different world than a fresh run.
  retries: 0,
  // Generous defaults: first requests warm the inspection facade over 73
  // flowgroups, and validate/generate runs stream for tens of seconds.
  timeout: 120_000,
  expect: { timeout: 15_000 },
  reporter: process.env.CI
    ? [['list'], ['html', { outputFolder: 'e2e/.report', open: 'never' }]]
    : [['list']],
  globalTeardown: './e2e/global-teardown.ts',
  use: {
    baseURL: `http://127.0.0.1:${FIXTURE_PORT}`,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    // setEditorContent replaces Monaco buffers via a real clipboard paste
    // (insertText would be auto-indent-mangled) — chromium needs these.
    permissions: ['clipboard-read', 'clipboard-write'],
  },
  // Deliberately NOT devices['Desktop Chrome']: that descriptor pins a
  // Windows userAgent, so Monaco binds Windows shortcuts (Ctrl+A) while
  // Playwright's ControlOrMeta sends Meta on a macOS host — select-all is
  // silently swallowed. The default context UA matches the real platform,
  // keeping Monaco's keybindings and Playwright's key synthesis in agreement
  // on every OS.
  projects: [{ name: 'chromium', use: { viewport: { width: 1280, height: 720 } } }],
  webServer: [
    {
      command: `node e2e/scripts/serve.mjs fixture ${FIXTURE_PORT}`,
      url: `http://127.0.0.1:${FIXTURE_PORT}/api/health`,
      reuseExistingServer: false,
      timeout: 60_000,
      stdout: 'pipe',
      stderr: 'pipe',
    },
    {
      command: `node e2e/scripts/serve.mjs empty ${EMPTY_PORT}`,
      url: `http://127.0.0.1:${EMPTY_PORT}/api/health`,
      reuseExistingServer: false,
      timeout: 60_000,
      stdout: 'pipe',
      stderr: 'pipe',
    },
  ],
})
