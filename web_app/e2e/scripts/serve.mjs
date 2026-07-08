// Playwright webServer launcher for the LHP web IDE backend.
//
// Usage: node e2e/scripts/serve.mjs <fixture|empty> <port>
//
// Playwright starts webServer commands BEFORE globalSetup runs (the web
// server is a plugin-setup task), so the temp project directory has to be
// prepared here, inside the command itself — not in a globalSetup script.
//
//   fixture — copies tests/e2e/fixtures/testing_project (the Python-e2e
//             fixture, 73 flowgroups / 24 pipelines) to a throwaway temp
//             dir. The original fixture is NEVER served or written to.
//   empty   — creates an empty temp dir, so the server boots in the
//             `no_project` state and serves the init wizard.
//
// The server is uvicorn running the app factory directly (the same factory
// `lhp web` hands to uvicorn). Launching via the `lhp web` CLI is deliberately
// avoided: the CLI unconditionally mints a session token, while the token
// guard middleware is documented as a no-op when LHP_WEBAPP_TOKEN is unset —
// the supported no-auth mode for tests. Project root is passed via
// LHP_WEBAPP_PROJECT_ROOT (read by lhp.webapp.settings.get_settings).
//
// The interpreter defaults to `python3` resolved from this process's cwd
// (the repo — so pyenv's .python-version applies); override with LHP_PYTHON.
//
// Temp dirs live under TMP_ROOT (see also e2e/helpers/servers.ts, which
// mirrors these paths for the specs and global teardown). Each launch
// recreates its dir from scratch; global-teardown.ts removes TMP_ROOT.

import { cpSync, existsSync, mkdirSync, rmSync } from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawn } from 'node:child_process'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const REPO_ROOT = path.resolve(HERE, '..', '..', '..')
const FIXTURE_SOURCE = path.join(REPO_ROOT, 'tests', 'e2e', 'fixtures', 'testing_project')
const STATIC_INDEX = path.join(REPO_ROOT, 'src', 'lhp', 'webapp', 'static', 'index.html')

// Keep in sync with e2e/helpers/servers.ts (TMP_ROOT).
const TMP_ROOT = path.join(os.tmpdir(), 'lhp-playwright-webapp')

const [mode, portArg] = process.argv.slice(2)
if ((mode !== 'fixture' && mode !== 'empty') || !portArg) {
  console.error('usage: node e2e/scripts/serve.mjs <fixture|empty> <port>')
  process.exit(2)
}
const port = Number(portArg)

if (!existsSync(STATIC_INDEX)) {
  console.error(
    `[serve.mjs] Built SPA not found at ${STATIC_INDEX}.\n` +
      '[serve.mjs] Run `bash scripts/build_webapp.sh` from the repo root first.',
  )
  process.exit(1)
}

const projectRoot = path.join(TMP_ROOT, `${mode}-project`)
rmSync(projectRoot, { recursive: true, force: true })
mkdirSync(projectRoot, { recursive: true })
if (mode === 'fixture') {
  console.log(`[serve.mjs] copying fixture project -> ${projectRoot}`)
  cpSync(FIXTURE_SOURCE, projectRoot, { recursive: true })
}

const python = process.env.LHP_PYTHON ?? 'python3'
const env = { ...process.env }
delete env.LHP_WEBAPP_TOKEN // no token -> token guard is a no-op (test mode)
env.LHP_WEBAPP_PROJECT_ROOT = projectRoot
env.LHP_WEBAPP_PORT = String(port)
env.LHP_WEBAPP_LOG_LEVEL = 'warning'
env.LHP_DISABLE_ANALYTICS = '1'

console.log(`[serve.mjs] starting ${mode} server on 127.0.0.1:${port} (root: ${projectRoot})`)
const child = spawn(
  python,
  [
    '-m',
    'uvicorn',
    'lhp.webapp.app:create_app',
    '--factory',
    '--host',
    '127.0.0.1',
    '--port',
    String(port),
    '--log-level',
    'warning',
  ],
  { env, stdio: 'inherit' },
)

child.on('exit', (code, signal) => {
  process.exit(code ?? (signal ? 1 : 0))
})
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => child.kill(sig))
}
