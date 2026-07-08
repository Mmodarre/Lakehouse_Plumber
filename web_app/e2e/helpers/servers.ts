// Shared constants for the Playwright suite: server ports, base URLs, and
// the temp project locations created by e2e/scripts/serve.mjs (keep TMP_ROOT
// in sync with that script — it is plain .mjs so it cannot import this file).

import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

/** Server backed by a temp COPY of tests/e2e/fixtures/testing_project. */
export const FIXTURE_PORT = 8123
/** Server backed by an empty temp dir (boots in `no_project` state). */
export const EMPTY_PORT = 8124

export const FIXTURE_BASE_URL = `http://127.0.0.1:${FIXTURE_PORT}`
export const EMPTY_BASE_URL = `http://127.0.0.1:${EMPTY_PORT}`

const HERE = path.dirname(fileURLToPath(import.meta.url))
export const REPO_ROOT = path.resolve(HERE, '..', '..', '..')

/** The original (read-only!) Python-e2e fixture project in the repo. */
export const FIXTURE_SOURCE = path.join(
  REPO_ROOT,
  'tests',
  'e2e',
  'fixtures',
  'testing_project',
)

/** Parent of the throwaway project dirs (mirrors serve.mjs TMP_ROOT). */
export const TMP_ROOT = path.join(os.tmpdir(), 'lhp-playwright-webapp')
