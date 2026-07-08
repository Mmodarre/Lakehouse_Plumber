// Removes the throwaway temp project dirs created by e2e/scripts/serve.mjs.
// (Setup happens inside serve.mjs itself: Playwright launches webServer
// commands before globalSetup, so a setup script could not prepare the dirs
// in time. Each serve.mjs run also recreates its dir from scratch, so this
// teardown is belt-and-braces hygiene, not a correctness requirement.)

import { rmSync } from 'node:fs'
import { TMP_ROOT } from './helpers/servers'

export default function globalTeardown(): void {
  rmSync(TMP_ROOT, { recursive: true, force: true })
}
