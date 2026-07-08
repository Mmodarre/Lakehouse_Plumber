// Direct (Node-side) API helpers against the fixture server — used for
// hermetic cleanup so a failed spec cannot leave a mutated file behind and
// poison the specs that follow. The tokenless test server accepts plain
// requests (the token guard is a no-op without LHP_WEBAPP_TOKEN).

import { readFileSync } from 'node:fs'
import path from 'node:path'
import { FIXTURE_BASE_URL, FIXTURE_SOURCE } from './servers'

/** Rewrite a file in the SERVED temp project back to its pristine content
 * from the repo fixture (unconditional PUT — no If-Match). */
export async function restoreFixtureFile(relPath: string): Promise<void> {
  const content = readFileSync(path.join(FIXTURE_SOURCE, relPath), 'utf8')
  const encoded = relPath.split('/').map(encodeURIComponent).join('/')
  const res = await fetch(`${FIXTURE_BASE_URL}/api/files/${encoded}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ content }),
  })
  if (!res.ok) {
    throw new Error(`restoreFixtureFile(${relPath}) failed: HTTP ${res.status}`)
  }
}
