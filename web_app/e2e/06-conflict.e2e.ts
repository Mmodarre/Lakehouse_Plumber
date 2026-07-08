// Optimistic-concurrency conflict flow: two browser contexts open the same
// file; the second save is rejected with 412 → persistent stale toast →
// ConflictDialog with its three resolutions; "Take disk version" reloads the
// buffer with the on-disk content.
//
// A non-YAML file is used on purpose — YAML saves auto-start a validate run,
// which is exercised separately in 04-edit-loop.

import { expect, test } from '@playwright/test'
import { restoreFixtureFile } from './helpers/api'
import {
  appendToEditor,
  gotoApp,
  openFileFromTree,
  saveActiveBuffer,
  toast,
} from './helpers/ui'

const FILE = 'sql/orders_europe_bronze_cleanse.sql'
const FILENAME = 'orders_europe_bronze_cleanse.sql'

// Hermetic: put the pristine file back whatever happened above.
test.afterEach(async () => {
  await restoreFixtureFile(FILE)
})

test('concurrent save → 412 → stale toast → ConflictDialog → take-theirs reload', async ({
  browser,
}) => {
  const contextA = await browser.newContext()
  const contextB = await browser.newContext()
  const pageA = await contextA.newPage()
  const pageB = await contextB.newPage()

  try {
    // Both contexts open the file and hold the same ETag.
    await gotoApp(pageA)
    await openFileFromTree(pageA, FILE)
    await gotoApp(pageB)
    await openFileFromTree(pageB, FILE)

    // A saves first — the on-disk ETag moves on.
    await appendToEditor(pageA, '\n-- e2e_marker_ctx_a')
    await saveActiveBuffer(pageA)
    await expect(toast(pageA, `Saved ${FILENAME}`)).toBeVisible({ timeout: 30_000 })

    // B's save now carries a stale ETag → 412 → persistent stale toast.
    await appendToEditor(pageB, '\n-- e2e_marker_ctx_b')
    await saveActiveBuffer(pageB)
    const staleToast = toast(pageB, `${FILENAME} changed on disk since you opened it`)
    await expect(staleToast).toBeVisible({ timeout: 30_000 })

    // Resolve → the ConflictDialog offers its three options.
    await staleToast.getByRole('button', { name: 'Resolve' }).click()
    const dialog = pageB.getByRole('dialog')
    await expect(dialog.getByText(`${FILENAME} changed on disk`)).toBeVisible()
    await expect(dialog.getByRole('button', { name: /Keep my version/ })).toBeVisible()
    await expect(dialog.getByRole('button', { name: /Merge manually/ })).toBeVisible()
    const takeTheirs = dialog.getByRole('button', { name: /Take disk version/ })
    await expect(takeTheirs).toBeVisible()

    // Take-theirs: B's buffer reloads with A's saved content.
    await takeTheirs.click()
    await expect(dialog).toBeHidden()
    await expect(
      pageB.locator('.monaco-editor .view-lines').getByText('e2e_marker_ctx_a'),
    ).toBeVisible({ timeout: 15_000 })
    await expect(staleToast).toBeHidden()
  } finally {
    await contextA.close()
    await contextB.close()
  }
})
