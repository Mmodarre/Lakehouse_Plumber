// The core file-edit loop: open a flowgroup YAML from the tree into a
// workspace tab, edit in Monaco (dirty dot appears), save. A save whose YAML
// is unparseable surfaces the syntax error in the Problems panel; a clean
// save auto-starts a scoped validate whose success lands in the status bar
// and the Validation page.

import { readFileSync } from 'node:fs'
import path from 'node:path'
import { expect, test } from '@playwright/test'
import { restoreFixtureFile } from './helpers/api'
import { FIXTURE_SOURCE } from './helpers/servers'
import {
  dirtyDot,
  gotoApp,
  openFileFromTree,
  saveActiveBuffer,
  setEditorContent,
  toast,
  workspaceTab,
} from './helpers/ui'

const FILE = 'pipelines/16_temp_table/staging_chain.yaml'
const FILENAME = 'staging_chain.yaml'

// The served project is a byte-identical copy of the repo fixture, so the
// pristine content can be read from the (read-only) original.
const ORIGINAL = readFileSync(path.join(FIXTURE_SOURCE, FILE), 'utf8')

// Hermetic: even if the spec dies between the broken-YAML save and the
// restore, put the pristine file back so later specs see a valid project.
test.afterEach(async () => {
  await restoreFixtureFile(FILE)
})

test('edit → save loop: dirty dot, YAML-syntax problem, auto-validate success', async ({
  page,
}) => {
  await gotoApp(page)
  await openFileFromTree(page, FILE)

  // Break the YAML → the tab goes dirty.
  await expect(dirtyDot(page, FILE)).toBeHidden()
  await setEditorContent(page, `${ORIGINAL}\nbroken: [unclosed\n`)
  await expect(dirtyDot(page, FILE)).toBeVisible()

  // Save persists the write but reports the YAML syntax error…
  await saveActiveBuffer(page)
  await expect(toast(page, `Saved ${FILENAME} with YAML syntax error`)).toBeVisible({
    timeout: 30_000,
  })
  await expect(dirtyDot(page, FILE)).toBeHidden()

  // …and the Problems panel carries the synthetic YAML-SYNTAX issue.
  await page.getByRole('link', { name: 'Validation' }).click()
  await expect(page.getByText('YAML-SYNTAX')).toBeVisible({ timeout: 30_000 })

  // Restore the pristine content: a clean YAML save auto-starts a validate
  // scoped to the file's pipeline.
  await workspaceTab(page, FILE).click()
  await expect(page.locator('.monaco-editor .view-lines').first()).toBeVisible()
  await setEditorContent(page, ORIGINAL)
  await expect(dirtyDot(page, FILE)).toBeVisible()
  await saveActiveBuffer(page)
  await expect(
    toast(page, FILENAME).filter({ hasNot: page.getByText('syntax') }).first(),
  ).toBeVisible({ timeout: 30_000 })
  await expect(dirtyDot(page, FILE)).toBeHidden()

  // The auto-validate stream reaches a successful terminal state (status
  // bar link; role-scoped — an sr-only live region carries the same text).
  await expect(page.getByRole('link', { name: /Validated ·/ })).toBeVisible({
    timeout: 120_000,
  })

  // And the Validation page shows the terminal success banner.
  await page.getByRole('link', { name: 'Validation' }).click()
  await expect(page.getByText(/completed successfully/)).toBeVisible({ timeout: 30_000 })
})
