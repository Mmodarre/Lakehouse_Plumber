// Create-flowgroup flow: the CreateFlowgroupDialog scaffolds a new flowgroup
// YAML which opens as a dirty, not-yet-saved workspace buffer (the buffer is
// deliberately left unsaved so the fixture copy's topology stays stable for
// the remaining specs).

import { expect, test } from '@playwright/test'
import { gotoApp } from './helpers/ui'

const NAME = 'e2e_scaffold_fg'

test('CreateFlowgroupDialog scaffolds a dirty workspace buffer', async ({ page }) => {
  await gotoApp(page, '/flowgroups')

  await page.getByRole('button', { name: 'New Flowgroup' }).first().click({ timeout: 60_000 })
  const dialog = page.getByRole('dialog')
  await expect(dialog.getByText('Create Flowgroup')).toBeVisible()

  // The pipeline select is pre-seeded with the first fixture pipeline; only
  // the name is required.
  await dialog.getByPlaceholder('e.g. customer_orders').fill(NAME)
  await expect(dialog.getByText('File path')).toBeVisible()
  await expect(dialog.getByText(new RegExp(`pipelines/.+/${NAME}\\.yaml`))).toBeVisible()
  await dialog.getByRole('button', { name: 'Create', exact: true }).click()
  await expect(dialog).toBeHidden()

  // The scaffold opens as a dirty (unsaved, not-on-disk) workspace tab.
  const tab = page.locator(`button[title$="${NAME}.yaml"]`)
  await expect(tab).toBeVisible()
  await expect(tab.getByText('(unsaved changes)')).toBeVisible()
  await expect(page.getByText("This file doesn't exist yet. Save to create it.")).toBeVisible()

  // Monaco holds the scaffolded YAML.
  const editor = page.locator('.monaco-editor .view-lines').first()
  await expect(editor.getByText(`flowgroup: ${NAME}`)).toBeVisible()
  await expect(editor.getByText('actions: []')).toBeVisible()
})
