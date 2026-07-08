// Navigation guard: route navigation while a buffer is dirty raises the
// discard AlertDialog — "Keep editing" cancels the navigation, "Discard
// changes" reverts the buffer and proceeds.

import { expect, test } from '@playwright/test'
import {
  appendToEditor,
  dirtyDot,
  gotoApp,
  openFileFromTree,
} from './helpers/ui'

const FILE = 'pipelines/09_test_python/sample_python_func_flow.yaml'

test('dirty buffer blocks navigation until confirmed', async ({ page }) => {
  await gotoApp(page)
  await openFileFromTree(page, FILE)
  await appendToEditor(page, '\n# e2e-nav-guard-edit')
  await expect(dirtyDot(page, FILE)).toBeVisible()

  // Attempt to navigate → the guard dialog appears; keep editing cancels.
  await page.getByRole('link', { name: 'Tables' }).click()
  const dialog = page.getByRole('alertdialog')
  await expect(dialog.getByText('Discard unsaved changes?')).toBeVisible()
  await dialog.getByRole('button', { name: 'Keep editing' }).click()
  await expect(dialog).toBeHidden()
  expect(new URL(page.url()).pathname).toBe('/')
  await expect(page.locator('.monaco-editor').first()).toBeVisible()
  await expect(dirtyDot(page, FILE)).toBeVisible()

  // Second attempt → discard proceeds, buffer reverts, route changes.
  await page.getByRole('link', { name: 'Tables' }).click()
  await expect(page.getByRole('alertdialog')).toBeVisible()
  await page.getByRole('alertdialog').getByRole('button', { name: 'Discard changes' }).click()
  await expect(page).toHaveURL(/\/tables$/)
  await expect(dirtyDot(page, FILE)).toBeHidden()
})
