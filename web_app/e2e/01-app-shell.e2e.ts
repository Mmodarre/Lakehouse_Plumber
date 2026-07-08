// App shell smoke: header nav renders, the sidebar file browser is present,
// and the Dashboard dependency graph lays out real nodes from the fixture
// project. Replaces the manual "does the app come up at all" eyeball pass.

import { expect, test } from '@playwright/test'
import { gotoApp } from './helpers/ui'

test('shell loads: header nav, sidebar, status bar', async ({ page }) => {
  await gotoApp(page)

  // Underline tab nav + Resources dropdown trigger.
  for (const label of ['Dashboard', 'Flowgroups', 'Tables', 'Validation', 'Runs']) {
    await expect(page.getByRole('link', { name: label })).toBeVisible()
  }
  // exact: the fixture's resources_baseline* tree dirs share the substring.
  await expect(page.getByRole('button', { name: 'Resources', exact: true })).toBeVisible()

  // Wordmark shows the fixture project name once /api/project resolves
  // (exact: graph nodes like acme_edw_bp_raw share the prefix).
  await expect(page.getByText('acme_edw', { exact: true })).toBeVisible({ timeout: 30_000 })

  // Sidebar file browser with real fixture entries.
  await expect(page.getByText('File Browser')).toBeVisible()
  const sidebar = page.getByRole('complementary').filter({ hasText: 'File Browser' })
  await expect(sidebar.getByRole('button', { name: 'pipelines', exact: true })).toBeVisible()

  // Status bar reports a healthy backend.
  await expect(page.getByText('Connected', { exact: true })).toBeVisible({ timeout: 30_000 })
})

test('dashboard renders dependency graph nodes', async ({ page }) => {
  await gotoApp(page)
  // First graph request warms the inspection facade over 73 flowgroups and
  // then runs ELK layout — allow a cold-start budget.
  const nodes = page.locator('.react-flow__node')
  await expect(nodes.first()).toBeVisible({ timeout: 90_000 })
  expect(await nodes.count()).toBeGreaterThan(3)
})
