// First-run init wizard, against the EMPTY-project server (port 8124): the
// no_project health state renders InitProjectPage in place of the IDE;
// submitting the wizard scaffolds the project and the app transitions to the
// initialized IDE without a server restart (health flips via invalidation).
//
// This spec consumes the empty server's one-shot no_project state, so it
// runs LAST (after the a11y sweep audited the pristine wizard).

import { expect, test } from '@playwright/test'
import { EMPTY_BASE_URL } from './helpers/servers'

const PROJECT_NAME = 'e2e_wizard_project'

test('init wizard scaffolds a project and the IDE takes over without restart', async ({
  page,
}) => {
  await page.goto(`${EMPTY_BASE_URL}/`)

  // no_project state → the wizard fills the routed area.
  await expect(page.getByRole('heading', { name: 'Initialize a project' })).toBeVisible({
    timeout: 30_000,
  })

  await page.getByLabel('Project name').fill(PROJECT_NAME)
  // Bundle support stays at its default (checked).
  await page.getByRole('button', { name: 'Create project' }).click()

  // Scaffold success is confirmed in-place…
  await expect(page.getByText('Project initialized')).toBeVisible({ timeout: 60_000 })

  // …then health flips out of no_project and the normal IDE shell replaces
  // the wizard: header wordmark shows the new project, the file browser
  // lists the scaffolded tree — all without restarting the server.
  await expect(page.getByText(PROJECT_NAME).first()).toBeVisible({ timeout: 60_000 })
  await expect(page.getByRole('heading', { name: 'Initialize a project' })).toBeHidden({
    timeout: 60_000,
  })
  await expect(page.getByText('File Browser')).toBeVisible()
  const sidebar = page.getByRole('complementary').filter({ hasText: 'File Browser' })
  await expect(sidebar.getByRole('button', { name: 'lhp.yaml', exact: true })).toBeVisible({
    timeout: 30_000,
  })
})
