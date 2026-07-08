// The lifecycle resource pages (Header "Resources" dropdown) plus the Runs
// tab all render real fixture data — replaces the manual per-page eyeball
// pass. The Runs page's populated state is asserted later, in the
// generate spec (08); here it renders its (real) empty state.

import { expect, test } from '@playwright/test'
import { gotoApp } from './helpers/ui'

async function openResource(page: import('@playwright/test').Page, label: string) {
  // exact: the fixture's resources_baseline* tree dirs share the substring.
  await page.getByRole('button', { name: 'Resources', exact: true }).click()
  await page.getByRole('menuitem', { name: label }).click()
}

test('Blueprints page lists the fixture blueprint', async ({ page }) => {
  await gotoApp(page)
  await openResource(page, 'Blueprints')
  await expect(page.getByRole('heading', { name: 'Blueprints' })).toBeVisible()
  await expect(page.getByText('medallion_demo')).toBeVisible({ timeout: 30_000 })
})

test('Presets page shows the resolved configuration', async ({ page }) => {
  await gotoApp(page)
  await openResource(page, 'Presets')
  await expect(page.getByRole('heading', { name: 'Presets' })).toBeVisible()

  // All four fixture presets appear in the list nav.
  const list = page.getByRole('navigation', { name: 'Presets' })
  for (const name of [
    'bronze_layer',
    'cloudfiles_defaults',
    'default_delta_properties',
    'write_defaults',
  ]) {
    await expect(list.getByRole('button', { name })).toBeVisible({ timeout: 30_000 })
  }

  // Detail panel renders the inheritance-resolved merge (the fixture presets
  // have single-element chains, so the breadcrumb stays hidden and the
  // resolved section is the chain surface).
  await list.getByRole('button', { name: 'cloudfiles_defaults' }).click()
  await expect(page.getByText(/Resolved configuration/)).toBeVisible()
  await expect(page.getByText('Raw file content')).toBeVisible()
})

test('Templates page lists fixture templates', async ({ page }) => {
  await gotoApp(page)
  await openResource(page, 'Templates')
  await expect(page.getByRole('heading', { name: 'Templates' })).toBeVisible()
  // Scoped to the list nav — the active template's name also renders in the
  // detail panel, so a bare text match would be ambiguous.
  const list = page.getByRole('navigation', { name: 'Templates' })
  await expect(list.getByRole('button', { name: 'json_ingestion_template' })).toBeVisible({
    timeout: 30_000,
  })
})

test('Environments page lists the fixture environments', async ({ page }) => {
  await gotoApp(page)
  await openResource(page, 'Environments')
  await expect(page.getByRole('heading', { name: 'Environments' })).toBeVisible()
  for (const env of ['dev', 'tst', 'prod']) {
    await expect(page.getByText(env, { exact: true }).first()).toBeVisible({ timeout: 30_000 })
  }
})

test('Runs tab renders (empty history before any run)', async ({ page }) => {
  await gotoApp(page)
  await page.getByRole('link', { name: 'Runs' }).click()
  await expect(page.getByRole('heading', { name: 'Run history' })).toBeVisible()
  await expect(page.getByText('No runs yet')).toBeVisible({ timeout: 30_000 })
})
