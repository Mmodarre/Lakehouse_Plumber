// Generate → run history: trigger a (pipeline-scoped) generate run from the
// header, watch it reach a successful terminal state, then find the recorded
// run on the RunHistoryPage. Generated files land in the throwaway temp copy
// of the fixture project — never in the repo fixture.

import { expect, test } from '@playwright/test'
import { gotoApp } from './helpers/ui'

const PIPELINE = '16_temp_table' // single-flowgroup pipeline → fast generate

test('generate run completes and lands in run history', async ({ page }) => {
  await gotoApp(page)

  // Scope the run to one small pipeline via the header combobox.
  await page.getByRole('combobox', { name: 'Filter by pipeline' }).click()
  await page.getByPlaceholder('Search pipelines…').fill(PIPELINE)
  await page.getByRole('option', { name: PIPELINE }).click()

  // exact: the fixture's generated_baseline* tree dirs share the substring.
  await page.getByRole('button', { name: 'Generate', exact: true }).click()

  // Status bar reports the terminal outcome of the streamed run (role-
  // scoped — an sr-only live region carries the same text).
  await expect(page.getByRole('link', { name: /Generated ·/ })).toBeVisible({
    timeout: 180_000,
  })

  // Run history records the completed generate run.
  await page.getByRole('link', { name: 'Runs' }).click()
  await expect(page.getByRole('heading', { name: 'Run history' })).toBeVisible()
  const row = page.getByRole('row').filter({ hasText: 'generate' }).filter({ hasText: PIPELINE })
  await expect(row.first()).toBeVisible({ timeout: 30_000 })
  await expect(row.first().getByText('completed')).toBeVisible()
})
