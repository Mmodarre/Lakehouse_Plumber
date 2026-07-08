// Theme toggle + persistence: cycling system → light → dark stamps
// html.dark, persists the preference under localStorage 'lhp-theme', and
// survives a reload; the Monaco editor re-themes with the app.

import { expect, test } from '@playwright/test'
import { gotoApp, openFileFromTree } from './helpers/ui'

test.use({ colorScheme: 'light' })

const THEME_TOGGLE = /theme — switch to/

async function bodyBackground(page: import('@playwright/test').Page): Promise<string> {
  return page.evaluate(() => getComputedStyle(document.body).backgroundColor)
}

test('theme toggles, persists across reload, and re-themes Monaco', async ({ page }) => {
  await gotoApp(page)

  // Default preference is 'system'; with a light OS scheme that resolves light.
  await expect(page.locator('html')).not.toHaveClass(/dark/)
  const lightBackground = await bodyBackground(page)

  // system → light → dark.
  const toggle = page.getByRole('button', { name: THEME_TOGGLE })
  await toggle.click()
  await expect(page.locator('html')).not.toHaveClass(/dark/)
  await toggle.click()
  await expect(page.locator('html')).toHaveClass(/dark/)
  expect(await bodyBackground(page)).not.toBe(lightBackground)

  const stored = await page.evaluate(() => window.localStorage.getItem('lhp-theme'))
  expect(stored).toContain('"theme":"dark"')

  // Reload: the persisted preference is applied before first paint.
  await page.reload()
  await expect(page.locator('html')).toHaveClass(/dark/)
  await expect(page.getByRole('button', { name: /Dark theme — switch to/ })).toBeVisible()

  // Monaco mounts with the dark lhp theme and follows a live theme flip.
  await openFileFromTree(page, 'lhp.yaml')
  const editor = page.locator('.monaco-editor').first()
  const darkEditorBackground = await editor.evaluate(
    (el) => getComputedStyle(el).backgroundColor,
  )
  // dark → system (OS scheme is light here), so the editor re-themes light.
  await page.getByRole('button', { name: THEME_TOGGLE }).click()
  await expect(page.locator('html')).not.toHaveClass(/dark/)
  await expect
    .poll(async () => editor.evaluate((el) => getComputedStyle(el).backgroundColor))
    .not.toBe(darkEditorBackground)
})
