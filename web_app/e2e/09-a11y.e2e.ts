// axe-core accessibility sweep: every main route, in both themes, must be
// free of CRITICAL violations. Serious/moderate findings are reported to the
// test output but do not gate (per the P6 plan). The init wizard is audited
// against the empty-project server — this file must run BEFORE the init
// wizard spec (10-…) consumes that server's one-shot no_project state.

import { expect, test, type Page } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'
import { EMPTY_BASE_URL } from './helpers/servers'
import { seedTheme } from './helpers/ui'

interface RouteCheck {
  name: string
  url: string
  /** Wait for this before running axe, so the real content is audited. */
  ready: (page: Page) => Promise<void>
}

const ROUTES: RouteCheck[] = [
  {
    name: 'dashboard',
    url: '/',
    ready: async (p) =>
      expect(p.locator('.react-flow__node').first()).toBeVisible({ timeout: 90_000 }),
  },
  {
    name: 'flowgroups',
    url: '/flowgroups',
    ready: async (p) =>
      expect(p.getByRole('button', { name: 'New Flowgroup' })).toBeVisible({ timeout: 60_000 }),
  },
  {
    name: 'tables',
    url: '/tables',
    ready: async (p) =>
      expect(p.getByPlaceholder('Filter by name…')).toBeVisible({ timeout: 60_000 }),
  },
  {
    name: 'validation',
    url: '/validation',
    ready: async (p) => expect(p.getByRole('heading', { name: 'Validation' })).toBeVisible(),
  },
  {
    name: 'runs',
    url: '/runs',
    ready: async (p) => expect(p.getByRole('heading', { name: 'Run history' })).toBeVisible(),
  },
  {
    name: 'blueprints',
    url: '/blueprints',
    ready: async (p) => expect(p.getByRole('heading', { name: 'Blueprints' })).toBeVisible(),
  },
  {
    name: 'presets',
    url: '/presets',
    ready: async (p) => expect(p.getByRole('heading', { name: 'Presets' })).toBeVisible(),
  },
  {
    name: 'templates',
    url: '/templates',
    ready: async (p) => expect(p.getByRole('heading', { name: 'Templates' })).toBeVisible(),
  },
  {
    name: 'environments',
    url: '/environments',
    ready: async (p) => expect(p.getByRole('heading', { name: 'Environments' })).toBeVisible(),
  },
  {
    name: 'init-wizard',
    url: `${EMPTY_BASE_URL}/`,
    ready: async (p) =>
      expect(p.getByRole('heading', { name: 'Initialize a project' })).toBeVisible({
        timeout: 30_000,
      }),
  },
]

/**
 * Critical findings tolerated until fixed in web_app/src. Map of axe rule
 * id → route names where the finding currently fires. Remove entries as
 * the source gets fixed; anything not listed still gates. Currently empty:
 * every route must be free of critical violations.
 */
const KNOWN_CRITICAL: Record<string, string[]> = {}

async function auditRoute(page: Page, route: RouteCheck): Promise<void> {
  await page.goto(route.url)
  await route.ready(page)

  const results = await new AxeBuilder({ page }).analyze()
  const critical = results.violations.filter((v) => v.impact === 'critical')
  const advisory = results.violations.filter(
    (v) => v.impact === 'serious' || v.impact === 'moderate',
  )
  for (const v of advisory) {
    console.log(
      `[axe advisory] ${route.name}: ${v.impact} ${v.id} (${v.nodes.length} node(s)) — ${v.help}`,
    )
  }

  const tolerated = critical.filter((v) => KNOWN_CRITICAL[v.id]?.includes(route.name))
  for (const v of tolerated) {
    console.log(
      `[axe KNOWN-CRITICAL, needs src fix] ${route.name}: ${v.id} (${v.nodes.length} node(s)) — ${v.help}`,
    )
  }
  const gating = critical.filter((v) => !KNOWN_CRITICAL[v.id]?.includes(route.name))
  expect(
    gating.map((v) => ({ id: v.id, help: v.help, nodes: v.nodes.map((n) => n.target) })),
  ).toEqual([])
}

for (const theme of ['light', 'dark'] as const) {
  test.describe(`a11y (${theme})`, () => {
    for (const route of ROUTES) {
      test(`${route.name} has no critical axe violations`, async ({ page }) => {
        await seedTheme(page, theme)
        await auditRoute(page, route)
      })
    }
  })
}
