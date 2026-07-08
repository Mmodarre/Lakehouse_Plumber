// Shared UI interaction helpers for the Playwright specs.
//
// Selector policy: role/label/text selectors only — the app source
// (web_app/src/**) is owned by other workstreams and must not grow
// data-testids from here.

import { expect, type Locator, type Page } from '@playwright/test'

/** Load the app shell and wait until the header nav is interactive. */
export async function gotoApp(page: Page, route = '/'): Promise<void> {
  await page.goto(route)
  await expect(page.getByRole('link', { name: 'Dashboard' })).toBeVisible()
}

/**
 * Open a file from the sidebar File Browser by expanding each directory
 * segment, then clicking the file row. Directory rows and file rows are
 * plain buttons whose accessible name is the node name.
 */
export async function openFileFromTree(page: Page, path: string): Promise<void> {
  const segments = path.split('/')
  const sidebar = page.getByRole('complementary').filter({ hasText: 'File Browser' })
  for (const dir of segments.slice(0, -1)) {
    await sidebar.getByRole('button', { name: dir, exact: true }).click()
  }
  const filename = segments[segments.length - 1]
  // The file row also carries a "Delete <name>" sibling button — exact-match
  // the open button only.
  await sidebar.getByRole('button', { name: filename, exact: true }).click()
  await expect(workspaceTab(page, path)).toBeVisible()
  // Wait for Monaco to mount with the buffer content.
  await expect(page.locator('.monaco-editor .view-lines').first()).toBeVisible()
}

/** The workspace tab-strip select button for a buffer. Keyed off the tab
 * button's `title` (the project-relative path) — the sidebar file rows share
 * the same accessible name, so the filename alone is ambiguous. */
export function workspaceTab(page: Page, path: string): Locator {
  return page.locator(`button[title="${path}"]`)
}

/** The dirty-dot indicator inside a workspace tab (sr-only text). */
export function dirtyDot(page: Page, path: string): Locator {
  return workspaceTab(page, path).getByText('(unsaved changes)')
}

/** Focus the (single mounted) Monaco editor body. Keyboard input must not
 * race the editor mount (e.g. right after a tab switch), so this waits until
 * the editor's input surface owns focus. (Monaco 0.55 uses the EditContext
 * API — a focusable div, not the classic hidden textarea — so the check is
 * "activeElement is inside .monaco-editor" rather than a textarea locator.) */
export async function focusEditor(page: Page): Promise<void> {
  const editor = page.locator('.monaco-editor').first()
  await editor.locator('.view-lines').click()
  await expect
    .poll(() =>
      page.evaluate(() => Boolean(document.activeElement?.closest('.monaco-editor'))),
    )
    .toBe(true)
}

/** Replace the whole Monaco buffer content (select-all + clipboard paste).
 *
 * Deliberately a real paste, not keyboard.insertText: insertText goes
 * through Monaco's "type" pipeline, whose auto-indent re-indents every line
 * of a multi-line insert and silently corrupts YAML. Paste inserts verbatim.
 * Requires the chromium clipboard permissions granted in playwright.config. */
export async function setEditorContent(page: Page, content: string): Promise<void> {
  // Monaco attaches its keybinding handlers asynchronously after mount, so a
  // select-all pressed too early is silently dropped and the paste lands at
  // the click cursor instead, corrupting the buffer. Verify the replacement
  // against the persisted workspace store (which captures editor content on
  // a 500ms debounce) and retry — a retry's select-all also covers any
  // corrupted intermediate state. Caveat: verification requires `content`
  // to differ from the buffer's saved baseline (the store partialize empties
  // a clean buffer's persisted content).
  let last: string | null = null
  for (let attempt = 0; attempt < 3; attempt++) {
    await focusEditor(page)
    await page.keyboard.press('ControlOrMeta+a')
    await page.evaluate(async (text) => {
      await navigator.clipboard.writeText(text)
    }, content)
    await page.keyboard.press('ControlOrMeta+v')
    const deadline = Date.now() + 4_000
    while (Date.now() < deadline) {
      last = await activeBufferContent(page)
      if (last === content) return
      await page.waitForTimeout(200)
    }
  }
  expect(last, 'Monaco content replacement did not land after 3 attempts').toBe(content)
}

/** Minimal shape of the zustand-persisted workspace state read below. */
interface WorkspacePersist {
  activePath?: string | null
  buffers?: Array<{ path: string; content?: string }>
}

/** The active buffer's content as persisted by the workspace store. */
function activeBufferContent(page: Page): Promise<string | null> {
  return page.evaluate(() => {
    const raw = window.localStorage.getItem('lhp-workspace')
    if (!raw) return null
    const state = (JSON.parse(raw) as { state?: WorkspacePersist }).state
    const active = state?.buffers?.find((b) => b.path === state?.activePath)
    return active?.content ?? null
  })
}

/** Append text at the end of the Monaco buffer. Monaco's auto-indent may
 * prefix the appended line with whitespace — callers must only append
 * content whose meaning survives leading indentation (comments, markers). */
export async function appendToEditor(page: Page, text: string): Promise<void> {
  await focusEditor(page)
  await page.keyboard.press('ControlOrMeta+a')
  await page.keyboard.press('ArrowRight')
  await page.keyboard.insertText(text)
}

/** Click the workspace Save button and wait for it to settle. */
export async function saveActiveBuffer(page: Page): Promise<void> {
  await page.getByRole('button', { name: 'Save', exact: true }).click()
}

/** A sonner toast containing `text`. */
export function toast(page: Page, text: string | RegExp): Locator {
  return page.locator('[data-sonner-toast]').filter({ hasText: text })
}

/**
 * Persist a theme preference before any page script runs, mirroring the
 * zustand-persist storage format of `themeStore` (name: 'lhp-theme').
 */
export async function seedTheme(page: Page, theme: 'light' | 'dark'): Promise<void> {
  await page.addInitScript((value: string) => {
    window.localStorage.setItem('lhp-theme', value)
  }, JSON.stringify({ state: { theme }, version: 0 }))
}
