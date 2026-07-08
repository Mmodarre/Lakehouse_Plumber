import { describe, expect, it, vi } from 'vitest'

// themeStore captures `window.matchMedia(...)` at module load, so every
// scenario installs a controllable matchMedia mock FIRST, then imports a
// fresh copy of the module via vi.resetModules() + dynamic import.

type Listener = (e: MediaQueryListEvent) => void

interface MediaControl {
  /** Flip the OS preference and fire registered 'change' listeners. */
  setMatches: (dark: boolean) => void
}

function installMatchMedia(initialDark: boolean): MediaControl {
  let matches = initialDark
  const listeners = new Set<Listener>()
  const mql = {
    get matches() {
      return matches
    },
    media: '(prefers-color-scheme: dark)',
    onchange: null,
    addEventListener: (_type: string, cb: Listener) => listeners.add(cb),
    removeEventListener: (_type: string, cb: Listener) => listeners.delete(cb),
    addListener: (cb: Listener) => listeners.add(cb),
    removeListener: (cb: Listener) => listeners.delete(cb),
    dispatchEvent: () => false,
  }
  window.matchMedia = vi.fn().mockReturnValue(mql) as unknown as typeof window.matchMedia
  return {
    setMatches(dark: boolean) {
      matches = dark
      for (const cb of listeners) cb({ matches } as MediaQueryListEvent)
    },
  }
}

async function loadThemeStore(opts: { osDark?: boolean; persisted?: 'light' | 'dark' | 'system' } = {}) {
  vi.resetModules()
  localStorage.clear()
  document.documentElement.classList.remove('dark')
  const media = installMatchMedia(opts.osDark ?? false)
  if (opts.persisted) {
    // zustand/persist JSON storage envelope
    localStorage.setItem('lhp-theme', JSON.stringify({ state: { theme: opts.persisted }, version: 0 }))
  }
  const mod = await import('@/store/themeStore')
  return { ...mod, media }
}

const hasDarkClass = () => document.documentElement.classList.contains('dark')

describe('themeStore', () => {
  it('defaults to system and resolves light when the OS is light', async () => {
    const { useThemeStore, initTheme } = await loadThemeStore({ osDark: false })
    expect(useThemeStore.getState().theme).toBe('system')
    expect(useThemeStore.getState().resolved).toBe('light')
    initTheme()
    expect(hasDarkClass()).toBe(false)
  })

  it('resolves system to dark when the OS is dark and applies .dark', async () => {
    const { useThemeStore, initTheme } = await loadThemeStore({ osDark: true })
    expect(useThemeStore.getState().resolved).toBe('dark')
    initTheme()
    expect(hasDarkClass()).toBe(true)
  })

  it('setTheme("dark") applies .dark and persists only the preference', async () => {
    const { useThemeStore } = await loadThemeStore({ osDark: false })
    useThemeStore.getState().setTheme('dark')
    expect(useThemeStore.getState().theme).toBe('dark')
    expect(useThemeStore.getState().resolved).toBe('dark')
    expect(hasDarkClass()).toBe(true)

    const stored = JSON.parse(localStorage.getItem('lhp-theme') ?? '{}')
    expect(stored.state).toEqual({ theme: 'dark' }) // `resolved` is not persisted
  })

  it('setTheme("light") removes .dark even when the OS is dark', async () => {
    const { useThemeStore } = await loadThemeStore({ osDark: true })
    useThemeStore.getState().setTheme('dark')
    expect(hasDarkClass()).toBe(true)
    useThemeStore.getState().setTheme('light')
    expect(useThemeStore.getState().resolved).toBe('light')
    expect(hasDarkClass()).toBe(false)
  })

  it('restores a persisted preference on load', async () => {
    const { useThemeStore, initTheme } = await loadThemeStore({ osDark: false, persisted: 'dark' })
    expect(useThemeStore.getState().theme).toBe('dark')
    initTheme()
    expect(useThemeStore.getState().resolved).toBe('dark')
    expect(hasDarkClass()).toBe(true)
  })

  it('follows OS changes live while the preference is system', async () => {
    const { useThemeStore, initTheme, media } = await loadThemeStore({ osDark: false })
    initTheme()
    expect(useThemeStore.getState().resolved).toBe('light')

    media.setMatches(true)
    expect(useThemeStore.getState().resolved).toBe('dark')
    expect(hasDarkClass()).toBe(true)

    media.setMatches(false)
    expect(useThemeStore.getState().resolved).toBe('light')
    expect(hasDarkClass()).toBe(false)
  })

  it('ignores OS changes when an explicit preference is set', async () => {
    const { useThemeStore, media } = await loadThemeStore({ osDark: false })
    useThemeStore.getState().setTheme('light')

    media.setMatches(true)
    expect(useThemeStore.getState().resolved).toBe('light')
    expect(hasDarkClass()).toBe(false)
  })
})
