import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export type ThemePreference = 'light' | 'dark' | 'system'
export type ResolvedTheme = 'light' | 'dark'

interface ThemeState {
  /** User preference (persisted to localStorage). */
  theme: ThemePreference
  /** The theme actually applied — `system` resolved against the OS setting. */
  resolved: ResolvedTheme
  setTheme: (theme: ThemePreference) => void
}

const media = window.matchMedia('(prefers-color-scheme: dark)')

function resolve(theme: ThemePreference): ResolvedTheme {
  return theme === 'system' ? (media.matches ? 'dark' : 'light') : theme
}

function applyToDocument(resolved: ResolvedTheme): void {
  document.documentElement.classList.toggle('dark', resolved === 'dark')
}

export const useThemeStore = create<ThemeState>()(
  persist(
    (set) => ({
      theme: 'system',
      resolved: resolve('system'),
      setTheme: (theme) => {
        const resolved = resolve(theme)
        applyToDocument(resolved)
        set({ theme, resolved })
      },
    }),
    {
      name: 'lhp-theme',
      // Only the preference is persisted; `resolved` is recomputed on boot
      // (and by the media listener below) so a changed OS theme wins.
      partialize: (s) => ({ theme: s.theme }),
    },
  ),
)

// Follow the OS live while the preference is 'system'.
media.addEventListener('change', () => {
  const { theme } = useThemeStore.getState()
  if (theme !== 'system') return
  const resolved = resolve(theme)
  applyToDocument(resolved)
  useThemeStore.setState({ resolved })
})

/**
 * Apply the persisted (or system) theme to `<html>`. Called from main.tsx at
 * module load, before the first render, so React never mounts under the wrong
 * theme. The inline script in index.html covers the earlier window between
 * HTML parse and bundle execution.
 */
export function initTheme(): void {
  const { theme } = useThemeStore.getState()
  const resolved = resolve(theme)
  applyToDocument(resolved)
  useThemeStore.setState({ resolved })
}
