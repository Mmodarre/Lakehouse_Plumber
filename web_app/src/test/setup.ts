// Global test setup — wired via `setupFiles` in vitest.config.ts.
import '@testing-library/jest-dom/vitest'
import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'

// jsdom does not implement matchMedia, but themeStore calls it at module
// load. Install a minimal always-light stub before any module under test
// imports it; tests that need control over the media query install their
// own richer mock (see themeStore.test.ts).
if (typeof window.matchMedia !== 'function') {
  window.matchMedia = (query: string): MediaQueryList =>
    ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: () => {},
      removeEventListener: () => {},
      addListener: () => {},
      removeListener: () => {},
      dispatchEvent: () => false,
    }) as MediaQueryList
}

// Unmount React trees between tests (explicit — does not rely on the
// globals-based auto-cleanup heuristic).
afterEach(() => {
  cleanup()
})
