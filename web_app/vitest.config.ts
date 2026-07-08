import path from 'path'
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

// Standalone test config (Vitest prefers vitest.config.ts over vite.config.ts).
// Deliberately does NOT reuse vite.config.ts: the dev proxy, Tailwind plugin,
// and manual chunking are irrelevant under jsdom — only the React transform
// and the `@` alias must match.
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test/setup.ts'],
    // No global coverage threshold gate in v1 — coverage is informational.
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html'],
      include: ['src/**/*.{ts,tsx}'],
      exclude: ['src/types/**', 'src/test/**', 'src/**/*.d.ts'],
    },
  },
})
