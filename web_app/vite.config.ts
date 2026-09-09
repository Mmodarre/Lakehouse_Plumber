import path from 'path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { visualizer } from 'rollup-plugin-visualizer'

export default defineConfig(({ mode }) => ({
  plugins: [
    react(),
    tailwindcss(),
    // Bundle-composition report, opt-in only: `npm run build:analyze`
    // (vite build --mode analyze) writes dist/stats.html. A normal
    // `npm run build` is unaffected — the plugin is not even loaded.
    ...(mode === 'analyze'
      ? [visualizer({ filename: 'dist/stats.html', gzipSize: true, brotliSize: true })]
      : []),
  ],
  resolve: {
    alias: {
      // Listed before the bare-package alias below: string aliases also match
      // "<find>/..." sub-paths, and the worker-side module must keep resolving
      // to the real package (its handshake is still correct).
      'monaco-worker-manager/worker': path.resolve(
        __dirname,
        'node_modules/monaco-worker-manager/worker.js'
      ),
      // monaco-worker-manager's main-thread half predates monaco-editor
      // >= 0.53's createWebWorker API and silently breaks YAML validation;
      // see src/lib/yaml-worker-manager.ts for the full story.
      'monaco-worker-manager': path.resolve(
        __dirname,
        './src/lib/yaml-worker-manager.ts'
      ),
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        // Override when the backend runs on a non-default port:
        // LHP_WEB_API_TARGET=http://localhost:8137 npm run dev
        target: process.env.LHP_WEB_API_TARGET ?? 'http://localhost:8000',
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on('proxyReq', (_proxyReq, _req, res) => {
            // Prevent Vite proxy timeout before backend responds
            res.setTimeout(180_000)
          })
        },
      },
    },
  },
  build: {
    chunkSizeWarningLimit: 1500,
    rollupOptions: {
      output: {
        manualChunks(id: string) {
          // Eager global styles must not acquire a lazy library's JS chunk as
          // their owner (the React Flow stylesheet is intentionally global).
          if (/\.css(?:\?|$)/.test(id)) return undefined
          // These helpers are shared by eager and lazy modules. Keeping them
          // in the small eager runtime prevents a helper import from pulling
          // the entire Monaco chunk (and its CSS) into the initial document.
          if (id.includes('vite/preload-helper') || id.includes('commonjsHelpers')) return 'react-vendor'
          if (id.includes('node_modules/react/') || id.includes('node_modules/scheduler/')) return 'react-vendor'
          if (id.includes('node_modules/react-dom')) return 'react-vendor'
          if (id.includes('node_modules/react-router')) return 'react-vendor'
          if (id.includes('node_modules/monaco-editor')) return 'monaco-editor'
          if (id.includes('node_modules/@xyflow')) return 'react-flow'
          if (id.includes('node_modules/elkjs')) return 'elkjs'
          if (id.includes('node_modules/@tanstack')) return 'react-vendor'
          if (id.includes('node_modules/zustand')) return 'react-vendor'
        },
      },
    },
  },
}))
