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
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
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
