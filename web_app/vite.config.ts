import path from 'path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
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
        headers: {
          'X-Forwarded-Email': 'dev@localhost',
          'X-Forwarded-Preferred-Username': 'dev-user',
          'X-Forwarded-User': '12345',
        },
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
        manualChunks(id) {
          if (id.includes('node_modules/react-dom')) return 'react-vendor'
          if (id.includes('node_modules/react-router')) return 'react-vendor'
          if (id.includes('node_modules/monaco-editor')) return 'monaco-editor'
          if (id.includes('node_modules/@xyflow')) return 'react-flow'
          if (id.includes('node_modules/elkjs')) return 'elkjs'
          if (id.includes('node_modules/@tanstack')) return 'react-vendor'
          if (id.includes('node_modules/zustand')) return 'react-vendor'
          if (id.includes('node_modules/react-markdown')) return 'markdown-vendor'
          if (id.includes('node_modules/remark-gfm')) return 'markdown-vendor'
          if (id.includes('node_modules/@radix-ui')) return 'radix-ui'
        },
      },
    },
  },
})
