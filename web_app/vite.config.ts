import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        headers: {
          'X-Forwarded-Email': 'dev@localhost',
          'X-Forwarded-User': 'dev-user',
          'X-Forwarded-User-Id': '12345',
        },
      },
      '/opencode': {
        target: 'http://localhost:4096',
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/opencode/, ''),
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
        },
      },
    },
  },
})
