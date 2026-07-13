import './lib/monaco-setup'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import './index.css'
import App from './App'
import { TooltipProvider } from './components/ui/tooltip'
import { bootstrapToken } from './lib/session-token'
import { initTheme } from './store/themeStore'

// Lift any `#token=…` fragment into sessionStorage and strip it from the URL
// before the first request fires. Runs once at module load (idempotent).
bootstrapToken()

// Apply the persisted/system theme to <html> before the first render.
initTheme()

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,      // Data stays fresh for 30s
      retry: 1,                // Retry failed requests once
      refetchOnWindowFocus: false,
    },
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <TooltipProvider delayDuration={300}>
        <App />
      </TooltipProvider>
    </QueryClientProvider>
  </StrictMode>,
)
