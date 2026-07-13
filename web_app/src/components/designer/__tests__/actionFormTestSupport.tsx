import type { ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { TooltipProvider } from '@/components/ui/tooltip'

// ── Shared render harness for the ActionForm suites ──────────
//
// ActionForm wraps its field tree in <SchemaKindProvider kind="flowgroup">,
// which calls useQuery(['schema','flowgroup']); seed that query so no real
// GET /api/schemas fires (staleTime: Infinity → the seeded value stays fresh),
// and supply the TooltipProvider the field-help "(i)" icons need. Mirrors
// task 7's config-form harness. Pass the result as RTL's `wrapper` option:
//   render(<ActionForm … />, { wrapper: flowgroupProviders() })
// A fresh (stable) QueryClient is created per call so suites don't share cache.

export function flowgroupProviders() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  queryClient.setQueryData(['schema', 'flowgroup'], {})
  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <TooltipProvider delayDuration={0}>{children}</TooltipProvider>
      </QueryClientProvider>
    )
  }
}
