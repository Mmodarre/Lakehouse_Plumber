import { createBrowserRouter } from 'react-router-dom'
import { AppShell } from './components/shell/AppShell'

// Data router (createBrowserRouter) rather than <BrowserRouter> JSX: required
// for useBlocker (NavigationGuard). Data is fetched with TanStack Query inside
// components — do not add route loaders/actions here.
// Created once at module scope so the router is stable across re-renders.
//
// The unified workspace collapses routing to two entries: the first-run
// scaffolding wizard at /init, and everything else → the AppShell, which owns
// its own in-app navigation (explorer + center tab strip; no per-view URLs).
export const router = createBrowserRouter([
  // First-run scaffolding wizard (also rendered inline by AppShell's
  // no_project branch; kept as a route for direct navigation).
  {
    path: '/init',
    lazy: async () => ({
      Component: (await import('./pages/InitProjectPage')).InitProjectPage,
    }),
  },
  { path: '*', element: <AppShell /> },
])
