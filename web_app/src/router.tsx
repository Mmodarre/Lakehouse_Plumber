import { createBrowserRouter } from 'react-router-dom'
import { Layout } from './components/layout/Layout'
import { DashboardPage } from './pages/DashboardPage'
import { FlowgroupsPage } from './pages/FlowgroupsPage'
import { ValidationPage } from './pages/ValidationPage'
import { TablesPage } from './pages/TablesPage'

// Data router (createBrowserRouter) rather than <BrowserRouter> JSX: required
// for useBlocker (navigation guards). Data is fetched with TanStack Query
// inside components — do not add route loaders/actions here.
// Created once at module scope so the router is stable across re-renders.
export const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { index: true, element: <DashboardPage /> },
      { path: 'flowgroups', element: <FlowgroupsPage /> },
      { path: 'tables', element: <TablesPage /> },
      { path: 'validation', element: <ValidationPage /> },
      // Add new top-level pages here (as children of Layout).
      // Lifecycle pages are route-lazy (route.lazy is the data-router
      // equivalent of React.lazy — code-split without a Suspense wrapper).
      {
        path: 'blueprints',
        lazy: async () => ({
          Component: (await import('./pages/BlueprintsPage')).BlueprintsPage,
        }),
      },
      {
        path: 'presets',
        lazy: async () => ({
          Component: (await import('./pages/PresetsPage')).PresetsPage,
        }),
      },
      {
        path: 'templates',
        lazy: async () => ({
          Component: (await import('./pages/TemplatesPage')).TemplatesPage,
        }),
      },
      {
        path: 'environments',
        lazy: async () => ({
          Component: (await import('./pages/EnvironmentsPage')).EnvironmentsPage,
        }),
      },
      {
        path: 'runs',
        lazy: async () => ({
          Component: (await import('./pages/RunHistoryPage')).RunHistoryPage,
        }),
      },
      // First-run scaffolding wizard (health project_state === 'no_project').
      // Rendering it automatically in Layout's no_project branch is a
      // post-4A integration step; until then it is reachable at /init.
      {
        path: 'init',
        lazy: async () => ({
          Component: (await import('./pages/InitProjectPage')).InitProjectPage,
        }),
      },
    ],
  },
])
