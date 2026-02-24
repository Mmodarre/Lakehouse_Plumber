import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { Layout } from './components/layout/Layout'
import { DashboardPage } from './pages/DashboardPage'
import { FlowgroupsPage } from './pages/FlowgroupsPage'
import { ValidationPage } from './pages/ValidationPage'
import { StalenessPage } from './pages/StalenessPage'
import { TablesPage } from './pages/TablesPage'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<DashboardPage />} />
          <Route path="flowgroups" element={<FlowgroupsPage />} />
          <Route path="tables" element={<TablesPage />} />
          <Route path="validation" element={<ValidationPage />} />
          <Route path="staleness" element={<StalenessPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
