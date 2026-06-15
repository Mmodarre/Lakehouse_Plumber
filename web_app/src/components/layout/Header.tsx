import { NavLink } from 'react-router-dom'
import { useProject, useHealth } from '../../hooks/useProject'
import { useEnvironments } from '../../hooks/useEnvironments'
import { usePipelines } from '../../hooks/usePipelines'
import { useUIStore } from '../../store/uiStore'
import { useRunController } from '../../store/runStore'

// ── Small helpers (not exported) ────────────────────────

function HealthDot({ isHealthy }: { isHealthy: boolean }) {
  return (
    <div className="flex items-center gap-1" title={isHealthy ? 'API connected' : 'API unreachable'}>
      <div className={`h-1.5 w-1.5 rounded-full ${isHealthy ? 'bg-green-500' : 'bg-red-400'}`} />
    </div>
  )
}

function ButtonSpinner() {
  return (
    <span className="h-3 w-3 animate-spin rounded-full border-2 border-white/40 border-t-white" />
  )
}

// ── Exported Header: single unconditional local-IDE header ──

export function Header() {
  const { data: project } = useProject()
  const { data: health } = useHealth()
  const { data: envData } = useEnvironments()
  const { data: pipelines } = usePipelines()
  const { selectedEnv, setSelectedEnv, pipelineFilter, setPipelineFilter, sidebarOpen, toggleSidebar } = useUIStore()
  const { isRunning, startValidate, startGenerate } = useRunController()

  const isHealthy = health?.status === 'healthy'

  return (
    <header className="flex h-11 items-center border-b border-slate-200 bg-white px-4">
      {/* Sidebar toggle */}
      <button
        onClick={toggleSidebar}
        className="mr-3 rounded p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
        title={sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar'}
      >
        <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
        </svg>
      </button>

      {/* Breadcrumb: Project > Pipeline */}
      <div className="flex items-center gap-1 text-xs">
        <span className="font-medium text-slate-500">
          {project?.name ?? 'Lakehouse Plumber'}
        </span>
        {pipelineFilter && (
          <>
            <span className="text-slate-300">&rsaquo;</span>
            <span className="font-semibold text-slate-800">{pipelineFilter}</span>
          </>
        )}
        {project?.version && (
          <span className="ml-1 rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-400">
            v{project.version}
          </span>
        )}
      </div>

      {/* Nav links */}
      <nav className="ml-6 flex gap-1">
        {[
          { to: '/', label: 'Dashboard' },
          { to: '/flowgroups', label: 'Flowgroups' },
          { to: '/tables', label: 'Tables' },
          { to: '/validation', label: 'Validation' },
        ].map((link) => (
          <NavLink
            key={link.to}
            to={link.to}
            end={link.to === '/'}
            className={({ isActive }) =>
              `rounded px-2.5 py-1 text-[11px] font-medium transition-colors ${
                isActive
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-slate-500 hover:bg-slate-50 hover:text-slate-700'
              }`
            }
          >
            {link.label}
          </NavLink>
        ))}
      </nav>

      <div className="ml-auto flex items-center gap-3">
        {/* Pipeline filter */}
        <select
          value={pipelineFilter ?? ''}
          onChange={(e) => setPipelineFilter(e.target.value || null)}
          className="rounded border border-slate-200 bg-white px-2 py-1 text-[11px] text-slate-700 focus:border-blue-400 focus:outline-none"
        >
          <option value="">All Pipelines</option>
          {pipelines?.pipelines.map((p) => (
            <option key={p.name} value={p.name}>{p.name}</option>
          ))}
        </select>

        {/* Environment selector */}
        <select
          value={selectedEnv}
          onChange={(e) => setSelectedEnv(e.target.value)}
          className="rounded border border-slate-200 bg-white px-2 py-1 text-[11px] text-slate-700 focus:border-blue-400 focus:outline-none"
        >
          {envData?.environments.map((env) => (
            <option key={env} value={env}>{env}</option>
          ))}
          {!envData && <option value="dev">dev</option>}
        </select>

        {/* Validate button */}
        <button
          className="flex items-center gap-1.5 rounded bg-slate-800 px-3 py-1 text-[11px] font-medium text-white hover:bg-slate-700 disabled:opacity-50"
          onClick={() => startValidate()}
          disabled={isRunning}
          title={`Validate ${pipelineFilter ?? 'all pipelines'} (${selectedEnv})`}
        >
          {isRunning && <ButtonSpinner />}
          Validate
        </button>

        {/* Generate button */}
        <button
          className="flex items-center gap-1.5 rounded bg-blue-600 px-3 py-1 text-[11px] font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          onClick={() => startGenerate()}
          disabled={isRunning}
          title={`Generate ${pipelineFilter ?? 'all pipelines'} (${selectedEnv})`}
        >
          {isRunning && <ButtonSpinner />}
          Generate
        </button>

        {/* Health indicator */}
        <HealthDot isHealthy={isHealthy} />
      </div>
    </header>
  )
}
