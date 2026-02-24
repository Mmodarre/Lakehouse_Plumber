import { NavLink } from 'react-router-dom'
import { useProject, useHealth } from '../../hooks/useProject'
import { useEnvironments } from '../../hooks/useEnvironments'
import { usePipelines } from '../../hooks/usePipelines'
import { useUIStore, type WorkspaceStatus } from '../../store/uiStore'
import { useChatStore } from '../../store/chatStore'
import { useValidation } from '../../hooks/useValidation'
import { StalenessIndicator } from '../staleness/StalenessIndicator'

// ── Small helpers (not exported) ────────────────────────

function WorkspaceBadge({ isDevMode, workspaceStatus }: { isDevMode: boolean; workspaceStatus: WorkspaceStatus }) {
  if (isDevMode) {
    return (
      <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-medium text-slate-500">
        Local
      </span>
    )
  }
  if (workspaceStatus === 'active') {
    return (
      <span className="rounded bg-green-50 px-1.5 py-0.5 text-[10px] font-medium text-green-700">
        Workspace
      </span>
    )
  }
  return (
    <span className="rounded bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700">
      No Workspace
    </span>
  )
}

function HealthDot({ isHealthy }: { isHealthy: boolean }) {
  return (
    <div className="flex items-center gap-1" title={isHealthy ? 'API connected' : 'API unreachable'}>
      <div className={`h-1.5 w-1.5 rounded-full ${isHealthy ? 'bg-green-500' : 'bg-red-400'}`} />
    </div>
  )
}

function ChatToggleButton() {
  const aiAvailable = useChatStore((s) => s.aiAvailable)
  const toggleChatPanel = useChatStore((s) => s.toggleChatPanel)
  const chatPanelOpen = useChatStore((s) => s.chatPanelOpen)

  if (!aiAvailable) return null

  return (
    <button
      onClick={toggleChatPanel}
      className={`rounded p-1 transition-colors ${
        chatPanelOpen
          ? 'bg-blue-50 text-blue-600'
          : 'text-slate-400 hover:bg-slate-100 hover:text-slate-600'
      }`}
      title="Toggle AI chat (Cmd+Shift+L)"
    >
      <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
      </svg>
    </button>
  )
}

// ── Minimal header: no data hooks, safe before workspace exists ──

function MinimalHeader() {
  const { data: health } = useHealth()
  const workspaceStatus = useUIStore((s) => s.workspaceStatus)
  const { sidebarOpen, toggleSidebar } = useUIStore()

  const isHealthy = health?.status === 'healthy'
  const isDevMode = health?.dev_mode ?? true

  return (
    <header className="flex h-11 items-center border-b border-slate-200 bg-white px-4">
      <button
        onClick={toggleSidebar}
        className="mr-3 rounded p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
        title={sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar'}
      >
        <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
        </svg>
      </button>

      <div className="flex items-center gap-1 text-xs">
        <span className="font-medium text-slate-500">Lakehouse Plumber</span>
      </div>

      <div className="ml-auto flex items-center gap-3">
        <WorkspaceBadge isDevMode={isDevMode} workspaceStatus={workspaceStatus} />
        <ChatToggleButton />
        <HealthDot isHealthy={isHealthy} />
      </div>
    </header>
  )
}

// ── Full header: all hooks, only rendered when workspace is active ──

function FullHeader() {
  const { data: project } = useProject()
  const { data: health } = useHealth()
  const { data: envData } = useEnvironments()
  const { data: pipelines } = usePipelines()
  const { selectedEnv, setSelectedEnv, pipelineFilter, setPipelineFilter, sidebarOpen, toggleSidebar } = useUIStore()
  const workspaceStatus = useUIStore((s) => s.workspaceStatus)
  const validation = useValidation()

  const isHealthy = health?.status === 'healthy'
  const isDevMode = health?.dev_mode ?? true

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
          { to: '/staleness', label: 'Staleness' },
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
        {/* Staleness indicator */}
        <StalenessIndicator />

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
          className="rounded bg-slate-800 px-3 py-1 text-[11px] font-medium text-white hover:bg-slate-700 disabled:opacity-50"
          onClick={() => validation.mutate({ env: selectedEnv, pipeline: pipelineFilter ?? undefined })}
          disabled={validation.isPending}
        >
          {validation.isPending ? 'Validating...' : 'Validate'}
        </button>

        {/* Workspace badge */}
        <WorkspaceBadge isDevMode={isDevMode} workspaceStatus={workspaceStatus} />

        {/* AI chat toggle */}
        <ChatToggleButton />

        {/* Health indicator */}
        <HealthDot isHealthy={isHealthy} />
      </div>
    </header>
  )
}

// ── Exported Header: delegates based on `minimal` prop ──

export function Header({ minimal = false }: { minimal?: boolean }) {
  if (minimal) {
    return <MinimalHeader />
  }
  return <FullHeader />
}
