import { useEffect } from 'react'
import {
  CircleCheck,
  Eye,
  FileCog,
  Layers,
  Loader2,
  Play,
  Sparkles,
  Wifi,
  WifiOff,
  X,
} from 'lucide-react'
import { toast } from 'sonner'
import { Badge } from '../ui/badge'
import { Button } from '../ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select'
import { ThemeToggle } from '../layout/ThemeToggle'
import { SandboxControl } from '../sandbox/SandboxControl'
import { cn } from '../../lib/utils'
import { useProject, useHealth } from '../../hooks/useProject'
import { useEnvironments } from '../../hooks/useEnvironments'
import { useFileList } from '../../hooks/useFiles'
import type { FileNode } from '../../types/api'
import { useUIStore } from '../../store/uiStore'
import { useRunController, useRunStore } from '../../store/runStore'
import { useLayoutStore } from '../../store/layoutStore'

// ── CommandBar — the unified-workspace top command bar (§3/§6.4) ──
//
// Replaces the old Header. Relocates its still-live controls verbatim —
// version chip, env selector, SandboxControl, run-config chip,
// Validate/Generate (useRunController), theme toggle, health indicator — and
// wires the viewer-lens + assistant toggles to layoutStore. The old Header's
// nav tabs / Resources dropdown / sidebar toggle die (the explorer replaces
// them); the pipeline filter is NOT here (it moves into the Project-map
// toolbar in a sibling task) — uiStore.pipelineFilter is left untouched.

/** 16px LHP pipe-glyph mark (mirrors public/favicon.svg). */
function LogoMark() {
  return (
    <svg viewBox="0 0 32 32" className="h-5 w-5 shrink-0 text-primary" aria-hidden="true">
      <path
        d="M7 4v10a11 11 0 0 0 11 11h10"
        fill="none"
        stroke="currentColor"
        strokeWidth="5"
        strokeLinecap="round"
      />
      <path
        d="M17 4v6a5 5 0 0 0 5 5h6"
        fill="none"
        stroke="currentColor"
        strokeWidth="5"
        strokeLinecap="round"
        opacity="0.5"
      />
    </svg>
  )
}

function HealthIndicator({ ok, pending }: { ok: boolean; pending: boolean }) {
  // Neutral while the first health response is in flight — red means a real
  // error/unhealthy answer, not "haven't heard back yet".
  const label = pending ? 'Connecting…' : ok ? 'API connected' : 'API unreachable'
  const Icon = pending ? Loader2 : ok ? Wifi : WifiOff
  return (
    <span
      role="status"
      title={label}
      className={cn(
        'inline-flex items-center px-1',
        pending ? 'text-muted-foreground' : ok ? 'text-success' : 'text-error',
      )}
    >
      <Icon className={cn('h-3.5 w-3.5', pending && 'animate-spin')} aria-hidden="true" />
      {/* Text (not attribute-only) change inside the live region so screen
          readers announce health transitions. */}
      <span className="sr-only">{label}</span>
    </span>
  )
}

/** Does the files tree contain `path` as a file? (local walk — the command
 * bar must not import components/config support modules, which live in the
 * lazy Config chunk). */
function treeHasFile(node: FileNode, path: string): boolean {
  if (node.type === 'file') return node.path === path
  return (node.children ?? []).some((child) => treeHasFile(child, path))
}

/**
 * Run-config indicator: a chip beside the env selector, visible while a
 * pipeline config is bound to runs (the pipeline tab's "Use for runs" toggle →
 * uiStore.selectedPipelineConfig). Shows the filename, carries the full path
 * in its tooltip, and clears via ✕.
 *
 * Also the stale-selection guard: the always-mounted command bar is the one
 * place that can watch the files tree, so when the tree confirms the bound
 * path no longer exists the selection self-clears with a toast (instead of
 * wedging every future run on the backend's 404). While the tree is still
 * loading the selection is trusted.
 *
 * Exported for tests.
 */
export function RunConfigChip() {
  const selected = useUIStore((s) => s.selectedPipelineConfig)
  const setSelected = useUIStore((s) => s.setSelectedPipelineConfig)
  const { data: tree } = useFileList()

  const stale = selected !== null && tree !== undefined && !treeHasFile(tree, selected)
  useEffect(() => {
    if (!stale || selected === null) return
    // Re-check against the live store: StrictMode re-runs effects, and the
    // first run already cleared it.
    if (useUIStore.getState().selectedPipelineConfig !== selected) return
    setSelected(null)
    const name = selected.split('/').pop() ?? selected
    toast.info(`${name} no longer exists — runs use no pipeline config`)
  }, [stale, selected, setSelected])

  if (selected === null) return null
  const name = selected.split('/').pop() ?? selected

  return (
    <Badge
      variant="outline"
      title={`Runs use ${selected} for Validate/Generate`}
      className="max-w-40 gap-1 rounded-sm px-1.5 text-2xs text-muted-foreground lg:max-w-56"
    >
      <FileCog className="size-3 shrink-0" aria-hidden="true" />
      <span className="truncate font-mono">{name}</span>
      <button
        type="button"
        onClick={() => {
          setSelected(null)
          toast.info('Runs no longer use a pipeline config')
        }}
        aria-label={`Stop using ${name} for runs`}
        className="shrink-0 rounded-xs text-muted-foreground transition-colors hover:text-foreground"
      >
        <X className="size-3" aria-hidden="true" />
      </button>
    </Badge>
  )
}

export function CommandBar() {
  const { data: project } = useProject()
  const { data: health, isError: healthError, isPending: healthPending } = useHealth()
  const { data: envData } = useEnvironments()
  const selectedEnv = useUIStore((s) => s.selectedEnv)
  const setSelectedEnv = useUIStore((s) => s.setSelectedEnv)
  const pipelineFilter = useUIStore((s) => s.pipelineFilter)
  const sandboxEnabled = useUIStore((s) => s.sandboxEnabled)
  const { isRunning, startValidate, startGenerate } = useRunController()
  const runKind = useRunStore((s) => s.runKind)

  const viewerMode = useLayoutStore((s) => s.viewerMode)
  const toggleViewerMode = useLayoutStore((s) => s.toggleViewerMode)
  const assistantOpen = useLayoutStore((s) => s.assistantOpen)
  const toggleAssistant = useLayoutStore((s) => s.toggleAssistant)

  const isHealthy = health?.status === 'healthy' && !healthError
  const scope = sandboxEnabled ? 'sandbox scope' : (pipelineFilter ?? 'all pipelines')

  return (
    <header className="flex h-11 shrink-0 items-center gap-2.5 border-b border-border bg-surface px-2.5">
      {/* Brand: logomark · wordmark · version chip */}
      <div className="flex min-w-0 items-center gap-2">
        <LogoMark />
        <span className="text-sm font-bold tracking-[0.14em] text-foreground">LHP</span>
        {project?.version && (
          <Badge
            variant="outline"
            className="rounded-sm px-1.5 font-mono text-2xs text-muted-foreground"
          >
            v{project.version}
          </Badge>
        )}
      </div>

      <div className="h-5 w-px shrink-0 bg-border" aria-hidden="true" />

      {/* Project name chip */}
      {project?.name && (
        <Badge
          variant="outline"
          title={project.name}
          className="max-w-40 rounded-sm px-2 font-mono text-2xs text-foreground"
        >
          <span className="truncate">{project.name}</span>
        </Badge>
      )}

      {/* Environment selector */}
      <Select value={selectedEnv} onValueChange={setSelectedEnv}>
        <SelectTrigger size="sm" aria-label="Environment" className="gap-1.5">
          <Layers className="size-3.5 text-muted-foreground" aria-hidden="true" />
          <SelectValue placeholder="env" />
        </SelectTrigger>
        <SelectContent position="popper" align="start">
          {(envData?.environments ?? ['dev']).map((env) => (
            <SelectItem key={env} value={env}>
              {env}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {/* Viewer-lens badge (read-only preview active) */}
      {viewerMode && (
        <Badge
          variant="outline"
          className="gap-1 rounded-sm border-info/40 bg-info/10 px-1.5 text-2xs font-semibold tracking-[0.09em] text-info uppercase"
        >
          <Eye className="size-3" aria-hidden="true" />
          Viewer
        </Badge>
      )}

      <div className="ml-auto flex items-center gap-2">
        {/* Sandbox mode (scope from .lhp/profile.yaml) */}
        <SandboxControl />

        {/* Run-config indicator (only while a pipeline config is bound) */}
        <RunConfigChip />

        {/* Validate (outline) — Generate below is the accent ▶ run action */}
        <Button
          variant="outline"
          size="sm"
          onClick={() => startValidate()}
          disabled={isRunning}
          title={`Validate ${scope} (${selectedEnv})`}
        >
          {isRunning && runKind === 'validate' ? (
            <Loader2 className="animate-spin" aria-hidden="true" />
          ) : (
            <CircleCheck aria-hidden="true" />
          )}
          Validate
        </Button>

        {/* Generate — the screen's one accent ▶ button (no separate "Run" op:
            only validate/generate streams exist, so Generate IS the run). */}
        <Button
          size="sm"
          onClick={() => startGenerate()}
          disabled={isRunning}
          title={`Generate ${scope} (${selectedEnv})`}
        >
          {isRunning && runKind === 'generate' ? (
            <Loader2 className="animate-spin" aria-hidden="true" />
          ) : (
            <Play aria-hidden="true" />
          )}
          Generate
        </Button>

        <div className="h-5 w-px shrink-0 bg-border" aria-hidden="true" />

        {/* Viewer-lens toggle (visual + badge only; enforcement lands later) */}
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={toggleViewerMode}
          aria-label={viewerMode ? 'Exit viewer lens' : 'Enter viewer lens (read-only preview)'}
          aria-pressed={viewerMode}
          title={viewerMode ? 'Exit viewer lens' : 'Viewer lens (read-only preview)'}
          className={cn('text-muted-foreground', viewerMode && 'bg-accent-weak text-primary')}
        >
          <Eye />
        </Button>

        {/* Theme toggle */}
        <ThemeToggle />

        {/* Health indicator */}
        <HealthIndicator ok={isHealthy} pending={healthPending} />

        {/* Assistant dock toggle (wired to layoutStore.assistantOpen) */}
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={toggleAssistant}
          aria-label={assistantOpen ? 'Close assistant dock' : 'Open assistant dock'}
          aria-pressed={assistantOpen}
          title={assistantOpen ? 'Close assistant dock' : 'Open assistant dock'}
          className={cn('text-muted-foreground', assistantOpen && 'bg-accent-weak text-primary')}
        >
          <Sparkles />
        </Button>
      </div>
    </header>
  )
}
