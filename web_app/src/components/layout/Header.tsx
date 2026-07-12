import { useEffect, useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import {
  Boxes,
  Check,
  ChevronDown,
  ChevronsUpDown,
  CircleCheck,
  FileCog,
  Layers,
  Loader2,
  PanelLeft,
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
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '../ui/command'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '../ui/dropdown-menu'
import { Popover, PopoverContent, PopoverTrigger } from '../ui/popover'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select'
import { ThemeToggle } from './ThemeToggle'
import { SandboxControl } from '../sandbox/SandboxControl'
import { cn } from '../../lib/utils'
import { useProject, useHealth } from '../../hooks/useProject'
import { useEnvironments } from '../../hooks/useEnvironments'
import { useFileList } from '../../hooks/useFiles'
import { usePipelines } from '../../hooks/usePipelines'
import type { FileNode } from '../../types/api'
import { useUIStore } from '../../store/uiStore'
import { useRunController, useRunStore } from '../../store/runStore'
import { useAssistantStore } from '../../store/assistantStore'

// ── Small helpers (only RunConfigChip is exported, for tests) ──

/** 16px LHP pipe-glyph mark (mirrors public/favicon.svg). */
function LogoMark() {
  return (
    <svg viewBox="0 0 32 32" className="h-4 w-4 shrink-0 text-primary" aria-hidden="true">
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

/** Searchable pipeline filter (shadcn combobox = popover + command).
 * Disabled while sandbox mode is on — scope then comes from
 * .lhp/profile.yaml and a single-pipeline filter would conflict with it. */
function PipelineCombobox({ disabled = false }: { disabled?: boolean }) {
  const { data: pipelines } = usePipelines()
  const pipelineFilter = useUIStore((s) => s.pipelineFilter)
  const setPipelineFilter = useUIStore((s) => s.setPipelineFilter)
  const [open, setOpen] = useState(false)

  const select = (value: string | null) => {
    setPipelineFilter(value)
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          role="combobox"
          aria-expanded={open}
          aria-label="Filter by pipeline"
          disabled={disabled}
          title={
            disabled
              ? 'Pipeline scope comes from your sandbox profile while sandbox mode is on'
              : undefined
          }
          className="w-44 justify-between font-normal"
        >
          <span className="flex min-w-0 items-center gap-1.5">
            <Boxes className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
            <span className="truncate">{disabled ? 'Sandbox scope' : pipelineFilter ?? 'All pipelines'}</span>
          </span>
          <ChevronsUpDown className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-56 p-0" align="end">
        <Command>
          <CommandInput placeholder="Search pipelines…" />
          <CommandList>
            <CommandEmpty>No pipelines found.</CommandEmpty>
            <CommandGroup>
              <CommandItem value="__all__" onSelect={() => select(null)}>
                <Check
                  className={cn('size-3.5', pipelineFilter === null ? 'opacity-100' : 'opacity-0')}
                />
                All pipelines
              </CommandItem>
              {pipelines?.pipelines.map((p) => (
                <CommandItem key={p.name} value={p.name} onSelect={() => select(p.name)}>
                  <Check
                    className={cn(
                      'size-3.5',
                      pipelineFilter === p.name ? 'opacity-100' : 'opacity-0',
                    )}
                  />
                  {p.name}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

/** Does the files tree contain `path` as a file? (local walk — Header must
 * not import components/config support modules, which live in the lazy
 * Config chunk). */
function treeHasFile(node: FileNode, path: string): boolean {
  if (node.type === 'file') return node.path === path
  return (node.children ?? []).some((child) => treeHasFile(child, path))
}

/**
 * Run-config indicator: a chip beside the env selector, visible on every
 * page while a pipeline config is bound to runs (the pipeline tab's "Use
 * for runs" toggle → uiStore.selectedPipelineConfig). Shows the filename,
 * carries the full path in its tooltip, and clears via ✕.
 *
 * Also the stale-selection guard: the always-mounted Header is the one
 * place that can watch the files tree from any page, so when the tree
 * confirms the bound path no longer exists the selection self-clears with
 * a toast (instead of wedging every future run on the backend's 404).
 * While the tree is still loading the selection is trusted, mirroring the
 * Config tab's picker behavior.
 *
 * Exported for tests; rendered only by Header.
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

const NAV_LINKS = [
  { to: '/', label: 'Dashboard' },
  { to: '/flowgroups', label: 'Flowgroups' },
  { to: '/tables', label: 'Tables' },
  { to: '/validation', label: 'Validation' },
  { to: '/runs', label: 'Runs' },
  // NavLink default (non-`end`) matching keeps this active for the whole
  // /config/:section? subtree.
  { to: '/config', label: 'Config' },
]

// Lifecycle resource pages grouped under one "Resources" dropdown so the
// underline nav stays uncluttered.
const RESOURCE_LINKS = [
  { to: '/blueprints', label: 'Blueprints' },
  { to: '/presets', label: 'Presets' },
  { to: '/templates', label: 'Templates' },
  { to: '/environments', label: 'Environments' },
]

/** Dropdown nav entry styled like the underline tabs; active (underlined)
 * when the current route is one of its children. */
function ResourcesNavDropdown() {
  const location = useLocation()
  const isActive = RESOURCE_LINKS.some((link) =>
    location.pathname.startsWith(link.to),
  )

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        className={cn(
          'inline-flex items-center gap-1 border-b-2 px-2.5 text-sm font-medium transition-colors duration-150 outline-none motion-reduce:transition-none',
          isActive
            ? 'border-primary text-foreground'
            : 'border-transparent text-muted-foreground hover:text-foreground',
        )}
      >
        Resources
        <ChevronDown className="size-3.5" aria-hidden="true" />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start">
        {RESOURCE_LINKS.map((link) => (
          <DropdownMenuItem key={link.to} asChild>
            <NavLink
              to={link.to}
              className={({ isActive: itemActive }) =>
                cn('w-full', itemActive && 'font-medium text-foreground')
              }
            >
              {link.label}
            </NavLink>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

// ── Exported Header: single unconditional local-IDE header ──

export function Header() {
  const { data: project } = useProject()
  const { data: health, isError: healthError, isPending: healthPending } = useHealth()
  const { data: envData } = useEnvironments()
  const { selectedEnv, setSelectedEnv, pipelineFilter, sidebarOpen, toggleSidebar, sandboxEnabled } =
    useUIStore()
  const { isRunning, startValidate, startGenerate } = useRunController()
  const runKind = useRunStore((s) => s.runKind)
  const assistantOpen = useAssistantStore((s) => s.panelOpen)
  const toggleAssistant = useAssistantStore((s) => s.togglePanel)

  const isHealthy = health?.status === 'healthy' && !healthError

  return (
    <header className="flex h-12 shrink-0 items-center border-b border-border bg-card px-3">
      {/* Sidebar toggle */}
      <Button
        variant="ghost"
        size="icon-sm"
        onClick={toggleSidebar}
        aria-label={sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar'}
        title={sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar'}
        className="mr-2 text-muted-foreground"
      >
        <PanelLeft />
      </Button>

      {/* Wordmark: logo · project name · version */}
      <div className="flex min-w-0 items-center gap-2">
        <LogoMark />
        <span className="truncate text-sm font-semibold text-foreground">
          {project?.name ?? 'Lakehouse Plumber'}
        </span>
        {project?.version && (
          <Badge variant="outline" className="rounded-sm px-1.5 text-2xs text-muted-foreground">
            v{project.version}
          </Badge>
        )}
      </div>

      {/* Underline tab nav */}
      <nav className="ml-6 flex h-full items-stretch gap-1 self-stretch">
        {NAV_LINKS.map((link) => (
          <NavLink
            key={link.to}
            to={link.to}
            end={link.to === '/'}
            className={({ isActive }) =>
              cn(
                'inline-flex items-center border-b-2 px-2.5 text-sm font-medium transition-colors duration-150 motion-reduce:transition-none',
                isActive
                  ? 'border-primary text-foreground'
                  : 'border-transparent text-muted-foreground hover:text-foreground',
              )
            }
          >
            {link.label}
          </NavLink>
        ))}
        <ResourcesNavDropdown />
      </nav>

      <div className="ml-auto flex items-center gap-2">
        {/* Sandbox mode (scope from .lhp/profile.yaml) */}
        <SandboxControl />

        {/* Pipeline filter — disabled while sandbox mode owns the scope */}
        <PipelineCombobox disabled={sandboxEnabled} />

        {/* Run-config indicator (only while a pipeline config is bound) */}
        <RunConfigChip />

        {/* Environment selector */}
        <Select value={selectedEnv} onValueChange={setSelectedEnv}>
          <SelectTrigger size="sm" aria-label="Environment" className="gap-1.5">
            <Layers className="size-3.5 text-muted-foreground" aria-hidden="true" />
            <SelectValue placeholder="env" />
          </SelectTrigger>
          <SelectContent position="popper" align="end">
            {(envData?.environments ?? ['dev']).map((env) => (
              <SelectItem key={env} value={env}>
                {env}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        {/* Validate (outline) — Generate below is the screen's one filled button */}
        <Button
          variant="outline"
          size="sm"
          onClick={() => startValidate()}
          disabled={isRunning}
          title={`Validate ${sandboxEnabled ? 'sandbox scope' : pipelineFilter ?? 'all pipelines'} (${selectedEnv})`}
        >
          {isRunning && runKind === 'validate' ? (
            <Loader2 className="animate-spin" aria-hidden="true" />
          ) : (
            <CircleCheck aria-hidden="true" />
          )}
          Validate
        </Button>

        {/* Generate (primary fill) */}
        <Button
          size="sm"
          onClick={() => startGenerate()}
          disabled={isRunning}
          title={`Generate ${sandboxEnabled ? 'sandbox scope' : pipelineFilter ?? 'all pipelines'} (${selectedEnv})`}
        >
          {isRunning && runKind === 'generate' ? (
            <Loader2 className="animate-spin" aria-hidden="true" />
          ) : (
            <Play aria-hidden="true" />
          )}
          Generate
        </Button>

        {/* Assistant panel toggle (persisted via assistantStore) */}
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={toggleAssistant}
          aria-label={assistantOpen ? 'Close assistant panel' : 'Open assistant panel'}
          aria-pressed={assistantOpen}
          title={assistantOpen ? 'Close assistant panel' : 'Open assistant panel'}
          className={cn('text-muted-foreground', assistantOpen && 'text-primary')}
        >
          <Sparkles />
        </Button>

        {/* Theme toggle */}
        <ThemeToggle />

        {/* Health indicator */}
        <HealthIndicator ok={isHealthy} pending={healthPending} />
      </div>
    </header>
  )
}
