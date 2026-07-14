import { useCallback, useMemo, useState } from 'react'
import type { LucideIcon } from 'lucide-react'
import {
  ChevronRight,
  FileCog,
  Layers,
  LayoutTemplate,
  Map as MapIcon,
  PackageOpen,
  Search,
  SlidersHorizontal,
  Workflow,
} from 'lucide-react'
import { usePipelines } from '../../../hooks/usePipelines'
import { useFlowgroups } from '../../../hooks/useFlowgroups'
import { useFileList } from '../../../hooks/useFiles'
import { usePresets } from '../../../hooks/usePresets'
import { useTemplates } from '../../../hooks/useTemplates'
import { useBlueprints } from '../../../hooks/useBlueprints'
import { useEnvironments } from '../../../hooks/useEnvironments'
import { useWorkspaceStore, entityTabId } from '../../../store/workspaceStore'
import { useUIStore } from '../../../store/uiStore'
import { useMapEnrichment } from '../../../hooks/useMapEnrichment'
import { useSandboxScope } from '../../sandbox/useSandboxScope'
import { filterFlowgroupsForScope } from '../../sandbox/scopeFilter'
import type { FlowgroupSummary } from '../../../types/api'
import { SectionHead, KindDots, SeverityDot } from './explorerPrimitives'
import { configFilesByKind, flattenFilePaths, resolveResourceFilePath } from './explorerData'
import { cn } from '@/lib/utils'

// ── StructureLens — the Structure explorer lens (T1.3) ───────
//
// A 3-region flex column: a FIXED top (Project-map row + "Search pipelines"
// input), a SINGLE scrolling middle holding ONLY the PIPELINES tree
// (pipeline ▸ flowgroup, kind dots), and a PINNED bottom (hairline top border)
// holding CONFIG (Project / Pipeline / Job) + RESOURCES (Presets / Templates /
// Blueprints / Environments) so they never scroll away. The tree is narrowed
// to the active sandbox scope (whole project when sandbox is off).
//
// Flowgroup rows open an entity tab (no explicit view — the store default
// applies); the Project / Pipeline / Job config rows open a structured config
// tab (openConfigTab); other config + environment rows open the raw YAML
// buffer; preset/template/blueprint rows open a ResourceTab.

/** One clickable explorer row. `depth` sets the left indent (0/1). */
function Row({
  depth = 0,
  icon: Icon,
  label,
  mono = false,
  active = false,
  onClick,
  trailing,
  ariaCurrent,
}: {
  depth?: 0 | 1
  icon?: LucideIcon
  label: string
  mono?: boolean
  active?: boolean
  onClick: () => void
  trailing?: React.ReactNode
  ariaCurrent?: boolean
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-current={ariaCurrent ? 'true' : undefined}
      title={label}
      className={cn(
        'relative flex w-full items-center gap-1.5 py-1 pr-2.5 text-left text-xs transition-colors',
        depth === 0 ? 'pl-3' : 'pl-7',
        active
          ? 'bg-accent-weak font-medium text-foreground'
          : 'text-foreground/90 hover:bg-card',
      )}
    >
      {active && (
        <span
          aria-hidden="true"
          className="absolute inset-y-1 left-0 w-0.5 rounded-full bg-primary"
        />
      )}
      {Icon && <Icon className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />}
      <span className={cn('min-w-0 flex-1 truncate', mono && 'font-mono')}>{label}</span>
      {trailing}
    </button>
  )
}

/** Expandable parent row + its lazily-shown children. */
function TreeGroup({
  id,
  label,
  icon: Icon,
  open,
  onToggle,
  children,
}: {
  id: string
  label: string
  icon?: LucideIcon
  open: boolean
  onToggle: (id: string) => void
  children: React.ReactNode
}) {
  return (
    <div>
      <button
        type="button"
        onClick={() => onToggle(id)}
        aria-expanded={open}
        title={label}
        className="flex w-full items-center gap-1.5 py-1 pr-2.5 pl-2 text-left text-xs font-semibold text-foreground transition-colors hover:bg-card"
      >
        <ChevronRight
          className={cn(
            'size-3.5 shrink-0 text-faint transition-transform',
            open && 'rotate-90',
          )}
          aria-hidden="true"
        />
        {Icon && <Icon className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />}
        <span className="min-w-0 flex-1 truncate font-mono">{label}</span>
      </button>
      {open && <div>{children}</div>}
    </div>
  )
}

/** Toggle-set for group open/closed state, honoring each group's default. A
 * single override set flips the default for touched groups. */
function useGroupToggle() {
  const [overrides, setOverrides] = useState<Set<string>>(new Set())
  const isOpen = useCallback(
    (id: string, defaultOpen: boolean) => (overrides.has(id) ? !defaultOpen : defaultOpen),
    [overrides],
  )
  const toggle = useCallback((id: string) => {
    setOverrides((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])
  return { isOpen, toggle }
}

/** Muted one-liner for a loading/empty section body. */
function Hint({ children, depth = 1 }: { children: React.ReactNode; depth?: 0 | 1 }) {
  return (
    <p className={cn('py-1 pr-2.5 text-2xs text-faint', depth === 0 ? 'pl-3' : 'pl-7')}>
      {children}
    </p>
  )
}

type PipelineNode = { name: string; flowgroups: FlowgroupSummary[] }

export function StructureLens() {
  const { data: pipelineData, isLoading: pipelinesLoading } = usePipelines()
  const { data: flowgroupData, isLoading: flowgroupsLoading } = useFlowgroups()
  const { data: tree } = useFileList()
  const { data: presetData } = usePresets()
  const { data: templateData } = useTemplates()
  const { data: blueprintData } = useBlueprints()
  const { data: envData } = useEnvironments()

  const openEntityTab = useWorkspaceStore((s) => s.openEntityTab)
  const openConfigTab = useWorkspaceStore((s) => s.openConfigTab)
  const openProjectMap = useWorkspaceStore((s) => s.openProjectMap)
  const openResourceTab = useWorkspaceStore((s) => s.openResourceTab)
  const openBuffer = useWorkspaceStore((s) => s.openBuffer)
  const activePath = useWorkspaceStore((s) => s.activePath)
  const selectedEnv = useUIStore((s) => s.selectedEnv)
  const enrichment = useMapEnrichment(selectedEnv)
  const scope = useSandboxScope()

  const { isOpen, toggle } = useGroupToggle()
  const [search, setSearch] = useState('')
  const query = search.trim().toLowerCase()

  const paths = useMemo(() => flattenFilePaths(tree), [tree])
  const configGroups = useMemo(() => configFilesByKind(paths), [paths])

  // Pipeline ▸ flowgroup tree, narrowed to the active sandbox scope (identity
  // when sandbox is off). Pipelines API order first, then any flowgroup-only
  // pipelines appended (sorted). Out-of-scope pipelines are dropped entirely,
  // mirroring how the DAG filters via filterGraphForScope.
  const pipelineTree = useMemo<PipelineNode[]>(() => {
    const flowgroups = filterFlowgroupsForScope(flowgroupData?.flowgroups ?? [], scope)
    const byPipeline = new Map<string, FlowgroupSummary[]>()
    for (const fg of flowgroups) {
      const arr = byPipeline.get(fg.pipeline)
      if (arr) arr.push(fg)
      else byPipeline.set(fg.pipeline, [fg])
    }
    const ordered: string[] = []
    const seen = new Set<string>()
    for (const p of pipelineData?.pipelines ?? []) {
      if (scope && !scope.has(p.name)) continue
      ordered.push(p.name)
      seen.add(p.name)
    }
    for (const name of [...byPipeline.keys()].sort()) {
      if (!seen.has(name)) ordered.push(name)
    }
    return ordered.map((name) => ({
      name,
      flowgroups: [...(byPipeline.get(name) ?? [])].sort((a, b) => a.name.localeCompare(b.name)),
    }))
  }, [pipelineData, flowgroupData, scope])

  // Live text filter over the (already scoped) tree: keep a pipeline whose name
  // matches, else keep only its matching flowgroups.
  const visibleTree = useMemo<PipelineNode[]>(() => {
    if (!query) return pipelineTree
    return pipelineTree
      .map((p) => {
        if (p.name.toLowerCase().includes(query)) return p
        const flowgroups = p.flowgroups.filter((fg) => fg.name.toLowerCase().includes(query))
        return flowgroups.length ? { ...p, flowgroups } : null
      })
      .filter((p): p is PipelineNode => p !== null)
  }, [pipelineTree, query])

  const presets = presetData?.presets ?? []
  const templates = templateData?.templates ?? []
  const blueprints = blueprintData?.blueprints ?? []
  const environments = envData?.environments ?? []

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      {/* Fixed top: project-map row + search */}
      <div className="flex-none pt-1.5" data-testid="explorer-top">
        <div className="px-1.5 pb-1.5">
          <button
            type="button"
            onClick={() => openProjectMap()}
            aria-current={activePath === 'project-map' ? 'true' : undefined}
            className={cn(
              'flex w-full items-center gap-2 rounded-sm border px-2.5 py-1.5 text-left text-xs font-semibold transition-colors',
              activePath === 'project-map'
                ? 'border-accent-line bg-accent-weak text-foreground'
                : 'border-border bg-card text-foreground hover:border-accent-line',
            )}
          >
            <MapIcon className="size-3.5 shrink-0 text-primary" aria-hidden="true" />
            <span className="flex-1">Project map</span>
            <span className="font-mono text-2xs text-faint">DAG</span>
          </button>
        </div>
        <div className="px-1.5 pb-1.5">
          <div className="flex h-[26px] items-center gap-1.5 rounded-sm border border-border bg-background px-2">
            <Search className="size-3.5 shrink-0 text-faint" aria-hidden="true" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search pipelines…"
              aria-label="Search pipelines"
              className="min-w-0 flex-1 bg-transparent text-xs text-foreground placeholder:text-faint focus:outline-none"
            />
          </div>
        </div>
      </div>

      {/* Single scrolling region: the pipelines tree only */}
      <div className="min-h-0 flex-1 overflow-auto pb-2" data-testid="explorer-pipelines">
        <SectionHead>Pipelines</SectionHead>
        {pipelinesLoading || flowgroupsLoading ? (
          <Hint depth={0}>Loading…</Hint>
        ) : visibleTree.length === 0 ? (
          <Hint depth={0}>{query ? 'No matches' : 'No pipelines'}</Hint>
        ) : (
          visibleTree.map((pipeline) => {
            const gid = `pipe:${pipeline.name}`
            return (
              <TreeGroup
                key={gid}
                id={gid}
                label={pipeline.name}
                open={query ? true : isOpen(gid, false)}
                onToggle={toggle}
              >
                {pipeline.flowgroups.length === 0 ? (
                  <Hint>No flowgroups</Hint>
                ) : (
                  pipeline.flowgroups.map((fg) => {
                    const id = entityTabId(fg.pipeline, fg.name)
                    return (
                      <Row
                        key={fg.name}
                        depth={1}
                        icon={Workflow}
                        label={fg.name}
                        mono
                        active={activePath === id}
                        ariaCurrent={activePath === id}
                        trailing={
                          <span className="ml-auto flex shrink-0 items-center gap-1.5">
                            <SeverityDot severity={enrichment.severityFor(fg.pipeline, fg.name)} />
                            <KindDots actionTypes={fg.action_types} />
                          </span>
                        }
                        onClick={() => openEntityTab(fg.pipeline, fg.name, fg.source_file)}
                      />
                    )
                  })
                )}
              </TreeGroup>
            )
          })
        )}
      </div>

      {/* Pinned bottom: Config + Resources (never scrolls off) */}
      <div
        className="max-h-[45%] flex-none overflow-auto border-t border-border bg-surface pt-1 pb-2"
        data-testid="explorer-pinned"
      >
        {/* Config */}
        <SectionHead>Config</SectionHead>
        <Row
          icon={FileCog}
          label="Project"
          active={activePath === 'config:lhp.yaml'}
          onClick={() => openConfigTab('lhp.yaml', 'project')}
        />
        <ConfigGroup
          id="cfg:pipeline"
          label="Pipeline"
          files={configGroups.pipeline}
          open={isOpen('cfg:pipeline', true)}
          onToggle={toggle}
          activePath={activePath}
          onOpen={(path) => openConfigTab(path, 'pipeline')}
          tabIdFor={configTabId}
        />
        <ConfigGroup
          id="cfg:job"
          label="Job"
          files={configGroups.job}
          open={isOpen('cfg:job', true)}
          onToggle={toggle}
          activePath={activePath}
          onOpen={(path) => openConfigTab(path, 'job')}
          tabIdFor={configTabId}
        />
        {configGroups.other.length > 0 && (
          <ConfigGroup
            id="cfg:other"
            label="Other"
            files={configGroups.other}
            open={isOpen('cfg:other', true)}
            onToggle={toggle}
            activePath={activePath}
            onOpen={openBuffer}
          />
        )}

        {/* Resources */}
        <SectionHead>Resources</SectionHead>
        <ResourceGroup
          id="res:presets"
          label="Presets"
          icon={SlidersHorizontal}
          open={isOpen('res:presets', false)}
          onToggle={toggle}
          items={presets}
          emptyHint="No presets"
          renderItem={(name) => (
            <Row
              key={name}
              depth={1}
              label={name}
              mono
              active={activePath === `resource:preset:${resolveResourceFilePath(paths, 'presets', name)}`}
              onClick={() =>
                openResourceTab('preset', name, resolveResourceFilePath(paths, 'presets', name))
              }
            />
          )}
        />
        <ResourceGroup
          id="res:templates"
          label="Templates"
          icon={LayoutTemplate}
          open={isOpen('res:templates', false)}
          onToggle={toggle}
          items={templates}
          emptyHint="No templates"
          renderItem={(name) => (
            <Row
              key={name}
              depth={1}
              label={name}
              mono
              active={
                activePath === `resource:template:${resolveResourceFilePath(paths, 'templates', name)}`
              }
              onClick={() =>
                openResourceTab('template', name, resolveResourceFilePath(paths, 'templates', name))
              }
            />
          )}
        />
        <ResourceGroup
          id="res:blueprints"
          label="Blueprints"
          icon={PackageOpen}
          open={isOpen('res:blueprints', false)}
          onToggle={toggle}
          items={blueprints.map((b) => b.name)}
          emptyHint="No blueprints"
          renderItem={(name) => (
            <Row
              key={name}
              depth={1}
              label={name}
              mono
              active={
                activePath === `resource:blueprint:${resolveResourceFilePath(paths, 'blueprints', name)}`
              }
              onClick={() =>
                openResourceTab('blueprint', name, resolveResourceFilePath(paths, 'blueprints', name))
              }
            />
          )}
        />
        <ResourceGroup
          id="res:environments"
          label="Environments"
          icon={Layers}
          open={isOpen('res:environments', false)}
          onToggle={toggle}
          items={environments}
          emptyHint="No environments"
          renderItem={(env) => {
            const path = `substitutions/${env}.yaml`
            return (
              <Row
                key={env}
                depth={1}
                label={env}
                mono
                active={activePath === path}
                onClick={() => openBuffer(path)}
              />
            )
          }}
        />
      </div>
    </div>
  )
}

/** A RESOURCES sub-group: expandable parent whose children are the individual
 * resource rows (or a muted empty hint). */
function ResourceGroup({
  id,
  label,
  icon,
  open,
  onToggle,
  items,
  emptyHint,
  renderItem,
}: {
  id: string
  label: string
  icon: LucideIcon
  open: boolean
  onToggle: (id: string) => void
  items: string[]
  emptyHint: string
  renderItem: (name: string) => React.ReactNode
}) {
  return (
    <TreeGroup id={id} label={label} icon={icon} open={open} onToggle={onToggle}>
      {items.length === 0 ? <Hint>{emptyHint}</Hint> : items.map(renderItem)}
    </TreeGroup>
  )
}

/** Strip-wide tab id for a config surface file (matches `workspaceTabId` for a
 * ConfigTab) — the active-state key for rows that open a structured config tab.
 */
const configTabId = (path: string): string => `config:${path}`

/** A CONFIG sub-group (Pipeline / Job / Other): the discovered config files.
 * Pipeline / Job rows open a structured config tab (`onOpen` bound to
 * `openConfigTab`, with `tabIdFor` mapping to the config tab id for the
 * active-state check); the Other group opens the raw YAML buffer (`tabIdFor`
 * defaults to identity). */
function ConfigGroup({
  id,
  label,
  files,
  open,
  onToggle,
  activePath,
  onOpen,
  tabIdFor = (path) => path,
}: {
  id: string
  label: string
  files: string[]
  open: boolean
  onToggle: (id: string) => void
  activePath: string | null
  onOpen: (path: string) => void
  tabIdFor?: (path: string) => string
}) {
  return (
    <TreeGroup id={id} label={label} open={open} onToggle={onToggle}>
      {files.length === 0 ? (
        <Hint>None</Hint>
      ) : (
        files.map((path) => (
          <Row
            key={path}
            depth={1}
            label={path.split('/').pop() ?? path}
            mono
            active={activePath === tabIdFor(path)}
            onClick={() => onOpen(path)}
          />
        ))
      )}
    </TreeGroup>
  )
}
