import type { ReactNode, RefObject } from 'react'
import { cn } from '@/lib/utils'
import type { ActionRead } from '@/lib/flowgroup-doc'
import type { FlowgroupActionSummary } from '@/types/api'
import { ActionForm } from './ActionForm'
import { NodeActionsToolbar } from './NodeActionsToolbar'
import type { CodeTarget } from './CodeModal'
import { actionConfigRows, resolvedSublabel, sourceSublabel } from './designerGraph'
import { getActionSpec } from './specs/registry'
import { useActionPresetBadges } from './presetTargets'
import type { DesignerMutator } from './useDesignerWrite'

// ── DesignerInspector — detail / editing panel for the selection ─
//
// The right-hand panel of the designer. When the selected action's sub-type
// has a registered spec, the panel is an editable ActionForm (immediate
// write-through). Otherwise it falls back to the read-only summary plus a
// note pointing at the YAML — so not-yet-built sub-types stay inspectable.

export type InspectorSelection =
  | { mode: 'source'; action: ActionRead; actionId: string }
  | { mode: 'resolved'; action: FlowgroupActionSummary }
  | { mode: 'external'; label: string; consumers: string[] }

/** Kind accent for the header rail — the panel's single use of color. */
const KIND_RAIL: Record<string, string> = {
  load: 'bg-kind-load',
  transform: 'bg-kind-transform',
  write: 'bg-kind-write',
  test: 'bg-kind-test',
}

const MAX_CONFIG_ROWS = 12

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <dt className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-0.5 break-all text-xs text-foreground">{children}</dd>
    </div>
  )
}

function MonoList({ items }: { items: string[] }) {
  return (
    <span className="flex flex-col gap-0.5">
      {items.map((item, i) => (
        <span key={`${item}-${i}`} className="font-mono">
          {item}
        </span>
      ))}
    </span>
  )
}

function Header({ name, sublabel, kind }: { name: string; sublabel: string; kind: string }) {
  return (
    <div className="border-b border-border px-4 pb-3 pt-4">
      <div className={cn('mb-2 h-0.5 w-8 rounded-full', KIND_RAIL[kind] ?? 'bg-border')} />
      <div className="break-all text-sm font-semibold text-foreground">{name}</div>
      <div className="mt-0.5 text-xs text-muted-foreground">{sublabel}</div>
    </div>
  )
}

function SourceDetail({ action }: { action: ActionRead }) {
  const description =
    typeof action.raw.description === 'string' ? action.raw.description : undefined
  const rows = actionConfigRows(action.raw)
  const shown = rows.slice(0, MAX_CONFIG_ROWS)

  return (
    <>
      <Header
        name={action.name !== '' ? action.name : `(action ${action.index + 1})`}
        sublabel={sourceSublabel(action.kind ?? 'unknown', action.subType, action.writeMode)}
        kind={action.kind ?? 'unknown'}
      />
      <dl className="flex flex-col gap-3 px-4 py-3">
        <div className="rounded-sm border border-dashed border-border px-2 py-1.5 text-2xs text-muted-foreground">
          A form for this sub-type is coming soon — open the file as text to edit it.
        </div>
        {action.target !== undefined && (
          <Row label="Target">
            <span className="font-mono">{action.target}</span>
          </Row>
        )}
        {action.sources.length > 0 && (
          <Row label="Sources">
            <MonoList items={action.sources} />
          </Row>
        )}
        {action.dependsOn.length > 0 && (
          <Row label="Depends on">
            <MonoList items={action.dependsOn} />
          </Row>
        )}
        {description !== undefined && description !== '' && (
          <Row label="Description">{description}</Row>
        )}
        {shown.length > 0 && (
          <>
            <div className="mt-1 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
              Configuration
            </div>
            {shown.map((row) => (
              <div key={row.key} className="flex flex-col">
                <dt className="break-all font-mono text-2xs text-muted-foreground">{row.key}</dt>
                <dd className="break-all font-mono text-xs text-foreground">{row.value}</dd>
              </div>
            ))}
            {rows.length > shown.length && (
              <div className="text-2xs text-muted-foreground">
                +{rows.length - shown.length} more — open the file as text to see everything
              </div>
            )}
          </>
        )}
      </dl>
    </>
  )
}

function ResolvedDetail({ action }: { action: FlowgroupActionSummary }) {
  return (
    <>
      <Header name={action.name} sublabel={resolvedSublabel(action)} kind={action.type} />
      <dl className="flex flex-col gap-3 px-4 py-3">
        {action.target != null && (
          <Row label="Target">
            <span className="font-mono">{action.target}</span>
          </Row>
        )}
        {action.target_full_name != null && (
          <Row label="Full name">
            <span className="font-mono">{action.target_full_name}</span>
          </Row>
        )}
        {action.write_mode != null && <Row label="Write mode">{action.write_mode}</Row>}
        {action.scd_type != null && <Row label="SCD type">{String(action.scd_type)}</Row>}
        {action.description != null && action.description !== '' && (
          <Row label="Description">{action.description}</Row>
        )}
        <div className="text-2xs text-muted-foreground">
          Resolved view — values after template and preset expansion.
        </div>
      </dl>
    </>
  )
}

function ExternalDetail({ label, consumers }: { label: string; consumers: string[] }) {
  return (
    <>
      <Header name={label} sublabel="External source" kind="external" />
      <dl className="flex flex-col gap-3 px-4 py-3">
        <div className="text-xs text-muted-foreground">
          Produced outside this flowgroup (another flowgroup&apos;s view or an existing
          table).
        </div>
        {consumers.length > 0 && (
          <Row label="Feeds">
            <MonoList items={consumers} />
          </Row>
        )}
      </dl>
    </>
  )
}

export interface DesignerInspectorProps {
  selection: InspectorSelection | null
  /** Flowgroup presets (for the per-action preset badge). */
  presets: string[]
  /** Apply an edit + write through (from useDesignerWrite). */
  commit: (mutator: DesignerMutator) => void
  /** Rename an action; resolves true only when it persisted. */
  rename: (actionId: string, newName: string) => Promise<boolean>
  /** Disable editing (dirty text buffer or unresolved conflict). */
  readOnly: boolean
  /** A write is in flight — gates the ActionForm structural list/map controls. */
  saving?: boolean
  /** Re-select by the new node id after a rename. */
  onRenamed: (newName: string) => void
  /** Open a preset file as a text buffer (badge click). */
  onOpenPreset?: (name: string) => void
  /** Open the Monaco code modal for a SQL/Python/expectations field. */
  onEditCode: (target: CodeTarget) => void
  /** Producer views selectable as a fan-in input for the selected action. */
  producerViews: string[]
  /** The selected action reads a string/list source (fan-in applies). */
  canAddInput: boolean
  /** Duplicate the selected action (unique name). */
  onDuplicate: () => void
  /** Delete the selected action (edges recompute; no cascade). */
  onDelete: () => void
  /** Append a producer view to the selected action's source (fan-in). */
  onAddInput: (view: string) => void
  /** Enter on a keyboard-selected canvas node hands focus to this panel. */
  panelRef?: RefObject<HTMLElement | null>
}

export function DesignerInspector({
  selection,
  presets,
  commit,
  rename,
  readOnly,
  saving,
  onRenamed,
  onOpenPreset,
  onEditCode,
  producerViews,
  canAddInput,
  onDuplicate,
  onDelete,
  onAddInput,
  panelRef,
}: DesignerInspectorProps) {
  const sourceSel = selection?.mode === 'source' ? selection : null
  // Hooks run unconditionally; undefined kind/subType yields no badges.
  const presetBadges = useActionPresetBadges(
    presets,
    sourceSel?.action.kind,
    sourceSel?.action.subType,
  )
  const spec = getActionSpec(sourceSel?.action.kind, sourceSel?.action.subType)

  let body: ReactNode
  if (selection === null) {
    body = (
      <div className="px-4 py-4">
        <div className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Inspector
        </div>
        <p className="mt-2 text-xs text-muted-foreground">
          Select an action on the canvas to see its details.
        </p>
      </div>
    )
  } else if (selection.mode === 'source') {
    body =
      spec !== undefined ? (
        <ActionForm
          spec={spec}
          action={selection.action}
          actionId={selection.actionId}
          commit={commit}
          rename={rename}
          readOnly={readOnly}
          saving={saving}
          presetBadges={presetBadges}
          onRenamed={onRenamed}
          onOpenPreset={onOpenPreset}
          onEditCode={onEditCode}
        />
      ) : (
        <SourceDetail action={selection.action} />
      )
  } else if (selection.mode === 'resolved') {
    body = <ResolvedDetail action={selection.action} />
  } else {
    body = <ExternalDetail label={selection.label} consumers={selection.consumers} />
  }

  return (
    <aside
      ref={panelRef}
      tabIndex={-1}
      aria-label="Inspector"
      className="flex w-72 shrink-0 flex-col overflow-y-auto border-l border-border bg-card outline-none"
    >
      {sourceSel !== null && (
        <NodeActionsToolbar
          readOnly={readOnly}
          producerViews={producerViews}
          canAddInput={canAddInput}
          onDuplicate={onDuplicate}
          onDelete={onDelete}
          onAddInput={onAddInput}
        />
      )}
      {body}
    </aside>
  )
}
