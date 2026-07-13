import {
  CircleCheck,
  CircleX,
  LayoutTemplate,
  Loader2,
  PlayCircle,
  Plus,
  SlidersHorizontal,
  TriangleAlert,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import type { FlowgroupMeta } from '@/lib/flowgroup-doc'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import type { ValidateStatus } from './useDesignerValidation'

interface DesignerTopBarProps {
  pipeline: string
  flowgroup: string
  /** `null` until the document loads (or when the flowgroup isn't found). */
  meta: FlowgroupMeta | null
  /** Template-authoring mode: identity reads "Template", resolve/validate hidden. */
  isTemplate?: boolean
  env: string
  resolved: boolean
  onResolvedChange: (resolved: boolean) => void
  /** Validation button state + counts (see useDesignerValidation). */
  validateStatus: ValidateStatus
  errorCount: number
  warningCount: number
  errored: boolean
  /** A validate run can be started. */
  canValidate: boolean
  /** Start (or re-run) an on-demand validate scoped to this pipeline. */
  onValidate: () => void
  /** Open the add-action palette (append, unwired). Omitted when composition
   * is unavailable (resolved view, template flowgroup, or read-only). */
  onAddAction?: () => void
}

/** "2 errors · 1 warning" (only the non-zero halves). */
function countsLabel(errors: number, warnings: number): string {
  const parts: string[] = []
  if (errors > 0) parts.push(`${errors} ${errors === 1 ? 'error' : 'errors'}`)
  if (warnings > 0) parts.push(`${warnings} ${warnings === 1 ? 'warning' : 'warnings'}`)
  return parts.join(' · ')
}

/** Informational chip for a preset or template the flowgroup uses. */
function ChromeChip({
  icon: Icon,
  label,
  title,
}: {
  icon: typeof SlidersHorizontal
  label: string
  title: string
}) {
  return (
    <Badge
      variant="outline"
      title={title}
      className="h-5 gap-1 rounded-sm px-1.5 font-mono text-2xs text-muted-foreground"
    >
      <Icon className="size-2.5" aria-hidden="true" />
      {label}
    </Badge>
  )
}

/**
 * Designer chrome: flowgroup identity, template/preset chips (informational),
 * the resolved-view toggle, and the Validate button (disabled until the
 * editing task lands).
 */
export function DesignerTopBar({
  pipeline,
  flowgroup,
  meta,
  isTemplate = false,
  env,
  resolved,
  onResolvedChange,
  validateStatus,
  errorCount,
  warningCount,
  errored,
  canValidate,
  onValidate,
  onAddAction,
}: DesignerTopBarProps) {
  const inherited = new Set(meta?.inherited ?? [])
  const presetTitle = (name: string) =>
    inherited.has('presets')
      ? `Preset '${name}' (inherited from the file root)`
      : `Preset '${name}'`

  return (
    <div className="flex shrink-0 items-center gap-3 border-b border-border bg-card px-4 py-2">
        <div className="min-w-0">
          <div className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            {isTemplate ? 'Template' : 'Designer'}
          </div>
          <div className="flex items-baseline gap-1.5">
            <span className="truncate text-sm font-semibold text-foreground">{flowgroup}</span>
            <span className="truncate text-xs text-muted-foreground">{pipeline}</span>
          </div>
        </div>

        <div className="flex min-w-0 flex-wrap items-center gap-1">
          {isTemplate && (
            <ChromeChip
              icon={LayoutTemplate}
              label="template"
              title={`Authoring template '${flowgroup}'`}
            />
          )}
          {meta?.use_template && (
            <ChromeChip
              icon={LayoutTemplate}
              label={meta.use_template}
              title={
                inherited.has('use_template')
                  ? `Template '${meta.use_template}' (inherited from the file root)`
                  : `Template '${meta.use_template}'`
              }
            />
          )}
          {meta?.presets?.map((preset) => (
            <ChromeChip
              key={preset}
              icon={SlidersHorizontal}
              label={preset}
              title={presetTitle(preset)}
            />
          ))}
        </div>

        <div className="flex-1" />

        {onAddAction && (
          <Button variant="outline" size="xs" onClick={onAddAction} className="shrink-0">
            <Plus aria-hidden="true" />
            Add action
          </Button>
        )}

        {/* Resolve + validate are pipeline/env concepts — a template has
            neither, so template mode drops both controls. */}
        {!isTemplate && (
          <>
            <label className="flex shrink-0 cursor-pointer items-center gap-1.5 text-xs text-muted-foreground">
              <Switch size="sm" checked={resolved} onCheckedChange={onResolvedChange} />
              View resolved
              <span className="font-mono text-2xs">({env})</span>
            </label>

            <ValidateButton
              status={validateStatus}
              errorCount={errorCount}
              warningCount={warningCount}
              errored={errored}
              canValidate={canValidate}
              onValidate={onValidate}
            />
          </>
        )}
      </div>
  )
}

/** The Validate control: idle → running (spinner) → done (severity + counts),
 * re-runnable. On-demand only; results are surfaced as node badges + strip. */
function ValidateButton({
  status,
  errorCount,
  warningCount,
  errored,
  canValidate,
  onValidate,
}: {
  status: ValidateStatus
  errorCount: number
  warningCount: number
  errored: boolean
  canValidate: boolean
  onValidate: () => void
}) {
  if (status === 'running') {
    return (
      <Button variant="outline" size="xs" disabled className="shrink-0">
        <Loader2 className="animate-spin" aria-hidden="true" />
        Validating…
      </Button>
    )
  }

  if (status === 'done') {
    let icon = <CircleCheck className="text-success" aria-hidden="true" />
    let label = 'Valid'
    if (errored) {
      icon = <CircleX className="text-error" aria-hidden="true" />
      label = 'Validation failed'
    } else if (errorCount > 0) {
      icon = <CircleX className="text-error" aria-hidden="true" />
      label = countsLabel(errorCount, warningCount)
    } else if (warningCount > 0) {
      icon = <TriangleAlert className="text-warning" aria-hidden="true" />
      label = countsLabel(errorCount, warningCount)
    }
    return (
      <Button variant="outline" size="xs" onClick={onValidate} className="shrink-0" title="Re-validate">
        {icon}
        {label}
      </Button>
    )
  }

  const button = (
    <Button
      variant="outline"
      size="xs"
      onClick={onValidate}
      disabled={!canValidate}
      className={cn('shrink-0', !canValidate && 'pointer-events-none')}
    >
      <PlayCircle aria-hidden="true" />
      Validate
    </Button>
  )

  if (canValidate) return button
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        {/* span keeps the tooltip alive over the disabled button */}
        <span className="inline-flex" tabIndex={0}>
          {button}
        </span>
      </TooltipTrigger>
      <TooltipContent>A run is already in progress, or this flowgroup has no pipeline.</TooltipContent>
    </Tooltip>
  )
}
