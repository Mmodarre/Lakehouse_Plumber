import type { ReactNode } from 'react'

// ── ConfigPageShell — layout frame for one Config tab ────────
//
// The per-surface editors (project form, pipeline editor, job editor —
// Phases 3–5) render inside this frame and only fill its slots; the frame
// itself owns scrolling: header + banner stay put, `children` scroll,
// `footer` (SaveBar) is pinned below the scroll region.

export interface ConfigPageShellProps {
  /** Tab heading (e.g. "Project configuration"). */
  title: string
  /** One-line explanation under the heading. */
  description?: string
  /** Slot beside the heading — the file picker on pipeline/job tabs. */
  picker?: ReactNode
  /** Right-aligned header actions (e.g. "New from template"). */
  actions?: ReactNode
  /** Non-modal notices above the content (ExternalChangeBanner). */
  banner?: ReactNode
  /** Pinned footer slot (SaveBar). */
  footer?: ReactNode
  /** Scrollable main content — the (future) form body. */
  children: ReactNode
}

export function ConfigPageShell({
  title,
  description,
  picker,
  actions,
  banner,
  footer,
  children,
}: ConfigPageShellProps) {
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex flex-wrap items-center gap-3 border-b border-border px-6 py-3">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-foreground">{title}</h2>
          {description && (
            <p className="mt-0.5 text-xs text-muted-foreground">{description}</p>
          )}
        </div>
        {picker && <div className="shrink-0">{picker}</div>}
        {actions && <div className="ml-auto flex shrink-0 items-center gap-2">{actions}</div>}
      </div>

      {banner}

      <div className="min-h-0 flex-1 overflow-y-auto px-6 py-4">
        <div className="mx-auto w-full max-w-3xl space-y-4">{children}</div>
      </div>

      {footer}
    </div>
  )
}
