import { useConfigReadOnly } from './shared/configEditingContext'
import { Children, isValidElement, useEffect, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { ChevronRight } from 'lucide-react'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { normalizeSettingSearch, useConfigSections } from './shared/configSectionsContext'

export interface SectionPresence {
  present: boolean
  onEnable: () => void
  onDisable: () => void
  confirmText: string
  disabled?: boolean
}

export interface SectionCardProps {
  title: string
  description?: string
  presence?: SectionPresence
  /** Whether any source keys in this always-visible section are set. */
  configured?: boolean
  children?: ReactNode
}

// Optional sections have no mounted fields until added. Index their YAML keys
// too, so discovery can reveal the Add action without modifying the document.
const OPTIONAL_KEYS: Record<string, string> = {
  Includes: 'include includes pipelines presets templates substitutions',
  'Operational metadata': 'operational_metadata enabled columns select audit source target',
  'Event log': 'event_log enabled catalog schema name',
  Monitoring:
    'monitoring enabled pipeline_name catalog schema streaming_table checkpoint_path job_config_path max_concurrent_streams enable_job_monitoring materialized_views sql sql_path',
  'UC tagging': 'uc_tagging enabled tags tables columns',
  'Test reporting': 'test_reporting enabled output_dir formats',
  'Wheel packaging': 'wheel name version description dependencies',
  Sandbox: 'sandbox enabled root catalog schema_prefix',
}

function fieldSearchText(children: ReactNode): string {
  return Children.toArray(children)
    .map((child): string => {
      if (!isValidElement<Record<string, unknown>>(child))
        return typeof child === 'string' ? child : ''
      const { label, id, helpPath, children: nested } = child.props
      return `${typeof label === 'string' ? label : ''} ${typeof id === 'string' ? id : ''} ${Array.isArray(helpPath) ? helpPath.join('.') : ''} ${fieldSearchText(nested as ReactNode)}`
    })
    .join(' ')
}

/** Expansion is presentation-only. Removing a YAML section is always explicit. */
export function SectionCard({
  title,
  description,
  presence,
  configured: configuredProp,
  children,
}: SectionCardProps) {
  const readOnly = useConfigReadOnly()
  const [confirming, setConfirming] = useState(false)
  const [localCollapsed, setLocalCollapsed] = useState(false)
  const contentRef = useRef<HTMLDivElement | null>(null)
  const present = presence?.present ?? true
  const configured = presence?.present ?? configuredProp ?? true
  const prevPresent = useRef(present)
  const sections = useConfigSections()
  const register = sections?.register
  const unregister = sections?.unregister
  const id = `config-section-${normalizeSettingSearch(title).replace(/[^a-z0-9]+/g, '-')}`
  const [searchText, setSearchText] = useState(() =>
    normalizeSettingSearch(`${title} ${description ?? ''} ${OPTIONAL_KEYS[title] ?? ''}`),
  )
  const [issueCount, setIssueCount] = useState(0)
  const collapsed = sections ? (sections.collapsed[id] ?? false) && !sections.query : localCollapsed
  const hidden = sections
    ? (sections.configuredOnly && !configured) ||
      (!!sections.query && !searchText.includes(sections.query))
    : false

  useEffect(() => {
    const element = contentRef.current
    let active = true
    const collect = () => {
      if (!active) return
      const labels = Array.from(element?.querySelectorAll('label, [data-setting-path]') ?? [])
        .map(
          (label) =>
            `${label.textContent ?? ''} ${label.getAttribute('for') ?? ''} ${label.getAttribute('data-setting-path') ?? ''}`,
        )
        .join(' ')
      const search = normalizeSettingSearch(
        `${title} ${description ?? ''} ${OPTIONAL_KEYS[title] ?? ''} ${fieldSearchText(children)} ${labels}`,
      )
      const issues = Array.from(element?.querySelectorAll('[role="alert"]') ?? []).filter((alert) =>
        alert.textContent?.trim(),
      ).length
      setSearchText(search)
      setIssueCount(issues)
      register?.({ id, title, search, present: configured, issues })
    }
    // Index mounted field labels/errors, including updates inside nested editors.
    // Hidden card content stays mounted so search and drafts survive collapsing.
    const observer = new MutationObserver(collect)
    if (element)
      observer.observe(element, {
        childList: true,
        subtree: true,
        characterData: true,
      })
    collect()
    return () => {
      active = false
      observer.disconnect()
    }
  }, [id, title, description, children, configured, register])

  useEffect(() => () => unregister?.(id), [id, unregister])

  useEffect(() => {
    if (present && !prevPresent.current) {
      contentRef.current
        ?.querySelector<HTMLElement>('input, textarea, [role="switch"], [role="combobox"]')
        ?.focus()
    }
    prevPresent.current = present
  }, [present])

  const toggle = (next: boolean) => {
    if (sections) sections.toggle(id, next)
    else setLocalCollapsed(next)
  }

  return (
    <Card id={id} hidden={hidden} className="scroll-mt-32 gap-3 py-4" data-config-section={title}>
      <CardHeader className="px-4">
        <div className="flex flex-wrap items-start justify-between gap-2">
          <div className="min-w-0 flex-1 space-y-1">
            <button
              type="button"
              data-section-disclosure
              aria-label={`${collapsed ? 'Expand' : 'Collapse'} ${title} section`}
              aria-expanded={present && !collapsed}
              aria-controls={`${id}-content`}
              disabled={!present}
              onClick={() => toggle(!collapsed)}
              className="flex items-center gap-1.5 rounded-sm text-left focus-visible:outline-2 focus-visible:outline-ring"
            >
              <ChevronRight
                className={`size-3.5 shrink-0 transition-transform ${present && !collapsed ? 'rotate-90' : ''}`}
                aria-hidden="true"
              />
              <CardTitle className="text-xs">{title}</CardTitle>
            </button>
            {description && <CardDescription className="text-2xs">{description}</CardDescription>}
            {(presence || configuredProp !== undefined) && (
              <p className="text-2xs text-muted-foreground">
                {configured ? 'Configured' : 'Not configured'}
              </p>
            )}
            {issueCount > 0 && (
              <button
                type="button"
                className="text-2xs text-destructive underline"
                onClick={() => {
                  toggle(false)
                  requestAnimationFrame(() => {
                    const issue = contentRef.current?.querySelector<HTMLElement>('[role="alert"]')
                    const field = issue?.parentElement?.querySelector<HTMLElement>(
                      'input, textarea, [role="combobox"], [role="switch"]',
                    )
                    field?.focus()
                    issue?.scrollIntoView?.({ block: 'nearest' })
                  })
                }}
              >
                {issueCount} {issueCount === 1 ? 'issue' : 'issues'}
              </button>
            )}
          </div>
          {presence && (
            <Button
              type="button"
              size="sm"
              variant="ghost"
              disabled={presence.disabled || readOnly}
              aria-label={`${present ? 'Remove' : 'Add'} ${title} section`}
              onClick={() => {
                if (present) setConfirming(true)
                else {
                  toggle(false)
                  presence.onEnable()
                }
              }}
            >
              {present ? 'Remove section' : 'Add section'}
            </Button>
          )}
        </div>
      </CardHeader>
      {present && children !== undefined && (
        <CardContent
          id={`${id}-content`}
          ref={contentRef}
          hidden={collapsed}
          className="space-y-3 px-4"
        >
          <fieldset
            disabled={readOnly}
            inert={readOnly ? true : undefined}
            aria-disabled={readOnly || undefined}
            className="min-w-0 space-y-3"
          >
            {children}
          </fieldset>
        </CardContent>
      )}
      {presence && (
        <AlertDialog open={confirming} onOpenChange={setConfirming}>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle className="text-base">Remove this section?</AlertDialogTitle>
              <AlertDialogDescription className="text-xs">
                {presence.confirmText}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel size="sm">Keep section</AlertDialogCancel>
              <AlertDialogAction
                disabled={readOnly}
                variant="destructive"
                size="sm"
                onClick={() => {
                  setConfirming(false)
                  presence.onDisable()
                }}
              >
                Remove section
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      )}
    </Card>
  )
}
