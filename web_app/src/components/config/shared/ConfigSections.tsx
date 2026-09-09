import { useShallow } from 'zustand/react/shallow'
import { useCallback, useState } from 'react'
import { normalizeSettingSearch, SectionsContext } from './configSectionsContext'
import type { SectionInfo } from './configSectionsContext'
import type { ReactNode } from 'react'
import { Search, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useConfigViewStore } from './configViewState'

/** Presentation-only navigation. Neither searching nor collapsing calls the document API. */
export function ConfigSections({ scope, children }: { scope: string; children: ReactNode }) {
  const view = useConfigViewStore(
    useShallow((state) => ({
      query: state.views[scope]?.query,
      configuredOnly: state.views[scope]?.configuredOnly,
      collapsed: state.views[scope]?.collapsed,
      section: state.views[scope]?.section,
    })),
  )
  const update = useConfigViewStore((state) => state.update)
  const [sections, setSections] = useState<SectionInfo[]>([])
  const query = view?.query ?? ''
  const configuredOnly = view?.configuredOnly ?? false
  const collapsed = view?.collapsed ?? {}
  const normalizedQuery = normalizeSettingSearch(query)
  const visible = sections.filter(
    (section) =>
      (!configuredOnly || section.present) &&
      (!normalizedQuery || section.search.includes(normalizedQuery)),
  )

  const register = useCallback((section: SectionInfo) => {
    setSections((current) => {
      const previous = current.find((item) => item.id === section.id)
      if (previous && JSON.stringify(previous) === JSON.stringify(section)) return current
      return previous
        ? current.map((item) => (item.id === section.id ? section : item))
        : [...current, section]
    })
  }, [])
  const unregister = useCallback((id: string) => {
    setSections((current) => current.filter((section) => section.id !== id))
  }, [])
  const toggle = useCallback(
    (id: string, next: boolean) => {
      const current = useConfigViewStore.getState().views[scope]
      update(scope, { collapsed: { ...current?.collapsed, [id]: next } })
    },
    [scope, update],
  )
  const value = {
    query: normalizedQuery,
    configuredOnly,
    collapsed,
    register,
    unregister,
    toggle,
  }

  const jump = (id: string) => {
    toggle(id, false)
    update(scope, { section: id })
    requestAnimationFrame(() => {
      const section = document.getElementById(id)
      section?.scrollIntoView?.({ block: 'start', behavior: 'smooth' })
      section
        ?.querySelector<HTMLElement>('[data-section-disclosure]')
        ?.focus({ preventScroll: true })
    })
  }

  return (
    <SectionsContext.Provider value={value}>
      <div
        className="sticky top-0 z-10 space-y-2 rounded-md border border-border bg-background p-3"
        data-testid="config-section-navigation"
      >
        <div className="flex flex-wrap items-center gap-2">
          <label className="flex min-w-40 flex-1 items-center gap-2 rounded-sm border border-border px-2 py-1.5">
            <Search className="size-3.5 text-muted-foreground" aria-hidden="true" />
            <input
              type="search"
              aria-label="Search settings"
              placeholder="Search settings or YAML keys…"
              value={query}
              onChange={(event) => update(scope, { query: event.target.value })}
              className="min-w-0 flex-1 bg-transparent text-xs outline-none"
            />
            {query && (
              <button
                type="button"
                aria-label="Clear settings search"
                onClick={() => update(scope, { query: '' })}
              >
                <X className="size-3.5" />
              </button>
            )}
          </label>
          <label className="flex items-center gap-1.5 text-xs">
            <input
              type="checkbox"
              checked={configuredOnly}
              onChange={(event) => update(scope, { configuredOnly: event.target.checked })}
            />
            Configured sections only
          </label>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <select
            aria-label="Jump to section"
            value={view?.section && visible.some((s) => s.id === view.section) ? view.section : ''}
            onChange={(event) => jump(event.target.value)}
            className="min-w-0 flex-1 rounded-sm border border-border bg-background px-2 py-1 text-xs"
          >
            <option value="" disabled>
              Jump to section ({visible.length})
            </option>
            {visible.map((section) => (
              <option key={section.id} value={section.id}>
                Go to {section.title}
                {section.issues ? ` · ${section.issues} issues` : ''}
              </option>
            ))}
          </select>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() =>
              update(scope, {
                query: '',
                collapsed: Object.fromEntries(sections.map((section) => [section.id, true])),
              })
            }
          >
            Collapse all
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => update(scope, { collapsed: {} })}
          >
            Expand all
          </Button>
        </div>
        {normalizedQuery && (
          <p role="status" className="text-2xs text-muted-foreground">
            {visible.length
              ? `${visible.length} matching sections`
              : 'No matching settings. Try another label or YAML key.'}
          </p>
        )}
      </div>
      {children}
    </SectionsContext.Provider>
  )
}
