import { lazy, Suspense, useEffect, useRef, useState } from 'react'
import { ArrowLeft, ArrowRight, Copy, Map, Search } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '../ui/button'
import { useWorkspaceStore, workspaceTabId, type WorkspaceTabRef } from '@/store/workspaceStore'
import { useLayoutStore } from '@/store/layoutStore'
import { openWorkspaceFile } from '@/workspace/openWorkspaceFile'
import { decodeTab, encodeTab, useNavigationStore } from '@/workspace/navigation'

const QuickOpenDialog = lazy(() => import('./QuickOpenDialog'))

async function openTab(tab: WorkspaceTabRef) {
  const s = useWorkspaceStore.getState()
  switch (tab.kind) {
    case 'file': await openWorkspaceFile(tab.path, { source: true }); break
    case 'config': s.openConfigTab(tab.path, tab.configKind, { view: tab.view }); break
    case 'entity': s.openEntityTab(tab.pipeline, tab.flowgroup, tab.filePath, { docKind: tab.docKind, view: tab.view }); break
    case 'project-map': s.openProjectMap(); break
    case 'pipeline-dag': s.openPipelineDag(tab.pipeline); break
    case 'table-detail': s.openTableDetail(tab.fqn); break
    case 'resource': s.openResourceTab(tab.resourceKind, tab.name, tab.filePath); break
  }
}

export function WorkspaceNavigation() {
  const [searchOpen, setSearchOpen] = useState(false)
  const tabs = useWorkspaceStore((s) => s.tabs)
  const activeId = useWorkspaceStore((s) => s.activePath)
  const projectRoot = useWorkspaceStore((s) => s.projectRoot)
  const { entries, index } = useNavigationStore()
  const active = tabs.find((t) => workspaceTabId(t) === activeId)
  const encoded = active ? encodeTab(active) : ''
  const [initializedRoot, setInitializedRoot] = useState<string | null>(null)
  const initialLink = useRef(new URLSearchParams(window.location.search).get('tab'))
  const invalidLinkReported = useRef(false)
  const oldProject = useRef(projectRoot)


  useEffect(() => {
    if (!projectRoot) return
    const changedProject = oldProject.current !== null && oldProject.current !== projectRoot
    if (changedProject) useNavigationStore.getState().reset()
    oldProject.current = projectRoot
    let cancelled = false
    void (async () => {
      const param = changedProject ? null : initialLink.current
      const linked = decodeTab(param)
      if (linked) await openTab(linked)
      else if (param && !invalidLinkReported.current) {
        invalidLinkReported.current = true
        toast.error('This workspace link is invalid. Choose a file or project map.')
      }
      if (!cancelled) setInitializedRoot(projectRoot)
    })()
    return () => { cancelled = true }
  }, [projectRoot])
  useEffect(() => {
    if (!encoded || initializedRoot !== projectRoot) return
    const tab = decodeTab(encoded)
    if (!tab) return
    useNavigationStore.getState().visit(tab)
    const url = new URL(window.location.href)
    url.searchParams.set('tab', encoded)
    // URL is shareable; internal history retains unsaved buffers and avoids a
    // route-unload prompt for ordinary document navigation.
    window.history.replaceState(window.history.state, '', url)
  }, [encoded, initializedRoot, projectRoot])
  const move = (offset: number) => {
    const tab = useNavigationStore.getState().move(offset)
    if (tab) openTab(tab)
  }
  useEffect(() => {
    const key = (e: KeyboardEvent) => {
      if (e.isComposing || e.defaultPrevented || e.target instanceof Element && e.target.closest('[role="dialog"], [role="alertdialog"]')) return
      const mod = e.metaKey || e.ctrlKey
      if (mod && e.key.toLowerCase() === 'k') { e.preventDefault(); setSearchOpen(true); return }
      if (mod && !e.shiftKey && !e.altKey) {
        const l = useLayoutStore.getState()
        const action = ({ b: l.toggleExplorer, i: l.toggleInspector, j: l.toggleBottom } as Record<string, () => void>)[e.key.toLowerCase()]
        if (action) { e.preventDefault(); action() }
      }
      if (mod && e.shiftKey && e.key.toLowerCase() === 'f') {
        e.preventDefault()
        useLayoutStore.getState().toggleFocusMode()
        if (useLayoutStore.getState().focusMode) document.querySelector<HTMLElement>('[data-workspace-center]')?.focus()
      }
      const typing = e.target instanceof Element && e.target.closest('input, textarea, [contenteditable="true"]')
      if (e.altKey && !typing && (e.key === 'ArrowLeft' || e.key === 'ArrowRight')) {
        e.preventDefault()
        const tab = useNavigationStore.getState().move(e.key === 'ArrowLeft' ? -1 : 1)
        if (tab) openTab(tab)
      }
    }
    window.addEventListener('keydown', key)
    return () => window.removeEventListener('keydown', key)
  }, [])

  return (
    <div className="flex min-h-8 shrink-0 items-center gap-1 border-b border-border bg-surface px-2">
      <Button variant="ghost" size="icon-xs" aria-label="Go back" title="Back (Alt+Left)" disabled={index <= 0} onClick={() => move(-1)}><ArrowLeft /></Button>
      <Button variant="ghost" size="icon-xs" aria-label="Go forward" title="Forward (Alt+Right)" disabled={index >= entries.length - 1} onClick={() => move(1)}><ArrowRight /></Button>
      <Button variant="ghost" size="xs" onClick={() => useWorkspaceStore.getState().openProjectMap()}><Map />Project map</Button>
      <Button variant="ghost" size="xs" className="ml-auto" onClick={() => setSearchOpen(true)} title="Quick open (Ctrl/⌘K)"><Search />Quick open</Button>
      <Button variant="ghost" size="icon-xs" aria-label="Copy link to current view" disabled={!active || initializedRoot !== projectRoot} onClick={() => {
        void (async () => {
          try { await navigator.clipboard.writeText(window.location.href); toast.success('Link copied') }
          catch { toast.error('Could not copy link') }
        })()
      }}><Copy /></Button>
      {searchOpen && <Suspense fallback={null}><QuickOpenDialog open onOpenChange={setSearchOpen} /></Suspense>}
    </div>
  )
}
