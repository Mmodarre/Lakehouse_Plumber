import { useCallback, useState } from 'react'
import { FileBrowser } from '../sidebar/FileBrowser'
import { useUIStore } from '../../store/uiStore'
import { cn } from '../../lib/utils'

const MIN_WIDTH = 200
const MAX_WIDTH = 360
const DEFAULT_WIDTH = 264
const KEYBOARD_STEP = 16
const STORAGE_KEY = 'lhp-sidebar-width'

function clampWidth(width: number): number {
  return Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, width))
}

function initialWidth(): number {
  const raw = Number(localStorage.getItem(STORAGE_KEY))
  return Number.isFinite(raw) && raw > 0 ? clampWidth(raw) : DEFAULT_WIDTH
}

export function Sidebar() {
  const sidebarOpen = useUIStore((s) => s.sidebarOpen)
  const [width, setWidth] = useState(initialWidth)
  const [resizing, setResizing] = useState(false)

  const persistWidth = useCallback((next: number) => {
    const clamped = clampWidth(next)
    setWidth(clamped)
    localStorage.setItem(STORAGE_KEY, String(clamped))
  }, [])

  const handlePointerDown = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      e.preventDefault()
      const startX = e.clientX
      const startWidth = width
      setResizing(true)
      const onMove = (ev: PointerEvent) => {
        setWidth(clampWidth(startWidth + (ev.clientX - startX)))
      }
      const onUp = (ev: PointerEvent) => {
        window.removeEventListener('pointermove', onMove)
        window.removeEventListener('pointerup', onUp)
        setResizing(false)
        persistWidth(startWidth + (ev.clientX - startX))
      }
      window.addEventListener('pointermove', onMove)
      window.addEventListener('pointerup', onUp)
    },
    [width, persistWidth],
  )

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLDivElement>) => {
      if (e.key === 'ArrowLeft') {
        e.preventDefault()
        persistWidth(width - KEYBOARD_STEP)
      } else if (e.key === 'ArrowRight') {
        e.preventDefault()
        persistWidth(width + KEYBOARD_STEP)
      }
    },
    [width, persistWidth],
  )

  return (
    <aside
      className={cn(
        'relative flex shrink-0 flex-col overflow-hidden border-r border-border bg-sidebar',
        // Width animates on collapse/expand only — never while dragging.
        !resizing &&
          'transition-[width] duration-300 ease-[cubic-bezier(0.4,0,0.2,1)] motion-reduce:transition-none',
      )}
      style={{ width: sidebarOpen ? width : 0 }}
    >
      {/* Fixed-width inner wrapper so content doesn't reflow mid-collapse */}
      <div className="flex min-h-0 flex-1 flex-col" style={{ width }}>
        <div className="custom-scrollbar flex-1 overflow-y-auto">
          <FileBrowser />
        </div>
      </div>

      {sidebarOpen && (
        <div
          role="separator"
          aria-orientation="vertical"
          aria-label="Resize file browser"
          aria-valuemin={MIN_WIDTH}
          aria-valuemax={MAX_WIDTH}
          aria-valuenow={width}
          tabIndex={0}
          onPointerDown={handlePointerDown}
          onKeyDown={handleKeyDown}
          className="absolute inset-y-0 right-0 w-1 cursor-col-resize transition-colors duration-150 hover:bg-primary/30 focus-visible:bg-primary/40 focus-visible:outline-none active:bg-primary/40 motion-reduce:transition-none"
        />
      )}
    </aside>
  )
}
