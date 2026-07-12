import { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { ArrowLeft, ArrowRight } from 'lucide-react'
import { useUIStore } from '../../../store/uiStore'
import type { ExternalConnection } from '../../../types/graph'

export function ExternalBadge({ connections }: { connections: ExternalConnection[] }) {
  const [dropdownOpen, setDropdownOpen] = useState(false)
  // Position of the badge button, captured in the click handler when the
  // dropdown opens. Stored in state (rather than read from the ref during
  // render) so the portal positions correctly on the very first open and the
  // render stays free of ref reads.
  const [rect, setRect] = useState<DOMRect | null>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)
  const { openPipelineModal } = useUIStore()

  // Escape closes the dropdown. Registered on `window` in the capture phase
  // so it runs BEFORE Radix's document-capture dismiss listener — the
  // preventDefault keeps a surrounding dialog (e.g. the pipeline drill
  // modal) open while only the dropdown closes.
  useEffect(() => {
    if (!dropdownOpen) return
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return
      e.preventDefault()
      setDropdownOpen(false)
      buttonRef.current?.focus()
    }
    window.addEventListener('keydown', onKeyDown, { capture: true })
    return () => window.removeEventListener('keydown', onKeyDown, { capture: true })
  }, [dropdownOpen])

  if (connections.length === 0) return null

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation()
    if (!dropdownOpen) {
      setRect(buttonRef.current?.getBoundingClientRect() ?? null)
    }
    setDropdownOpen(!dropdownOpen)
  }

  const handleSelect = (conn: ExternalConnection, e: React.MouseEvent) => {
    e.stopPropagation()
    // Cross-pipeline edges carry a target pipeline to drill into; plain
    // external sources (an existing table, another flowgroup's view) have no
    // pipeline — opening the drill modal with an empty id would show the full
    // unfiltered graph under a blank title, so skip it.
    if (conn.targetPipeline !== '') openPipelineModal(conn.targetPipeline)
    setDropdownOpen(false)
  }

  return (
    <div className="absolute -top-1.5 -right-1.5">
      <button
        ref={buttonRef}
        onClick={handleClick}
        aria-haspopup="menu"
        aria-expanded={dropdownOpen}
        aria-label={`${connections.length} external connection${connections.length > 1 ? 's' : ''}`}
        className="flex h-4.5 min-w-4.5 items-center justify-center rounded-full border border-warning/60 bg-card px-1 font-mono text-2xs font-semibold text-foreground shadow-xs transition-colors duration-150 hover:border-warning"
        title={`${connections.length} external connection${connections.length > 1 ? 's' : ''}`}
      >
        {connections.length}
      </button>

      {dropdownOpen && rect && createPortal(
        <>
          {/* Backdrop to close dropdown */}
          <div
            className="fixed inset-0"
            style={{ zIndex: 9998 }}
            onClick={(e) => { e.stopPropagation(); setDropdownOpen(false) }}
          />
          <div
            role="menu"
            className="fixed min-w-40 rounded-md border border-border bg-popover py-1 shadow-lg"
            style={{
              zIndex: 9999,
              top: rect.bottom + 4,
              left: rect.right - 160,
            }}
          >
            {connections.map((conn, i) => (
              <button
                key={i}
                role="menuitem"
                className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-xs text-popover-foreground hover:bg-accent"
                onClick={(e) => handleSelect(conn, e)}
              >
                {conn.direction === 'upstream' ? (
                  <ArrowLeft className="size-3 shrink-0 text-info" aria-hidden="true" />
                ) : (
                  <ArrowRight className="size-3 shrink-0 text-warning" aria-hidden="true" />
                )}
                {/* Fall back to the source/target node name when the connection
                    has no pipeline to drill into (a plain external source). */}
                <span className="truncate font-mono">
                  {conn.targetPipeline !== '' ? conn.targetPipeline : conn.targetNodeId}
                </span>
              </button>
            ))}
          </div>
        </>,
        document.body,
      )}
    </div>
  )
}
