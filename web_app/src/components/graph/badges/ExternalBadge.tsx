import { useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useUIStore } from '../../../store/uiStore'
import type { ExternalConnection } from '../../../types/graph'

export function ExternalBadge({ connections }: { connections: ExternalConnection[] }) {
  const [dropdownOpen, setDropdownOpen] = useState(false)
  const buttonRef = useRef<HTMLButtonElement>(null)
  const { openPipelineModal } = useUIStore()

  if (connections.length === 0) return null

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation()
    setDropdownOpen(!dropdownOpen)
  }

  const handleSelect = (conn: ExternalConnection, e: React.MouseEvent) => {
    e.stopPropagation()
    openPipelineModal(conn.targetPipeline)
    setDropdownOpen(false)
  }

  // Compute dropdown position from the badge button's bounding rect
  const rect = buttonRef.current?.getBoundingClientRect()

  return (
    <div className="absolute -right-1 -top-1">
      <button
        ref={buttonRef}
        onClick={handleClick}
        className="flex h-4 w-4 items-center justify-center rounded-full bg-amber-400 text-[8px] font-bold text-white shadow-sm hover:bg-amber-500"
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
            className="fixed min-w-[160px] rounded border border-slate-200 bg-white py-1 shadow-lg"
            style={{
              zIndex: 9999,
              top: rect.bottom + 4,
              left: rect.right - 160,
            }}
          >
            {connections.map((conn, i) => (
              <button
                key={i}
                className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[11px] text-slate-700 hover:bg-slate-50"
                onClick={(e) => handleSelect(conn, e)}
              >
                <span className={`text-[9px] ${conn.direction === 'upstream' ? 'text-blue-500' : 'text-orange-500'}`}>
                  {conn.direction === 'upstream' ? '\u2190' : '\u2192'}
                </span>
                <span className="truncate">{conn.targetPipeline}</span>
              </button>
            ))}
          </div>
        </>,
        document.body,
      )}
    </div>
  )
}
