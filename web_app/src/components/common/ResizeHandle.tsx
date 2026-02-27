/**
 * Vertical drag handle for resizing panels.
 *
 * Renders a thin strip on the left edge of a panel. Drag to resize,
 * double-click to reset to default width.
 */

import { useCallback, useRef } from 'react'

interface ResizeHandleProps {
  onResize: (deltaX: number) => void
  onReset: () => void
}

export function ResizeHandle({ onResize, onReset }: ResizeHandleProps) {
  const startXRef = useRef(0)

  const handleMouseMove = useCallback(
    (e: MouseEvent) => {
      const delta = startXRef.current - e.clientX
      startXRef.current = e.clientX
      onResize(delta)
    },
    [onResize],
  )

  const handleMouseUp = useCallback(() => {
    document.removeEventListener('mousemove', handleMouseMove)
    document.removeEventListener('mouseup', handleMouseUp)
    document.body.style.userSelect = ''
    document.body.style.cursor = ''
  }, [handleMouseMove])

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault()
      startXRef.current = e.clientX
      document.body.style.userSelect = 'none'
      document.body.style.cursor = 'col-resize'
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
    },
    [handleMouseMove, handleMouseUp],
  )

  return (
    <div
      onMouseDown={handleMouseDown}
      onDoubleClick={onReset}
      className="group absolute top-0 left-0 z-10 h-full w-1 cursor-col-resize"
    >
      {/* Visible grip indicator on hover */}
      <div className="absolute top-1/2 left-0 h-8 w-1 -translate-y-1/2 rounded-r bg-slate-300 opacity-0 transition-opacity group-hover:opacity-100" />
    </div>
  )
}
