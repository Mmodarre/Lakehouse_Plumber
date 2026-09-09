/** Capture mounted editors before commands inspect or persist their buffers. */
const captures = new Set<() => void>()

export function registerWorkspaceEditor(capture: () => void): () => void {
  captures.add(capture)
  return () => { captures.delete(capture) }
}

export function captureWorkspaceEditors(): void {
  // Structured config inputs commit on blur; include their current draft in
  // keyboard saves and bulk-close review before reading dirty buffers.
  const active = document.activeElement
  if ((active instanceof HTMLInputElement || active instanceof HTMLTextAreaElement) && !active.closest('.monaco-editor')) active.blur()
  for (const capture of captures) capture()
}

/** Keep malformed typed values visible until the user corrects or restores them. */
export function focusInvalidWorkspaceDraft(): boolean {
  const invalid = document.querySelector<HTMLInputElement | HTMLTextAreaElement>('[data-workspace-draft][aria-invalid="true"]')
  if (!invalid) return false
  invalid.focus()
  return true
}
