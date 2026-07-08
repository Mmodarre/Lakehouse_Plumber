import { useEffect } from 'react'

/**
 * Warn on tab close / reload while there are unsaved changes.
 *
 * Registers a `beforeunload` listener only while `dirty` is true and removes
 * it when the buffer becomes clean or the component unmounts. The add/remove
 * pair lives in a single effect so it is idempotent under React StrictMode's
 * double-invocation in development.
 */
export function useBeforeUnloadGuard(dirty: boolean): void {
  useEffect(() => {
    if (!dirty) return
    const handler = (e: BeforeUnloadEvent) => {
      e.preventDefault()
      // Legacy requirement: some browsers still gate the prompt on a
      // truthy `returnValue`.
      e.returnValue = ''
    }
    window.addEventListener('beforeunload', handler)
    return () => window.removeEventListener('beforeunload', handler)
  }, [dirty])
}
