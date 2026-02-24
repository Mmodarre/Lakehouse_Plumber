import { useState } from 'react'
import { useGitStatus, useCommit, usePush, usePull } from '../../hooks/useGitStatus'
import { useHealth } from '../../hooks/useProject'

export function GitPanel() {
  const { data: health } = useHealth()
  const { data: status } = useGitStatus()
  const commit = useCommit()
  const push = usePush()
  const pull = usePull()
  const [message, setMessage] = useState('')

  const isDevMode = health?.dev_mode ?? false
  const anyPending = commit.isPending || push.isPending || pull.isPending
  const hasChanges = status
    ? status.modified.length + status.staged.length + status.untracked.length > 0
    : false
  const commitDisabled = isDevMode || anyPending || !message.trim() || !hasChanges

  const modifiedCount = status ? status.modified.length : 0
  const untrackedCount = status ? status.untracked.length : 0

  const error =
    commit.error?.message || push.error?.message || pull.error?.message || null

  const handleCommit = () => {
    commit.mutate(message.trim(), {
      onSuccess: (data) => {
        if (data.committed) {
          setMessage('')
        }
      },
    })
  }

  return (
    <div className="shrink-0 border-t border-slate-200 bg-white px-3 py-2">
      {/* Branch + stats row */}
      <div className="mb-2 flex items-center gap-2 text-xs text-slate-600">
        <svg className="h-3.5 w-3.5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
        </svg>
        <span className="truncate font-medium">{status?.branch ?? '...'}</span>
        {status && (
          <>
            <span className="text-[10px] text-slate-400">
              &uarr;{status.ahead} &darr;{status.behind}
            </span>
            <span className="ml-auto text-[10px] text-slate-400">
              {modifiedCount > 0 && <span className="text-amber-500">{modifiedCount}M</span>}
              {modifiedCount > 0 && untrackedCount > 0 && ' '}
              {untrackedCount > 0 && <span>{untrackedCount}U</span>}
            </span>
          </>
        )}
      </div>

      {isDevMode ? (
        /* Dev mode: show info line instead of commit/push/pull controls */
        <p className="text-[10px] text-slate-400 italic">
          Git operations: use your local CLI
        </p>
      ) : (
        <>
          {/* Commit message */}
          <textarea
            className="mb-2 w-full resize-none rounded border border-slate-200 px-2 py-1 text-xs text-slate-700 placeholder:text-slate-400 focus:border-blue-400 focus:outline-none"
            rows={2}
            placeholder="Commit message..."
            value={message}
            onChange={(e) => setMessage(e.target.value)}
          />

          {/* Action buttons */}
          <div className="flex gap-1.5">
            <button
              className="flex-1 rounded bg-blue-600 px-2 py-1 text-[10px] font-medium text-white hover:bg-blue-700 disabled:opacity-50"
              disabled={commitDisabled}
              onClick={handleCommit}
            >
              {commit.isPending ? 'Committing...' : 'Commit'}
            </button>
            <button
              className="flex-1 rounded bg-slate-100 px-2 py-1 text-[10px] font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
              disabled={anyPending}
              onClick={() => push.mutate()}
            >
              {push.isPending ? 'Pushing...' : 'Push'}
            </button>
            <button
              className="flex-1 rounded bg-slate-100 px-2 py-1 text-[10px] font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
              disabled={anyPending}
              onClick={() => pull.mutate()}
            >
              {pull.isPending ? 'Pulling...' : 'Pull'}
            </button>
          </div>
        </>
      )}

      {/* Inline error / status messages */}
      {!isDevMode && error && (
        <p className="mt-1 text-[10px] text-red-500">{error}</p>
      )}
      {!isDevMode && commit.isSuccess && !commit.data?.committed && (
        <p className="mt-1 text-[10px] text-slate-500">Nothing to commit</p>
      )}
    </div>
  )
}
