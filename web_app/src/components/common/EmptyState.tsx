export function EmptyState({
  title = 'No data',
  message = 'Nothing to display yet.',
}: {
  title?: string
  message?: string
}) {
  return (
    <div className="flex flex-col items-center justify-center py-12 text-slate-400">
      <svg className="mb-3 h-12 w-12" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
          d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
      </svg>
      <p className="text-sm font-medium">{title}</p>
      <p className="text-xs">{message}</p>
    </div>
  )
}
