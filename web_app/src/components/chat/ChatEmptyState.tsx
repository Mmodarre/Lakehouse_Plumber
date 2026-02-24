/** Suggestion chips shown before the first message. */

const suggestions = [
  'Explain this flowgroup configuration',
  'Add a bronze CloudFiles load for the orders table',
  'What presets are available?',
  'Show me the substitutions for dev environment',
]

export function ChatEmptyState({ onSuggestion }: { onSuggestion: (text: string) => void }) {
  return (
    <div className="flex flex-1 flex-col items-center justify-center px-4">
      <div className="mb-4 rounded-full bg-slate-100 p-3">
        <svg className="h-6 w-6 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
        </svg>
      </div>
      <h3 className="text-xs font-medium text-slate-600">LHP Assistant</h3>
      <p className="mt-1 text-center text-[10px] text-slate-400">
        Ask about your pipeline configuration or request YAML changes
      </p>
      <div className="mt-4 flex flex-col gap-1.5">
        {suggestions.map((s) => (
          <button
            key={s}
            onClick={() => onSuggestion(s)}
            className="rounded-md border border-slate-200 bg-white px-3 py-1.5 text-left text-[10px] text-slate-600 transition-colors hover:border-blue-300 hover:bg-blue-50"
          >
            {s}
          </button>
        ))}
      </div>
    </div>
  )
}
