import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'

interface SqlOrFileToggleProps {
  sql: string
  sqlFile: string
  onSqlChange: (value: string) => void
  onSqlFileChange: (value: string) => void
  sqlPlaceholder?: string
  filePlaceholder?: string
  label?: string
  rows?: number
}

export function SqlOrFileToggle({
  sql,
  sqlFile,
  onSqlChange,
  onSqlFileChange,
  sqlPlaceholder = 'SELECT * FROM ...',
  filePlaceholder = 'sql/query.sql',
  label = 'SQL definition',
  rows = 6,
}: SqlOrFileToggleProps) {
  const mode = sqlFile !== undefined && sqlFile !== '' ? 'file' : 'inline'

  return (
    <div className="space-y-2">
      <div className="flex gap-2">
        <button
          className={`rounded px-2 py-1 text-xs ${mode === 'inline' ? 'bg-blue-100 text-blue-700' : 'text-slate-500'}`}
          onClick={() => onSqlFileChange('')}
        >
          Inline {label}
        </button>
        <button
          className={`rounded px-2 py-1 text-xs ${mode === 'file' ? 'bg-blue-100 text-blue-700' : 'text-slate-500'}`}
          onClick={() => { onSqlChange(''); onSqlFileChange(sqlFile || ' ') }}
        >
          File Path
        </button>
      </div>

      {mode === 'inline' ? (
        <Textarea
          value={sql}
          onChange={(e) => onSqlChange(e.target.value)}
          placeholder={sqlPlaceholder}
          rows={rows}
          className="font-mono text-xs"
        />
      ) : (
        <Input
          value={sqlFile.trim()}
          onChange={(e) => onSqlFileChange(e.target.value)}
          placeholder={filePlaceholder}
        />
      )}
    </div>
  )
}
