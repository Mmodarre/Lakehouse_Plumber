import { useResolvedFieldHelp } from '@/components/common/SchemaKindContext'
import type { SchemaPath } from '@/lib/schema-help'
import { hintId } from './fieldSupport'

export function FieldHint({ id, helpPath, help, fallback }: { id: string; helpPath?: SchemaPath; help?: string; fallback?: string }) {
  const resolved = useResolvedFieldHelp(helpPath, help)
  const text = resolved?.summary ?? fallback
  return <p id={hintId(id)} hidden={!text} className="text-xs leading-relaxed text-muted-foreground">{text}</p>
}
