import type { YAMLError } from 'yaml'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

// ── ParseErrorsCard — broken-YAML guard rail ─────────────────
//
// Shown INSTEAD of a form when the file has YAML parse errors: yaml-doc
// refuses mutations on a broken handle, so the only way forward is the
// raw editor ("Open raw YAML" in the SaveBar).

export interface ParseErrorsCardProps {
  errors: readonly YAMLError[]
}

export function ParseErrorsCard({ errors }: ParseErrorsCardProps) {
  return (
    <Card className="gap-3 border-destructive/40 py-4">
      <CardHeader className="px-4">
        <CardTitle className="text-xs text-destructive">
          {errors.length} YAML parse {errors.length === 1 ? 'error' : 'errors'}
        </CardTitle>
        <CardDescription className="text-2xs">
          Form editing is blocked until the YAML parses — fix it via “Open raw YAML” below.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-1 px-4">
        {errors.map((err, i) => (
          <p key={i} className="font-mono text-2xs text-muted-foreground">
            {err.message}
          </p>
        ))}
      </CardContent>
    </Card>
  )
}
