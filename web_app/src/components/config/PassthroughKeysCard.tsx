import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

// ── PassthroughKeysCard — unknown-key transparency ───────────
//
// Lists the keys the config-model layer classifies as passthrough:
// present in the file but not owned by any form field. The save path
// keeps them byte-identical (yaml-doc), so the card is purely
// informational. The key list is computed by the caller (config-model
// list*PassthroughKeys) — this component just renders it.

export interface PassthroughKeysCardProps {
  /** Passthrough key names (config-model computes these). */
  keys: string[]
  /** Surface-specific caption (defaults to the generic not-editable note). */
  description?: string
}

export function PassthroughKeysCard({ keys, description }: PassthroughKeysCardProps) {
  if (keys.length === 0) return null
  return (
    <Card className="gap-3 py-4">
      <CardHeader className="px-4">
        <CardTitle className="text-xs">Passthrough keys</CardTitle>
        <CardDescription className="text-2xs">
          {description ?? 'Kept exactly as written on save — not editable here.'}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-wrap gap-1.5 px-4">
        {keys.map((key) => (
          <Badge
            key={key}
            variant="outline"
            className="rounded-sm px-1.5 font-mono text-2xs font-normal text-muted-foreground"
          >
            {key}
          </Badge>
        ))}
      </CardContent>
    </Card>
  )
}
