import {
  ArrowDownToLine,
  CircleHelp,
  Database,
  FlaskConical,
  Wand2,
  type LucideIcon,
} from 'lucide-react'

// Kind identity resolves to the shared --kind-* tokens (same palette as the
// table badges) — chip tint always paired with a distinct icon. `port` is the
// solid kind fill for the designer canvas's wiring ports (kind-colored pipe
// mouths). Shared by the dashboard's ActionNode and the designer canvas nodes.
export const KIND_STYLES: Record<string, { chip: string; icon: LucideIcon; port: string }> = {
  load: { chip: 'bg-kind-load/12 text-kind-load', icon: ArrowDownToLine, port: 'bg-kind-load' },
  transform: {
    chip: 'bg-kind-transform/12 text-kind-transform',
    icon: Wand2,
    port: 'bg-kind-transform',
  },
  write: { chip: 'bg-kind-write/12 text-kind-write', icon: Database, port: 'bg-kind-write' },
  test: { chip: 'bg-kind-test/12 text-kind-test', icon: FlaskConical, port: 'bg-kind-test' },
}

export const FALLBACK_KIND_STYLE = {
  chip: 'bg-muted text-muted-foreground',
  icon: CircleHelp,
  port: 'bg-muted-foreground',
}
