import { Monitor, Moon, Sun } from 'lucide-react'
import { Button } from '../ui/button'
import { useThemeStore, type ThemePreference } from '../../store/themeStore'

const CYCLE: Record<ThemePreference, ThemePreference> = {
  light: 'dark',
  dark: 'system',
  system: 'light',
}

const LABEL: Record<ThemePreference, string> = {
  light: 'Light theme',
  dark: 'Dark theme',
  system: 'System theme',
}

const ICON: Record<ThemePreference, typeof Sun> = {
  light: Sun,
  dark: Moon,
  system: Monitor,
}

/** Cycles light → dark → system, mirroring the current preference's icon. */
export function ThemeToggle() {
  const theme = useThemeStore((s) => s.theme)
  const setTheme = useThemeStore((s) => s.setTheme)
  const Icon = ICON[theme]
  const label = `${LABEL[theme]} — switch to ${LABEL[CYCLE[theme]].toLowerCase()}`
  return (
    <Button
      variant="ghost"
      size="icon-sm"
      onClick={() => setTheme(CYCLE[theme])}
      aria-label={label}
      title={label}
      className="text-muted-foreground"
    >
      <Icon />
    </Button>
  )
}
