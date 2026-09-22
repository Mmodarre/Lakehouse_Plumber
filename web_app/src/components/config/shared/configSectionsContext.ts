import { createContext, useContext } from 'react'

export interface SectionInfo {
  id: string
  title: string
  search: string
  present: boolean
  issues: number
}

export function normalizeSettingSearch(value: string): string {
  return value
    .toLowerCase()
    .replace(/[_\-.]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

interface SectionsContextValue {
  query: string
  configuredOnly: boolean
  collapsed: Record<string, boolean>
  register: (section: SectionInfo) => void
  unregister: (id: string) => void
  toggle: (id: string, collapsed: boolean) => void
}

export const SectionsContext = createContext<SectionsContextValue | null>(null)
export const useConfigSections = () => useContext(SectionsContext)
