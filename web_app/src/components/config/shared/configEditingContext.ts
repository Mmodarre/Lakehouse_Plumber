import { createContext, useContext } from 'react'

/** Editing is disabled independently of config search, navigation and previews. */
export const ConfigEditingContext = createContext(false)
export const useConfigReadOnly = () => useContext(ConfigEditingContext)
