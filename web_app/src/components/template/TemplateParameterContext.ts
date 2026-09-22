import { createContext, useContext } from 'react'
import type { TemplateParamRead } from '@/lib/flowgroup-doc'
export const TemplateParameterContext = createContext<readonly TemplateParamRead[] | null>(null)
export const TemplateParameterProvider = TemplateParameterContext.Provider
export function useTemplateParameters() { return useContext(TemplateParameterContext) }
