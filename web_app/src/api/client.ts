import type { ErrorDetail } from '../types/api'
import { useUIStore } from '../store/uiStore'

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api'

export class ApiError extends Error {
  status: number
  code: string
  suggestions: string[]

  constructor(status: number, detail: ErrorDetail) {
    super(detail.message)
    this.name = 'ApiError'
    this.status = status
    this.code = detail.code
    this.suggestions = detail.suggestions
  }
}

export async function fetchApi<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const url = `${BASE_URL}${path}`
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  })

  if (!response.ok) {
    // 409 = workspace required (production mode)
    if (response.status === 409) {
      const body = await response.json().catch(() => ({}))
      const detail = typeof body.detail === 'string' ? body.detail : ''
      if (detail.includes('No active workspace')) {
        // Don't interrupt an in-progress workspace creation
        const currentStatus = useUIStore.getState().workspaceStatus
        if (currentStatus !== 'creating') {
          useUIStore.getState().setWorkspaceLost()
        }
      }
      throw new ApiError(response.status, {
        code: 'NO_WORKSPACE',
        category: 'workspace',
        message: 'Workspace required',
        details: detail,
        suggestions: ['Create a workspace to continue'],
        context: {},
        http_status: 409,
      })
    }

    let detail: ErrorDetail | undefined
    try {
      const body = await response.json()
      detail = body.error ?? body
    } catch {
      // non-JSON error response
    }

    if (detail?.code) {
      throw new ApiError(response.status, detail)
    }
    throw new ApiError(response.status, {
      code: 'UNKNOWN_ERROR',
      category: 'unknown',
      message: `Request failed: ${response.status} ${response.statusText}`,
      details: '',
      suggestions: [],
      context: {},
      http_status: response.status,
    })
  }

  return response.json() as Promise<T>
}
