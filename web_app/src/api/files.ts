import { fetchApi, fetchApiTextWithMeta } from './client'
import type { FileListResponse, FileWriteResponse } from '../types/api'

function encodePath(path: string): string {
  return path.split('/').map(encodeURIComponent).join('/')
}

// `GET /api/files` → the full recursive project tree (one payload).
export function fetchFiles(): Promise<FileListResponse> {
  return fetchApi('/files')
}

// `GET /api/files/{path}` → the file's raw text content (plain-text body, NOT
// a JSON envelope), plus the response `ETag` (unquoted) so the caller can
// seed the optimistic-concurrency token. `etag` is `null` when the backend
// does not emit the header.
export function fetchFileContentWithMeta(
  path: string,
): Promise<{ content: string; etag: string | null }> {
  return fetchApiTextWithMeta(`/files/${encodePath(path)}`)
}

// `PUT /api/files/{path}`. When `etag` is provided it is sent as a quoted
// `If-Match` header; a backend that enforces ETags then returns 412 on a
// stale write. Omitting `etag` (new files / older backend) skips the header.
export function writeFile(
  path: string,
  content: string,
  etag?: string | null,
): Promise<FileWriteResponse> {
  const headers: Record<string, string> = {}
  if (etag) headers['If-Match'] = `"${etag}"`
  return fetchApi(`/files/${encodePath(path)}`, {
    method: 'PUT',
    body: JSON.stringify({ content }),
    headers,
  })
}

export interface FileDeleteResponse {
  deleted: boolean
  path: string
}

export function deleteFile(path: string): Promise<FileDeleteResponse> {
  return fetchApi(`/files/${encodePath(path)}`, {
    method: 'DELETE',
  })
}
