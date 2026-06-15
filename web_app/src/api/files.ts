import { fetchApi, fetchApiText } from './client'
import type { FileListResponse, FileWriteResponse } from '../types/api'

function encodePath(path: string): string {
  return path.split('/').map(encodeURIComponent).join('/')
}

// `GET /api/files` → the full recursive project tree (one payload).
export function fetchFiles(): Promise<FileListResponse> {
  return fetchApi('/files')
}

// `GET /api/files/{path}` → the file's raw text content (plain-text body, NOT
// a JSON envelope).
export function fetchFileContent(path: string): Promise<string> {
  return fetchApiText(`/files/${encodePath(path)}`)
}

export function writeFile(path: string, content: string): Promise<FileWriteResponse> {
  return fetchApi(`/files/${encodePath(path)}`, {
    method: 'PUT',
    body: JSON.stringify({ content }),
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
