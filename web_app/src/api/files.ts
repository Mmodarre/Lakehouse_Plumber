import { fetchApi } from './client'
import type { FileListResponse, FileReadResponse, FileWriteResponse } from '../types/api'

export function fetchFiles(): Promise<FileListResponse> {
  return fetchApi('/files')
}

export function fetchFilePath(path: string): Promise<FileReadResponse> {
  const encoded = path.split('/').map(encodeURIComponent).join('/')
  return fetchApi(`/files/${encoded}`)
}

export function writeFile(path: string, content: string): Promise<FileWriteResponse> {
  const encoded = path.split('/').map(encodeURIComponent).join('/')
  return fetchApi(`/files/${encoded}`, {
    method: 'PUT',
    body: JSON.stringify({ content }),
  })
}
