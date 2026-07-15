import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import type { OperationalMetadataResponse } from '@/types/api'

// Mock the one network-bound hook so the widget renders a fixed catalogue with
// no query client / fetch. Returned synchronously → no async resolution, so no
// act() warnings.
const useOperationalMetadataMock = vi.fn()
vi.mock('@/hooks/useOperationalMetadata', () => ({
  useOperationalMetadata: () => useOperationalMetadataMock(),
}))

import { MetadataMultiSelect } from '../MetadataMultiSelect'

// Three columns with distinct `applies_to` reach so the filter has something to
// exclude: `_ingestion_timestamp` applies everywhere, `_source_file` is
// view-only (excluded for streaming_table), `_pipeline_run_id` is
// streaming-table-only.
const DATA: OperationalMetadataResponse = {
  columns: [
    {
      name: '_ingestion_timestamp',
      expression: 'current_timestamp()',
      description: 'Ingestion time',
      applies_to: ['view', 'streaming_table', 'materialized_view'],
      source: 'builtin',
    },
    {
      name: '_source_file',
      expression: '_metadata.file_path',
      description: 'Source file',
      applies_to: ['view'],
      source: 'builtin',
    },
    {
      name: '_pipeline_run_id',
      expression: "'run'",
      description: 'Run id',
      applies_to: ['streaming_table'],
      source: 'project',
    },
  ],
  presets: [],
}

function mockMetadata(extra: Record<string, unknown> = {}) {
  useOperationalMetadataMock.mockReturnValue({
    data: DATA,
    isLoading: false,
    error: null,
    ...extra,
  })
}

describe('MetadataMultiSelect', () => {
  it('offers only columns whose applies_to includes the surface', () => {
    mockMetadata()
    render(
      <MetadataMultiSelect value={undefined} onChange={vi.fn()} applies_to="streaming_table" />,
    )

    expect(screen.getByRole('button', { name: /_ingestion_timestamp/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /_pipeline_run_id/ })).toBeInTheDocument()
    // view-only column is not applicable to a streaming_table surface
    expect(screen.queryByRole('button', { name: /_source_file/ })).toBeNull()
  })

  it('renders an unknown column (not in the catalogue) with a hint and does not drop it', () => {
    mockMetadata()
    render(
      <MetadataMultiSelect value={['x_unknown']} onChange={vi.fn()} applies_to="streaming_table" />,
    )

    expect(screen.getByRole('button', { name: /x_unknown/ })).toBeInTheDocument()
    expect(screen.getByText(/not defined in lhp\.yaml/i)).toBeInTheDocument()
  })

  it('toggling an applicable chip emits the updated string[]', () => {
    mockMetadata()
    const onChange = vi.fn()
    render(
      <MetadataMultiSelect
        value={['_pipeline_run_id']}
        onChange={onChange}
        applies_to="streaming_table"
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: /_ingestion_timestamp/ }))

    expect(onChange).toHaveBeenCalledTimes(1)
    // Exact, not arrayContaining — an unexpected leaked element must fail.
    expect(onChange.mock.calls[0][0]).toEqual(['_pipeline_run_id', '_ingestion_timestamp'])
  })

  it('retains an unknown column across a toggle of another chip (round-trip fidelity)', () => {
    // The regression the widget exists to prevent: a refactor to
    // `base = applicable.map(...)` would drop `x_unknown` here and fail.
    mockMetadata()
    const onChange = vi.fn()
    render(
      <MetadataMultiSelect
        value={['x_unknown', '_ingestion_timestamp']}
        onChange={onChange}
        applies_to="streaming_table"
      />,
    )

    // Toggle the KNOWN, applicable chip OFF.
    fireEvent.click(screen.getByRole('button', { name: /_ingestion_timestamp/ }))

    expect(onChange).toHaveBeenCalledTimes(1)
    // x_unknown is retained; only the toggled applicable column is removed.
    expect(onChange.mock.calls[0][0]).toEqual(['x_unknown'])
  })

  it('the "All" affordance emits the boolean true', () => {
    mockMetadata()
    const onChange = vi.fn()
    render(
      <MetadataMultiSelect value={undefined} onChange={onChange} applies_to="streaming_table" />,
    )

    fireEvent.click(screen.getByRole('button', { name: 'All' }))

    expect(onChange).toHaveBeenCalledWith(true)
  })
})
