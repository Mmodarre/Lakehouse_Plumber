import { expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { ConfigChangeReview } from '../../ConfigChangeReview'

vi.mock('../../../editor/DiffEditorWrapper', () => ({
  default: ({
    original,
    modified,
    readOnly,
  }: {
    original: string
    modified: string
    readOnly: boolean
  }) => (
    <div data-testid="review-diff" data-read-only={readOnly}>
      {original} → {modified}
    </div>
  ),
}))

it('opens the existing diff editor with both sides read-only and exact source text', async () => {
  render(<ConfigChangeReview original="# keep\nname: saved" modified="# keep\nname: working" />)
  expect(screen.queryByTestId('review-diff')).not.toBeInTheDocument()
  await userEvent.setup().click(screen.getByRole('button', { name: 'Review unsaved changes' }))
  const diff = await screen.findByTestId('review-diff')
  expect(diff).toHaveAttribute('data-read-only', 'true')
  expect(diff).toHaveTextContent('name: saved')
  expect(diff).toHaveTextContent('name: working')
})
