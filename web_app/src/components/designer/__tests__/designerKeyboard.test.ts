import { describe, expect, it } from 'vitest'
import { isArrowKey, nextNodeInDirection, type NodePoint } from '../designerKeyboard'

// A small grid, left-to-right flow (as ELK lays the canvas out):
//   a(0,0)    b(100,0)   c(200,0)
//   d(0,100)             e(200,100)
const NODES: NodePoint[] = [
  { id: 'a', x: 0, y: 0 },
  { id: 'b', x: 100, y: 0 },
  { id: 'c', x: 200, y: 0 },
  { id: 'd', x: 0, y: 100 },
  { id: 'e', x: 200, y: 100 },
]

describe('isArrowKey', () => {
  it('recognizes the four arrows and nothing else', () => {
    expect(isArrowKey('ArrowUp')).toBe(true)
    expect(isArrowKey('ArrowDown')).toBe(true)
    expect(isArrowKey('ArrowLeft')).toBe(true)
    expect(isArrowKey('ArrowRight')).toBe(true)
    expect(isArrowKey('Enter')).toBe(false)
    expect(isArrowKey('a')).toBe(false)
  })
})

describe('nextNodeInDirection', () => {
  it('moves to the nearest node in the pressed direction', () => {
    expect(nextNodeInDirection('a', 'ArrowRight', NODES)).toBe('b')
    expect(nextNodeInDirection('b', 'ArrowRight', NODES)).toBe('c')
    expect(nextNodeInDirection('a', 'ArrowDown', NODES)).toBe('d')
    expect(nextNodeInDirection('c', 'ArrowLeft', NODES)).toBe('b')
    expect(nextNodeInDirection('d', 'ArrowUp', NODES)).toBe('a')
  })

  it('biases toward the same lane before crossing to another row', () => {
    // From b, moving right prefers c (same row) over e (a row down).
    expect(nextNodeInDirection('b', 'ArrowRight', NODES)).toBe('c')
  })

  it('returns null when there is nowhere to go in that direction', () => {
    expect(nextNodeInDirection('a', 'ArrowLeft', NODES)).toBeNull()
    expect(nextNodeInDirection('a', 'ArrowUp', NODES)).toBeNull()
  })

  it('lands on the entry node (leftmost, then topmost) with no current selection', () => {
    expect(nextNodeInDirection(null, 'ArrowRight', NODES)).toBe('a')
    expect(nextNodeInDirection(null, 'ArrowDown', NODES)).toBe('a')
  })

  it('treats an unknown current id as no selection', () => {
    expect(nextNodeInDirection('ghost', 'ArrowRight', NODES)).toBe('a')
  })

  it('returns null for an empty canvas', () => {
    expect(nextNodeInDirection('a', 'ArrowRight', [])).toBeNull()
    expect(nextNodeInDirection(null, 'ArrowRight', [])).toBeNull()
  })
})
