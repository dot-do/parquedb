/**
 * Prompt Utilities Tests
 *
 * Tests for the CLI prompt utilities used in interactive wizards.
 */

import { describe, it, expect } from 'vitest'
import { Readable, Writable } from 'node:stream'
import {
  promptText,
  promptConfirm,
  promptSelect,
  promptList,
  isInteractive,
} from '../../../src/cli/prompt'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a mock IO for testing prompts
 */
function createMockIO(inputs: string[]): {
  input: Readable
  output: Writable
  getOutput: () => string
} {
  const inputQueue = [...inputs]
  let outputBuffer = ''

  const input = new Readable({
    read() {
      if (inputQueue.length > 0) {
        setImmediate(() => {
          this.push(inputQueue.shift() + '\n')
        })
      }
    },
  })

  const output = new Writable({
    write(chunk, _encoding, callback) {
      outputBuffer += chunk.toString()
      callback()
    },
  })

  return {
    input,
    output,
    getOutput: () => outputBuffer,
  }
}

// =============================================================================
// isInteractive Tests
// =============================================================================

describe('isInteractive', () => {
  it('should return true when IO is provided', () => {
    const mockIO = createMockIO([])
    expect(isInteractive(mockIO)).toBe(true)
  })
})

// =============================================================================
// promptText Tests
// =============================================================================

describe('promptText', () => {
  it('should return user input', async () => {
    const mockIO = createMockIO(['hello world'])
    const result = await promptText('Enter text:', {}, mockIO)
    expect(result).toBe('hello world')
  })

  it('should return default value on empty input', async () => {
    const mockIO = createMockIO([''])
    const result = await promptText('Enter text:', { default: 'default value' }, mockIO)
    expect(result).toBe('default value')
  })

  it('should trim whitespace', async () => {
    const mockIO = createMockIO(['  trimmed  '])
    const result = await promptText('Enter text:', {}, mockIO)
    expect(result).toBe('trimmed')
  })

  it('should validate input and re-prompt on error', async () => {
    const mockIO = createMockIO(['invalid', 'valid'])
    const result = await promptText(
      'Enter text:',
      {
        validate: (value) => (value === 'valid' ? true : 'Must be "valid"'),
      },
      mockIO
    )
    expect(result).toBe('valid')
    expect(mockIO.getOutput()).toContain('Must be "valid"')
  })

  it('should accept undefined from validate as valid', async () => {
    const mockIO = createMockIO(['anything'])
    const result = await promptText(
      'Enter text:',
      {
        validate: () => undefined, // undefined means valid
      },
      mockIO
    )
    expect(result).toBe('anything')
  })

  it('should show default in prompt', async () => {
    const mockIO = createMockIO([''])
    await promptText('Enter text:', { default: 'mydefault' }, mockIO)
    expect(mockIO.getOutput()).toContain('(mydefault)')
  })
})

// =============================================================================
// promptConfirm Tests
// =============================================================================

describe('promptConfirm', () => {
  it('should return true for "y"', async () => {
    const mockIO = createMockIO(['y'])
    const result = await promptConfirm('Continue?', {}, mockIO)
    expect(result).toBe(true)
  })

  it('should return true for "yes"', async () => {
    const mockIO = createMockIO(['yes'])
    const result = await promptConfirm('Continue?', {}, mockIO)
    expect(result).toBe(true)
  })

  it('should return false for "n"', async () => {
    const mockIO = createMockIO(['n'])
    const result = await promptConfirm('Continue?', {}, mockIO)
    expect(result).toBe(false)
  })

  it('should return false for "no"', async () => {
    const mockIO = createMockIO(['no'])
    const result = await promptConfirm('Continue?', {}, mockIO)
    expect(result).toBe(false)
  })

  it('should return default on empty input', async () => {
    const mockIO1 = createMockIO([''])
    const result1 = await promptConfirm('Continue?', { default: true }, mockIO1)
    expect(result1).toBe(true)

    const mockIO2 = createMockIO([''])
    const result2 = await promptConfirm('Continue?', { default: false }, mockIO2)
    expect(result2).toBe(false)
  })

  it('should show Y/n hint when default is true', async () => {
    const mockIO = createMockIO([''])
    await promptConfirm('Continue?', { default: true }, mockIO)
    expect(mockIO.getOutput()).toContain('Y/n')
  })

  it('should show y/N hint when default is false', async () => {
    const mockIO = createMockIO([''])
    await promptConfirm('Continue?', { default: false }, mockIO)
    expect(mockIO.getOutput()).toContain('y/N')
  })

  it('should handle case-insensitive input', async () => {
    const mockIO1 = createMockIO(['Y'])
    expect(await promptConfirm('Continue?', {}, mockIO1)).toBe(true)

    const mockIO2 = createMockIO(['YES'])
    expect(await promptConfirm('Continue?', {}, mockIO2)).toBe(true)

    const mockIO3 = createMockIO(['N'])
    expect(await promptConfirm('Continue?', {}, mockIO3)).toBe(false)
  })

  it('should use default for invalid input', async () => {
    const mockIO = createMockIO(['maybe'])
    const result = await promptConfirm('Continue?', { default: true }, mockIO)
    expect(result).toBe(true)
  })
})

// =============================================================================
// promptSelect Tests
// =============================================================================

describe('promptSelect', () => {
  const choices = [
    { value: 'a' as const, label: 'Option A', description: 'First option' },
    { value: 'b' as const, label: 'Option B', description: 'Second option' },
    { value: 'c' as const, label: 'Option C' },
  ]

  it('should return selected value by number', async () => {
    const mockIO = createMockIO(['2'])
    const result = await promptSelect('Choose:', { choices }, mockIO)
    expect(result).toBe('b')
  })

  it('should return first option for number 1', async () => {
    const mockIO = createMockIO(['1'])
    const result = await promptSelect('Choose:', { choices }, mockIO)
    expect(result).toBe('a')
  })

  it('should return last option for last number', async () => {
    const mockIO = createMockIO(['3'])
    const result = await promptSelect('Choose:', { choices }, mockIO)
    expect(result).toBe('c')
  })

  it('should return default on empty input', async () => {
    const mockIO = createMockIO([''])
    const result = await promptSelect('Choose:', { choices, default: 'b' }, mockIO)
    expect(result).toBe('b')
  })

  it('should return first choice when no default and empty input', async () => {
    const mockIO = createMockIO([''])
    const result = await promptSelect('Choose:', { choices }, mockIO)
    expect(result).toBe('a')
  })

  it('should accept value typed directly', async () => {
    const mockIO = createMockIO(['b'])
    const result = await promptSelect('Choose:', { choices }, mockIO)
    expect(result).toBe('b')
  })

  it('should be case-insensitive for direct value input', async () => {
    const mockIO = createMockIO(['B'])
    const result = await promptSelect('Choose:', { choices }, mockIO)
    expect(result).toBe('b')
  })

  it('should re-prompt on invalid number', async () => {
    const mockIO = createMockIO(['5', '2'])
    const result = await promptSelect('Choose:', { choices }, mockIO)
    expect(result).toBe('b')
    expect(mockIO.getOutput()).toContain('Please enter a number from 1 to 3')
  })

  it('should display choices with descriptions', async () => {
    const mockIO = createMockIO(['1'])
    await promptSelect('Choose:', { choices }, mockIO)
    const output = mockIO.getOutput()
    expect(output).toContain('Option A')
    expect(output).toContain('First option')
    expect(output).toContain('Option B')
  })

  it('should mark default choice', async () => {
    const mockIO = createMockIO([''])
    await promptSelect('Choose:', { choices, default: 'b' }, mockIO)
    // The output should indicate option B is the default
    expect(mockIO.getOutput()).toContain('[2]')
  })
})

// =============================================================================
// promptList Tests
// =============================================================================

describe('promptList', () => {
  it('should return array from comma-separated input', async () => {
    const mockIO = createMockIO(['one, two, three'])
    const result = await promptList('Enter items:', {}, mockIO)
    expect(result).toEqual(['one', 'two', 'three'])
  })

  it('should trim whitespace from items', async () => {
    const mockIO = createMockIO(['  one  ,  two  '])
    const result = await promptList('Enter items:', {}, mockIO)
    expect(result).toEqual(['one', 'two'])
  })

  it('should filter empty items', async () => {
    const mockIO = createMockIO(['one,, two, ,three'])
    const result = await promptList('Enter items:', {}, mockIO)
    expect(result).toEqual(['one', 'two', 'three'])
  })

  it('should return empty array on empty input', async () => {
    const mockIO = createMockIO([''])
    const result = await promptList('Enter items:', {}, mockIO)
    expect(result).toEqual([])
  })

  it('should use default on empty input when provided', async () => {
    const mockIO = createMockIO([''])
    const result = await promptList('Enter items:', { default: ['a', 'b'] }, mockIO)
    expect(result).toEqual(['a', 'b'])
  })

  it('should validate list items', async () => {
    const mockIO = createMockIO(['invalid', 'valid'])
    const result = await promptList(
      'Enter items:',
      {
        validate: (items) =>
          items.includes('valid') ? true : 'Must include "valid"',
      },
      mockIO
    )
    expect(result).toEqual(['valid'])
    expect(mockIO.getOutput()).toContain('Must include "valid"')
  })

  it('should show comma-separated hint', async () => {
    const mockIO = createMockIO(['a'])
    await promptList('Enter items:', {}, mockIO)
    expect(mockIO.getOutput()).toContain('comma-separated')
  })
})
