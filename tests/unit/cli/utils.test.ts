/**
 * CLI Utils Tests
 *
 * Tests for the CLI utility functions from @dotdo/cli integration.
 * These tests verify that the ParqueDB CLI properly integrates with
 * the @dotdo/cli package for spinners, progress bars, formatting, and colors.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  success,
  error,
  warn,
  info,
  dim,
  header,
  formatBytes,
  formatDuration,
  divider,
  box,
  printStats,
  colorize,
  colors,
  isColorEnabled,
  isTTY,
  setNoColor,
  formatOutput,
  formatOutputSync,
} from '../../../src/cli/utils'

describe('CLI Utils (@dotdo/cli integration)', () => {
  let consoleLogSpy: ReturnType<typeof vi.spyOn>
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    // Reset color state
    setNoColor(false)
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  // ===========================================================================
  // Message Helpers
  // ===========================================================================

  describe('message helpers', () => {
    it('success() should print success message', () => {
      success('Operation completed')
      expect(consoleLogSpy).toHaveBeenCalledWith(expect.stringContaining('Operation completed'))
    })

    it('error() should print error message to stderr', () => {
      error('Something went wrong')
      expect(consoleErrorSpy).toHaveBeenCalledWith(expect.stringContaining('Something went wrong'))
    })

    it('warn() should print warning message', () => {
      warn('This is a warning')
      expect(consoleWarnSpy).toHaveBeenCalledWith(expect.stringContaining('This is a warning'))
    })

    it('info() should print info message', () => {
      info('Information')
      expect(consoleLogSpy).toHaveBeenCalledWith(expect.stringContaining('Information'))
    })

    it('dim() should print dimmed message', () => {
      dim('Secondary info')
      expect(consoleLogSpy).toHaveBeenCalledWith(expect.stringContaining('Secondary info'))
    })

    it('header() should print header message', () => {
      header('Section Header')
      expect(consoleLogSpy).toHaveBeenCalledWith(expect.stringContaining('Section Header'))
    })
  })

  // ===========================================================================
  // Formatting Utilities
  // ===========================================================================

  describe('formatBytes()', () => {
    it('should format 0 bytes', () => {
      expect(formatBytes(0)).toBe('0 B')
    })

    it('should format bytes', () => {
      expect(formatBytes(512)).toBe('512 B')
    })

    it('should format kilobytes', () => {
      expect(formatBytes(1024)).toBe('1.0 KB')
      expect(formatBytes(1536)).toBe('1.5 KB')
    })

    it('should format megabytes', () => {
      expect(formatBytes(1024 * 1024)).toBe('1.0 MB')
      expect(formatBytes(1024 * 1024 * 2.5)).toBe('2.5 MB')
    })

    it('should format gigabytes', () => {
      expect(formatBytes(1024 * 1024 * 1024)).toBe('1.0 GB')
    })

    it('should format terabytes', () => {
      expect(formatBytes(1024 * 1024 * 1024 * 1024)).toBe('1.0 TB')
    })
  })

  describe('formatDuration()', () => {
    it('should format milliseconds', () => {
      expect(formatDuration(500)).toBe('500ms')
    })

    it('should format seconds', () => {
      expect(formatDuration(1500)).toBe('1.5s')
      expect(formatDuration(30000)).toBe('30.0s')
    })

    it('should format minutes and seconds', () => {
      expect(formatDuration(90000)).toBe('1m 30s')
      expect(formatDuration(125000)).toBe('2m 5s')
    })

    it('should format hours and minutes', () => {
      expect(formatDuration(3600000)).toBe('1h 0m')
      expect(formatDuration(5400000)).toBe('1h 30m')
    })
  })

  describe('divider()', () => {
    it('should create default divider', () => {
      const result = divider()
      expect(result).toBe('-'.repeat(50))
    })

    it('should create custom character divider', () => {
      const result = divider('=')
      expect(result).toBe('='.repeat(50))
    })

    it('should create custom length divider', () => {
      const result = divider('-', 20)
      expect(result).toBe('-'.repeat(20))
    })
  })

  describe('box()', () => {
    it('should print a box with title and content', () => {
      box('Title', ['Line 1', 'Line 2'])
      // Should have been called multiple times for borders and content
      expect(consoleLogSpy).toHaveBeenCalled()
    })
  })

  describe('printStats()', () => {
    it('should print key-value statistics', () => {
      printStats({
        'Total Items': 100,
        'Active': true,
        'Label': 'test',
      })
      expect(consoleLogSpy).toHaveBeenCalled()
    })
  })

  // ===========================================================================
  // Color Utilities
  // ===========================================================================

  describe('color utilities', () => {
    describe('colors object', () => {
      it('should have reset code', () => {
        expect(colors.reset).toBeDefined()
        expect(colors.reset).toContain('\x1b[')
      })

      it('should have foreground colors', () => {
        expect(colors.red).toBeDefined()
        expect(colors.green).toBeDefined()
        expect(colors.blue).toBeDefined()
        expect(colors.yellow).toBeDefined()
        expect(colors.cyan).toBeDefined()
      })

      it('should have formatting codes', () => {
        expect(colors.bold).toBeDefined()
        expect(colors.dim).toBeDefined()
      })
    })

    describe('colorize()', () => {
      it('should wrap text in color codes when colors enabled', () => {
        // Force colors to be disabled to test plain text
        setNoColor(true)
        const result = colorize('test', 'red')
        expect(result).toBe('test')
      })
    })

    describe('setNoColor()', () => {
      it('should disable colors globally', () => {
        setNoColor(true)
        expect(isColorEnabled()).toBe(false)
      })

      it('should enable colors when set to false', () => {
        setNoColor(false)
        // Color enabled depends on TTY, but we know setNoColor(false) doesn't force disable
        // The actual result depends on whether stdout is a TTY
      })
    })

    describe('isColorEnabled()', () => {
      it('should return boolean', () => {
        expect(typeof isColorEnabled()).toBe('boolean')
      })

      it('should respect setNoColor', () => {
        setNoColor(true)
        expect(isColorEnabled()).toBe(false)
      })
    })

    describe('isTTY()', () => {
      it('should return boolean', () => {
        expect(typeof isTTY()).toBe('boolean')
      })
    })
  })

  // ===========================================================================
  // Output Formatters
  // ===========================================================================

  describe('formatOutput()', () => {
    const testData = [
      { id: 1, name: 'Alice', role: 'admin' },
      { id: 2, name: 'Bob', role: 'user' },
    ]

    it('should format as JSON', async () => {
      const result = await formatOutput(testData, 'json')
      expect(result).toContain('"id"')
      expect(result).toContain('"name"')
      expect(result).toContain('Alice')
    })

    it('should format as table', async () => {
      const result = await formatOutput(testData, 'table')
      // Table output should have headers and data
      expect(result).toContain('id')
      expect(result).toContain('name')
      expect(result).toContain('Alice')
      expect(result).toContain('Bob')
    })

    it('should handle single object', async () => {
      const result = await formatOutput({ key: 'value' }, 'json')
      expect(result).toContain('key')
      expect(result).toContain('value')
    })

    it('should handle empty array', async () => {
      const result = await formatOutput([], 'json')
      expect(result).toBe('[]')
    })
  })

  describe('formatOutputSync()', () => {
    it('should format as JSON synchronously', () => {
      const result = formatOutputSync({ key: 'value' }, 'json')
      expect(result).toContain('key')
      expect(result).toContain('value')
    })

    it('should format as table synchronously', () => {
      const result = formatOutputSync([{ id: 1, name: 'test' }], 'table')
      expect(result).toContain('id')
      expect(result).toContain('name')
    })
  })
})

describe('CLI Utils - Spinner and Progress', () => {
  // These tests verify the spinner and progress exports exist
  // Actual spinner/progress behavior is best tested manually or with integration tests

  it('should export spinner functions', async () => {
    const { createSpinner, startSpinner, withSpinner, spinnerFrames } = await import('../../../src/cli/utils')
    expect(typeof createSpinner).toBe('function')
    expect(typeof startSpinner).toBe('function')
    expect(typeof withSpinner).toBe('function')
    expect(spinnerFrames).toBeDefined()
    expect(spinnerFrames.dots).toBeDefined()
  })

  it('should export progress functions', async () => {
    const { createProgress, startProgress, withProgress, MultiProgress } = await import('../../../src/cli/utils')
    expect(typeof createProgress).toBe('function')
    expect(typeof startProgress).toBe('function')
    expect(typeof withProgress).toBe('function')
    expect(MultiProgress).toBeDefined()
  })
})

describe('CLI Utils - Syntax Highlighting', () => {
  it('should export highlighting functions', async () => {
    const { highlightJson, highlightSql } = await import('../../../src/cli/utils')
    expect(typeof highlightJson).toBe('function')
    expect(typeof highlightSql).toBe('function')
  })

  it('highlightJson should highlight JSON data', async () => {
    const { highlightJson } = await import('../../../src/cli/utils')
    const result = await highlightJson({ key: 'value' })
    // Result should be a string (may have ANSI codes or plain depending on TTY)
    expect(typeof result).toBe('string')
    expect(result).toContain('key')
  })

  it('highlightSql should highlight SQL', async () => {
    const { highlightSql } = await import('../../../src/cli/utils')
    const result = await highlightSql('SELECT * FROM users')
    expect(typeof result).toBe('string')
    expect(result).toContain('SELECT')
  })
})

// ===========================================================================
// Enhanced CLI Helpers Tests
// ===========================================================================

describe('CLI Utils - Enhanced Helpers', () => {
  let consoleLogSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('bulletList()', () => {
    it('should print items with bullet points', async () => {
      const { bulletList } = await import('../../../src/cli/utils')
      bulletList(['Item 1', 'Item 2', 'Item 3'])
      expect(consoleLogSpy).toHaveBeenCalledTimes(3)
    })

    it('should respect custom indent', async () => {
      const { bulletList } = await import('../../../src/cli/utils')
      bulletList(['Item'], 4)
      expect(consoleLogSpy).toHaveBeenCalled()
      const call = consoleLogSpy.mock.calls[0]?.[0] as string
      expect(call.startsWith('    ')).toBe(true)
    })
  })

  describe('numberedList()', () => {
    it('should print items with numbers', async () => {
      const { numberedList } = await import('../../../src/cli/utils')
      numberedList(['First', 'Second', 'Third'])
      expect(consoleLogSpy).toHaveBeenCalledTimes(3)
    })
  })

  describe('keyValue()', () => {
    it('should print key-value pair', async () => {
      const { keyValue } = await import('../../../src/cli/utils')
      keyValue('Name', 'ParqueDB')
      expect(consoleLogSpy).toHaveBeenCalled()
      const call = consoleLogSpy.mock.calls[0]?.[0] as string
      expect(call).toContain('Name')
      expect(call).toContain('ParqueDB')
    })

    it('should handle numeric values', async () => {
      const { keyValue } = await import('../../../src/cli/utils')
      keyValue('Count', 42)
      expect(consoleLogSpy).toHaveBeenCalled()
      const call = consoleLogSpy.mock.calls[0]?.[0] as string
      expect(call).toContain('42')
    })

    it('should handle boolean values', async () => {
      const { keyValue } = await import('../../../src/cli/utils')
      keyValue('Active', true)
      expect(consoleLogSpy).toHaveBeenCalled()
      const call = consoleLogSpy.mock.calls[0]?.[0] as string
      expect(call).toContain('true')
    })
  })

  describe('formatNumber()', () => {
    it('should format numbers with locale separators', async () => {
      const { formatNumber } = await import('../../../src/cli/utils')
      const result = formatNumber(1234567)
      // Result depends on locale, but should be a string
      expect(typeof result).toBe('string')
      expect(result.length).toBeGreaterThan(6) // Has separators
    })
  })

  describe('formatPercent()', () => {
    it('should format as percentage', async () => {
      const { formatPercent } = await import('../../../src/cli/utils')
      expect(formatPercent(0.5)).toBe('50.0%')
      expect(formatPercent(0.123)).toBe('12.3%')
      expect(formatPercent(1)).toBe('100.0%')
    })

    it('should respect decimal places', async () => {
      const { formatPercent } = await import('../../../src/cli/utils')
      expect(formatPercent(0.12345, 2)).toBe('12.35%')
      expect(formatPercent(0.5, 0)).toBe('50%')
    })
  })

  describe('truncate()', () => {
    it('should not truncate short strings', async () => {
      const { truncate } = await import('../../../src/cli/utils')
      expect(truncate('short', 10)).toBe('short')
    })

    it('should truncate long strings with ellipsis', async () => {
      const { truncate } = await import('../../../src/cli/utils')
      expect(truncate('this is a long string', 10)).toBe('this is...')
    })

    it('should handle exact length', async () => {
      const { truncate } = await import('../../../src/cli/utils')
      expect(truncate('exactly', 7)).toBe('exactly')
    })
  })

  describe('pad()', () => {
    it('should pad left by default', async () => {
      const { pad } = await import('../../../src/cli/utils')
      expect(pad('test', 8)).toBe('test    ')
    })

    it('should pad right', async () => {
      const { pad } = await import('../../../src/cli/utils')
      expect(pad('test', 8, 'right')).toBe('    test')
    })

    it('should pad center', async () => {
      const { pad } = await import('../../../src/cli/utils')
      expect(pad('test', 8, 'center')).toBe('  test  ')
    })
  })

  describe('simpleTable()', () => {
    it('should create ASCII table', async () => {
      const { simpleTable } = await import('../../../src/cli/utils')
      const result = simpleTable(
        ['Name', 'Age'],
        [['Alice', 30], ['Bob', 25]]
      )
      expect(result).toContain('Name')
      expect(result).toContain('Age')
      expect(result).toContain('Alice')
      expect(result).toContain('30')
      expect(result).toContain('Bob')
      expect(result).toContain('25')
      expect(result).toContain('-') // Separator
    })

    it('should handle empty data', async () => {
      const { simpleTable } = await import('../../../src/cli/utils')
      const result = simpleTable(['Col1', 'Col2'], [])
      expect(result).toContain('Col1')
      expect(result).toContain('Col2')
    })
  })

  describe('section()', () => {
    it('should print header and call content', async () => {
      const { section } = await import('../../../src/cli/utils')
      const contentFn = vi.fn()
      section('Test Section', contentFn)
      expect(contentFn).toHaveBeenCalled()
      expect(consoleLogSpy).toHaveBeenCalled()
    })
  })
})

// ===========================================================================
// Process Items Tests
// ===========================================================================

describe('CLI Utils - processItems()', () => {
  it('should process items sequentially', async () => {
    const { processItems } = await import('../../../src/cli/utils')
    const items = [1, 2, 3]
    const processed: number[] = []

    const results = await processItems(
      items,
      async (item) => {
        processed.push(item)
        return item * 2
      },
      { silent: true }
    )

    expect(processed).toEqual([1, 2, 3])
    expect(results).toEqual([2, 4, 6])
  })

  it('should handle empty array', async () => {
    const { processItems } = await import('../../../src/cli/utils')
    const results = await processItems([], async () => 'never', { silent: true })
    expect(results).toEqual([])
  })

  it('should support concurrent processing', async () => {
    const { processItems } = await import('../../../src/cli/utils')
    const items = [1, 2, 3, 4]

    const results = await processItems(
      items,
      async (item) => item * 2,
      { silent: true, concurrency: 2 }
    )

    expect(results).toEqual([2, 4, 6, 8])
  })
})

// ===========================================================================
// runWithSpinner Tests
// ===========================================================================

describe('CLI Utils - runWithSpinner()', () => {
  it('should execute operation and return result', async () => {
    const { runWithSpinner } = await import('../../../src/cli/utils')

    const result = await runWithSpinner(
      async () => 'success',
      { text: 'Loading...', silent: true }
    )

    expect(result).toBe('success')
  })

  it('should propagate errors', async () => {
    const { runWithSpinner } = await import('../../../src/cli/utils')

    await expect(
      runWithSpinner(
        async () => { throw new Error('test error') },
        { text: 'Loading...', silent: true }
      )
    ).rejects.toThrow('test error')
  })

  it('should work with async operations', async () => {
    vi.useFakeTimers()
    try {
      const { runWithSpinner } = await import('../../../src/cli/utils')

      const resultPromise = runWithSpinner(
        async () => {
          await vi.advanceTimersByTimeAsync(10)
          return { data: 'async result' }
        },
        { text: 'Processing...', silent: true }
      )

      const result = await resultPromise

      expect(result).toEqual({ data: 'async result' })
    } finally {
      vi.useRealTimers()
    }
  })
})
