/**
 * Stats Command Tests
 *
 * Tests for the stats command argument parsing and basic behavior.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { statsCommand } from '../../../src/cli/commands/stats'
import { initCommand } from '../../../src/cli/commands/init'
import type { ParsedArgs } from '../../../src/cli/index'

describe('stats command', () => {
  let tempDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write

  beforeEach(async () => {
    // Create a unique temp directory
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-cli-test-'))

    // Reset output capture
    stdoutOutput = []
    stderrOutput = []

    // Mock stdout and stderr
    process.stdout.write = vi.fn((chunk: string | Uint8Array) => {
      stdoutOutput.push(chunk.toString())
      return true
    })
    process.stderr.write = vi.fn((chunk: string | Uint8Array) => {
      stderrOutput.push(chunk.toString())
      return true
    })

    // Initialize a test database
    await initCommand(createArgs('init', []))
  })

  afterEach(async () => {
    // Restore stdout and stderr
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite

    // Clean up temp directory
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  /**
   * Create a ParsedArgs object for testing
   */
  function createArgs(command: string, args: string[], options: Partial<ParsedArgs['options']> = {}): ParsedArgs {
    return {
      command,
      args,
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
        ...options,
      },
    }
  }

  it('should fail if database not initialized', async () => {
    await fs.unlink(join(tempDir, 'parquedb.json'))
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('stats', [])
    const code = await statsCommand(args)

    expect(code).toBe(1)
    expect(stderrOutput.join('')).toContain('not initialized')
  })

  it('should show stats for empty database in text format', async () => {
    stdoutOutput = []
    stderrOutput = []

    // Use ndjson to get text output
    const args = createArgs('stats', [], { format: 'ndjson' })
    const code = await statsCommand(args)

    expect(code).toBe(0)
    expect(stdoutOutput.join('')).toContain('No data found')
  })

  it('should output JSON format for empty database', async () => {
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('stats', [], { format: 'json' })
    const code = await statsCommand(args)

    expect(code).toBe(0)

    const output = stdoutOutput.join('')
    const stats = JSON.parse(output)

    expect(stats).toHaveProperty('name')
    expect(stats).toHaveProperty('namespaces')
    expect(stats).toHaveProperty('totalNamespaces')
    expect(stats).toHaveProperty('totalSizeBytes')
    expect(stats).toHaveProperty('totalSizeFormatted')
    expect(stats).toHaveProperty('eventLogSize')
    expect(stats).toHaveProperty('eventLogSizeFormatted')
    expect(stats.namespaces).toEqual([])
    expect(stats.totalNamespaces).toBe(0)
  })

  it('should output pretty JSON format', async () => {
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('stats', [], { format: 'json', pretty: true })
    const code = await statsCommand(args)

    expect(code).toBe(0)

    const output = stdoutOutput.join('')
    // Pretty JSON has indentation
    expect(output).toContain('\n  ')
    // Should still be valid JSON
    expect(() => JSON.parse(output)).not.toThrow()
  })

  it('should show database name from config', async () => {
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('stats', [], { format: 'json' })
    await statsCommand(args)

    const output = stdoutOutput.join('')
    const stats = JSON.parse(output)

    // Name should come from the temp directory name
    expect(stats.name).toBeDefined()
    expect(typeof stats.name).toBe('string')
  })

  it('should include storage metrics in JSON output', async () => {
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('stats', [], { format: 'json' })
    await statsCommand(args)

    const output = stdoutOutput.join('')
    const stats = JSON.parse(output)

    // Should have all expected metrics
    expect(typeof stats.totalSizeBytes).toBe('number')
    expect(typeof stats.totalSizeFormatted).toBe('string')
    expect(typeof stats.eventLogSize).toBe('number')
    expect(typeof stats.eventLogSizeFormatted).toBe('string')
    expect(typeof stats.totalEntities).toBe('number')
  })
})
