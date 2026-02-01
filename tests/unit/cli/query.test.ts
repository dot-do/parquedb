/**
 * Query Command Tests
 *
 * Tests for the query command argument parsing and basic behavior.
 * Note: Full integration tests with data persistence require proper
 * database setup which is tested in integration tests.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { queryCommand } from '../../../src/cli/commands/query'
import { initCommand } from '../../../src/cli/commands/init'
import type { ParsedArgs } from '../../../src/cli/index'

describe('query command', () => {
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

  it('should fail if no namespace provided', async () => {
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('query', [])
    const code = await queryCommand(args)

    expect(code).toBe(1)
    expect(stderrOutput.join('')).toContain('Missing namespace')
  })

  it('should fail if database not initialized', async () => {
    // Delete the config file
    await fs.unlink(join(tempDir, 'parquedb.json'))
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('query', ['posts'])
    const code = await queryCommand(args)

    expect(code).toBe(1)
    expect(stderrOutput.join('')).toContain('not initialized')
  })

  it('should query empty namespace', async () => {
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('query', ['posts'])
    const code = await queryCommand(args)

    expect(code).toBe(0)
    expect(stdoutOutput.join('')).toContain('No results found')
  })

  it('should fail with invalid JSON filter', async () => {
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('query', ['posts', 'not-json'])
    const code = await queryCommand(args)

    expect(code).toBe(1)
    expect(stderrOutput.join('')).toContain('Invalid JSON filter')
  })

  it('should accept valid JSON filter', async () => {
    stdoutOutput = []
    stderrOutput = []

    // Valid JSON filter on empty namespace should not error
    const args = createArgs('query', ['posts', '{"status": "published"}'])
    const code = await queryCommand(args)

    expect(code).toBe(0)
    expect(stdoutOutput.join('')).toContain('No results found')
  })

  it('should accept limit option', async () => {
    stdoutOutput = []
    stderrOutput = []

    const args = createArgs('query', ['posts'], { limit: 10 })
    const code = await queryCommand(args)

    expect(code).toBe(0)
    // Empty namespace with limit still works
    expect(stdoutOutput.join('')).toContain('No results found')
  })
})
