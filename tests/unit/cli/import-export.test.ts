/**
 * Import and Export Command Tests
 *
 * Tests for the import and export commands argument parsing and basic behavior.
 * Note: Full integration tests with data persistence require proper
 * database setup which is tested in integration tests.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { importCommand } from '../../../src/cli/commands/import'
import { exportCommand } from '../../../src/cli/commands/export'
import { initCommand } from '../../../src/cli/commands/init'
import type { ParsedArgs } from '../../../src/cli/index'

describe('import and export commands', () => {
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

  // ===========================================================================
  // Import Command Tests
  // ===========================================================================

  describe('import command', () => {
    it('should fail if no arguments provided', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('import', [])
      const code = await importCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing arguments')
    })

    it('should fail if only namespace provided', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('import', ['posts'])
      const code = await importCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing arguments')
    })

    it('should fail if file not found', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('import', ['posts', '/nonexistent/file.json'])
      const code = await importCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('File not found')
    })

    it('should fail if database not initialized', async () => {
      await fs.unlink(join(tempDir, 'parquedb.json'))
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'data.json')
      await fs.writeFile(filePath, '[]')

      const args = createArgs('import', ['posts', filePath])
      const code = await importCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('not initialized')
    })

    it('should import empty JSON array', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'empty.json')
      await fs.writeFile(filePath, '[]')

      const args = createArgs('import', ['posts', filePath])
      const code = await importCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Imported 0 entities')
    })

    it('should import JSON array with data', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'posts.json')
      const data = [
        { $type: 'Post', name: 'Post 1', title: 'First Post' },
        { $type: 'Post', name: 'Post 2', title: 'Second Post' },
      ]
      await fs.writeFile(filePath, JSON.stringify(data))

      const args = createArgs('import', ['posts', filePath])
      const code = await importCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Imported 2 entities')
    })

    it('should import NDJSON file', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'posts.ndjson')
      const lines = [
        JSON.stringify({ $type: 'Post', name: 'Post 1', title: 'First' }),
        JSON.stringify({ $type: 'Post', name: 'Post 2', title: 'Second' }),
      ]
      await fs.writeFile(filePath, lines.join('\n'))

      const args = createArgs('import', ['posts', filePath], { format: 'ndjson' })
      const code = await importCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Imported 2 entities')
    })

    it('should import CSV file', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'posts.csv')
      const csv = `$type,name,title
Post,Post 1,First Post
Post,Post 2,Second Post`
      await fs.writeFile(filePath, csv)

      const args = createArgs('import', ['posts', filePath], { format: 'csv' })
      const code = await importCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Imported 2 entities')
    })
  })

  // ===========================================================================
  // Export Command Tests
  // ===========================================================================

  describe('export command', () => {
    it('should fail if no arguments provided', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('export', [])
      const code = await exportCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing arguments')
    })

    it('should fail if only namespace provided', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('export', ['posts'])
      const code = await exportCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing arguments')
    })

    it('should fail if database not initialized', async () => {
      await fs.unlink(join(tempDir, 'parquedb.json'))
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.json')
      const args = createArgs('export', ['posts', filePath])
      const code = await exportCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('not initialized')
    })

    it('should export empty namespace to JSON', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.json')
      const args = createArgs('export', ['posts', filePath])
      const code = await exportCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Exported 0 entities')

      // Verify file content
      const content = await fs.readFile(filePath, 'utf-8')
      const data = JSON.parse(content)
      expect(data).toEqual([])
    })

    it('should export empty namespace to CSV', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.csv')
      const args = createArgs('export', ['posts', filePath], { format: 'csv' })
      const code = await exportCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Exported 0 entities')

      // Verify file is created (empty)
      const content = await fs.readFile(filePath, 'utf-8')
      expect(content).toBe('')
    })
  })
})
