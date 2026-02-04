/**
 * Export Command Tests
 *
 * Dedicated tests for the export command focusing on:
 * - Path validation and security
 * - Argument handling
 * - Error cases
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import { exportCommand } from '../../../src/cli/commands/export'
import { initCommand } from '../../../src/cli/commands/init'
import type { ParsedArgs } from '../../../src/cli/types'

describe('export command', () => {
  let tempDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write
  const originalCwd = process.cwd

  beforeEach(async () => {
    // Create a unique temp directory
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-export-test-'))

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

    // Mock cwd to return tempDir
    process.cwd = vi.fn(() => tempDir)

    // Initialize a test database
    await initCommand(createArgs('init', []))
  })

  afterEach(async () => {
    // Restore stdout, stderr, and cwd
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite
    process.cwd = originalCwd

    // Clean up temp directory
    await fs.rm(tempDir, { recursive: true, force: true })

    vi.restoreAllMocks()
  })

  /**
   * Create a ParsedArgs object for testing
   */
  function createArgs(
    command: string,
    args: string[],
    options: Partial<ParsedArgs['options']> = {}
  ): ParsedArgs {
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
  // Argument Validation Tests
  // ===========================================================================

  describe('argument validation', () => {
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

    it('should show usage message on missing arguments', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('export', [])
      await exportCommand(args)

      const output = stdoutOutput.join('')
      expect(output).toContain('Usage:')
      expect(output).toContain('parquedb export')
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
  })

  // ===========================================================================
  // Path Validation Tests
  // ===========================================================================

  describe('path validation', () => {
    it('should reject paths with path traversal sequences', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('export', ['posts', '../../../etc/passwd'])
      const code = await exportCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid file path')
      expect(stderrOutput.join('')).toContain('traversal')
    })

    it('should reject paths with multiple traversal sequences', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('export', ['posts', 'a/b/../../../etc/passwd'])
      const code = await exportCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid file path')
    })

    it('should reject paths with null bytes', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('export', ['posts', 'output\0.json'])
      const code = await exportCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid file path')
      expect(stderrOutput.join('')).toContain('dangerous characters')
    })

    it('should reject paths with newlines', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('export', ['posts', 'output\n.json'])
      const code = await exportCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid file path')
      expect(stderrOutput.join('')).toContain('dangerous characters')
    })

    it('should reject absolute paths outside allowed directories', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('export', ['posts', '/etc/passwd'])
      const code = await exportCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid file path')
    })

    it('should accept absolute paths within the data directory', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'valid.json')
      const args = createArgs('export', ['posts', filePath])
      const code = await exportCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Exported 0 entities')
    })

    it('should accept relative paths that stay within allowed directories', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('export', ['posts', 'output.json'])
      const code = await exportCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Exported 0 entities')

      // Verify file was created
      const filePath = join(tempDir, 'output.json')
      const exists = await fs
        .access(filePath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })

    it('should accept paths with subdirectories', async () => {
      stdoutOutput = []
      stderrOutput = []

      // Create subdirectory
      const subDir = join(tempDir, 'exports')
      await fs.mkdir(subDir)

      const args = createArgs('export', ['posts', 'exports/output.json'])
      const code = await exportCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Exported 0 entities')
    })
  })

  // ===========================================================================
  // Format Handling Tests
  // ===========================================================================

  describe('format handling', () => {
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

    it('should export empty namespace to NDJSON', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.ndjson')
      const args = createArgs('export', ['posts', filePath], { format: 'ndjson' })
      const code = await exportCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Exported 0 entities')

      // Verify file is created
      const content = await fs.readFile(filePath, 'utf-8')
      expect(content).toBe('')
    })

    it('should auto-detect NDJSON format from .ndjson extension', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.ndjson')
      // Don't set format option - should be detected from extension
      const args = createArgs('export', ['posts', filePath])
      const code = await exportCommand(args)

      expect(code).toBe(0)
    })

    it('should auto-detect NDJSON format from .jsonl extension', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.jsonl')
      const args = createArgs('export', ['posts', filePath])
      const code = await exportCommand(args)

      expect(code).toBe(0)
    })

    it('should auto-detect CSV format from .csv extension', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.csv')
      // Don't set format option - should be detected from extension
      const args = createArgs('export', ['posts', filePath])
      const code = await exportCommand(args)

      expect(code).toBe(0)
    })

    it('should support pretty JSON output', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.json')
      const args = createArgs('export', ['posts', filePath], { pretty: true })
      const code = await exportCommand(args)

      expect(code).toBe(0)

      // Check file was created
      const exists = await fs
        .access(filePath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })
  })

  // ===========================================================================
  // Quiet Mode Tests
  // ===========================================================================

  describe('quiet mode', () => {
    it('should suppress output in quiet mode', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.json')
      const args = createArgs('export', ['posts', filePath], { quiet: true })
      const code = await exportCommand(args)

      expect(code).toBe(0)
      // Should not have "Exported" message
      expect(stdoutOutput.join('')).not.toContain('Exported')
    })
  })

  // ===========================================================================
  // Error Handling Tests
  // ===========================================================================

  describe('error handling', () => {
    it('should handle non-existent namespace gracefully', async () => {
      stdoutOutput = []
      stderrOutput = []

      const filePath = join(tempDir, 'out.json')
      const args = createArgs('export', ['nonexistent_namespace_12345', filePath])
      const code = await exportCommand(args)

      // Should succeed with 0 entities (namespace doesn't exist = empty)
      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Exported 0 entities')
    })

    it('should report write errors', async () => {
      stdoutOutput = []
      stderrOutput = []

      // Try to write to a directory that doesn't exist
      const filePath = join(tempDir, 'nonexistent-dir', 'nested', 'out.json')
      const args = createArgs('export', ['posts', filePath])
      const code = await exportCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Export failed')
    })
  })
})
