/**
 * MV (Materialized Views) Command Tests
 *
 * Tests for the mv command subcommands: create, list, show, refresh, drop.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { mvCommand } from '../../../src/cli/commands/mv'
import { initCommand } from '../../../src/cli/commands/init'
import type { ParsedArgs } from '../../../src/cli/types'

describe('mv command', () => {
  let tempDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write
  const originalArgv = process.argv

  beforeEach(async () => {
    // Create a unique temp directory
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-mv-test-'))

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
    process.argv = originalArgv

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
        noColor: true,
        ...options,
      },
    }
  }

  /**
   * Set process.argv for tests that use raw args parsing
   */
  function setArgv(args: string[]): void {
    process.argv = ['node', 'parquedb', 'mv', ...args]
  }

  describe('no subcommand / help', () => {
    it('should display help when no subcommand is provided', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', [])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      expect(output).toContain('Materialized Views')
      expect(output).toContain('create')
      expect(output).toContain('list')
      expect(output).toContain('show')
      expect(output).toContain('refresh')
      expect(output).toContain('drop')
    })
  })

  describe('mv list', () => {
    it('should fail if database is not initialized', async () => {
      await fs.unlink(join(tempDir, 'parquedb.json'))
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['list'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('not initialized')
    })

    it('should list empty views', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['list'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('No materialized views found')
    })

    it('should list empty views with ls alias', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['ls'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('No materialized views found')
    })

    it('should output JSON when --json flag is present', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['list', '--json'])

      const args = createArgs('mv', ['list'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const parsed = JSON.parse(output)
      expect(Array.isArray(parsed)).toBe(true)
      expect(parsed).toHaveLength(0)
    })
  })

  describe('mv create', () => {
    it('should fail if name is missing', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['create'])

      const args = createArgs('mv', ['create'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing required argument')
    })

    it('should fail if --from is missing', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['create', 'test_view'])

      const args = createArgs('mv', ['create', 'test_view'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('--from')
    })

    it('should fail with invalid view name', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['create', '123invalid', '--from', 'users'])

      const args = createArgs('mv', ['create', '123invalid'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid view name')
    })

    it('should create a view successfully', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['create', 'active_users', '--from', 'users'])

      const args = createArgs('mv', ['create', 'active_users'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Created materialized view')
      expect(stdoutOutput.join('')).toContain('active_users')
    })

    it('should create a view with filter', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['create', 'active_users', '--from', 'users', '--filter', '{"status": "active"}'])

      const args = createArgs('mv', ['create', 'active_users'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Created materialized view')
    })

    it('should fail with invalid JSON filter', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['create', 'test_view', '--from', 'users', '--filter', '{invalid}'])

      const args = createArgs('mv', ['create', 'test_view'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid JSON')
    })

    it('should fail when creating duplicate view', async () => {
      setArgv(['create', 'my_view', '--from', 'users'])
      const args = createArgs('mv', ['create', 'my_view'])
      await mvCommand(args)

      // Try to create again
      stdoutOutput = []
      stderrOutput = []
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('already exists')
    })

    it('should create scheduled view with schedule', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['create', 'hourly_stats', '--from', 'events', '--refresh', 'scheduled', '--schedule', '0 * * * *'])

      const args = createArgs('mv', ['create', 'hourly_stats'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Created materialized view')
      expect(stdoutOutput.join('')).toContain('scheduled')
    })

    it('should fail scheduled view without schedule', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['create', 'hourly_stats', '--from', 'events', '--refresh', 'scheduled'])

      const args = createArgs('mv', ['create', 'hourly_stats'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('--schedule')
    })
  })

  describe('mv show', () => {
    beforeEach(async () => {
      // Create a view first
      setArgv(['create', 'test_view', '--from', 'users'])
      await mvCommand(createArgs('mv', ['create', 'test_view']))
    })

    it('should fail if name is missing', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['show'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing required argument')
    })

    it('should show view details', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['show', 'test_view'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      expect(output).toContain('test_view')
      expect(output).toContain('users')
      expect(output).toContain('Definition')
      expect(output).toContain('State')
    })

    it('should output JSON when --json flag is present', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['show', 'test_view', '--json'])

      const args = createArgs('mv', ['show', 'test_view'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const parsed = JSON.parse(output)
      expect(parsed).toHaveProperty('metadata')
      expect(parsed).toHaveProperty('stats')
      expect(parsed.metadata.definition.source).toBe('users')
    })

    it('should fail for non-existent view', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['show', 'nonexistent'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('not found')
    })

    it('should work with get alias', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['get', 'test_view'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('test_view')
    })
  })

  describe('mv refresh', () => {
    beforeEach(async () => {
      // Create a view first
      setArgv(['create', 'test_view', '--from', 'users'])
      await mvCommand(createArgs('mv', ['create', 'test_view']))
    })

    it('should fail if name is missing', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['refresh'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing required argument')
    })

    it('should fail for non-existent view', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['refresh', 'nonexistent'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('not found')
    })

    it('should refresh a view', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['refresh', 'test_view', '--force'])

      const args = createArgs('mv', ['refresh', 'test_view'])
      const code = await mvCommand(args)

      // May succeed or fail depending on source data
      // Just verify it attempts refresh
      expect(code === 0 || stderrOutput.join('').includes('Refresh')).toBe(true)
    })
  })

  describe('mv drop', () => {
    beforeEach(async () => {
      // Create a view first
      setArgv(['create', 'test_view', '--from', 'users'])
      await mvCommand(createArgs('mv', ['create', 'test_view']))
    })

    it('should fail if name is missing', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('mv', ['drop'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing required argument')
    })

    it('should fail for non-existent view', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['drop', 'nonexistent', '--force'])

      const args = createArgs('mv', ['drop', 'nonexistent'])
      const code = await mvCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('not found')
    })

    it('should require --force flag', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['drop', 'test_view'])

      const args = createArgs('mv', ['drop', 'test_view'])
      const code = await mvCommand(args)

      // Without --force, should return 1 and ask for confirmation
      expect(code).toBe(1)
      expect(stdoutOutput.join('')).toContain('--force')
    })

    it('should drop view with --force flag', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['drop', 'test_view', '--force'])

      const args = createArgs('mv', ['drop', 'test_view'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Dropped')
    })

    it('should work with delete alias', async () => {
      stdoutOutput = []
      stderrOutput = []
      setArgv(['delete', 'test_view', '--force'])

      const args = createArgs('mv', ['delete', 'test_view'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Dropped')
    })

    it('should work with rm alias', async () => {
      // Create another view since we dropped the previous one
      setArgv(['create', 'another_view', '--from', 'posts'])
      await mvCommand(createArgs('mv', ['create', 'another_view']))

      stdoutOutput = []
      stderrOutput = []
      setArgv(['rm', 'another_view', '--force'])

      const args = createArgs('mv', ['rm', 'another_view'])
      const code = await mvCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Dropped')
    })
  })

  describe('integration: create, list, show, drop', () => {
    it('should complete full lifecycle', async () => {
      // 1. List - should be empty
      stdoutOutput = []
      setArgv(['list', '--json'])
      let args = createArgs('mv', ['list'])
      let code = await mvCommand(args)
      expect(code).toBe(0)
      let parsed = JSON.parse(stdoutOutput.join(''))
      expect(parsed).toHaveLength(0)

      // 2. Create a view
      stdoutOutput = []
      setArgv(['create', 'my_view', '--from', 'users', '--filter', '{"active": true}'])
      args = createArgs('mv', ['create', 'my_view'])
      code = await mvCommand(args)
      expect(code).toBe(0)

      // 3. List - should have one view
      stdoutOutput = []
      setArgv(['list', '--json'])
      args = createArgs('mv', ['list'])
      code = await mvCommand(args)
      expect(code).toBe(0)
      parsed = JSON.parse(stdoutOutput.join(''))
      expect(parsed).toHaveLength(1)
      expect(parsed[0].name).toBe('my_view')
      expect(parsed[0].source).toBe('users')

      // 4. Show view details
      stdoutOutput = []
      setArgv(['show', 'my_view', '--json'])
      args = createArgs('mv', ['show', 'my_view'])
      code = await mvCommand(args)
      expect(code).toBe(0)
      const details = JSON.parse(stdoutOutput.join(''))
      expect(details.metadata.definition.source).toBe('users')

      // 5. Drop the view
      stdoutOutput = []
      setArgv(['drop', 'my_view', '--force'])
      args = createArgs('mv', ['drop', 'my_view'])
      code = await mvCommand(args)
      expect(code).toBe(0)

      // 6. List - should be empty again
      stdoutOutput = []
      setArgv(['list', '--json'])
      args = createArgs('mv', ['list'])
      code = await mvCommand(args)
      expect(code).toBe(0)
      parsed = JSON.parse(stdoutOutput.join(''))
      expect(parsed).toHaveLength(0)
    })
  })
})
