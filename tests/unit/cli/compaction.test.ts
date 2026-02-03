/**
 * Compaction Command Tests
 *
 * Tests for the compaction CLI commands.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { compactionCommand } from '../../../src/cli/commands/compaction'
import { initCommand } from '../../../src/cli/commands/init'
import type { ParsedArgs } from '../../../src/cli/index'

describe('compaction command', () => {
  let tempDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write

  beforeEach(async () => {
    // Create a unique temp directory
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-cli-compaction-test-'))

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

  describe('help', () => {
    it('should show help with no subcommand', async () => {
      stdoutOutput = []
      const args = createArgs('compaction', [])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Compaction Commands')
      expect(stdoutOutput.join('')).toContain('status')
      expect(stdoutOutput.join('')).toContain('retry')
      expect(stdoutOutput.join('')).toContain('cleanup')
      expect(stdoutOutput.join('')).toContain('trigger')
    })

    it('should show help with help subcommand', async () => {
      stdoutOutput = []
      const args = createArgs('compaction', ['help'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Compaction Commands')
    })
  })

  describe('status subcommand', () => {
    it('should fail if database not initialized', async () => {
      await fs.unlink(join(tempDir, 'parquedb.json'))
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['status'])
      const code = await compactionCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('not initialized')
    })

    it('should show status for initialized database', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['status'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const status = JSON.parse(output)

      expect(status).toHaveProperty('pendingJobs')
      expect(status).toHaveProperty('processingJobs')
      expect(status).toHaveProperty('completedJobs')
      expect(status).toHaveProperty('failedJobs')
      expect(status).toHaveProperty('recentJobs')
      expect(status.pendingJobs).toBe(0)
      expect(status.failedJobs).toBe(0)
    })

    it('should show status in text format', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['status'], { format: 'ndjson' })
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      expect(output).toContain('Compaction Status')
      expect(output).toContain('Job Summary')
      expect(output).toContain('Pending:')
      expect(output).toContain('Failed:')
    })

    it('should support pretty JSON output', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['status'], { format: 'json', pretty: true })
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      // Pretty JSON has indentation
      expect(output).toContain('\n  ')
      expect(() => JSON.parse(output)).not.toThrow()
    })
  })

  describe('retry subcommand', () => {
    it('should require job ID', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['retry'])
      const code = await compactionCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Job ID is required')
    })

    it('should error if job not found', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['retry', 'nonexistent-job'])
      const code = await compactionCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Job not found')
    })

    it('should retry a failed job', async () => {
      // First, create a failed job state
      const stateFile = join(tempDir, '.compaction-state.json')
      const state = {
        jobs: [
          {
            id: 'job-test123',
            namespace: 'users',
            windowStart: Date.now() - 3600000,
            windowEnd: Date.now(),
            files: ['data/users/test.parquet'],
            status: 'failed',
            createdAt: Date.now() - 3600000,
            error: 'Test error',
          },
        ],
      }
      await fs.writeFile(stateFile, JSON.stringify(state))

      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['retry', 'job-test123'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const result = JSON.parse(output)
      expect(result.success).toBe(true)
      expect(result.status).toBe('pending')

      // Verify the state was updated
      const updatedState = JSON.parse(await fs.readFile(stateFile, 'utf-8'))
      expect(updatedState.jobs[0].status).toBe('pending')
      expect(updatedState.jobs[0].error).toBeUndefined()
    })

    it('should error if job is not in failed state', async () => {
      // Create a pending job
      const stateFile = join(tempDir, '.compaction-state.json')
      const state = {
        jobs: [
          {
            id: 'job-pending123',
            namespace: 'users',
            windowStart: Date.now() - 3600000,
            windowEnd: Date.now(),
            files: ['data/users/test.parquet'],
            status: 'pending',
            createdAt: Date.now() - 3600000,
          },
        ],
      }
      await fs.writeFile(stateFile, JSON.stringify(state))

      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['retry', 'job-pending123'])
      const code = await compactionCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('not in failed state')
    })
  })

  describe('cleanup subcommand', () => {
    it('should report no orphaned files when clean', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['cleanup'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const result = JSON.parse(output)
      expect(result.success).toBe(true)
      expect(result.filesRemoved).toBe(0)
    })

    it('should find orphaned files', async () => {
      // Create orphaned .tmp files
      const dataDir = join(tempDir, 'data', 'users')
      await fs.mkdir(dataDir, { recursive: true })
      await fs.writeFile(join(dataDir, 'temp.tmp'), 'test')
      await fs.writeFile(join(dataDir, 'partial.partial.parquet'), 'test')

      stdoutOutput = []
      stderrOutput = []

      // Without --force, should just list files
      const args = createArgs('compaction', ['cleanup'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      expect(output).toContain('orphaned files')
      expect(output).toContain('--force')
    })

    it('should remove orphaned files with --force', async () => {
      // Create orphaned .tmp files
      const dataDir = join(tempDir, 'data', 'users')
      await fs.mkdir(dataDir, { recursive: true })
      await fs.writeFile(join(dataDir, 'temp.tmp'), 'test')

      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['cleanup', '--force'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const result = JSON.parse(output)
      expect(result.success).toBe(true)
      expect(result.filesRemoved).toBeGreaterThan(0)

      // Verify file was removed
      const files = await fs.readdir(dataDir)
      expect(files).not.toContain('temp.tmp')
    })
  })

  describe('trigger subcommand', () => {
    it('should report no namespaces when empty', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['trigger'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const result = JSON.parse(output)
      expect(result.success).toBe(true)
      expect(result.message).toContain('No namespaces')
    })

    it('should create compaction jobs for namespace with files', async () => {
      // Create some data files
      const dataDir = join(tempDir, 'data', 'users')
      await fs.mkdir(dataDir, { recursive: true })
      await fs.writeFile(join(dataDir, 'file1.parquet'), 'test1')
      await fs.writeFile(join(dataDir, 'file2.parquet'), 'test2')

      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['trigger'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const result = JSON.parse(output)
      expect(result.success).toBe(true)
      expect(result.jobsCreated).toBeGreaterThan(0)
      expect(result.jobs).toBeDefined()
      expect(result.jobs[0].namespace).toBe('users')
    })

    it('should trigger compaction for specific namespace', async () => {
      // Create data for two namespaces
      const usersDir = join(tempDir, 'data', 'users')
      const postsDir = join(tempDir, 'data', 'posts')
      await fs.mkdir(usersDir, { recursive: true })
      await fs.mkdir(postsDir, { recursive: true })
      await fs.writeFile(join(usersDir, 'file1.parquet'), 'test1')
      await fs.writeFile(join(postsDir, 'file2.parquet'), 'test2')

      stdoutOutput = []
      stderrOutput = []

      // Trigger for only 'users' namespace
      const args = createArgs('compaction', ['trigger', 'users'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const result = JSON.parse(output)
      expect(result.success).toBe(true)
      expect(result.jobs.length).toBe(1)
      expect(result.jobs[0].namespace).toBe('users')
    })

    it('should skip already compacted files', async () => {
      // Create compacted file (should be skipped)
      const dataDir = join(tempDir, 'data', 'users')
      await fs.mkdir(dataDir, { recursive: true })
      await fs.writeFile(join(dataDir, 'compacted-123.parquet'), 'test')

      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['trigger', 'users'])
      const code = await compactionCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const result = JSON.parse(output)
      expect(result.success).toBe(true)
      expect(result.message).toContain('No files need compaction')
    })
  })

  describe('unknown subcommand', () => {
    it('should error on unknown subcommand', async () => {
      stdoutOutput = []
      stderrOutput = []

      const args = createArgs('compaction', ['unknown'])
      const code = await compactionCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Unknown compaction subcommand')
    })
  })
})
