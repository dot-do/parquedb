/**
 * Diff Command Tests
 *
 * Tests for the diff CLI command that shows changes between branches or commits.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { ParsedArgs } from '../../../src/cli/types'

// Mock branch manager
const mockBranchManager = {
  current: vi.fn().mockResolvedValue('main'),
}

// Mock ref manager
const mockRefManager = {
  resolveRef: vi.fn(),
}

// Mock commit loader
const mockLoadCommit = vi.fn()

vi.mock('../../../src/sync/branch-manager', () => ({
  createBranchManager: vi.fn().mockReturnValue(mockBranchManager),
}))

vi.mock('../../../src/sync/refs', () => ({
  createRefManager: vi.fn().mockReturnValue(mockRefManager),
}))

vi.mock('../../../src/sync/commit', () => ({
  loadCommit: (...args: unknown[]) => mockLoadCommit(...args),
}))

vi.mock('../../../src/storage/FsBackend', () => ({
  FsBackend: vi.fn().mockImplementation(() => ({})),
}))

describe('Diff Command', () => {
  let testDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write

  // Sample commit data
  const sampleCommit1 = {
    hash: 'abc123def456789',
    message: 'Initial commit',
    timestamp: Date.now(),
    author: 'test@example.com',
    parents: [],
    state: {
      collections: {
        users: { rowCount: 100, dataHash: 'hash1', schemaHash: 'schema1' },
        posts: { rowCount: 50, dataHash: 'hash2', schemaHash: 'schema2' },
      },
      relationships: { forwardHash: 'fwd1', reverseHash: 'rev1' },
      eventLogPosition: { segmentId: 'seg1', offset: 100 },
    },
  }

  const sampleCommit2 = {
    hash: 'xyz789abc123456',
    message: 'Add comments',
    timestamp: Date.now() + 1000,
    author: 'test@example.com',
    parents: ['abc123def456789'],
    state: {
      collections: {
        users: { rowCount: 120, dataHash: 'hash1-modified', schemaHash: 'schema1' },
        posts: { rowCount: 50, dataHash: 'hash2', schemaHash: 'schema2' },
        comments: { rowCount: 30, dataHash: 'hash3', schemaHash: 'schema3' },
      },
      relationships: { forwardHash: 'fwd2', reverseHash: 'rev2' },
      eventLogPosition: { segmentId: 'seg1', offset: 200 },
    },
  }

  beforeEach(async () => {
    stdoutOutput = []
    stderrOutput = []

    process.stdout.write = vi.fn((chunk: string | Uint8Array) => {
      stdoutOutput.push(chunk.toString())
      return true
    })
    process.stderr.write = vi.fn((chunk: string | Uint8Array) => {
      stderrOutput.push(chunk.toString())
      return true
    })

    testDir = join(tmpdir(), `parquedb-diff-test-${Date.now()}`)
    await fs.mkdir(testDir, { recursive: true })

    // Reset mocks
    vi.clearAllMocks()
    mockBranchManager.current.mockResolvedValue('feature')
    mockRefManager.resolveRef.mockImplementation((ref: string) => {
      if (ref === 'feature') return Promise.resolve('xyz789abc123456')
      if (ref === 'main') return Promise.resolve('abc123def456789')
      return Promise.resolve(null)
    })
    mockLoadCommit.mockImplementation((_storage: unknown, hash: string) => {
      if (hash === 'abc123def456789') return Promise.resolve(sampleCommit1)
      if (hash === 'xyz789abc123456') return Promise.resolve(sampleCommit2)
      return Promise.reject(new Error('Commit not found'))
    })
  })

  afterEach(async () => {
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite

    try {
      await fs.rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  function createParsedArgs(args: string[] = []): ParsedArgs {
    return {
      command: 'diff',
      args,
      options: {
        help: false,
        version: false,
        directory: testDir,
        format: 'json',
        pretty: false,
        quiet: false,
      },
    }
  }

  // ===========================================================================
  // Basic Diff Tests
  // ===========================================================================

  describe('basic diff', () => {
    it('should diff current branch against main by default', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs([])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(mockRefManager.resolveRef).toHaveBeenCalledWith('feature')
      expect(mockRefManager.resolveRef).toHaveBeenCalledWith('main')
      expect(stdoutOutput.join('')).toContain('Comparing feature with main')
    })

    it('should diff against specified target', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['main'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Comparing feature with main')
    })

    it('should show no differences when commits are the same', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      mockRefManager.resolveRef.mockResolvedValue('abc123def456789')

      const parsed = createParsedArgs(['main'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('No differences')
    })

    it('should error when not on a branch', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      mockBranchManager.current.mockResolvedValue(null)

      const parsed = createParsedArgs([])
      const code = await diffCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Cannot diff: not currently on a branch')
    })

    it('should error when target ref not found', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      mockRefManager.resolveRef.mockImplementation((ref: string) => {
        if (ref === 'feature') return Promise.resolve('xyz789abc123456')
        return Promise.resolve(null)
      })

      const parsed = createParsedArgs(['nonexistent'])
      const code = await diffCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Target not found: nonexistent')
    })
  })

  // ===========================================================================
  // Detailed Diff Output Tests
  // ===========================================================================

  describe('detailed diff output', () => {
    it('should show added collections', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['main'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('+ comments')
      expect(stdoutOutput.join('')).toContain('new collection')
    })

    it('should show modified collections', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['main'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('M users')
      expect(stdoutOutput.join('')).toContain('+20 rows')
    })

    it('should show removed collections', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      // Swap commits so the "current" has fewer collections
      mockLoadCommit.mockImplementation((_storage: unknown, hash: string) => {
        if (hash === 'xyz789abc123456') return Promise.resolve(sampleCommit1)
        if (hash === 'abc123def456789') return Promise.resolve(sampleCommit2)
        return Promise.reject(new Error('Commit not found'))
      })

      const parsed = createParsedArgs(['main'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('- comments')
    })

    it('should show relationship changes', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['main'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('M Relationships')
    })
  })

  // ===========================================================================
  // Stat Mode Tests
  // ===========================================================================

  describe('--stat mode', () => {
    it('should show summary statistics', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['--stat'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('collection(s) added')
      expect(stdoutOutput.join('')).toContain('collection(s) modified')
    })

    it('should show relationship status in stat mode', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['--stat'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Relationships modified')
    })
  })

  // ===========================================================================
  // JSON Output Tests
  // ===========================================================================

  describe('--json mode', () => {
    it('should output valid JSON', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['--json'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const json = JSON.parse(output)

      expect(json).toHaveProperty('current')
      expect(json).toHaveProperty('target')
      expect(json).toHaveProperty('collections')
      expect(json).toHaveProperty('relationships')
    })

    it('should include event log position with --events flag', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['--json', '--events'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const json = JSON.parse(output)

      expect(json).toHaveProperty('eventLogPosition')
      expect(json.eventLogPosition).toHaveProperty('current')
      expect(json.eventLogPosition).toHaveProperty('target')
    })

    it('should show collection status in JSON', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['--json'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const json = JSON.parse(output)

      expect(json.collections.comments.status).toBe('added')
      expect(json.collections.users.status).toBe('modified')
    })
  })

  // ===========================================================================
  // Events Flag Tests
  // ===========================================================================

  describe('--events flag', () => {
    it('should show event log position in detailed output', async () => {
      const { diffCommand } = await import('../../../src/cli/commands/diff')

      const parsed = createParsedArgs(['--events'])
      const code = await diffCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Event log position')
      expect(stdoutOutput.join('')).toContain('seg1:100')
      expect(stdoutOutput.join('')).toContain('seg1:200')
    })
  })
})
