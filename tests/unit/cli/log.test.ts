/**
 * Log Command Tests
 *
 * Tests for the log CLI command that shows commit history for a branch.
 * Similar to git log functionality.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { ParsedArgs } from '../../../src/cli/types'

// Mock ref manager
const mockRefManager = {
  resolveRef: vi.fn(),
}

// Mock commit loader
const mockLoadCommit = vi.fn()

vi.mock('../../../src/sync/refs', () => ({
  createRefManager: vi.fn().mockReturnValue(mockRefManager),
}))

vi.mock('../../../src/sync/commit', () => ({
  loadCommit: (...args: unknown[]) => mockLoadCommit(...args),
}))

vi.mock('../../../src/storage/FsBackend', () => ({
  FsBackend: vi.fn().mockImplementation(() => ({})),
}))

describe('Log Command', () => {
  let testDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write

  // Sample commit data
  const commit3 = {
    hash: 'commit3hash123456',
    message: 'Third commit\n\nWith multi-line description',
    timestamp: Date.now(),
    author: 'alice@example.com',
    parents: ['commit2hash123456'],
    state: {},
  }

  const commit2 = {
    hash: 'commit2hash123456',
    message: 'Second commit',
    timestamp: Date.now() - 86400000, // 1 day ago
    author: 'bob@example.com',
    parents: ['commit1hash123456'],
    state: {},
  }

  const commit1 = {
    hash: 'commit1hash123456',
    message: 'Initial commit',
    timestamp: Date.now() - 604800000, // 1 week ago
    author: 'alice@example.com',
    parents: [],
    state: {},
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

    testDir = join(tmpdir(), `parquedb-log-test-${Date.now()}`)
    await fs.mkdir(testDir, { recursive: true })

    // Reset mocks
    vi.clearAllMocks()
    mockRefManager.resolveRef.mockImplementation((ref: string) => {
      if (ref === 'HEAD') return Promise.resolve('commit3hash123456')
      if (ref === 'main') return Promise.resolve('commit3hash123456')
      if (ref === 'feature') return Promise.resolve('commit2hash123456')
      return Promise.resolve(null)
    })
    mockLoadCommit.mockImplementation((_storage: unknown, hash: string) => {
      if (hash === 'commit3hash123456') return Promise.resolve(commit3)
      if (hash === 'commit2hash123456') return Promise.resolve(commit2)
      if (hash === 'commit1hash123456') return Promise.resolve(commit1)
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
      command: 'log',
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
  // Basic Log Tests
  // ===========================================================================

  describe('basic log', () => {
    it('should show commit history for current branch', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      expect(mockRefManager.resolveRef).toHaveBeenCalledWith('HEAD')
      expect(stdoutOutput.join('')).toContain('commit commit3hash123456')
      expect(stdoutOutput.join('')).toContain('Third commit')
    })

    it('should show commit history for specified branch', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs(['feature'])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      expect(mockRefManager.resolveRef).toHaveBeenCalledWith('feature')
      expect(stdoutOutput.join('')).toContain('commit commit2hash123456')
    })

    it('should error when no commits exist', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      mockRefManager.resolveRef.mockResolvedValue(null)

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No commits yet')
      expect(stdoutOutput.join('')).toContain('parquedb commit')
    })

    it('should error when branch not found', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      mockRefManager.resolveRef.mockResolvedValue(null)

      const parsed = createParsedArgs(['nonexistent'])
      const code = await logCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Branch not found: nonexistent')
    })

    it('should traverse commit history', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      // Should show all 3 commits in history
      expect(mockLoadCommit).toHaveBeenCalledTimes(3)
      expect(stdoutOutput.join('')).toContain('Third commit')
      expect(stdoutOutput.join('')).toContain('Second commit')
      expect(stdoutOutput.join('')).toContain('Initial commit')
    })
  })

  // ===========================================================================
  // Full Format Tests
  // ===========================================================================

  describe('full format', () => {
    it('should show full commit hash', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('commit commit3hash123456')
    })

    it('should show author', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Author: alice@example.com')
    })

    it('should show date with relative time', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Date:')
      // Should contain relative time indicator
      expect(stdoutOutput.join('')).toMatch(/\(.*ago\)/)
    })

    it('should show parent commits', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Parents: commit2h')
    })

    it('should indent commit message', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      // Commit message should be indented
      expect(stdoutOutput.join('')).toContain('    Third commit')
    })

    it('should show multi-line commit messages', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('    With multi-line description')
    })
  })

  // ===========================================================================
  // Oneline Format Tests
  // ===========================================================================

  describe('--oneline format', () => {
    it('should show compact output', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs(['--oneline'])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      // Should show short hash and first line of message
      expect(stdoutOutput.join('')).toContain('commit3h Third commit')
      expect(stdoutOutput.join('')).not.toContain('Author:')
      expect(stdoutOutput.join('')).not.toContain('Date:')
    })

    it('should truncate long commit messages', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const longCommit = {
        ...commit3,
        message: 'This is a very long commit message that should be truncated in oneline mode to fit nicely',
      }
      mockLoadCommit.mockImplementation((_storage: unknown, hash: string) => {
        if (hash === 'commit3hash123456') return Promise.resolve(longCommit)
        if (hash === 'commit2hash123456') return Promise.resolve(commit2)
        if (hash === 'commit1hash123456') return Promise.resolve(commit1)
        return Promise.reject(new Error('Commit not found'))
      })

      const parsed = createParsedArgs(['--oneline'])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      // Should contain "..." for truncated message
      expect(stdoutOutput.join('')).toContain('...')
    })
  })

  // ===========================================================================
  // Max Count Tests
  // ===========================================================================

  describe('-n / --max-count option', () => {
    it('should limit number of commits with -n', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs(['-n', '2'])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      // Should only load 2 commits
      expect(mockLoadCommit).toHaveBeenCalledTimes(2)
      expect(stdoutOutput.join('')).toContain('Third commit')
      expect(stdoutOutput.join('')).toContain('Second commit')
      expect(stdoutOutput.join('')).not.toContain('Initial commit')
    })

    it('should limit number of commits with --max-count', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs(['--max-count', '1'])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      expect(mockLoadCommit).toHaveBeenCalledTimes(1)
    })

    it('should error on invalid max count', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs(['-n', 'abc'])
      const code = await logCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid max count: abc')
    })

    it('should error on zero max count', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs(['-n', '0'])
      const code = await logCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid max count')
    })
  })

  // ===========================================================================
  // Graph Flag Tests
  // ===========================================================================

  describe('--graph flag', () => {
    it('should recognize --graph flag with warning', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      const parsed = createParsedArgs(['--graph'])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      // Should warn that graph is not implemented and fall back to oneline
      expect(stdoutOutput.join('')).toContain('--graph flag is recognized but not yet implemented')
    })
  })

  // ===========================================================================
  // Empty Repository Tests
  // ===========================================================================

  describe('empty repository', () => {
    it('should show message when no commits found', async () => {
      const { logCommand } = await import('../../../src/cli/commands/log')

      mockRefManager.resolveRef.mockResolvedValue('somehash')
      mockLoadCommit.mockRejectedValue(new Error('Not found'))

      const parsed = createParsedArgs([])
      const code = await logCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('No commits found')
    })
  })
})
