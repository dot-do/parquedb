/**
 * Branch Command Tests
 *
 * Tests for the branch CLI command that manages database branches.
 * Similar to git branch functionality.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { ParsedArgs } from '../../../src/cli/types'

// Mock the branch manager
const mockBranchManager = {
  list: vi.fn().mockResolvedValue([]),
  create: vi.fn().mockResolvedValue(undefined),
  delete: vi.fn().mockResolvedValue(undefined),
  rename: vi.fn().mockResolvedValue(undefined),
  current: vi.fn().mockResolvedValue('main'),
  checkout: vi.fn().mockResolvedValue(undefined),
  exists: vi.fn().mockResolvedValue(false),
}

vi.mock('../../../src/sync/branch-manager', () => ({
  createBranchManager: vi.fn().mockReturnValue(mockBranchManager),
}))

vi.mock('../../../src/storage/FsBackend', () => ({
  FsBackend: vi.fn().mockImplementation(() => ({})),
}))

describe('Branch Command', () => {
  let testDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write

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

    testDir = join(tmpdir(), `parquedb-branch-test-${Date.now()}`)
    await fs.mkdir(testDir, { recursive: true })

    // Reset mocks
    vi.clearAllMocks()
    mockBranchManager.list.mockResolvedValue([])
    mockBranchManager.create.mockResolvedValue(undefined)
    mockBranchManager.delete.mockResolvedValue(undefined)
    mockBranchManager.rename.mockResolvedValue(undefined)
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
      command: 'branch',
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
  // List Branches Tests
  // ===========================================================================

  describe('list branches', () => {
    it('should list branches when no arguments provided', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      mockBranchManager.list.mockResolvedValue([
        { name: 'main', commit: 'abc123def456', isCurrent: true, isRemote: false },
        { name: 'feature', commit: 'xyz789', isCurrent: false, isRemote: false },
      ])

      const parsed = createParsedArgs([])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.list).toHaveBeenCalled()
      expect(stdoutOutput.join('')).toContain('main')
      expect(stdoutOutput.join('')).toContain('feature')
    })

    it('should show message when no branches found', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      mockBranchManager.list.mockResolvedValue([])

      const parsed = createParsedArgs([])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('No branches found')
    })

    it('should mark current branch with asterisk', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      mockBranchManager.list.mockResolvedValue([
        { name: 'main', commit: 'abc123', isCurrent: true, isRemote: false },
      ])

      const parsed = createParsedArgs([])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('* main')
    })

    it('should support --all flag to show remote branches', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      mockBranchManager.list.mockResolvedValue([
        { name: 'main', commit: 'abc123', isCurrent: true, isRemote: false },
        { name: 'origin/main', commit: 'abc123', isCurrent: false, isRemote: true },
      ])

      const parsed = createParsedArgs(['-a'])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('main')
      expect(stdoutOutput.join('')).toContain('origin/main')
    })

    it('should support --remote flag to show only remote branches', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      mockBranchManager.list.mockResolvedValue([
        { name: 'main', commit: 'abc123', isCurrent: true, isRemote: false },
        { name: 'origin/main', commit: 'abc123', isCurrent: false, isRemote: true },
      ])

      const parsed = createParsedArgs(['-r'])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('origin/main')
      expect(stdoutOutput.join('')).not.toContain('* main')
    })
  })

  // ===========================================================================
  // Create Branch Tests
  // ===========================================================================

  describe('create branch', () => {
    it('should create a new branch', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      const parsed = createParsedArgs(['feature-branch'])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.create).toHaveBeenCalledWith('feature-branch', undefined)
      expect(stdoutOutput.join('')).toContain("Created branch 'feature-branch'")
    })

    it('should create a branch from a base', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      const parsed = createParsedArgs(['feature-branch', 'develop'])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.create).toHaveBeenCalledWith('feature-branch', { from: 'develop' })
      expect(stdoutOutput.join('')).toContain('from develop')
    })

    it('should handle create error', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      mockBranchManager.create.mockRejectedValue(new Error('Branch already exists'))

      const parsed = createParsedArgs(['existing-branch'])
      const code = await branchCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Branch already exists')
    })
  })

  // ===========================================================================
  // Delete Branch Tests
  // ===========================================================================

  describe('delete branch', () => {
    it('should delete a branch with -d flag', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      const parsed = createParsedArgs(['-d', 'old-branch'])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.delete).toHaveBeenCalledWith('old-branch', { force: false })
      expect(stdoutOutput.join('')).toContain("Deleted branch 'old-branch'")
    })

    it('should delete a branch with --delete flag', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      const parsed = createParsedArgs(['--delete', 'old-branch'])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.delete).toHaveBeenCalledWith('old-branch', { force: false })
    })

    it('should support --force flag for unmerged branches', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      const parsed = createParsedArgs(['-d', 'unmerged-branch', '--force'])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.delete).toHaveBeenCalledWith('unmerged-branch', { force: true })
    })

    it('should suggest --force when deleting unmerged branch fails', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      mockBranchManager.delete.mockRejectedValue(new Error('Branch has unmerged changes'))

      const parsed = createParsedArgs(['-d', 'unmerged-branch'])
      const code = await branchCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('unmerged')
      expect(stdoutOutput.join('')).toContain('--force')
    })
  })

  // ===========================================================================
  // Rename Branch Tests
  // ===========================================================================

  describe('rename branch', () => {
    it('should rename a branch with -m flag', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      const parsed = createParsedArgs(['-m', 'old-name', 'new-name'])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.rename).toHaveBeenCalledWith('old-name', 'new-name')
      expect(stdoutOutput.join('')).toContain("Renamed branch 'old-name' to 'new-name'")
    })

    it('should rename a branch with --move flag', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      const parsed = createParsedArgs(['--move', 'old-name', 'new-name'])
      const code = await branchCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.rename).toHaveBeenCalledWith('old-name', 'new-name')
    })

    it('should error when new name not provided', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      const parsed = createParsedArgs(['-m', 'old-name'])
      const code = await branchCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Usage: parquedb branch -m')
    })

    it('should handle rename error', async () => {
      const { branchCommand } = await import('../../../src/cli/commands/branch')

      mockBranchManager.rename.mockRejectedValue(new Error('Branch not found'))

      const parsed = createParsedArgs(['-m', 'nonexistent', 'new-name'])
      const code = await branchCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Branch not found')
    })
  })
})
