/**
 * Checkout Command Tests
 *
 * Tests for the checkout CLI command that switches branches
 * or restores database state to a specific commit.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { ParsedArgs } from '../../../src/cli/types'

// Mock the branch manager
const mockBranchManager = {
  checkout: vi.fn().mockResolvedValue(undefined),
  exists: vi.fn().mockResolvedValue(false),
  current: vi.fn().mockResolvedValue('main'),
}

vi.mock('../../../src/sync/branch-manager', () => ({
  createBranchManager: vi.fn().mockReturnValue(mockBranchManager),
}))

vi.mock('../../../src/storage/FsBackend', () => ({
  FsBackend: vi.fn().mockImplementation(() => ({})),
}))

// Mock execSync for git branch detection
vi.mock('child_process', () => ({
  execSync: vi.fn().mockReturnValue('main\n'),
}))

describe('Checkout Command', () => {
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

    testDir = join(tmpdir(), `parquedb-checkout-test-${Date.now()}`)
    await fs.mkdir(testDir, { recursive: true })

    // Reset mocks
    vi.clearAllMocks()
    mockBranchManager.checkout.mockResolvedValue(undefined)
    mockBranchManager.exists.mockResolvedValue(false)
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
      command: 'checkout',
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
  // Basic Checkout Tests
  // ===========================================================================

  describe('basic checkout', () => {
    it('should show usage when no arguments provided', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')

      const parsed = createParsedArgs([])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Usage: parquedb checkout')
    })

    it('should checkout an existing branch', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')

      const parsed = createParsedArgs(['feature-branch'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.checkout).toHaveBeenCalledWith('feature-branch', { force: false })
      expect(stdoutOutput.join('')).toContain("Switched to branch 'feature-branch'")
    })

    it('should handle checkout error for non-existent branch', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')

      mockBranchManager.checkout.mockRejectedValue(new Error('Branch not found'))

      const parsed = createParsedArgs(['nonexistent'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('not found')
      expect(stdoutOutput.join('')).toContain('-b nonexistent')
    })

    it('should suggest commit when uncommitted changes present', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')

      mockBranchManager.checkout.mockRejectedValue(new Error('You have uncommitted changes'))

      const parsed = createParsedArgs(['other-branch'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('uncommitted changes')
      expect(stdoutOutput.join('')).toContain('parquedb commit')
    })
  })

  // ===========================================================================
  // Force Checkout Tests
  // ===========================================================================

  describe('force checkout', () => {
    it('should support -f flag', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')

      const parsed = createParsedArgs(['-f', 'feature-branch'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.checkout).toHaveBeenCalledWith('feature-branch', { force: true })
    })

    it('should support --force flag', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')

      const parsed = createParsedArgs(['--force', 'feature-branch'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.checkout).toHaveBeenCalledWith('feature-branch', { force: true })
    })
  })

  // ===========================================================================
  // Create and Checkout Tests
  // ===========================================================================

  describe('create and checkout (-b)', () => {
    it('should create and checkout a new branch with -b', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')

      const parsed = createParsedArgs(['-b', 'new-branch'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.checkout).toHaveBeenCalledWith('new-branch', { create: true })
      expect(stdoutOutput.join('')).toContain("Created and switched to branch 'new-branch'")
    })

    it('should create and checkout a new branch with --create', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')

      const parsed = createParsedArgs(['--create', 'new-branch'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.checkout).toHaveBeenCalledWith('new-branch', { create: true })
    })

    it('should handle create error when branch already exists', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')

      mockBranchManager.checkout.mockRejectedValue(new Error('Branch already exists'))

      const parsed = createParsedArgs(['-b', 'existing-branch'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Branch already exists')
    })
  })

  // ===========================================================================
  // From Git Tests
  // ===========================================================================

  describe('checkout from git (--from-git)', () => {
    it('should sync to current git branch when branch exists', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')
      const { execSync } = await import('child_process')

      vi.mocked(execSync).mockReturnValue('feature/my-feature\n')
      mockBranchManager.exists.mockResolvedValue(true)

      const parsed = createParsedArgs(['--from-git'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.exists).toHaveBeenCalledWith('feature/my-feature')
      expect(mockBranchManager.checkout).toHaveBeenCalledWith('feature/my-feature', { force: false })
      expect(stdoutOutput.join('')).toContain("Switched to existing branch 'feature/my-feature'")
    })

    it('should create branch when it does not exist', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')
      const { execSync } = await import('child_process')

      vi.mocked(execSync).mockReturnValue('new-git-branch\n')
      mockBranchManager.exists.mockResolvedValue(false)

      const parsed = createParsedArgs(['--from-git'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.checkout).toHaveBeenCalledWith('new-git-branch', { create: true, force: false })
      expect(stdoutOutput.join('')).toContain("Created and switched to branch 'new-git-branch'")
    })

    it('should error when in detached HEAD state', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')
      const { execSync } = await import('child_process')

      vi.mocked(execSync).mockReturnValue('HEAD\n')

      const parsed = createParsedArgs(['--from-git'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Not in a git repository or in detached HEAD state')
    })

    it('should error when not in a git repository', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')
      const { execSync } = await import('child_process')

      vi.mocked(execSync).mockImplementation(() => {
        throw new Error('Not a git repository')
      })

      const parsed = createParsedArgs(['--from-git'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Not in a git repository')
    })

    it('should support --from-git with -f for force checkout', async () => {
      const { checkoutCommand } = await import('../../../src/cli/commands/checkout')
      const { execSync } = await import('child_process')

      vi.mocked(execSync).mockReturnValue('main\n')
      mockBranchManager.exists.mockResolvedValue(true)

      const parsed = createParsedArgs(['--from-git', '-f'])
      const code = await checkoutCommand(parsed)

      expect(code).toBe(0)
      expect(mockBranchManager.checkout).toHaveBeenCalledWith('main', { force: true })
    })
  })
})
