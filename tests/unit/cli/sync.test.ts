/**
 * Sync Command Tests
 *
 * Tests for the sync, push, and pull CLI commands.
 * These commands handle syncing databases between local filesystem and remote R2 storage.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { ParsedArgs } from '../../../src/cli/types'

// Mock the oauth.do module
vi.mock('oauth.do/node', () => ({
  ensureLoggedIn: vi.fn().mockResolvedValue({
    token: 'mock-token',
    isNewLogin: false,
  }),
  getUser: vi.fn().mockResolvedValue({
    user: { id: 'user-123', email: 'test@example.com', username: 'testuser' },
  }),
}))

// Mock the config loader
vi.mock('../../../src/config/loader', () => ({
  loadConfig: vi.fn().mockResolvedValue({
    defaultNamespace: 'default',
    visibility: 'private',
  }),
}))

// Mock the sync client
vi.mock('../../../src/sync/client', () => ({
  createSyncClient: vi.fn().mockReturnValue({
    registerDatabase: vi.fn().mockResolvedValue({ success: true, databaseId: 'db-123' }),
    getManifest: vi.fn().mockResolvedValue(null),
    lookupDatabase: vi.fn().mockResolvedValue(null),
    getUploadUrls: vi.fn().mockResolvedValue([]),
    getDownloadUrls: vi.fn().mockResolvedValue([]),
    uploadFile: vi.fn().mockResolvedValue(undefined),
    downloadFile: vi.fn().mockResolvedValue(new Uint8Array()),
    updateManifest: vi.fn().mockResolvedValue(undefined),
  }),
}))

// Mock FsBackend
vi.mock('../../../src/storage/FsBackend', () => ({
  FsBackend: vi.fn().mockImplementation(() => ({
    list: vi.fn().mockResolvedValue({ files: [] }),
    stat: vi.fn().mockResolvedValue(null),
    read: vi.fn().mockResolvedValue(new Uint8Array()),
    write: vi.fn().mockResolvedValue(undefined),
    mkdir: vi.fn().mockResolvedValue(undefined),
  })),
}))

describe('Sync Commands', () => {
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

    testDir = join(tmpdir(), `parquedb-sync-test-${Date.now()}`)
    await fs.mkdir(testDir, { recursive: true })
  })

  afterEach(async () => {
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite
    vi.clearAllMocks()

    try {
      await fs.rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  function createParsedArgs(args: string[] = [], options: Partial<ParsedArgs['options']> = {}): ParsedArgs {
    return {
      command: 'sync',
      args,
      options: {
        help: false,
        version: false,
        directory: testDir,
        format: 'json',
        pretty: false,
        quiet: false,
        ...options,
      },
    }
  }

  // ===========================================================================
  // Push Command Tests
  // ===========================================================================

  describe('pushCommand', () => {
    it('should authenticate and push to remote', async () => {
      const { pushCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs([])

      const code = await pushCommand(parsed)

      // Should succeed but with no files to upload
      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Authenticating')
    })

    it('should validate visibility option', async () => {
      const { pushCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs(['--visibility', 'invalid'])

      const code = await pushCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid visibility')
    })

    it('should validate slug option', async () => {
      const { pushCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs(['--slug', 'A_INVALID!slug'])

      const code = await pushCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid slug')
    })

    it('should accept valid visibility values', async () => {
      const { pushCommand } = await import('../../../src/cli/commands/sync')

      for (const visibility of ['public', 'unlisted', 'private']) {
        const parsed = createParsedArgs(['--visibility', visibility])
        const code = await pushCommand(parsed)

        // Should succeed or fail on something other than visibility
        expect(stderrOutput.join('')).not.toContain('Invalid visibility')
      }
    })

    it('should support --dry-run flag', async () => {
      const { pushCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs(['--dry-run'])

      const code = await pushCommand(parsed)

      expect(code).toBe(0)
      // In dry-run mode, nothing should be uploaded
    })
  })

  // ===========================================================================
  // Pull Command Tests
  // ===========================================================================

  describe('pullCommand', () => {
    it('should error when no database reference provided', async () => {
      const { pullCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs([])

      const code = await pullCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Usage: parquedb pull')
    })

    it('should error on invalid database reference format', async () => {
      const { pullCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs(['invalid-format'])

      const code = await pullCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid database reference')
    })

    it('should parse owner/database format', async () => {
      const { pullCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs(['owner/database'])

      const code = await pullCommand(parsed)

      // Will fail because database not found, but format is valid
      expect(code).toBe(1)
      expect(stderrOutput.join('')).not.toContain('Invalid database reference')
    })

    it('should support --dry-run flag', async () => {
      const { pullCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs(['owner/database', '--dry-run'])

      // Will fail at lookup, but dry-run flag should be recognized
      const code = await pullCommand(parsed)

      expect(code).toBe(1) // Fails because db not found
    })
  })

  // ===========================================================================
  // Sync Command Tests
  // ===========================================================================

  describe('syncCommand', () => {
    it('should error without manifest', async () => {
      const { syncCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs([])

      const code = await syncCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No manifest found')
    })

    it('should validate strategy option', async () => {
      const { syncCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs(['--strategy', 'invalid'])

      const code = await syncCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid strategy')
    })

    it('should accept valid strategy values', async () => {
      const { syncCommand } = await import('../../../src/cli/commands/sync')

      for (const strategy of ['local-wins', 'remote-wins', 'newest', 'manual']) {
        vi.clearAllMocks()
        const parsed = createParsedArgs(['--strategy', strategy])
        await syncCommand(parsed)

        // Should not error on strategy validation
        expect(stderrOutput.join('')).not.toContain('Invalid strategy')
      }
    })

    it('should support --status flag', async () => {
      const { syncCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs(['--status'])

      // Even though it fails due to missing manifest, status flag should be recognized
      const code = await syncCommand(parsed)

      expect(code).toBe(1)
    })

    it('should support --dry-run flag', async () => {
      const { syncCommand } = await import('../../../src/cli/commands/sync')
      const parsed = createParsedArgs(['--dry-run'])

      const code = await syncCommand(parsed)

      // Should fail due to no manifest, but dry-run is recognized
      expect(code).toBe(1)
    })
  })
})
