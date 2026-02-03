/**
 * Backup/Restore CLI Command Tests
 *
 * Tests for the backup and restore CLI commands.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { backupCommand } from '../../../src/cli/commands/backup'
import { restoreCommand } from '../../../src/cli/commands/restore'
import type { ParsedArgs } from '../../../src/cli/types'

// =============================================================================
// Test Setup
// =============================================================================

function createParsedArgs(args: string[] = [], options: Partial<ParsedArgs['options']> = {}): ParsedArgs {
  return {
    command: 'backup',
    args,
    options: {
      help: false,
      version: false,
      directory: process.cwd(),
      format: 'json',
      pretty: false,
      quiet: true,
      ...options,
    },
  }
}

describe('Backup Command', () => {
  let testDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write

  beforeEach(async () => {
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

    // Create temp directory
    testDir = join(tmpdir(), `parquedb-backup-test-${Date.now()}`)
    await fs.mkdir(testDir, { recursive: true })
  })

  afterEach(async () => {
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite

    // Cleanup temp directory
    try {
      await fs.rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('argument validation', () => {
    it('should error when neither namespace nor --all is provided', async () => {
      const parsed = createParsedArgs([], { directory: testDir, quiet: false })

      const code = await backupCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Must specify either -n/--namespace')
    })

    it('should parse -n/--namespace option', async () => {
      // Create config file to pass initialization check
      await fs.writeFile(join(testDir, 'parquedb.json'), JSON.stringify({ name: 'test' }))

      const outputFile = join(testDir, 'backup.json')
      const parsed = createParsedArgs(['-n', 'posts', '-o', outputFile], {
        directory: testDir,
        quiet: false,
      })

      // This succeeds with 0 entities because the namespace is empty
      const code = await backupCommand(parsed)

      // Should succeed with 0 entities
      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('0 entities')
    })

    it('should parse --all option', async () => {
      await fs.writeFile(join(testDir, 'parquedb.json'), JSON.stringify({ name: 'test' }))

      const parsed = createParsedArgs(['--all', '-o', './backup.json'], {
        directory: testDir,
        quiet: false,
      })

      const code = await backupCommand(parsed)

      // Should fail because no namespaces found
      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No namespaces found to backup')
    })
  })

  describe('database check', () => {
    it('should error when database is not initialized', async () => {
      const parsed = createParsedArgs(['--all', '-o', './backup.json'], {
        directory: testDir,
        quiet: false,
      })

      const code = await backupCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('ParqueDB is not initialized')
    })
  })

  describe('file path validation', () => {
    it('should generate default filename when output not specified', async () => {
      await fs.writeFile(join(testDir, 'parquedb.json'), JSON.stringify({ name: 'test' }))

      // Use -a but no -o, should use default filename
      const parsed = createParsedArgs(['-a'], {
        directory: testDir,
        quiet: false,
      })

      const code = await backupCommand(parsed)

      // Will fail for "no namespaces" but proves we got past arg parsing
      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No namespaces found')
    })
  })
})

describe('Restore Command', () => {
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

    testDir = join(tmpdir(), `parquedb-restore-test-${Date.now()}`)
    await fs.mkdir(testDir, { recursive: true })
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

  describe('argument validation', () => {
    it('should error when no file path is provided', async () => {
      const parsed: ParsedArgs = {
        command: 'restore',
        args: [],
        options: {
          help: false,
          version: false,
          directory: testDir,
          format: 'json',
          pretty: false,
          quiet: false,
        },
      }

      const code = await restoreCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing backup file path')
    })
  })

  describe('file validation', () => {
    it('should error when backup file does not exist', async () => {
      await fs.writeFile(join(testDir, 'parquedb.json'), JSON.stringify({ name: 'test' }))

      const parsed: ParsedArgs = {
        command: 'restore',
        args: ['nonexistent.json'],
        options: {
          help: false,
          version: false,
          directory: testDir,
          format: 'json',
          pretty: false,
          quiet: false,
        },
      }

      const code = await restoreCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Backup file not found')
    })
  })

  describe('database check', () => {
    it('should error when database is not initialized', async () => {
      // Create backup file
      const backupFile = join(testDir, 'backup.json')
      await fs.writeFile(backupFile, JSON.stringify({
        _metadata: { version: '1.0', createdAt: new Date().toISOString(), namespaces: [], entityCounts: {}, format: 'json' },
        data: {},
      }))

      const parsed: ParsedArgs = {
        command: 'restore',
        args: [backupFile],
        options: {
          help: false,
          version: false,
          directory: testDir,
          format: 'json',
          pretty: false,
          quiet: false,
        },
      }

      const code = await restoreCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('ParqueDB is not initialized')
    })
  })

  describe('backup format validation', () => {
    it('should error on invalid JSON backup file', async () => {
      await fs.writeFile(join(testDir, 'parquedb.json'), JSON.stringify({ name: 'test' }))

      // Create invalid backup file
      const backupFile = join(testDir, 'backup.json')
      await fs.writeFile(backupFile, 'not valid json')

      const parsed: ParsedArgs = {
        command: 'restore',
        args: [backupFile],
        options: {
          help: false,
          version: false,
          directory: testDir,
          format: 'json',
          pretty: false,
          quiet: false,
        },
      }

      const code = await restoreCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid JSON backup file')
    })

    it('should error on backup file without metadata', async () => {
      await fs.writeFile(join(testDir, 'parquedb.json'), JSON.stringify({ name: 'test' }))

      // Create backup file without metadata
      const backupFile = join(testDir, 'backup.json')
      await fs.writeFile(backupFile, JSON.stringify({ data: {} }))

      const parsed: ParsedArgs = {
        command: 'restore',
        args: [backupFile],
        options: {
          help: false,
          version: false,
          directory: testDir,
          format: 'json',
          pretty: false,
          quiet: false,
        },
      }

      const code = await restoreCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('missing _metadata')
    })
  })

  describe('dry run mode', () => {
    it('should parse --dry-run option', async () => {
      await fs.writeFile(join(testDir, 'parquedb.json'), JSON.stringify({ name: 'test' }))

      // Create valid backup file with no data
      const backupFile = join(testDir, 'backup.json')
      await fs.writeFile(backupFile, JSON.stringify({
        _metadata: {
          version: '1.0',
          createdAt: new Date().toISOString(),
          namespaces: [],
          entityCounts: {},
          format: 'json',
        },
        data: {},
      }))

      const parsed: ParsedArgs = {
        command: 'restore',
        args: [backupFile, '--dry-run'],
        options: {
          help: false,
          version: false,
          directory: testDir,
          format: 'json',
          pretty: false,
          quiet: false,
        },
      }

      const code = await restoreCommand(parsed)

      // Should succeed with 0 entities
      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('DRY RUN')
    })
  })

  describe('namespace filtering', () => {
    it('should parse -n/--namespace option for filtering', async () => {
      await fs.writeFile(join(testDir, 'parquedb.json'), JSON.stringify({ name: 'test' }))

      // Create valid backup file
      const backupFile = join(testDir, 'backup.json')
      await fs.writeFile(backupFile, JSON.stringify({
        _metadata: {
          version: '1.0',
          createdAt: new Date().toISOString(),
          namespaces: ['posts', 'users'],
          entityCounts: { posts: 0, users: 0 },
          format: 'json',
        },
        data: {
          posts: [],
          users: [],
        },
      }))

      const parsed: ParsedArgs = {
        command: 'restore',
        args: [backupFile, '-n', 'posts', '--dry-run'],
        options: {
          help: false,
          version: false,
          directory: testDir,
          format: 'json',
          pretty: false,
          quiet: false,
        },
      }

      const code = await restoreCommand(parsed)

      expect(code).toBe(0)
    })
  })
})

describe('Command Registration', () => {
  it('backup and restore commands should be importable', async () => {
    // Just verify the modules can be imported and export their functions
    const { backupCommand } = await import('../../../src/cli/commands/backup')
    const { restoreCommand } = await import('../../../src/cli/commands/restore')

    expect(backupCommand).toBeDefined()
    expect(typeof backupCommand).toBe('function')
    expect(restoreCommand).toBeDefined()
    expect(typeof restoreCommand).toBe('function')
  })
})
