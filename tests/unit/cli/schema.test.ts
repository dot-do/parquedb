/**
 * Schema Command Tests
 *
 * Tests for the schema CLI command that manages and inspects database schemas.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { ParsedArgs } from '../../../src/cli/types'

// Mock the config loader
const mockConfig = {
  schema: {
    users: {
      fields: {
        name: { type: 'string', required: true },
        email: { type: 'string', required: true, unique: true },
      },
    },
  },
  defaultNamespace: 'default',
}

vi.mock('../../../src/config/loader', () => ({
  loadConfig: vi.fn().mockResolvedValue(mockConfig),
}))

// Mock schema functions
const mockCapturedSchema = {
  hash: 'schema-hash-123',
  capturedAt: new Date().toISOString(),
  commitHash: null,
  collections: {
    users: {
      hash: 'users-hash-123',
      version: 1,
      fields: [
        { name: 'name', type: 'string', required: true, indexed: false, unique: false, array: false },
        { name: 'email', type: 'string', required: true, indexed: false, unique: true, array: false },
      ],
      options: {},
    },
  },
}

const mockSchemaAtCommit = {
  hash: 'old-schema-hash',
  capturedAt: new Date(Date.now() - 86400000).toISOString(),
  commitHash: 'abc123',
  collections: {
    users: {
      hash: 'old-users-hash',
      version: 1,
      fields: [
        { name: 'name', type: 'string', required: true, indexed: false, unique: false, array: false },
      ],
      options: {},
    },
  },
}

const mockSchemaDiff = {
  changes: [
    { description: 'Added field email to users', breaking: false },
  ],
  breakingChanges: [],
  compatible: true,
  summary: '1 change(s) detected',
}

vi.mock('../../../src/sync/schema-snapshot', () => ({
  captureSchema: vi.fn().mockResolvedValue(mockCapturedSchema),
  loadSchemaAtCommit: vi.fn().mockResolvedValue(mockSchemaAtCommit),
  diffSchemas: vi.fn().mockReturnValue(mockSchemaDiff),
}))

vi.mock('../../../src/sync/schema-evolution', () => ({
  detectBreakingChanges: vi.fn().mockReturnValue([]),
  generateMigrationHints: vi.fn().mockReturnValue([]),
}))

// Mock ref manager
const mockRefManager = {
  resolveRef: vi.fn().mockResolvedValue('abc123def456'),
}

vi.mock('../../../src/sync/refs', () => ({
  RefManager: vi.fn().mockImplementation(() => mockRefManager),
}))

vi.mock('../../../src/storage/FsBackend', () => ({
  FsBackend: vi.fn().mockImplementation(() => ({})),
}))

describe('Schema Command', () => {
  let testDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write
  const originalArgv = process.argv

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

    testDir = join(tmpdir(), `parquedb-schema-test-${Date.now()}`)
    await fs.mkdir(testDir, { recursive: true })

    // Reset mocks and process.argv
    vi.clearAllMocks()
    process.argv = ['node', 'parquedb', 'schema']
  })

  afterEach(async () => {
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite
    process.argv = originalArgv

    try {
      await fs.rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  function createParsedArgs(args: string[] = []): ParsedArgs {
    return {
      command: 'schema',
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
  // Help Tests
  // ===========================================================================

  describe('help', () => {
    it('should show help when no subcommand provided', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      const parsed = createParsedArgs([])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('ParqueDB Schema Command')
      expect(stdoutOutput.join('')).toContain('USAGE:')
    })

    it('should show help with unknown subcommand', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      const parsed = createParsedArgs(['unknown'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('ParqueDB Schema Command')
    })
  })

  // ===========================================================================
  // Show Subcommand Tests
  // ===========================================================================

  describe('show subcommand', () => {
    it('should show current schema', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { captureSchema } = await import('../../../src/sync/schema-snapshot')

      const parsed = createParsedArgs(['show'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(captureSchema).toHaveBeenCalled()
      expect(stdoutOutput.join('')).toContain('Schema: current')
      expect(stdoutOutput.join('')).toContain('Hash: schema-hash-123')
      expect(stdoutOutput.join('')).toContain('users')
    })

    it('should show collection fields', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      const parsed = createParsedArgs(['show'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('name: string')
      expect(stdoutOutput.join('')).toContain('email: string')
      expect(stdoutOutput.join('')).toContain('required')
      expect(stdoutOutput.join('')).toContain('unique')
    })

    it('should show schema at specific ref with --at', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { loadSchemaAtCommit } = await import('../../../src/sync/schema-snapshot')

      process.argv = ['node', 'parquedb', 'schema', 'show', '--at', 'v1.0.0']

      const parsed = createParsedArgs(['show'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(loadSchemaAtCommit).toHaveBeenCalled()
      expect(stdoutOutput.join('')).toContain('v1.0.0')
    })

    it('should error when ref not found', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      mockRefManager.resolveRef.mockResolvedValue(null)
      process.argv = ['node', 'parquedb', 'schema', 'show', '--at', 'nonexistent']

      const parsed = createParsedArgs(['show'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Reference not found: nonexistent')
    })

    it('should output JSON with --json flag', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      process.argv = ['node', 'parquedb', 'schema', 'show', '--json']

      const parsed = createParsedArgs(['show'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      const json = JSON.parse(output)
      expect(json).toHaveProperty('hash')
      expect(json).toHaveProperty('collections')
    })

    it('should error when no config found', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { loadConfig } = await import('../../../src/config/loader')

      vi.mocked(loadConfig).mockResolvedValue(null)

      const parsed = createParsedArgs(['show'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No parquedb.config.ts found')
    })

    it('should error when no schema in config', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { loadConfig } = await import('../../../src/config/loader')

      vi.mocked(loadConfig).mockResolvedValue({ defaultNamespace: 'test' })

      const parsed = createParsedArgs(['show'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No schema defined')
    })
  })

  // ===========================================================================
  // Diff Subcommand Tests
  // ===========================================================================

  describe('diff subcommand', () => {
    beforeEach(() => {
      mockRefManager.resolveRef.mockResolvedValue('abc123def456')
    })

    it('should error when from ref not provided', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      const parsed = createParsedArgs(['diff'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Missing required argument: <from>')
    })

    it('should diff two refs', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { diffSchemas } = await import('../../../src/sync/schema-snapshot')

      const parsed = createParsedArgs(['diff', 'main', 'feature'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(diffSchemas).toHaveBeenCalled()
      expect(stdoutOutput.join('')).toContain('Schema diff: main..feature')
    })

    it('should default to HEAD as target ref', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      const parsed = createParsedArgs(['diff', 'main'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('main..HEAD')
    })

    it('should error when from ref not found', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      mockRefManager.resolveRef.mockImplementation((ref: string) => {
        if (ref === 'main') return Promise.resolve(null)
        return Promise.resolve('abc123')
      })

      const parsed = createParsedArgs(['diff', 'main', 'feature'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Reference not found: main')
    })

    it('should error when to ref not found', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      mockRefManager.resolveRef.mockImplementation((ref: string) => {
        if (ref === 'feature') return Promise.resolve(null)
        return Promise.resolve('abc123')
      })

      const parsed = createParsedArgs(['diff', 'main', 'feature'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Reference not found: feature')
    })

    it('should show no changes message when schemas are equal', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { diffSchemas } = await import('../../../src/sync/schema-snapshot')

      vi.mocked(diffSchemas).mockReturnValue({
        changes: [],
        breakingChanges: [],
        compatible: true,
        summary: 'No changes',
      })

      const parsed = createParsedArgs(['diff', 'main', 'feature'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('No schema changes')
    })

    it('should return exit code 1 when breaking changes detected', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { diffSchemas } = await import('../../../src/sync/schema-snapshot')

      vi.mocked(diffSchemas).mockReturnValue({
        changes: [
          { description: 'Removed field name from users', breaking: true },
        ],
        breakingChanges: [
          { description: 'Removed field name from users', breaking: true },
        ],
        compatible: false,
        summary: '1 breaking change(s)',
      })

      const parsed = createParsedArgs(['diff', 'main', 'feature'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
    })

    it('should show only breaking changes with --breaking-only', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { diffSchemas } = await import('../../../src/sync/schema-snapshot')

      vi.mocked(diffSchemas).mockReturnValue({
        changes: [
          { description: 'Added field age to users', breaking: false },
          { description: 'Removed field name from users', breaking: true },
        ],
        breakingChanges: [
          { description: 'Removed field name from users', breaking: true },
        ],
        compatible: false,
        summary: '2 change(s)',
      })

      process.argv = ['node', 'parquedb', 'schema', 'diff', 'main', 'feature', '--breaking-only']

      const parsed = createParsedArgs(['diff', 'main', 'feature'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
    })
  })

  // ===========================================================================
  // Check Subcommand Tests
  // ===========================================================================

  describe('check subcommand', () => {
    beforeEach(async () => {
      mockRefManager.resolveRef.mockResolvedValue('abc123def456')
      const { loadConfig } = await import('../../../src/config/loader')
      vi.mocked(loadConfig).mockResolvedValue(mockConfig)
    })

    it('should check schema compatibility', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { diffSchemas } = await import('../../../src/sync/schema-snapshot')

      vi.mocked(diffSchemas).mockReturnValue({
        changes: [
          { description: 'Added field age to users', breaking: false },
        ],
        breakingChanges: [],
        compatible: true,
        summary: '1 change(s)',
      })

      const parsed = createParsedArgs(['check'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Schema compatibility check')
      expect(stdoutOutput.join('')).toContain('compatible')
    })

    it('should report no changes when schemas match', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { diffSchemas } = await import('../../../src/sync/schema-snapshot')

      vi.mocked(diffSchemas).mockReturnValue({
        changes: [],
        breakingChanges: [],
        compatible: true,
        summary: 'No changes',
      })

      const parsed = createParsedArgs(['check'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('No schema changes detected')
    })

    it('should report breaking changes', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { diffSchemas } = await import('../../../src/sync/schema-snapshot')

      vi.mocked(diffSchemas).mockReturnValue({
        changes: [
          { description: 'Removed field name from users', breaking: true },
        ],
        breakingChanges: [
          { description: 'Removed field name from users', breaking: true },
        ],
        compatible: false,
        summary: '1 breaking change(s)',
      })

      const parsed = createParsedArgs(['check'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('breaking changes')
    })

    it('should error when HEAD not found', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')

      mockRefManager.resolveRef.mockResolvedValue(null)

      const parsed = createParsedArgs(['check'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No HEAD commit found')
    })

    it('should error when no schema in config', async () => {
      const { schemaCommand } = await import('../../../src/cli/commands/schema')
      const { loadConfig } = await import('../../../src/config/loader')

      vi.mocked(loadConfig).mockResolvedValue({ defaultNamespace: 'test' })

      const parsed = createParsedArgs(['check'])
      const code = await schemaCommand(parsed)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No schema in current config')
    })
  })
})
