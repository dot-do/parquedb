/**
 * Init Command Tests
 *
 * Tests for the init command that initializes a ParqueDB database.
 * Includes tests for both non-interactive and interactive wizard modes.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { Readable, Writable } from 'node:stream'
import {
  initCommand,
  parseInitArgs,
  validateDbName,
  validateNamespace,
  validateNamespaces,
  getDefaultDbName,
  runInitWizard,
} from '../../../src/cli/commands/init'
import type { ParsedArgs } from '../../../src/cli/index'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a mock IO for testing prompts
 */
function createMockIO(inputs: string[]): {
  input: Readable
  output: Writable
  getOutput: () => string
} {
  const inputQueue = [...inputs]
  let outputBuffer = ''

  const input = new Readable({
    read() {
      if (inputQueue.length > 0) {
        // Small delay to simulate user typing
        setImmediate(() => {
          this.push(inputQueue.shift() + '\n')
        })
      }
    },
  })

  const output = new Writable({
    write(chunk, _encoding, callback) {
      outputBuffer += chunk.toString()
      callback()
    },
  })

  return {
    input,
    output,
    getOutput: () => outputBuffer,
  }
}

// =============================================================================
// Basic Init Tests
// =============================================================================

describe('init command', () => {
  let tempDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write

  beforeEach(async () => {
    // Create a unique temp directory
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-cli-test-'))

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
  function createArgs(overrides: Partial<ParsedArgs> = {}): ParsedArgs {
    return {
      command: 'init',
      args: [],
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
      },
      ...overrides,
    }
  }

  it('should create config file and directories', async () => {
    const args = createArgs()
    const code = await initCommand(args)

    expect(code).toBe(0)

    // Check config file exists
    const configPath = join(tempDir, 'parquedb.json')
    const configExists = await fs
      .access(configPath)
      .then(() => true)
      .catch(() => false)
    expect(configExists).toBe(true)

    // Check config content
    const config = JSON.parse(await fs.readFile(configPath, 'utf-8'))
    expect(config.version).toBe('1.0')
    expect(config.storage.type).toBe('fs')
    expect(config.storage.dataDir).toBe('data')
    expect(config.storage.eventsDir).toBe('events')
    expect(config.createdAt).toBeDefined()

    // Check directories exist
    const dataExists = await fs
      .stat(join(tempDir, 'data'))
      .then((s) => s.isDirectory())
      .catch(() => false)
    const eventsExists = await fs
      .stat(join(tempDir, 'events'))
      .then((s) => s.isDirectory())
      .catch(() => false)
    expect(dataExists).toBe(true)
    expect(eventsExists).toBe(true)
  })

  it('should use directory from args', async () => {
    const subDir = join(tempDir, 'mydb')
    const args = createArgs({ args: [subDir] })

    const code = await initCommand(args)

    expect(code).toBe(0)

    // Check config file exists in subdirectory
    const configPath = join(subDir, 'parquedb.json')
    const configExists = await fs
      .access(configPath)
      .then(() => true)
      .catch(() => false)
    expect(configExists).toBe(true)
  })

  it('should fail if already initialized', async () => {
    // First init
    const args = createArgs()
    await initCommand(args)

    // Second init should fail
    const code = await initCommand(args)

    expect(code).toBe(1)
    expect(stderrOutput.join('')).toContain('already initialized')
  })

  it('should print success message', async () => {
    const args = createArgs()
    await initCommand(args)

    const output = stdoutOutput.join('')
    expect(output).toContain('Initialized ParqueDB database')
    expect(output).toContain('parquedb.json')
    expect(output).toContain('data/')
    expect(output).toContain('events/')
    expect(output).toContain('Next steps')
  })

  it('should not print output in quiet mode', async () => {
    const args = createArgs({
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: true,
      },
    })

    await initCommand(args)

    // Should not have verbose output
    expect(stdoutOutput.join('')).not.toContain('Next steps')
  })

  it('should set database name from directory', async () => {
    const subDir = join(tempDir, 'my-database')
    const args = createArgs({ args: [subDir] })

    await initCommand(args)

    const config = JSON.parse(await fs.readFile(join(subDir, 'parquedb.json'), 'utf-8'))
    expect(config.name).toBe('my-database')
  })
})

// =============================================================================
// Argument Parsing Tests
// =============================================================================

describe('parseInitArgs', () => {
  function createParsedArgs(args: string[], directory = '/test'): ParsedArgs {
    return {
      command: 'init',
      args,
      options: {
        help: false,
        version: false,
        directory,
        format: 'json',
        pretty: false,
        quiet: false,
      },
    }
  }

  it('should parse --interactive flag', () => {
    const result = parseInitArgs(createParsedArgs(['--interactive']))
    expect(result.interactive).toBe(true)
  })

  it('should parse -i flag', () => {
    const result = parseInitArgs(createParsedArgs(['-i']))
    expect(result.interactive).toBe(true)
  })

  it('should parse --name option', () => {
    const result = parseInitArgs(createParsedArgs(['--name', 'mydb']))
    expect(result.name).toBe('mydb')
  })

  it('should parse --storage option', () => {
    const result = parseInitArgs(createParsedArgs(['--storage', 'r2']))
    expect(result.storageType).toBe('r2')
  })

  it('should parse --namespace option', () => {
    const result = parseInitArgs(createParsedArgs(['--namespace', 'users', '--ns', 'posts']))
    expect(result.namespaces).toEqual(['users', 'posts'])
  })

  it('should parse directory as positional argument', () => {
    const result = parseInitArgs(createParsedArgs(['/path/to/db']))
    expect(result.targetDir).toBe('/path/to/db')
  })

  it('should parse combined options', () => {
    const result = parseInitArgs(createParsedArgs(['-i', '--name', 'testdb', '-s', 'memory', '/my/dir']))
    expect(result.interactive).toBe(true)
    expect(result.name).toBe('testdb')
    expect(result.storageType).toBe('memory')
    expect(result.targetDir).toBe('/my/dir')
  })
})

// =============================================================================
// Validation Tests
// =============================================================================

describe('validation functions', () => {
  describe('validateDbName', () => {
    it('should accept valid names', () => {
      expect(validateDbName('mydb')).toBe(true)
      expect(validateDbName('MyDatabase')).toBe(true)
      expect(validateDbName('my-db')).toBe(true)
      expect(validateDbName('my_db')).toBe(true)
      expect(validateDbName('db123')).toBe(true)
    })

    it('should reject empty names', () => {
      expect(validateDbName('')).toContain('required')
    })

    it('should reject names starting with numbers', () => {
      expect(validateDbName('123db')).toContain('must start with a letter')
    })

    it('should reject names with invalid characters', () => {
      expect(validateDbName('my.db')).toContain('letters, numbers')
      expect(validateDbName('my db')).toContain('letters, numbers')
    })

    it('should reject names over 64 characters', () => {
      const longName = 'a'.repeat(65)
      expect(validateDbName(longName)).toContain('64 characters')
    })
  })

  describe('validateNamespace', () => {
    it('should accept valid namespaces', () => {
      expect(validateNamespace('users')).toBe(true)
      expect(validateNamespace('User')).toBe(true)
      expect(validateNamespace('user_posts')).toBe(true)
      expect(validateNamespace('posts123')).toBe(true)
    })

    it('should reject invalid namespaces', () => {
      expect(validateNamespace('')).toContain('required')
      expect(validateNamespace('123users')).toContain('must start with a letter')
      expect(validateNamespace('user-posts')).toContain('letters, numbers')
    })
  })

  describe('validateNamespaces', () => {
    it('should accept valid namespace lists', () => {
      expect(validateNamespaces(['users', 'posts'])).toBe(true)
      expect(validateNamespaces([])).toBe(true)
    })

    it('should reject lists with invalid namespaces', () => {
      const result = validateNamespaces(['users', '123invalid'])
      expect(result).toContain('123invalid')
    })
  })
})

// =============================================================================
// Helper Function Tests
// =============================================================================

describe('getDefaultDbName', () => {
  it('should extract name from directory path', () => {
    expect(getDefaultDbName('/path/to/mydb')).toBe('mydb')
    expect(getDefaultDbName('/path/to/My-Database')).toBe('My-Database')
  })

  it('should sanitize invalid characters', () => {
    expect(getDefaultDbName('/path/to/my.db')).toBe('my-db')
    expect(getDefaultDbName('/path/to/my db')).toBe('my-db')
  })

  it('should handle edge cases', () => {
    expect(getDefaultDbName('.')).toBe('parquedb')
    expect(getDefaultDbName('/')).toBe('parquedb')
    expect(getDefaultDbName('')).toBe('parquedb')
  })

  it('should remove leading non-letter characters', () => {
    expect(getDefaultDbName('/path/to/123db')).toBe('db')
    expect(getDefaultDbName('/path/to/--mydb')).toBe('mydb')
  })
})

// =============================================================================
// Interactive Wizard Tests
// =============================================================================

describe('interactive wizard', () => {
  let tempDir: string

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-wizard-test-'))
  })

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('should collect wizard values', async () => {
    const mockIO = createMockIO([
      'testdb', // Database name
      '1', // Storage type (fs)
      'users, posts, comments', // Namespaces
      'n', // Don't create schema
    ])

    const values = await runInitWizard(tempDir, {}, mockIO)

    expect(values.name).toBe('testdb')
    expect(values.storageType).toBe('fs')
    expect(values.namespaces).toEqual(['users', 'posts', 'comments'])
    expect(values.createSchema).toBe(false)
  })

  it('should use defaults when user presses Enter', async () => {
    const mockIO = createMockIO([
      '', // Accept default name
      '', // Accept default storage (fs)
      '', // Accept default namespaces
      '', // Accept default (no schema)
    ])

    const values = await runInitWizard(
      tempDir,
      {
        name: 'defaultdb',
        storageType: 'fs',
        namespaces: ['default_ns'],
      },
      mockIO
    )

    expect(values.name).toBe('defaultdb')
    expect(values.storageType).toBe('fs')
    expect(values.namespaces).toEqual(['default_ns'])
    expect(values.createSchema).toBe(false)
  })

  it('should allow selecting storage type by number', async () => {
    const mockIO = createMockIO([
      'mydb',
      '3', // R2 storage
      'data',
      'n',
    ])

    const values = await runInitWizard(tempDir, {}, mockIO)

    expect(values.storageType).toBe('r2')
  })

  it('should allow selecting storage type by name', async () => {
    const mockIO = createMockIO([
      'mydb',
      's3', // S3 storage by name
      'data',
      'n',
    ])

    const values = await runInitWizard(tempDir, {}, mockIO)

    expect(values.storageType).toBe('s3')
  })
})

// =============================================================================
// Interactive Init Command Tests
// =============================================================================

describe('init command with --interactive', () => {
  let tempDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-cli-interactive-test-'))
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
  })

  afterEach(async () => {
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('should create database with wizard values', async () => {
    const mockIO = createMockIO([
      'wizarddb', // Database name
      '1', // Storage type (fs)
      'products, orders', // Namespaces
      'n', // Don't create schema
      'y', // Confirm
    ])

    const args: ParsedArgs = {
      command: 'init',
      args: ['--interactive'],
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
      },
    }

    const code = await initCommand(args, mockIO)

    expect(code).toBe(0)

    // Verify config
    const configPath = join(tempDir, 'parquedb.json')
    const config = JSON.parse(await fs.readFile(configPath, 'utf-8'))

    expect(config.name).toBe('wizarddb')
    expect(config.storage.type).toBe('fs')
    expect(config.namespaces).toEqual(['products', 'orders'])

    // Verify namespace directories were created
    const productsDir = await fs
      .stat(join(tempDir, 'data', 'products'))
      .then((s) => s.isDirectory())
      .catch(() => false)
    const ordersDir = await fs
      .stat(join(tempDir, 'data', 'orders'))
      .then((s) => s.isDirectory())
      .catch(() => false)

    expect(productsDir).toBe(true)
    expect(ordersDir).toBe(true)
  })

  it('should cancel when user declines confirmation', async () => {
    const mockIO = createMockIO([
      'mydb',
      '1',
      'users',
      'n',
      'n', // Decline confirmation
    ])

    const args: ParsedArgs = {
      command: 'init',
      args: ['-i'],
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
      },
    }

    const code = await initCommand(args, mockIO)

    expect(code).toBe(0)

    // Config should not exist
    const configExists = await fs
      .access(join(tempDir, 'parquedb.json'))
      .then(() => true)
      .catch(() => false)
    expect(configExists).toBe(false)
  })

  it('should create schema file when requested', async () => {
    const mockIO = createMockIO([
      'schemadb',
      '1',
      'users, posts',
      'y', // Create schema
      'y', // Confirm
    ])

    const args: ParsedArgs = {
      command: 'init',
      args: ['-i'],
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
      },
    }

    const code = await initCommand(args, mockIO)

    expect(code).toBe(0)

    // Schema file should exist
    const schemaPath = join(tempDir, 'parquedb.schema.ts')
    const schemaExists = await fs
      .access(schemaPath)
      .then(() => true)
      .catch(() => false)
    expect(schemaExists).toBe(true)

    // Check schema content
    const schemaContent = await fs.readFile(schemaPath, 'utf-8')
    expect(schemaContent).toContain('schemadb')
    expect(schemaContent).toContain('users')
    expect(schemaContent).toContain('posts')
    expect(schemaContent).toContain('defineSchema')
  })
})

// =============================================================================
// Non-Interactive Options Tests
// =============================================================================

describe('init command with CLI options', () => {
  let tempDir: string
  let stdoutOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-cli-options-test-'))
    stdoutOutput = []

    process.stdout.write = vi.fn((chunk: string | Uint8Array) => {
      stdoutOutput.push(chunk.toString())
      return true
    })
    process.stderr.write = vi.fn(() => true)
  })

  afterEach(async () => {
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('should use --name option', async () => {
    const args: ParsedArgs = {
      command: 'init',
      args: ['--name', 'customdb'],
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
      },
    }

    await initCommand(args)

    const config = JSON.parse(await fs.readFile(join(tempDir, 'parquedb.json'), 'utf-8'))
    expect(config.name).toBe('customdb')
  })

  it('should use --storage option', async () => {
    const args: ParsedArgs = {
      command: 'init',
      args: ['--storage', 'memory'],
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
      },
    }

    await initCommand(args)

    const config = JSON.parse(await fs.readFile(join(tempDir, 'parquedb.json'), 'utf-8'))
    expect(config.storage.type).toBe('memory')
  })

  it('should use --namespace options', async () => {
    const args: ParsedArgs = {
      command: 'init',
      args: ['--namespace', 'products', '--ns', 'inventory'],
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
      },
    }

    await initCommand(args)

    const config = JSON.parse(await fs.readFile(join(tempDir, 'parquedb.json'), 'utf-8'))
    expect(config.namespaces).toEqual(['products', 'inventory'])

    // Verify namespace directories
    const productsDir = await fs
      .stat(join(tempDir, 'data', 'products'))
      .then((s) => s.isDirectory())
      .catch(() => false)
    expect(productsDir).toBe(true)
  })

  it('should combine multiple options', async () => {
    const args: ParsedArgs = {
      command: 'init',
      args: ['--name', 'combined', '-s', 'r2', '--ns', 'data'],
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
      },
    }

    await initCommand(args)

    const config = JSON.parse(await fs.readFile(join(tempDir, 'parquedb.json'), 'utf-8'))
    expect(config.name).toBe('combined')
    expect(config.storage.type).toBe('r2')
    expect(config.namespaces).toEqual(['data'])
  })
})
