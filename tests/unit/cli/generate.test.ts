/**
 * Generate Command Tests
 *
 * Tests for the generate command that creates typed exports from config.
 * Focuses on:
 * - Path validation and security
 * - Argument handling
 * - Error cases
 * - Config file handling
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import { generateCommand } from '../../../src/cli/commands/generate'
import type { ParsedArgs } from '../../../src/cli/types'

// Mock the loadConfig function
vi.mock('../../../src/config', () => ({
  loadConfig: vi.fn(),
}))

import { loadConfig } from '../../../src/config'

const mockedLoadConfig = vi.mocked(loadConfig)

describe('generate command', () => {
  let tempDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write
  const originalCwd = process.cwd
  const originalArgv = process.argv

  beforeEach(async () => {
    // Create a unique temp directory
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-generate-test-'))

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

    // Mock cwd to return tempDir
    process.cwd = vi.fn(() => tempDir)

    // Reset argv
    process.argv = ['node', 'parquedb', 'generate']

    // Reset mocks
    mockedLoadConfig.mockReset()
  })

  afterEach(async () => {
    // Restore stdout, stderr, cwd, and argv
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite
    process.cwd = originalCwd
    process.argv = originalArgv

    // Clean up temp directory
    await fs.rm(tempDir, { recursive: true, force: true })

    vi.restoreAllMocks()
  })

  /**
   * Create a ParsedArgs object for testing
   */
  function createArgs(args: string[] = [], options: Partial<ParsedArgs['options']> = {}): ParsedArgs {
    return {
      command: 'generate',
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

  // ===========================================================================
  // Help Output Tests
  // ===========================================================================

  describe('help output', () => {
    it('should show help when --help flag is set', async () => {
      const args = createArgs([], { help: true })
      const code = await generateCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      expect(output).toContain('ParqueDB Generate')
      expect(output).toContain('--output')
      expect(output).toContain('USAGE')
    })
  })

  // ===========================================================================
  // Config Loading Tests
  // ===========================================================================

  describe('config loading', () => {
    it('should fail if no config file found', async () => {
      mockedLoadConfig.mockResolvedValue(null)

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No parquedb.config.ts found')
    })

    it('should fail if config has no schema', async () => {
      mockedLoadConfig.mockResolvedValue({
        // Config without schema
      })

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('No schema defined')
    })

    it('should provide helpful error message when no config', async () => {
      mockedLoadConfig.mockResolvedValue(null)

      const args = createArgs()
      await generateCommand(args)

      const output = stdoutOutput.join('')
      expect(output).toContain('Create a config file')
      expect(output).toContain('defineConfig')
    })

    it('should provide helpful error message when no schema', async () => {
      mockedLoadConfig.mockResolvedValue({})

      const args = createArgs()
      await generateCommand(args)

      const output = stdoutOutput.join('')
      expect(output).toContain('Add a schema')
      expect(output).toContain('defineConfig')
    })
  })

  // ===========================================================================
  // Path Validation Tests
  // ===========================================================================

  describe('path validation', () => {
    beforeEach(() => {
      // Set up a valid config for path validation tests
      mockedLoadConfig.mockResolvedValue({
        schema: {
          User: { email: 'string!', name: 'string' },
        },
      })
    })

    it('should reject paths with path traversal sequences', async () => {
      process.argv = ['node', 'parquedb', 'generate', '--output', '../../../etc/malicious.ts']

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid output path')
      expect(stderrOutput.join('')).toContain('traversal')
    })

    it('should reject paths with null bytes', async () => {
      process.argv = ['node', 'parquedb', 'generate', '--output', 'output\0.ts']

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid output path')
      expect(stderrOutput.join('')).toContain('dangerous characters')
    })

    it('should reject paths with newlines', async () => {
      process.argv = ['node', 'parquedb', 'generate', '--output', 'output\n.ts']

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid output path')
      expect(stderrOutput.join('')).toContain('dangerous characters')
    })

    it('should reject absolute paths outside allowed directories', async () => {
      process.argv = ['node', 'parquedb', 'generate', '--output', '/etc/passwd']

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Invalid output path')
    })

    it('should accept paths within current working directory', async () => {
      // Create src directory
      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      process.argv = ['node', 'parquedb', 'generate', '--output', 'src/db.generated.ts']

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Generated typed exports')
    })

    it('should accept absolute paths within current working directory', async () => {
      // Create src directory
      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const outputPath = join(tempDir, 'src', 'db.generated.ts')
      process.argv = ['node', 'parquedb', 'generate', '--output', outputPath]

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Generated typed exports')
    })
  })

  // ===========================================================================
  // Output Path Tests
  // ===========================================================================

  describe('output path handling', () => {
    beforeEach(() => {
      mockedLoadConfig.mockResolvedValue({
        schema: {
          User: { email: 'string!', name: 'string' },
        },
      })
    })

    it('should use default output path when none specified', async () => {
      // Create src directory for default path
      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Check default path exists
      const defaultPath = join(tempDir, 'src', 'db.generated.ts')
      const exists = await fs
        .access(defaultPath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })

    it('should use --output flag when provided', async () => {
      // Create custom directory
      await fs.mkdir(join(tempDir, 'lib'), { recursive: true })

      process.argv = ['node', 'parquedb', 'generate', '--output', 'lib/database.ts']

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Check custom path exists
      const customPath = join(tempDir, 'lib', 'database.ts')
      const exists = await fs
        .access(customPath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })

    it('should use -o flag when provided', async () => {
      // Create custom directory
      await fs.mkdir(join(tempDir, 'out'), { recursive: true })

      process.argv = ['node', 'parquedb', 'generate', '-o', 'out/types.ts']

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Check custom path exists
      const customPath = join(tempDir, 'out', 'types.ts')
      const exists = await fs
        .access(customPath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })

    it('should use positional argument as output path', async () => {
      // Create custom directory
      await fs.mkdir(join(tempDir, 'types'), { recursive: true })

      const args = createArgs(['types/db.ts'])
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Check custom path exists
      const customPath = join(tempDir, 'types', 'db.ts')
      const exists = await fs
        .access(customPath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })

    it('should create output directory if it does not exist', async () => {
      process.argv = ['node', 'parquedb', 'generate', '--output', 'new-dir/nested/db.ts']

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Check directory and file were created
      const outputPath = join(tempDir, 'new-dir', 'nested', 'db.ts')
      const exists = await fs
        .access(outputPath)
        .then(() => true)
        .catch(() => false)
      expect(exists).toBe(true)
    })
  })

  // ===========================================================================
  // Code Generation Tests
  // ===========================================================================

  describe('code generation', () => {
    it('should generate typed interfaces for collections', async () => {
      mockedLoadConfig.mockResolvedValue({
        schema: {
          User: {
            email: 'string!#',
            name: 'string',
            age: 'int?',
          },
        },
      })

      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Read generated file
      const outputPath = join(tempDir, 'src', 'db.generated.ts')
      const content = await fs.readFile(outputPath, 'utf-8')

      // Check for interface
      expect(content).toContain('interface UserEntity')
      expect(content).toContain('email')
      expect(content).toContain('name')
      expect(content).toContain('age')
    })

    it('should generate typed collection methods', async () => {
      mockedLoadConfig.mockResolvedValue({
        schema: {
          Post: {
            title: 'string!',
            content: 'text',
          },
        },
      })

      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Read generated file
      const outputPath = join(tempDir, 'src', 'db.generated.ts')
      const content = await fs.readFile(outputPath, 'utf-8')

      // Check for collection methods
      expect(content).toContain('TypedPostCollection')
      expect(content).toContain('create(')
      expect(content).toContain('find(')
      expect(content).toContain('update(')
      expect(content).toContain('delete(')
    })

    it('should generate TypedDB interface', async () => {
      mockedLoadConfig.mockResolvedValue({
        schema: {
          User: { email: 'string!' },
          Post: { title: 'string!' },
        },
      })

      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Read generated file
      const outputPath = join(tempDir, 'src', 'db.generated.ts')
      const content = await fs.readFile(outputPath, 'utf-8')

      // Check for TypedDB interface
      expect(content).toContain('interface TypedDB')
      expect(content).toContain('User: TypedUserCollection')
      expect(content).toContain('Post: TypedPostCollection')
    })

    it('should skip flexible schema collections', async () => {
      mockedLoadConfig.mockResolvedValue({
        schema: {
          User: { email: 'string!' },
          Flexible: 'flexible',
        },
      })

      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Read generated file
      const outputPath = join(tempDir, 'src', 'db.generated.ts')
      const content = await fs.readFile(outputPath, 'utf-8')

      // Should have User but not Flexible
      expect(content).toContain('UserEntity')
      expect(content).not.toContain('FlexibleEntity')
    })

    it('should handle relationship fields', async () => {
      mockedLoadConfig.mockResolvedValue({
        schema: {
          Post: {
            title: 'string!',
            author: '-> User',
            comments: '<- Comment[]',
          },
        },
      })

      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)

      // Read generated file
      const outputPath = join(tempDir, 'src', 'db.generated.ts')
      const content = await fs.readFile(outputPath, 'utf-8')

      // Should have EntityRef types for relationships
      expect(content).toContain('EntityRef')
    })

    it('should include DO NOT EDIT warning', async () => {
      mockedLoadConfig.mockResolvedValue({
        schema: {
          User: { email: 'string!' },
        },
      })

      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const args = createArgs()
      await generateCommand(args)

      const outputPath = join(tempDir, 'src', 'db.generated.ts')
      const content = await fs.readFile(outputPath, 'utf-8')

      expect(content).toContain('DO NOT EDIT')
      expect(content).toContain('parquedb generate')
    })
  })

  // ===========================================================================
  // Success Output Tests
  // ===========================================================================

  describe('success output', () => {
    beforeEach(() => {
      mockedLoadConfig.mockResolvedValue({
        schema: {
          User: { email: 'string!' },
        },
      })
    })

    it('should print success message with path', async () => {
      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('Generated typed exports')
      expect(stdoutOutput.join('')).toContain('db.generated.ts')
    })

    it('should print usage examples', async () => {
      await fs.mkdir(join(tempDir, 'src'), { recursive: true })

      const args = createArgs()
      await generateCommand(args)

      const output = stdoutOutput.join('')
      expect(output).toContain('Usage:')
      expect(output).toContain('import')
      expect(output).toContain('db.User.create')
    })
  })

  // ===========================================================================
  // Error Handling Tests
  // ===========================================================================

  describe('error handling', () => {
    it('should handle loadConfig errors gracefully', async () => {
      mockedLoadConfig.mockRejectedValue(new Error('Config parsing failed'))

      const args = createArgs()
      const code = await generateCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Config parsing failed')
    })
  })
})
