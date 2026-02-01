/**
 * Init Command Tests
 *
 * Tests for the init command that initializes a ParqueDB database.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { initCommand } from '../../../src/cli/commands/init'
import type { ParsedArgs } from '../../../src/cli/index'

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
    const configExists = await fs.access(configPath).then(() => true).catch(() => false)
    expect(configExists).toBe(true)

    // Check config content
    const config = JSON.parse(await fs.readFile(configPath, 'utf-8'))
    expect(config.version).toBe('1.0')
    expect(config.storage.type).toBe('fs')
    expect(config.storage.dataDir).toBe('data')
    expect(config.storage.eventsDir).toBe('events')
    expect(config.createdAt).toBeDefined()

    // Check directories exist
    const dataExists = await fs.stat(join(tempDir, 'data')).then(s => s.isDirectory()).catch(() => false)
    const eventsExists = await fs.stat(join(tempDir, 'events')).then(s => s.isDirectory()).catch(() => false)
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
    const configExists = await fs.access(configPath).then(() => true).catch(() => false)
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
