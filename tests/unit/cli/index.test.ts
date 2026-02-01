/**
 * CLI Index Tests
 *
 * Tests for the CLI entry point and argument parser.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { parseArgs, main } from '../../../src/cli/index'

describe('CLI', () => {
  // ===========================================================================
  // Argument Parser Tests
  // ===========================================================================

  describe('parseArgs', () => {
    it('should parse command without options', () => {
      const result = parseArgs(['init'])

      expect(result.command).toBe('init')
      expect(result.args).toEqual([])
      expect(result.options.help).toBe(false)
      expect(result.options.version).toBe(false)
    })

    it('should parse command with arguments', () => {
      const result = parseArgs(['query', 'posts', '{"status": "published"}'])

      expect(result.command).toBe('query')
      expect(result.args).toEqual(['posts', '{"status": "published"}'])
    })

    it('should parse help flag short form', () => {
      const result = parseArgs(['-h'])
      expect(result.options.help).toBe(true)
    })

    it('should parse help flag long form', () => {
      const result = parseArgs(['--help'])
      expect(result.options.help).toBe(true)
    })

    it('should parse version flag short form', () => {
      const result = parseArgs(['-v'])
      expect(result.options.version).toBe(true)
    })

    it('should parse version flag long form', () => {
      const result = parseArgs(['--version'])
      expect(result.options.version).toBe(true)
    })

    it('should parse directory option short form', () => {
      const result = parseArgs(['-d', '/path/to/db', 'stats'])

      expect(result.options.directory).toBe('/path/to/db')
      expect(result.command).toBe('stats')
    })

    it('should parse directory option long form', () => {
      const result = parseArgs(['--directory', '/path/to/db', 'stats'])

      expect(result.options.directory).toBe('/path/to/db')
      expect(result.command).toBe('stats')
    })

    it('should parse format option', () => {
      const result = parseArgs(['-f', 'ndjson', 'export', 'posts', 'out.ndjson'])

      expect(result.options.format).toBe('ndjson')
      expect(result.command).toBe('export')
      expect(result.args).toEqual(['posts', 'out.ndjson'])
    })

    it('should throw on invalid format', () => {
      expect(() => parseArgs(['-f', 'xml'])).toThrow('Invalid format: xml')
    })

    it('should parse limit option', () => {
      const result = parseArgs(['-l', '10', 'query', 'posts'])

      expect(result.options.limit).toBe(10)
      expect(result.command).toBe('query')
    })

    it('should throw on invalid limit', () => {
      expect(() => parseArgs(['-l', 'abc'])).toThrow('Invalid limit: abc')
    })

    it('should throw on negative limit', () => {
      expect(() => parseArgs(['-l', '-5'])).toThrow('Invalid limit: -5')
    })

    it('should parse pretty flag', () => {
      const result = parseArgs(['-p', 'query', 'posts'])

      expect(result.options.pretty).toBe(true)
    })

    it('should parse quiet flag', () => {
      const result = parseArgs(['-q', 'import', 'posts', 'data.json'])

      expect(result.options.quiet).toBe(true)
    })

    it('should throw on unknown option', () => {
      expect(() => parseArgs(['--unknown'])).toThrow('Unknown option: --unknown')
    })

    it('should handle empty args', () => {
      const result = parseArgs([])

      expect(result.command).toBe('')
      expect(result.args).toEqual([])
    })

    it('should parse multiple options together', () => {
      const result = parseArgs([
        '-d', '/mydb',
        '-f', 'csv',
        '-l', '100',
        '-p',
        'export',
        'users',
        'users.csv'
      ])

      expect(result.options.directory).toBe('/mydb')
      expect(result.options.format).toBe('csv')
      expect(result.options.limit).toBe(100)
      expect(result.options.pretty).toBe(true)
      expect(result.command).toBe('export')
      expect(result.args).toEqual(['users', 'users.csv'])
    })
  })

  // ===========================================================================
  // Main Entry Point Tests
  // ===========================================================================

  describe('main', () => {
    let stdoutOutput: string[] = []
    let stderrOutput: string[] = []
    const originalStdoutWrite = process.stdout.write
    const originalStderrWrite = process.stderr.write

    beforeEach(() => {
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

    afterEach(() => {
      process.stdout.write = originalStdoutWrite
      process.stderr.write = originalStderrWrite
    })

    it('should show help with --help', async () => {
      const code = await main(['--help'])

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('ParqueDB CLI')
      expect(stdoutOutput.join('')).toContain('USAGE:')
      expect(stdoutOutput.join('')).toContain('COMMANDS:')
    })

    it('should show help with -h', async () => {
      const code = await main(['-h'])

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('ParqueDB CLI')
    })

    it('should show version with --version', async () => {
      const code = await main(['--version'])

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('parquedb v')
    })

    it('should show version with -v', async () => {
      const code = await main(['-v'])

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('parquedb v')
    })

    it('should show help when no command provided', async () => {
      const code = await main([])

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('ParqueDB CLI')
    })

    it('should show help for help command', async () => {
      const code = await main(['help'])

      expect(code).toBe(0)
      expect(stdoutOutput.join('')).toContain('ParqueDB CLI')
    })

    it('should error on unknown command', async () => {
      const code = await main(['unknown-command'])

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Unknown command: unknown-command')
    })

    it('should error on invalid option', async () => {
      const code = await main(['--invalid-option'])

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Unknown option')
    })
  })
})
