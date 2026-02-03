/**
 * Deploy Command Tests
 *
 * Tests for the deploy CLI command.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { deployCommand } from '../../../src/cli/commands/deploy'
import type { ParsedArgs } from '../../../src/cli/types'

describe('deploy command', () => {
  let tempDir: string
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write
  const originalEnv = process.env

  beforeEach(async () => {
    // Create a unique temp directory
    tempDir = await fs.mkdtemp(join(tmpdir(), 'parquedb-deploy-test-'))

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

    // Reset env
    process.env = { ...originalEnv }
  })

  afterEach(async () => {
    // Restore stdout, stderr, and env
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite
    process.env = originalEnv

    // Clean up temp directory
    await fs.rm(tempDir, { recursive: true, force: true })

    vi.restoreAllMocks()
  })

  /**
   * Create a ParsedArgs object for testing
   */
  function createArgs(args: string[] = []): ParsedArgs {
    return {
      command: 'deploy',
      args,
      options: {
        help: false,
        version: false,
        directory: tempDir,
        format: 'json',
        pretty: false,
        quiet: false,
        noColor: true,
      },
    }
  }

  describe('help output', () => {
    it('should show help when no arguments provided', async () => {
      const args = createArgs([])
      const code = await deployCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      expect(output).toContain('Deploy ParqueDB to edge platforms')
      expect(output).toContain('snippets')
      expect(output).toContain('CLOUDFLARE_API_TOKEN')
    })
  })

  describe('snippets subcommand', () => {
    it('should show usage when snippet name is missing', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      const args = createArgs(['snippets'])
      const code = await deployCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Snippet name is required')
    })

    it('should show usage when file is missing', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      const args = createArgs(['snippets', 'my_snippet'])
      const code = await deployCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('JavaScript file is required')
    })

    it('should fail when environment variables are missing', async () => {
      delete process.env.CLOUDFLARE_API_TOKEN
      delete process.env.CLOUDFLARE_ZONE_ID

      // Create a test file
      const testFile = join(tempDir, 'worker.js')
      await fs.writeFile(testFile, 'export default { fetch() {} }')

      const args = createArgs(['snippets', 'my_snippet', 'worker.js'])
      const code = await deployCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('CLOUDFLARE_API_TOKEN')
    })

    it('should fail when file does not exist', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      const args = createArgs(['snippets', 'my_snippet', 'nonexistent.js'])
      const code = await deployCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Failed to read file')
    })

    it('should warn about invalid snippet names', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      // Create a test file
      const testFile = join(tempDir, 'worker.js')
      await fs.writeFile(testFile, 'export default { fetch() { return new Response("OK") } }')

      // Mock fetch to succeed
      const originalFetch = global.fetch
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: { snippet_name: 'invalid_name', created_on: '', modified_on: '' },
            errors: [],
            messages: [],
          }),
      })

      try {
        const args = createArgs(['snippets', 'Invalid-Name!', 'worker.js'])
        await deployCommand(args)

        const output = stdoutOutput.join('')
        expect(output).toContain('invalid characters')
        expect(output).toContain('normalized')
      } finally {
        global.fetch = originalFetch
      }
    })

    it('should support dry-run mode', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      // Create a test file
      const testFile = join(tempDir, 'worker.js')
      await fs.writeFile(testFile, 'export default { fetch() { return new Response("OK") } }')

      // Mock fetch - should NOT be called in dry-run
      const originalFetch = global.fetch
      global.fetch = vi.fn()

      try {
        const args = createArgs(['snippets', 'my_snippet', 'worker.js', '--dry-run'])
        const code = await deployCommand(args)

        expect(code).toBe(0)
        const output = stdoutOutput.join('')
        expect(output).toContain('Dry run')
        expect(output).toContain('my_snippet')
        expect(global.fetch).not.toHaveBeenCalled()
      } finally {
        global.fetch = originalFetch
      }
    })

    it('should support dry-run with rule', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      // Create a test file
      const testFile = join(tempDir, 'worker.js')
      await fs.writeFile(testFile, 'export default { fetch() { return new Response("OK") } }')

      const args = createArgs([
        'snippets',
        'my_snippet',
        'worker.js',
        '--dry-run',
        '--rule',
        'http.request.uri.path eq "/api"',
        '--description',
        'API route',
      ])
      const code = await deployCommand(args)

      expect(code).toBe(0)
      const output = stdoutOutput.join('')
      expect(output).toContain('Rule expression')
      expect(output).toContain('/api')
      expect(output).toContain('API route')
    })
  })

  describe('list subcommand', () => {
    it('should list snippets', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      const originalFetch = global.fetch
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: [
              { snippet_name: 'snippet_one', created_on: '2024-01-01', modified_on: '2024-01-02' },
              { snippet_name: 'snippet_two', created_on: '2024-01-03', modified_on: '2024-01-04' },
            ],
            errors: [],
            messages: [],
          }),
      })

      try {
        const args = createArgs(['snippets', 'list'])
        const code = await deployCommand(args)

        expect(code).toBe(0)
        const output = stdoutOutput.join('')
        expect(output).toContain('snippet_one')
        expect(output).toContain('snippet_two')
        expect(output).toContain('2 snippet(s)')
      } finally {
        global.fetch = originalFetch
      }
    })

    it('should handle empty snippet list', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      const originalFetch = global.fetch
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: [],
            errors: [],
            messages: [],
          }),
      })

      try {
        const args = createArgs(['snippets', 'list'])
        const code = await deployCommand(args)

        expect(code).toBe(0)
        expect(stdoutOutput.join('')).toContain('No snippets found')
      } finally {
        global.fetch = originalFetch
      }
    })
  })

  describe('rules subcommand', () => {
    it('should list rules', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      const originalFetch = global.fetch
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: {
              rules: [
                {
                  snippet_name: 'my_api',
                  expression: 'http.request.uri.path starts_with "/api"',
                  enabled: true,
                  description: 'API route',
                },
              ],
            },
            errors: [],
            messages: [],
          }),
      })

      try {
        const args = createArgs(['snippets', 'rules'])
        const code = await deployCommand(args)

        expect(code).toBe(0)
        const output = stdoutOutput.join('')
        expect(output).toContain('my_api')
        expect(output).toContain('/api')
        expect(output).toContain('API route')
      } finally {
        global.fetch = originalFetch
      }
    })
  })

  describe('delete subcommand', () => {
    it('should delete a snippet', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      let callCount = 0
      const originalFetch = global.fetch
      global.fetch = vi.fn().mockImplementation(() => {
        callCount++
        if (callCount === 1) {
          // listRules
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                success: true,
                result: { rules: [] },
                errors: [],
                messages: [],
              }),
          })
        } else {
          // deleteSnippet
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                success: true,
                result: 'deleted',
                errors: [],
                messages: [],
              }),
          })
        }
      })

      try {
        const args = createArgs(['snippets', 'delete', 'my_snippet'])
        const code = await deployCommand(args)

        expect(code).toBe(0)
        expect(stdoutOutput.join('')).toContain('Deleted')
      } finally {
        global.fetch = originalFetch
      }
    })

    it('should require snippet name for delete', async () => {
      process.env.CLOUDFLARE_API_TOKEN = 'test-token'
      process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

      const args = createArgs(['snippets', 'delete'])
      const code = await deployCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Snippet name is required')
    })
  })

  describe('unknown platform', () => {
    it('should reject unknown platforms', async () => {
      const args = createArgs(['unknown-platform'])
      const code = await deployCommand(args)

      expect(code).toBe(1)
      expect(stderrOutput.join('')).toContain('Unknown platform')
    })
  })
})
