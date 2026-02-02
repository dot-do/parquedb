/**
 * Command Registry Tests
 *
 * Tests for the CLI command registry system.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { CommandRegistry } from '../../../src/cli/registry'
import type { Command } from '../../../src/cli/registry'
import type { ParsedArgs } from '../../../src/cli/index'

describe('CommandRegistry', () => {
  let registry: CommandRegistry

  // Create a minimal ParsedArgs for testing
  const createArgs = (command: string): ParsedArgs => ({
    command,
    args: [],
    options: {
      help: false,
      version: false,
      directory: process.cwd(),
      format: 'json',
      pretty: false,
      quiet: false,
    },
  })

  // Create a test command
  const createCommand = (name: string, overrides?: Partial<Command>): Command => ({
    name,
    description: `Test ${name} command`,
    usage: `parquedb ${name}`,
    execute: async () => 0,
    ...overrides,
  })

  beforeEach(() => {
    registry = new CommandRegistry()
  })

  // ===========================================================================
  // register() Tests
  // ===========================================================================

  describe('register', () => {
    it('should register a command', () => {
      const command = createCommand('test')
      registry.register(command)

      expect(registry.has('test')).toBe(true)
    })

    it('should register a command with aliases', () => {
      const command = createCommand('test', { aliases: ['t', 'tst'] })
      registry.register(command)

      expect(registry.has('test')).toBe(true)
      expect(registry.has('t')).toBe(true)
      expect(registry.has('tst')).toBe(true)
    })

    it('should throw if command name is already registered', () => {
      registry.register(createCommand('test'))

      expect(() => registry.register(createCommand('test'))).toThrow(
        'Command "test" is already registered'
      )
    })

    it('should throw if alias conflicts with existing command', () => {
      registry.register(createCommand('test'))

      expect(() =>
        registry.register(createCommand('other', { aliases: ['test'] }))
      ).toThrow('Alias "test" is already registered')
    })

    it('should throw if alias conflicts with existing alias', () => {
      registry.register(createCommand('test', { aliases: ['t'] }))

      expect(() =>
        registry.register(createCommand('other', { aliases: ['t'] }))
      ).toThrow('Alias "t" is already registered')
    })
  })

  // ===========================================================================
  // unregister() Tests
  // ===========================================================================

  describe('unregister', () => {
    it('should unregister a command', () => {
      registry.register(createCommand('test'))
      const result = registry.unregister('test')

      expect(result).toBe(true)
      expect(registry.has('test')).toBe(false)
    })

    it('should unregister aliases when command is unregistered', () => {
      registry.register(createCommand('test', { aliases: ['t', 'tst'] }))
      registry.unregister('test')

      expect(registry.has('test')).toBe(false)
      expect(registry.has('t')).toBe(false)
      expect(registry.has('tst')).toBe(false)
    })

    it('should return false if command does not exist', () => {
      const result = registry.unregister('nonexistent')

      expect(result).toBe(false)
    })
  })

  // ===========================================================================
  // get() Tests
  // ===========================================================================

  describe('get', () => {
    it('should get a command by name', () => {
      const command = createCommand('test')
      registry.register(command)

      expect(registry.get('test')).toBe(command)
    })

    it('should get a command by alias', () => {
      const command = createCommand('test', { aliases: ['t'] })
      registry.register(command)

      expect(registry.get('t')).toBe(command)
    })

    it('should return undefined for unknown command', () => {
      expect(registry.get('unknown')).toBeUndefined()
    })
  })

  // ===========================================================================
  // has() Tests
  // ===========================================================================

  describe('has', () => {
    it('should return true for registered command', () => {
      registry.register(createCommand('test'))

      expect(registry.has('test')).toBe(true)
    })

    it('should return true for registered alias', () => {
      registry.register(createCommand('test', { aliases: ['t'] }))

      expect(registry.has('t')).toBe(true)
    })

    it('should return false for unknown command', () => {
      expect(registry.has('unknown')).toBe(false)
    })
  })

  // ===========================================================================
  // list() Tests
  // ===========================================================================

  describe('list', () => {
    it('should return empty array when no commands registered', () => {
      expect(registry.list()).toEqual([])
    })

    it('should return all registered commands', () => {
      const cmd1 = createCommand('cmd1')
      const cmd2 = createCommand('cmd2')
      const cmd3 = createCommand('cmd3')

      registry.register(cmd1)
      registry.register(cmd2)
      registry.register(cmd3)

      const list = registry.list()
      expect(list).toHaveLength(3)
      expect(list).toContain(cmd1)
      expect(list).toContain(cmd2)
      expect(list).toContain(cmd3)
    })
  })

  // ===========================================================================
  // names() Tests
  // ===========================================================================

  describe('names', () => {
    it('should return empty array when no commands registered', () => {
      expect(registry.names()).toEqual([])
    })

    it('should return all command names (not aliases)', () => {
      registry.register(createCommand('cmd1', { aliases: ['c1'] }))
      registry.register(createCommand('cmd2', { aliases: ['c2'] }))

      const names = registry.names()
      expect(names).toHaveLength(2)
      expect(names).toContain('cmd1')
      expect(names).toContain('cmd2')
      expect(names).not.toContain('c1')
      expect(names).not.toContain('c2')
    })
  })

  // ===========================================================================
  // byCategory() Tests
  // ===========================================================================

  describe('byCategory', () => {
    it('should return empty map when no commands registered', () => {
      const categories = registry.byCategory()
      expect(categories.size).toBe(0)
    })

    it('should group commands by category', () => {
      registry.register(createCommand('init', { category: 'Database' }))
      registry.register(createCommand('stats', { category: 'Database' }))
      registry.register(createCommand('query', { category: 'Data' }))
      registry.register(createCommand('import', { category: 'Data' }))

      const categories = registry.byCategory()
      expect(categories.size).toBe(2)
      expect(categories.get('Database')).toHaveLength(2)
      expect(categories.get('Data')).toHaveLength(2)
    })

    it('should use "General" for commands without category', () => {
      registry.register(createCommand('cmd1'))
      registry.register(createCommand('cmd2'))

      const categories = registry.byCategory()
      expect(categories.get('General')).toHaveLength(2)
    })
  })

  // ===========================================================================
  // clear() Tests
  // ===========================================================================

  describe('clear', () => {
    it('should clear all commands', () => {
      registry.register(createCommand('cmd1'))
      registry.register(createCommand('cmd2', { aliases: ['c2'] }))

      registry.clear()

      expect(registry.list()).toEqual([])
      expect(registry.has('cmd1')).toBe(false)
      expect(registry.has('cmd2')).toBe(false)
      expect(registry.has('c2')).toBe(false)
    })
  })

  // ===========================================================================
  // Command Execution Tests
  // ===========================================================================

  describe('command execution', () => {
    it('should execute command and return exit code', async () => {
      let executedWith: ParsedArgs | null = null
      const command = createCommand('test', {
        execute: async (args) => {
          executedWith = args
          return 0
        },
      })
      registry.register(command)

      const args = createArgs('test')
      const result = await registry.get('test')!.execute(args)

      expect(result).toBe(0)
      expect(executedWith).toBe(args)
    })

    it('should return non-zero exit code on failure', async () => {
      const command = createCommand('failing', {
        execute: async () => 1,
      })
      registry.register(command)

      const result = await registry.get('failing')!.execute(createArgs('failing'))

      expect(result).toBe(1)
    })
  })
})
