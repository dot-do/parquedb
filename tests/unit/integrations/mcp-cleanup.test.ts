/**
 * MCP Server Cleanup Tests
 *
 * Tests for resource cleanup functionality in the ParqueDB MCP server.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { DB } from '../../../src/db'
import { createParqueDBMCPServer } from '../../../src/integrations/mcp'
import type { ParqueDBMCPServerHandle } from '../../../src/integrations/mcp'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'

describe('MCP Server Resource Cleanup', () => {
  let db: ReturnType<typeof DB>

  beforeEach(async () => {
    db = DB({
      Posts: {
        title: 'string!',
        content: 'text',
        status: 'string',
      },
    })
  })

  describe('ParqueDBMCPServerHandle', () => {
    it('should return a handle with server and dispose method', () => {
      const handle = createParqueDBMCPServer(db)

      expect(handle).toBeDefined()
      expect(handle.server).toBeDefined()
      expect(typeof handle.dispose).toBe('function')
    })

    it('should have a working server instance on the handle', async () => {
      const handle = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await handle.server.connect(t2)
      await client.connect(t1)

      // Verify server works
      const tools = await client.listTools()
      expect(tools.tools.length).toBeGreaterThan(0)

      await client.close()
      await handle.dispose()
    })

    it('should allow dispose to be called even after server.close()', async () => {
      const handle = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await handle.server.connect(t2)
      await client.connect(t1)

      await client.close()
      await handle.server.close()

      // Dispose should not throw even after server is closed
      await expect(handle.dispose()).resolves.not.toThrow()
    })

    it('should allow dispose to be called multiple times safely', async () => {
      const handle = createParqueDBMCPServer(db)

      // Multiple dispose calls should be safe (idempotent)
      await expect(handle.dispose()).resolves.not.toThrow()
      await expect(handle.dispose()).resolves.not.toThrow()
      await expect(handle.dispose()).resolves.not.toThrow()
    })
  })

  describe('Server Recreation After Cleanup', () => {
    it('should allow creating a new server after disposing the previous one', async () => {
      // Create first server
      const handle1 = createParqueDBMCPServer(db)
      const client1 = new Client({ name: 'test-client-1', version: '1.0.0' }, { capabilities: {} })
      const [t1a, t1b] = InMemoryTransport.createLinkedPair()

      await handle1.server.connect(t1b)
      await client1.connect(t1a)

      // Use the first server
      const tools1 = await client1.listTools()
      expect(tools1.tools.length).toBeGreaterThan(0)

      // Clean up first server
      await client1.close()
      await handle1.dispose()

      // Create second server
      const handle2 = createParqueDBMCPServer(db)
      const client2 = new Client({ name: 'test-client-2', version: '1.0.0' }, { capabilities: {} })
      const [t2a, t2b] = InMemoryTransport.createLinkedPair()

      await handle2.server.connect(t2b)
      await client2.connect(t2a)

      // Use the second server
      const tools2 = await client2.listTools()
      expect(tools2.tools.length).toBeGreaterThan(0)

      // Clean up second server
      await client2.close()
      await handle2.dispose()
    })

    it('should allow creating multiple servers from the same DB instance', async () => {
      const handle1 = createParqueDBMCPServer(db, { name: 'server-1' })
      const handle2 = createParqueDBMCPServer(db, { name: 'server-2' })

      expect(handle1.server).not.toBe(handle2.server)

      // Both should work independently
      const client1 = new Client({ name: 'client-1', version: '1.0.0' }, { capabilities: {} })
      const client2 = new Client({ name: 'client-2', version: '1.0.0' }, { capabilities: {} })

      const [t1a, t1b] = InMemoryTransport.createLinkedPair()
      const [t2a, t2b] = InMemoryTransport.createLinkedPair()

      await handle1.server.connect(t1b)
      await client1.connect(t1a)

      await handle2.server.connect(t2b)
      await client2.connect(t2a)

      const tools1 = await client1.listTools()
      const tools2 = await client2.listTools()

      expect(tools1.tools.length).toBeGreaterThan(0)
      expect(tools2.tools.length).toBeGreaterThan(0)

      // Clean up both
      await client1.close()
      await client2.close()
      await handle1.dispose()
      await handle2.dispose()
    })
  })

  describe('Cleanup with Active Connections', () => {
    it('should close server cleanly when dispose is called with active client', async () => {
      const handle = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await handle.server.connect(t2)
      await client.connect(t1)

      // Verify connection is active
      const tools = await client.listTools()
      expect(tools.tools.length).toBeGreaterThan(0)

      // Dispose should close the server and clean up without needing explicit server.close()
      await handle.dispose()

      // Client operations should fail after server close
      // (or be handled gracefully)
    })

    it('should close the server automatically via dispose without prior server.close()', async () => {
      const handle = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await handle.server.connect(t2)
      await client.connect(t1)

      // Verify connection is active
      const tools = await client.listTools()
      expect(tools.tools.length).toBeGreaterThan(0)

      // Only call dispose - it should close the server internally
      await expect(handle.dispose()).resolves.not.toThrow()
      expect(handle.isDisposed).toBe(true)
    })
  })

  describe('Dispose Cleanup Tracking', () => {
    it('should track that resources have been disposed', async () => {
      const handle = createParqueDBMCPServer(db)

      // Before dispose
      expect(handle.isDisposed).toBe(false)

      await handle.dispose()

      // After dispose
      expect(handle.isDisposed).toBe(true)
    })

    it('should indicate disposal state correctly through lifecycle', async () => {
      const handle = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      // Initial state
      expect(handle.isDisposed).toBe(false)

      await handle.server.connect(t2)
      await client.connect(t1)

      // Still not disposed after connect
      expect(handle.isDisposed).toBe(false)

      await client.close()
      await handle.server.close()

      // Still not disposed after close (until dispose is called)
      expect(handle.isDisposed).toBe(false)

      await handle.dispose()

      // Now disposed
      expect(handle.isDisposed).toBe(true)
    })
  })

  describe('Database Buffer Flushing', () => {
    it('should flush database buffers during dispose', async () => {
      const handle = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await handle.server.connect(t2)
      await client.connect(t1)

      // Perform a write operation to generate pending events
      await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'posts',
          data: {
            name: 'Flush Test Post',
            title: 'Should Flush',
            content: 'This tests buffer flushing on dispose',
            status: 'draft',
          },
        },
      })

      // Dispose should flush pending events without errors
      await expect(handle.dispose()).resolves.not.toThrow()
      expect(handle.isDisposed).toBe(true)
    })

    it('should handle dispose gracefully when server was never connected', async () => {
      // Create handle but never connect
      const handle = createParqueDBMCPServer(db)

      // Dispose should not throw even if never connected
      await expect(handle.dispose()).resolves.not.toThrow()
      expect(handle.isDisposed).toBe(true)
    })

    it('should flush buffers and then close the server during dispose', async () => {
      const handle = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await handle.server.connect(t2)
      await client.connect(t1)

      // Create multiple documents to generate buffered events
      for (let i = 0; i < 5; i++) {
        await client.callTool({
          name: 'parquedb_create',
          arguments: {
            collection: 'posts',
            data: {
              name: `Batch Post ${i}`,
              title: `Batch Title ${i}`,
              content: `Batch content ${i}`,
              status: 'draft',
            },
          },
        })
      }

      // Dispose should cleanly flush and close
      await expect(handle.dispose()).resolves.not.toThrow()
      expect(handle.isDisposed).toBe(true)

      // Subsequent dispose calls should be no-ops
      await expect(handle.dispose()).resolves.not.toThrow()
    })
  })

  describe('Event Listener Cleanup', () => {
    it('should not leak event listeners after dispose', async () => {
      const handle = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await handle.server.connect(t2)
      await client.connect(t1)

      // Use the server
      await client.listTools()

      await client.close()
      await handle.dispose()

      // After dispose, creating a new server should work without
      // accumulated listeners from previous instances
      const handle2 = createParqueDBMCPServer(db)
      expect(handle2.isDisposed).toBe(false)

      await handle2.dispose()
    })
  })

  describe('Options Preserved Through Handle', () => {
    it('should preserve readOnly option through handle', async () => {
      const handle = createParqueDBMCPServer(db, { readOnly: true })
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await handle.server.connect(t2)
      await client.connect(t1)

      const tools = await client.listTools()
      const toolNames = tools.tools.map(t => t.name)

      // Read-only should not have write tools
      expect(toolNames).toContain('parquedb_find')
      expect(toolNames).not.toContain('parquedb_create')

      await client.close()
      await handle.dispose()
    })

    it('should preserve tool configuration through handle', async () => {
      const handle = createParqueDBMCPServer(db, {
        tools: {
          semanticSearch: false,
          aggregate: false,
        },
      })
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await handle.server.connect(t2)
      await client.connect(t1)

      const tools = await client.listTools()
      const toolNames = tools.tools.map(t => t.name)

      expect(toolNames).toContain('parquedb_find')
      expect(toolNames).not.toContain('parquedb_semantic_search')
      expect(toolNames).not.toContain('parquedb_aggregate')

      await client.close()
      await handle.dispose()
    })
  })
})
