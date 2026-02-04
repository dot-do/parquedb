/**
 * Transport Implementations Test Suite
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  MockWriter,
  SSEWriter,
  createSSEResponse,
} from '@/subscriptions'
import type { SubscriptionMessage } from '@/subscriptions'

// =============================================================================
// MockWriter Tests
// =============================================================================

describe('MockWriter', () => {
  let writer: MockWriter

  beforeEach(() => {
    writer = new MockWriter()
  })

  it('starts in open state', () => {
    expect(writer.isOpen()).toBe(true)
  })

  it('collects sent messages', async () => {
    const message: SubscriptionMessage = {
      type: 'connected',
      connectionId: 'conn1',
    }

    await writer.send(message)

    expect(writer.messages).toHaveLength(1)
    expect(writer.messages[0]).toEqual(message)
  })

  it('collects multiple messages', async () => {
    await writer.send({ type: 'connected', connectionId: 'conn1' })
    await writer.send({ type: 'subscribed', subscriptionId: 'sub1', ns: 'posts' })
    await writer.send({ type: 'pong', ts: 12345 })

    expect(writer.messages).toHaveLength(3)
  })

  it('filters messages by type', async () => {
    await writer.send({ type: 'connected', connectionId: 'conn1' })
    await writer.send({ type: 'subscribed', subscriptionId: 'sub1', ns: 'posts' })
    await writer.send({ type: 'subscribed', subscriptionId: 'sub2', ns: 'users' })
    await writer.send({ type: 'pong', ts: 12345 })

    const subscribed = writer.getMessagesOfType('subscribed')
    expect(subscribed).toHaveLength(2)
    expect(subscribed[0].subscriptionId).toBe('sub1')
    expect(subscribed[1].subscriptionId).toBe('sub2')
  })

  it('clears messages', async () => {
    await writer.send({ type: 'connected', connectionId: 'conn1' })
    await writer.send({ type: 'pong', ts: 12345 })

    writer.clear()

    expect(writer.messages).toHaveLength(0)
  })

  it('closes the connection', async () => {
    await writer.close()

    expect(writer.isOpen()).toBe(false)
  })

  it('throws when sending after close', async () => {
    await writer.close()

    await expect(writer.send({ type: 'pong', ts: 12345 })).rejects.toThrow(
      'Connection is closed'
    )
  })
})

// =============================================================================
// SSEWriter Tests
// =============================================================================

describe('SSEWriter', () => {
  it('writes messages in SSE format', async () => {
    const chunks: Uint8Array[] = []
    const writable = new WritableStream<Uint8Array>({
      write(chunk) {
        chunks.push(chunk)
      },
    })

    const writer = new SSEWriter(writable)

    await writer.send({ type: 'pong', ts: 12345 })

    const decoder = new TextDecoder()
    const text = decoder.decode(chunks[0])

    expect(text).toBe('data: {"type":"pong","ts":12345}\n\n')
  })

  it('writes multiple messages', async () => {
    const chunks: Uint8Array[] = []
    const writable = new WritableStream<Uint8Array>({
      write(chunk) {
        chunks.push(chunk)
      },
    })

    const writer = new SSEWriter(writable)

    await writer.send({ type: 'connected', connectionId: 'conn1' })
    await writer.send({ type: 'pong', ts: 12345 })

    expect(chunks).toHaveLength(2)
  })

  it('closes the stream', async () => {
    let closed = false
    const writable = new WritableStream<Uint8Array>({
      close() {
        closed = true
      },
    })

    const writer = new SSEWriter(writable)
    await writer.close()

    expect(closed).toBe(true)
    expect(writer.isOpen()).toBe(false)
  })

  it('throws when sending after close', async () => {
    const writable = new WritableStream<Uint8Array>()
    const writer = new SSEWriter(writable)

    await writer.close()

    await expect(writer.send({ type: 'pong', ts: 12345 })).rejects.toThrow(
      'SSE connection is closed'
    )
  })
})

// =============================================================================
// createSSEResponse Tests
// =============================================================================

describe('createSSEResponse', () => {
  it('creates a Response and writer', () => {
    const { response, writer } = createSSEResponse()

    expect(response).toBeInstanceOf(Response)
    expect(writer).toBeInstanceOf(SSEWriter)
  })

  it('sets correct headers', () => {
    const { response } = createSSEResponse()

    expect(response.headers.get('Content-Type')).toBe('text/event-stream')
    expect(response.headers.get('Cache-Control')).toBe('no-cache')
    expect(response.headers.get('Connection')).toBe('keep-alive')
  })

  it('writer can send messages', async () => {
    const { response, writer } = createSSEResponse()

    // Consume the readable side to prevent TransformStream backpressure
    // from blocking writes in the Node.js test environment
    const reader = response.body!.getReader()
    const readPromise = (async () => {
      const chunks: Uint8Array[] = []
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        chunks.push(value)
      }
      return chunks
    })()

    await writer.send({ type: 'connected', connectionId: 'conn1' })
    await writer.send({ type: 'pong', ts: 12345 })

    expect(writer.isOpen()).toBe(true)

    await writer.close()
    expect(writer.isOpen()).toBe(false)

    // Verify the chunks were written
    const chunks = await readPromise
    expect(chunks.length).toBe(2)

    const decoder = new TextDecoder()
    expect(decoder.decode(chunks[0])).toContain('"type":"connected"')
    expect(decoder.decode(chunks[1])).toContain('"type":"pong"')
  })
})

// =============================================================================
// Integration: MockWriter with Subscription Messages
// =============================================================================

describe('MockWriter integration', () => {
  it('handles all subscription message types', async () => {
    const writer = new MockWriter()

    // Send all message types
    await writer.send({ type: 'connected', connectionId: 'conn1' })
    await writer.send({ type: 'subscribed', subscriptionId: 'sub1', ns: 'posts' })
    await writer.send({ type: 'unsubscribed', subscriptionId: 'sub1' })
    await writer.send({ type: 'error', error: 'Something went wrong', code: 'ERR_TEST' })
    await writer.send({ type: 'pong', ts: 12345 })
    await writer.send({
      type: 'change',
      data: {
        id: 'evt1',
        ts: 12345,
        op: 'CREATE',
        ns: 'posts',
        entityId: 'post1',
        fullId: 'posts/post1' as any,
        after: { title: 'New Post' },
      },
    })

    expect(writer.messages).toHaveLength(6)

    // Filter by type
    expect(writer.getMessagesOfType('connected')).toHaveLength(1)
    expect(writer.getMessagesOfType('subscribed')).toHaveLength(1)
    expect(writer.getMessagesOfType('unsubscribed')).toHaveLength(1)
    expect(writer.getMessagesOfType('error')).toHaveLength(1)
    expect(writer.getMessagesOfType('pong')).toHaveLength(1)
    expect(writer.getMessagesOfType('change')).toHaveLength(1)
  })
})

// =============================================================================
// WebSocketWriter Tests (Mocked WebSocket)
// =============================================================================

describe('WebSocketWriter', () => {
  // Create a mock WebSocket for testing
  function createMockWebSocket() {
    const listeners: Record<string, Array<(event: any) => void>> = {}
    const sentMessages: string[] = []

    const mockWs = {
      readyState: 1, // WebSocket.OPEN
      addEventListener(event: string, handler: (event: any) => void) {
        listeners[event] = listeners[event] || []
        listeners[event].push(handler)
      },
      removeEventListener(event: string, handler: (event: any) => void) {
        const handlers = listeners[event] || []
        const idx = handlers.indexOf(handler)
        if (idx >= 0) handlers.splice(idx, 1)
      },
      send(data: string) {
        sentMessages.push(data)
      },
      close(_code?: number, _reason?: string) {
        mockWs.readyState = 3 // WebSocket.CLOSED
        const closeHandlers = listeners['close'] || []
        closeHandlers.forEach((h) => h({}))
      },
      // Test helpers
      _sentMessages: sentMessages,
      _triggerClose() {
        mockWs.readyState = 3
        const closeHandlers = listeners['close'] || []
        closeHandlers.forEach((h) => h({}))
      },
      _triggerError() {
        const errorHandlers = listeners['error'] || []
        errorHandlers.forEach((h) => h(new Error('WebSocket error')))
      },
    }

    return mockWs as unknown as WebSocket & {
      _sentMessages: string[]
      _triggerClose: () => void
      _triggerError: () => void
    }
  }

  it('sends messages as JSON', async () => {
    const { WebSocketWriter } = await import('@/subscriptions/transports')
    const mockWs = createMockWebSocket()
    const writer = new WebSocketWriter(mockWs as WebSocket)

    await writer.send({ type: 'pong', ts: 12345 })

    expect(mockWs._sentMessages).toHaveLength(1)
    expect(mockWs._sentMessages[0]).toBe('{"type":"pong","ts":12345}')
  })

  it('reports open state correctly', async () => {
    const { WebSocketWriter } = await import('@/subscriptions/transports')
    const mockWs = createMockWebSocket()
    const writer = new WebSocketWriter(mockWs as WebSocket)

    expect(writer.isOpen()).toBe(true)

    mockWs._triggerClose()

    expect(writer.isOpen()).toBe(false)
  })

  it('closes the WebSocket', async () => {
    const { WebSocketWriter } = await import('@/subscriptions/transports')
    const mockWs = createMockWebSocket()
    const writer = new WebSocketWriter(mockWs as WebSocket)

    await writer.close()

    expect(writer.isOpen()).toBe(false)
  })

  it('handles close event', async () => {
    const { WebSocketWriter } = await import('@/subscriptions/transports')
    const mockWs = createMockWebSocket()
    const writer = new WebSocketWriter(mockWs as WebSocket)

    expect(writer.isOpen()).toBe(true)

    mockWs._triggerClose()

    expect(writer.isOpen()).toBe(false)
  })

  it('handles error event', async () => {
    const { WebSocketWriter } = await import('@/subscriptions/transports')
    const mockWs = createMockWebSocket()
    const writer = new WebSocketWriter(mockWs as WebSocket)

    expect(writer.isOpen()).toBe(true)

    mockWs._triggerError()

    expect(writer.isOpen()).toBe(false)
  })

  it('throws when sending on closed WebSocket', async () => {
    const { WebSocketWriter } = await import('@/subscriptions/transports')
    const mockWs = createMockWebSocket()
    const writer = new WebSocketWriter(mockWs as WebSocket)

    mockWs._triggerClose()

    await expect(writer.send({ type: 'pong', ts: 12345 })).rejects.toThrow(
      'WebSocket is not open'
    )
  })

  it('handles close when already closed', async () => {
    const { WebSocketWriter } = await import('@/subscriptions/transports')
    const mockWs = createMockWebSocket()
    const writer = new WebSocketWriter(mockWs as WebSocket)

    mockWs._triggerClose()

    // Should not throw
    await expect(writer.close()).resolves.not.toThrow()
  })
})

// =============================================================================
// NodeSSEWriter Tests (Mocked Node Response)
// =============================================================================

describe('NodeSSEWriter', () => {
  function createMockNodeResponse() {
    const listeners: Record<string, Array<() => void>> = {}
    const writtenData: string[] = []
    let headersWritten = false
    let ended = false

    return {
      writeHead(statusCode: number, headers: Record<string, string>) {
        headersWritten = true
        return { statusCode, headers }
      },
      write(data: string): boolean {
        writtenData.push(data)
        return true
      },
      end() {
        ended = true
      },
      on(event: 'close' | 'error', listener: () => void) {
        listeners[event] = listeners[event] || []
        listeners[event].push(listener)
      },
      // Test helpers
      _writtenData: writtenData,
      _isHeadersWritten: () => headersWritten,
      _isEnded: () => ended,
      _triggerClose() {
        const handlers = listeners['close'] || []
        handlers.forEach((h) => h())
      },
      _triggerError() {
        const handlers = listeners['error'] || []
        handlers.forEach((h) => h())
      },
    }
  }

  it('writes SSE headers on construction', async () => {
    const { NodeSSEWriter } = await import('@/subscriptions/transports')
    const mockRes = createMockNodeResponse()

    new NodeSSEWriter(mockRes)

    expect(mockRes._isHeadersWritten()).toBe(true)
  })

  it('sends messages in SSE format', async () => {
    const { NodeSSEWriter } = await import('@/subscriptions/transports')
    const mockRes = createMockNodeResponse()
    const writer = new NodeSSEWriter(mockRes)

    await writer.send({ type: 'pong', ts: 12345 })

    expect(mockRes._writtenData).toHaveLength(1)
    expect(mockRes._writtenData[0]).toBe('data: {"type":"pong","ts":12345}\n\n')
  })

  it('sends multiple messages', async () => {
    const { NodeSSEWriter } = await import('@/subscriptions/transports')
    const mockRes = createMockNodeResponse()
    const writer = new NodeSSEWriter(mockRes)

    await writer.send({ type: 'connected', connectionId: 'conn1' })
    await writer.send({ type: 'pong', ts: 12345 })

    expect(mockRes._writtenData).toHaveLength(2)
  })

  it('reports open state', async () => {
    const { NodeSSEWriter } = await import('@/subscriptions/transports')
    const mockRes = createMockNodeResponse()
    const writer = new NodeSSEWriter(mockRes)

    expect(writer.isOpen()).toBe(true)
  })

  it('closes the response', async () => {
    const { NodeSSEWriter } = await import('@/subscriptions/transports')
    const mockRes = createMockNodeResponse()
    const writer = new NodeSSEWriter(mockRes)

    await writer.close()

    expect(mockRes._isEnded()).toBe(true)
    expect(writer.isOpen()).toBe(false)
  })

  it('handles close event', async () => {
    const { NodeSSEWriter } = await import('@/subscriptions/transports')
    const mockRes = createMockNodeResponse()
    const writer = new NodeSSEWriter(mockRes)

    expect(writer.isOpen()).toBe(true)

    mockRes._triggerClose()

    expect(writer.isOpen()).toBe(false)
  })

  it('handles error event', async () => {
    const { NodeSSEWriter } = await import('@/subscriptions/transports')
    const mockRes = createMockNodeResponse()
    const writer = new NodeSSEWriter(mockRes)

    expect(writer.isOpen()).toBe(true)

    mockRes._triggerError()

    expect(writer.isOpen()).toBe(false)
  })

  it('throws when sending after close', async () => {
    const { NodeSSEWriter } = await import('@/subscriptions/transports')
    const mockRes = createMockNodeResponse()
    const writer = new NodeSSEWriter(mockRes)

    mockRes._triggerClose()

    await expect(writer.send({ type: 'pong', ts: 12345 })).rejects.toThrow(
      'Response is closed'
    )
  })

  it('handles close when already closed', async () => {
    const { NodeSSEWriter } = await import('@/subscriptions/transports')
    const mockRes = createMockNodeResponse()
    const writer = new NodeSSEWriter(mockRes)

    mockRes._triggerClose()

    // Should not throw
    await expect(writer.close()).resolves.not.toThrow()
  })
})

// =============================================================================
// SSEWriter Error Handling Tests
// =============================================================================

describe('SSEWriter error handling', () => {
  it('marks closed on write error', async () => {
    let writeCount = 0
    const writable = new WritableStream<Uint8Array>({
      write() {
        writeCount++
        if (writeCount > 1) {
          throw new Error('Write failed')
        }
      },
    })

    const writer = new SSEWriter(writable)

    // First write succeeds
    await writer.send({ type: 'connected', connectionId: 'conn1' })
    expect(writer.isOpen()).toBe(true)

    // Second write fails
    await expect(writer.send({ type: 'pong', ts: 12345 })).rejects.toThrow('Write failed')
    expect(writer.isOpen()).toBe(false)
  })

  it('handles close errors gracefully', async () => {
    const writable = new WritableStream<Uint8Array>({
      close() {
        throw new Error('Close failed')
      },
    })

    const writer = new SSEWriter(writable)

    // Should not throw
    await expect(writer.close()).resolves.not.toThrow()
    expect(writer.isOpen()).toBe(false)
  })
})

// =============================================================================
// MockWriter Edge Cases
// =============================================================================

describe('MockWriter edge cases', () => {
  it('can send and receive error messages with code', async () => {
    const writer = new MockWriter()

    await writer.send({ type: 'error', error: 'Test error', code: 'TEST_CODE' })

    const errors = writer.getMessagesOfType('error')
    expect(errors).toHaveLength(1)
    expect(errors[0].error).toBe('Test error')
    expect(errors[0].code).toBe('TEST_CODE')
  })

  it('can send and receive error messages without code', async () => {
    const writer = new MockWriter()

    await writer.send({ type: 'error', error: 'Test error' })

    const errors = writer.getMessagesOfType('error')
    expect(errors).toHaveLength(1)
    expect(errors[0].code).toBeUndefined()
  })

  it('handles change messages with all fields', async () => {
    const writer = new MockWriter()

    await writer.send({
      type: 'change',
      data: {
        id: 'evt1',
        ts: 12345,
        op: 'UPDATE',
        ns: 'posts',
        entityId: 'post1',
        fullId: 'posts/post1' as any,
        before: { title: 'Old Title' },
        after: { title: 'New Title' },
        actor: 'users/admin',
        metadata: { source: 'api' },
      },
    })

    const changes = writer.getMessagesOfType('change')
    expect(changes).toHaveLength(1)
    expect(changes[0].data.before).toEqual({ title: 'Old Title' })
    expect(changes[0].data.after).toEqual({ title: 'New Title' })
    expect(changes[0].data.actor).toBe('users/admin')
    expect(changes[0].data.metadata).toEqual({ source: 'api' })
  })

  it('returns empty array for non-existent message types', async () => {
    const writer = new MockWriter()

    await writer.send({ type: 'connected', connectionId: 'conn1' })

    const changes = writer.getMessagesOfType('change')
    expect(changes).toHaveLength(0)
  })

  it('can clear and resend messages', async () => {
    const writer = new MockWriter()

    await writer.send({ type: 'connected', connectionId: 'conn1' })
    expect(writer.messages).toHaveLength(1)

    writer.clear()
    expect(writer.messages).toHaveLength(0)

    await writer.send({ type: 'pong', ts: 12345 })
    expect(writer.messages).toHaveLength(1)
  })
})
