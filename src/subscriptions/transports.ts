/**
 * Transport Implementations for Subscriptions
 *
 * Provides WebSocket and SSE (Server-Sent Events) implementations
 * for different runtime environments.
 */

import type { SubscriptionWriter, SubscriptionMessage } from './types'

// =============================================================================
// WebSocket Writer (Works in Workers and Node.js)
// =============================================================================

/**
 * WebSocket-based subscription writer
 *
 * Works with standard WebSocket API (Workers, browsers, Node.js ws)
 */
export class WebSocketWriter implements SubscriptionWriter {
  private ws: WebSocket
  private closed = false

  constructor(ws: WebSocket) {
    this.ws = ws

    // Track close state
    this.ws.addEventListener('close', () => {
      this.closed = true
    })
    this.ws.addEventListener('error', () => {
      this.closed = true
    })
  }

  async send(message: SubscriptionMessage): Promise<void> {
    if (this.closed || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket is not open')
    }

    this.ws.send(JSON.stringify(message))
  }

  async close(): Promise<void> {
    if (!this.closed && this.ws.readyState === WebSocket.OPEN) {
      this.ws.close(1000, 'Closing connection')
    }
    this.closed = true
  }

  isOpen(): boolean {
    return !this.closed && this.ws.readyState === WebSocket.OPEN
  }
}

// =============================================================================
// SSE Writer (Node.js with standard Response)
// =============================================================================

/**
 * Server-Sent Events writer using a writable stream
 *
 * Works with Node.js streams and Web Streams API
 */
export class SSEWriter implements SubscriptionWriter {
  private writer: WritableStreamDefaultWriter<Uint8Array>
  private encoder = new TextEncoder()
  private closed = false

  constructor(writable: WritableStream<Uint8Array>) {
    this.writer = writable.getWriter()
  }

  async send(message: SubscriptionMessage): Promise<void> {
    if (this.closed) {
      throw new Error('SSE connection is closed')
    }

    const data = JSON.stringify(message)
    const sseMessage = `data: ${data}\n\n`

    try {
      await this.writer.write(this.encoder.encode(sseMessage))
    } catch (error) {
      this.closed = true
      throw error
    }
  }

  async close(): Promise<void> {
    if (!this.closed) {
      try {
        await this.writer.close()
      } catch {
        // Ignore close errors
      }
      this.closed = true
    }
  }

  isOpen(): boolean {
    return !this.closed
  }
}

// =============================================================================
// Response SSE Writer (for Cloudflare Workers)
// =============================================================================

/**
 * Create an SSE Response for Cloudflare Workers
 *
 * @returns Object with Response and SSEWriter
 *
 * @example
 * ```typescript
 * const { response, writer } = createSSEResponse()
 * const connection = manager.addConnection(writer)
 * return response
 * ```
 */
export function createSSEResponse(): {
  response: Response
  writer: SSEWriter
} {
  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>()

  const response = new Response(readable, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no', // Disable nginx buffering
    },
  })

  const writer = new SSEWriter(writable)

  return { response, writer }
}

// =============================================================================
// Node.js HTTP Response Writer
// =============================================================================

/**
 * SSE writer for Node.js http.ServerResponse
 *
 * @example
 * ```typescript
 * import http from 'http'
 *
 * const server = http.createServer((req, res) => {
 *   if (req.url === '/events') {
 *     const writer = new NodeSSEWriter(res)
 *     const connection = manager.addConnection(writer)
 *   }
 * })
 * ```
 */
export class NodeSSEWriter implements SubscriptionWriter {
  private res: NodeResponseLike
  private closed = false

  constructor(res: NodeResponseLike) {
    this.res = res

    // Set SSE headers
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    })

    // Track close
    res.on('close', () => {
      this.closed = true
    })
    res.on('error', () => {
      this.closed = true
    })
  }

  async send(message: SubscriptionMessage): Promise<void> {
    if (this.closed) {
      throw new Error('Response is closed')
    }

    const data = JSON.stringify(message)
    this.res.write(`data: ${data}\n\n`)
  }

  async close(): Promise<void> {
    if (!this.closed) {
      this.res.end()
      this.closed = true
    }
  }

  isOpen(): boolean {
    return !this.closed
  }
}

/**
 * Minimal interface for Node.js HTTP response
 */
export interface NodeResponseLike {
  writeHead(statusCode: number, headers: Record<string, string>): void
  write(data: string): boolean
  end(): void
  on(event: 'close' | 'error', listener: () => void): void
}

// =============================================================================
// Mock Writer (for testing)
// =============================================================================

/**
 * Mock writer for testing subscriptions
 *
 * Collects all messages sent for later inspection.
 */
export class MockWriter implements SubscriptionWriter {
  public messages: SubscriptionMessage[] = []
  private closed = false

  async send(message: SubscriptionMessage): Promise<void> {
    if (this.closed) {
      throw new Error('Connection is closed')
    }
    this.messages.push(message)
  }

  async close(): Promise<void> {
    this.closed = true
  }

  isOpen(): boolean {
    return !this.closed
  }

  /**
   * Get all messages of a specific type
   */
  getMessagesOfType<T extends SubscriptionMessage['type']>(
    type: T
  ): Extract<SubscriptionMessage, { type: T }>[] {
    return this.messages.filter((m): m is Extract<SubscriptionMessage, { type: T }> => m.type === type)
  }

  /**
   * Clear all collected messages
   */
  clear(): void {
    this.messages = []
  }
}

// =============================================================================
// WebSocket Handler (for Workers Durable Objects)
// =============================================================================

/**
 * Configuration for WebSocket handler
 */
export interface WebSocketHandlerConfig {
  /** Handler for incoming messages */
  onMessage?: (connectionId: string, data: unknown) => void
  /** Handler for connection close */
  onClose?: (connectionId: string) => void
  /** Handler for errors */
  onError?: (connectionId: string, error: Error) => void
}

/**
 * Handle WebSocket upgrade and create a subscription connection
 *
 * For use with Cloudflare Workers Durable Objects.
 *
 * @example
 * ```typescript
 * export class SubscriptionDO extends DurableObject {
 *   private manager = new SubscriptionManager()
 *
 *   async fetch(request: Request): Promise<Response> {
 *     const upgradeHeader = request.headers.get('Upgrade')
 *     if (upgradeHeader === 'websocket') {
 *       return handleWebSocketUpgrade(request, this.manager, {
 *         onMessage: (connId, data) => {
 *           // Handle subscribe/unsubscribe messages
 *         }
 *       })
 *     }
 *     return new Response('Expected WebSocket', { status: 400 })
 *   }
 * }
 * ```
 */
export function handleWebSocketUpgrade(
  request: Request,
  manager: { addConnection: (writer: SubscriptionWriter) => { id: string }; removeConnection: (id: string) => Promise<void>; handlePing: (id: string) => void },
  config: WebSocketHandlerConfig = {}
): Response {
  const pair = new WebSocketPair()
  const [client, server] = [pair[0], pair[1]]

  // Accept the WebSocket
  server.accept()

  // Create writer and connection
  const writer = new WebSocketWriter(server)
  const connection = manager.addConnection(writer)
  const connectionId = connection.id

  // Handle messages
  server.addEventListener('message', (event) => {
    try {
      const data = JSON.parse(event.data as string)

      // Handle ping internally
      if (data.type === 'ping') {
        manager.handlePing(connectionId)
        return
      }

      config.onMessage?.(connectionId, data)
    } catch (error) {
      config.onError?.(connectionId, error as Error)
    }
  })

  // Handle close
  server.addEventListener('close', () => {
    manager.removeConnection(connectionId)
    config.onClose?.(connectionId)
  })

  // Handle error
  server.addEventListener('error', (event) => {
    const error = event instanceof ErrorEvent ? event.error : new Error('WebSocket error')
    config.onError?.(connectionId, error)
  })

  return new Response(null, {
    status: 101,
    webSocket: client,
  })
}
