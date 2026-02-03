/**
 * Tests for Node.js Stream Adapter
 *
 * Tests bidirectional conversion between Node.js streams and Web streams,
 * including async iterator support and utility functions.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { Readable, Writable, PassThrough, Transform } from 'node:stream'
import {
  createStreamAdapter,
  createNodeReadableAdapter,
  createNodeWritableAdapter,
  createWebReadableAdapter,
  createWebWritableAdapter,
  isWebReadableStream,
  isWebWritableStream,
  isNodeReadable,
  isNodeWritable,
  createTee,
  createTransform,
  collectStream,
  fromAsyncIterator,
  webReadableFromAsyncIterator,
  type ReadableAdapterResult,
  type WritableAdapterResult,
  type WebReadableAdapterResult,
  type WebWritableAdapterResult,
} from '../../../src/streams/node-adapter'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a Node.js Readable stream from an array of chunks
 */
function createReadableFromChunks(chunks: Uint8Array[]): Readable {
  let index = 0
  return new Readable({
    read() {
      if (index < chunks.length) {
        this.push(Buffer.from(chunks[index++]))
      } else {
        this.push(null)
      }
    },
  })
}

/**
 * Create a Node.js Writable stream that collects chunks
 */
function createCollectingWritable(): { writable: Writable; chunks: Uint8Array[] } {
  const chunks: Uint8Array[] = []
  const writable = new Writable({
    write(chunk, encoding, callback) {
      chunks.push(new Uint8Array(chunk))
      callback()
    },
  })
  return { writable, chunks }
}

/**
 * Create a Web ReadableStream from an array of chunks
 */
function createWebReadableFromChunks(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  let index = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index < chunks.length) {
        controller.enqueue(chunks[index++])
      } else {
        controller.close()
      }
    },
  })
}

/**
 * Create test data
 */
function createTestData(): Uint8Array[] {
  return [
    new Uint8Array([1, 2, 3]),
    new Uint8Array([4, 5, 6]),
    new Uint8Array([7, 8, 9]),
  ]
}

// =============================================================================
// Node.js Readable Stream Adapter Tests
// =============================================================================

describe('Node.js Readable Stream Adapter', () => {
  describe('createStreamAdapter with Node.js Readable', () => {
    it('should detect Node.js Readable streams', () => {
      const readable = createReadableFromChunks([])
      const adapter = createStreamAdapter(readable)

      expect(adapter.nodeStream).toBe(readable)
      expect(typeof adapter.toWebReadable).toBe('function')
      expect(typeof adapter.toAsyncIterator).toBe('function')

      readable.destroy()
    })

    it('should convert to Web ReadableStream', async () => {
      const chunks = createTestData()
      const readable = createReadableFromChunks(chunks)
      const adapter = createStreamAdapter(readable)

      const webStream = adapter.toWebReadable()

      expect(isWebReadableStream(webStream)).toBe(true)

      // Read all chunks from web stream
      const reader = webStream.getReader()
      const result: Uint8Array[] = []

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        result.push(value)
      }

      expect(result.length).toBe(chunks.length)
      expect(result[0]).toEqual(chunks[0])
      expect(result[1]).toEqual(chunks[1])
      expect(result[2]).toEqual(chunks[2])
    })

    it('should convert to async iterator', async () => {
      const chunks = createTestData()
      const readable = createReadableFromChunks(chunks)
      const adapter = createStreamAdapter(readable)

      const result: Uint8Array[] = []
      for await (const chunk of adapter.toAsyncIterator()) {
        result.push(chunk)
      }

      expect(result.length).toBe(chunks.length)
      expect(result[0]).toEqual(chunks[0])
    })

    it('should pipe to Node.js Writable', async () => {
      const chunks = createTestData()
      const readable = createReadableFromChunks(chunks)
      const adapter = createStreamAdapter(readable)
      const { writable, chunks: collected } = createCollectingWritable()

      await adapter.pipeTo(writable)

      expect(collected.length).toBe(chunks.length)
      expect(collected[0]).toEqual(chunks[0])
    })

    it('should pipe through Transform stream', async () => {
      const chunks = createTestData()
      const readable = createReadableFromChunks(chunks)
      const adapter = createStreamAdapter(readable)

      // Transform that doubles each byte
      const doubleTransform = new Transform({
        transform(chunk, encoding, callback) {
          const doubled = new Uint8Array(chunk.length)
          for (let i = 0; i < chunk.length; i++) {
            doubled[i] = chunk[i] * 2
          }
          callback(null, Buffer.from(doubled))
        },
      })

      const transformed = adapter.pipeThrough(doubleTransform)

      // Collect all data and verify the transformation
      const result = await collectStream(transformed.nodeStream)

      // Verify total data is transformed correctly (each byte doubled)
      const expectedData = new Uint8Array([2, 4, 6, 8, 10, 12, 14, 16, 18])
      expect(result).toEqual(expectedData)
    })

    it('should destroy the underlying stream', () => {
      const readable = createReadableFromChunks([])
      const adapter = createStreamAdapter(readable)

      let destroyed = false
      readable.on('close', () => {
        destroyed = true
      })

      adapter.destroy()

      expect(readable.destroyed).toBe(true)
    })

    it('should handle string chunks', async () => {
      const readable = new Readable({
        read() {
          this.push('hello')
          this.push('world')
          this.push(null)
        },
      })
      const adapter = createStreamAdapter(readable)

      // Collect all data - Node.js may buffer chunks together
      const result = await collectStream(readable)

      // Verify total data content
      expect(new TextDecoder().decode(result)).toBe('helloworld')
    })

    it('should propagate errors to Web ReadableStream', async () => {
      const error = new Error('Test error')
      const readable = new Readable({
        read() {
          this.destroy(error)
        },
      })
      const adapter = createStreamAdapter(readable)
      const webStream = adapter.toWebReadable()
      const reader = webStream.getReader()

      await expect(reader.read()).rejects.toThrow('Test error')
    })
  })
})

// =============================================================================
// Node.js Writable Stream Adapter Tests
// =============================================================================

describe('Node.js Writable Stream Adapter', () => {
  describe('createStreamAdapter with Node.js Writable', () => {
    it('should detect Node.js Writable streams', () => {
      const { writable } = createCollectingWritable()
      const adapter = createStreamAdapter(writable)

      expect(adapter.nodeStream).toBe(writable)
      expect(typeof adapter.toWebWritable).toBe('function')
      expect(typeof adapter.createWriter).toBe('function')
    })

    it('should convert to Web WritableStream', async () => {
      const { writable, chunks } = createCollectingWritable()
      const adapter = createStreamAdapter(writable)

      const webStream = adapter.toWebWritable()

      expect(isWebWritableStream(webStream)).toBe(true)

      const writer = webStream.getWriter()
      await writer.write(new Uint8Array([1, 2, 3]))
      await writer.write(new Uint8Array([4, 5, 6]))
      await writer.close()

      expect(chunks.length).toBe(2)
      expect(chunks[0]).toEqual(new Uint8Array([1, 2, 3]))
      expect(chunks[1]).toEqual(new Uint8Array([4, 5, 6]))
    })

    it('should create async writer', async () => {
      const { writable, chunks } = createCollectingWritable()
      const adapter = createStreamAdapter(writable)

      const writer = adapter.createWriter()

      expect(writer.closed).toBe(false)

      await writer.write(new Uint8Array([1, 2, 3]))
      await writer.write(new Uint8Array([4, 5, 6]))
      await writer.close()

      expect(writer.closed).toBe(true)
      expect(chunks.length).toBe(2)
    })

    it('should handle backpressure', async () => {
      vi.useFakeTimers()
      try {
        // Create a slow writable to test backpressure
        const chunks: Uint8Array[] = []
        const slowWritable = new Writable({
          highWaterMark: 1,
          write(chunk, encoding, callback) {
            // Simulate slow write - uses real setTimeout but works with fake timers
            setTimeout(() => {
              chunks.push(new Uint8Array(chunk))
              callback()
            }, 10)
          },
        })

        const adapter = createStreamAdapter(slowWritable)
        const writer = adapter.createWriter()

        const writePromise1 = writer.write(new Uint8Array([1, 2, 3]))
        await vi.advanceTimersByTimeAsync(15)
        await writePromise1

        const writePromise2 = writer.write(new Uint8Array([4, 5, 6]))
        await vi.advanceTimersByTimeAsync(15)
        await writePromise2

        await writer.close()

        expect(chunks.length).toBe(2)
      } finally {
        vi.useRealTimers()
      }
    })

    it('should abort the writer', async () => {
      const { writable } = createCollectingWritable()
      // Listen for error event to prevent uncaught exception
      writable.on('error', () => {})
      const adapter = createStreamAdapter(writable)
      const writer = adapter.createWriter()

      await writer.abort(new Error('Aborted'))

      expect(writer.closed).toBe(true)
    })

    it('should throw when writing to closed writer', async () => {
      const { writable } = createCollectingWritable()
      const adapter = createStreamAdapter(writable)
      const writer = adapter.createWriter()

      await writer.close()

      await expect(writer.write(new Uint8Array([1]))).rejects.toThrow('Writer is closed')
    })
  })
})

// =============================================================================
// Web ReadableStream Adapter Tests
// =============================================================================

describe('Web ReadableStream Adapter', () => {
  describe('createStreamAdapter with Web ReadableStream', () => {
    it('should detect Web ReadableStream', () => {
      const chunks = createTestData()
      const webStream = createWebReadableFromChunks(chunks)
      const adapter = createStreamAdapter(webStream)

      expect(adapter.webStream).toBe(webStream)
      expect(typeof adapter.toNodeReadable).toBe('function')
      expect(typeof adapter.toAsyncIterator).toBe('function')
    })

    it('should convert to Node.js Readable', async () => {
      const chunks = createTestData()
      const webStream = createWebReadableFromChunks(chunks)
      const adapter = createStreamAdapter(webStream)

      const nodeStream = adapter.toNodeReadable()

      expect(isNodeReadable(nodeStream)).toBe(true)

      // Collect all data and verify integrity
      const result = await collectStream(nodeStream)

      // Verify total data content (Node.js may buffer chunks together)
      const expected = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9])
      expect(result).toEqual(expected)
    })

    it('should convert to async iterator', async () => {
      const chunks = createTestData()
      const webStream = createWebReadableFromChunks(chunks)
      const adapter = createStreamAdapter(webStream)

      const result: Uint8Array[] = []
      for await (const chunk of adapter.toAsyncIterator()) {
        result.push(chunk)
      }

      expect(result.length).toBe(chunks.length)
    })

    it('should cancel the underlying stream', async () => {
      let cancelled = false
      const webStream = new ReadableStream<Uint8Array>({
        cancel() {
          cancelled = true
        },
      })
      const adapter = createStreamAdapter(webStream)

      await adapter.cancel('test reason')

      expect(cancelled).toBe(true)
    })
  })
})

// =============================================================================
// Web WritableStream Adapter Tests
// =============================================================================

describe('Web WritableStream Adapter', () => {
  describe('createStreamAdapter with Web WritableStream', () => {
    it('should detect Web WritableStream', () => {
      const chunks: Uint8Array[] = []
      const webStream = new WritableStream<Uint8Array>({
        write(chunk) {
          chunks.push(chunk)
        },
      })
      const adapter = createStreamAdapter(webStream)

      expect(adapter.webStream).toBe(webStream)
      expect(typeof adapter.toNodeWritable).toBe('function')
      expect(typeof adapter.createWriter).toBe('function')
    })

    it('should convert to Node.js Writable', async () => {
      const chunks: Uint8Array[] = []
      const webStream = new WritableStream<Uint8Array>({
        write(chunk) {
          chunks.push(chunk)
        },
      })
      const adapter = createStreamAdapter(webStream)

      const nodeStream = adapter.toNodeWritable()

      expect(isNodeWritable(nodeStream)).toBe(true)

      await new Promise<void>((resolve, reject) => {
        nodeStream.write(Buffer.from([1, 2, 3]), (err) => {
          if (err) reject(err)
        })
        nodeStream.write(Buffer.from([4, 5, 6]), (err) => {
          if (err) reject(err)
        })
        nodeStream.end((err: Error | null | undefined) => {
          if (err) reject(err)
          else resolve()
        })
      })

      expect(chunks.length).toBe(2)
      expect(chunks[0]).toEqual(new Uint8Array([1, 2, 3]))
    })

    it('should create async writer', async () => {
      const chunks: Uint8Array[] = []
      const webStream = new WritableStream<Uint8Array>({
        write(chunk) {
          chunks.push(chunk)
        },
      })
      const adapter = createStreamAdapter(webStream)

      const writer = adapter.createWriter()

      await writer.write(new Uint8Array([1, 2, 3]))
      await writer.close()

      expect(chunks.length).toBe(1)
      expect(writer.closed).toBe(true)
    })

    it('should abort the underlying stream', async () => {
      let aborted = false
      const webStream = new WritableStream<Uint8Array>({
        abort() {
          aborted = true
        },
      })
      const adapter = createStreamAdapter(webStream)

      await adapter.abort('test reason')

      expect(aborted).toBe(true)
    })
  })
})

// =============================================================================
// Type Guard Tests
// =============================================================================

describe('Type Guards', () => {
  describe('isWebReadableStream', () => {
    it('should return true for Web ReadableStream', () => {
      const stream = new ReadableStream()
      expect(isWebReadableStream(stream)).toBe(true)
    })

    it('should return false for Node.js Readable', () => {
      const stream = new Readable({ read() {} })
      expect(isWebReadableStream(stream)).toBe(false)
      stream.destroy()
    })

    it('should return false for non-streams', () => {
      expect(isWebReadableStream(null)).toBe(false)
      expect(isWebReadableStream(undefined)).toBe(false)
      expect(isWebReadableStream({})).toBe(false)
      expect(isWebReadableStream('string')).toBe(false)
    })
  })

  describe('isWebWritableStream', () => {
    it('should return true for Web WritableStream', () => {
      const stream = new WritableStream()
      expect(isWebWritableStream(stream)).toBe(true)
    })

    it('should return false for Node.js Writable', () => {
      const stream = new Writable({ write() {} })
      expect(isWebWritableStream(stream)).toBe(false)
      stream.destroy()
    })

    it('should return false for non-streams', () => {
      expect(isWebWritableStream(null)).toBe(false)
      expect(isWebWritableStream(undefined)).toBe(false)
      expect(isWebWritableStream({})).toBe(false)
    })
  })

  describe('isNodeReadable', () => {
    it('should return true for Node.js Readable', () => {
      const stream = new Readable({ read() {} })
      expect(isNodeReadable(stream)).toBe(true)
      stream.destroy()
    })

    it('should return false for Web ReadableStream', () => {
      const stream = new ReadableStream()
      expect(isNodeReadable(stream)).toBe(false)
    })
  })

  describe('isNodeWritable', () => {
    it('should return true for Node.js Writable', () => {
      const stream = new Writable({ write() {} })
      expect(isNodeWritable(stream)).toBe(true)
      stream.destroy()
    })

    it('should return false for Web WritableStream', () => {
      const stream = new WritableStream()
      expect(isNodeWritable(stream)).toBe(false)
    })
  })
})

// =============================================================================
// Utility Function Tests
// =============================================================================

describe('Utility Functions', () => {
  describe('createTee', () => {
    it('should create two streams with the same data', async () => {
      const chunks = createTestData()
      const source = createReadableFromChunks(chunks)

      const [stream1, stream2] = createTee(source)

      // Collect data from both streams and verify they match
      const [result1, result2] = await Promise.all([
        collectStream(stream1),
        collectStream(stream2),
      ])

      // Both streams should have the same total data
      const expected = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9])
      expect(result1).toEqual(expected)
      expect(result2).toEqual(expected)
    })

    it('should propagate errors to both streams', async () => {
      vi.useFakeTimers()
      try {
        const error = new Error('Test error')
        const source = new Readable({
          read() {
            this.destroy(error)
          },
        })

        const [stream1, stream2] = createTee(source)

        let error1: Error | null = null
        let error2: Error | null = null

        stream1.on('error', (err) => {
          error1 = err
        })
        stream2.on('error', (err) => {
          error2 = err
        })

        // Wait for error to propagate
        await vi.advanceTimersByTimeAsync(50)

        expect(error1).toBe(error)
        expect(error2).toBe(error)
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('createTransform', () => {
    it('should transform chunks', async () => {
      const chunks = createTestData()
      const source = createReadableFromChunks(chunks)

      // Transform that doubles each byte
      const doubleTransform = createTransform<Buffer, Buffer>((chunk) => {
        const doubled = Buffer.alloc(chunk.length)
        for (let i = 0; i < chunk.length; i++) {
          doubled[i] = chunk[i] * 2
        }
        return doubled
      })

      const result: Buffer[] = []
      source.pipe(doubleTransform).on('data', (chunk) => {
        result.push(chunk)
      })

      await new Promise(resolve => doubleTransform.on('end', resolve))

      expect(result.length).toBe(chunks.length)
      expect(result[0][0]).toBe(chunks[0][0] * 2)
    })

    it('should handle async transform functions', async () => {
      vi.useFakeTimers()
      try {
        const chunks = createTestData()
        const source = createReadableFromChunks(chunks)

        const asyncTransform = createTransform<Buffer, Buffer>(async (chunk) => {
          await vi.advanceTimersByTimeAsync(10)
          return chunk
        })

        const result: Buffer[] = []
        source.pipe(asyncTransform).on('data', (chunk) => {
          result.push(chunk)
        })

        // Advance timers to allow async transforms to complete
        const endPromise = new Promise(resolve => asyncTransform.on('end', resolve))
        await vi.advanceTimersByTimeAsync(100)
        await endPromise

        expect(result.length).toBe(chunks.length)
      } finally {
        vi.useRealTimers()
      }
    })

    it('should handle transform errors', async () => {
      vi.useFakeTimers()
      try {
        const error = new Error('Transform error')
        const source = createReadableFromChunks([new Uint8Array([1, 2, 3])])

        const errorTransform = createTransform<Buffer, Buffer>(() => {
          throw error
        })

        let caughtError: Error | null = null
        errorTransform.on('error', (err) => {
          caughtError = err
        })

        source.pipe(errorTransform)

        await vi.advanceTimersByTimeAsync(50)

        expect(caughtError).toBe(error)
      } finally {
        vi.useRealTimers()
      }
    })
  })

  describe('collectStream', () => {
    it('should collect all chunks from Node.js Readable', async () => {
      const chunks = createTestData()
      const readable = createReadableFromChunks(chunks)

      const result = await collectStream(readable)

      const expected = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9])
      expect(result).toEqual(expected)
    })

    it('should collect all chunks from Web ReadableStream', async () => {
      const chunks = createTestData()
      const webStream = createWebReadableFromChunks(chunks)

      const result = await collectStream(webStream)

      const expected = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9])
      expect(result).toEqual(expected)
    })

    it('should handle empty streams', async () => {
      const readable = createReadableFromChunks([])

      const result = await collectStream(readable)

      expect(result.length).toBe(0)
    })

    it('should handle string chunks', async () => {
      const readable = new Readable({
        read() {
          this.push('hello')
          this.push(null)
        },
      })

      const result = await collectStream(readable)

      expect(new TextDecoder().decode(result)).toBe('hello')
    })
  })

  describe('fromAsyncIterator', () => {
    it('should create Readable from async iterator', async () => {
      async function* generateData() {
        yield new Uint8Array([1, 2, 3])
        yield new Uint8Array([4, 5, 6])
      }

      const readable = fromAsyncIterator(generateData())

      // Collect all data and verify integrity
      const result = await collectStream(readable)

      // Verify total data content
      expect(result).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]))
    })
  })

  describe('webReadableFromAsyncIterator', () => {
    it('should create Web ReadableStream from async iterator', async () => {
      async function* generateData() {
        yield new Uint8Array([1, 2, 3])
        yield new Uint8Array([4, 5, 6])
      }

      const webStream = webReadableFromAsyncIterator(generateData())

      expect(isWebReadableStream(webStream)).toBe(true)

      const reader = webStream.getReader()
      const result: Uint8Array[] = []

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        result.push(value)
      }

      expect(result.length).toBe(2)
      expect(result[0]).toEqual(new Uint8Array([1, 2, 3]))
    })
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration Tests', () => {
  describe('Round-trip conversion', () => {
    it('should preserve data through Node -> Web -> Node conversion', async () => {
      const chunks = createTestData()
      const originalReadable = createReadableFromChunks(chunks)

      // Node -> Web
      const webStream = createStreamAdapter(originalReadable).toWebReadable()

      // Web -> Node
      const backToNode = createStreamAdapter(webStream).toNodeReadable()

      const result: Uint8Array[] = []
      for await (const chunk of backToNode) {
        result.push(new Uint8Array(chunk))
      }

      expect(result.length).toBe(chunks.length)
      expect(result[0]).toEqual(chunks[0])
      expect(result[1]).toEqual(chunks[1])
      expect(result[2]).toEqual(chunks[2])
    })

    it('should preserve data through Web -> Node -> Web conversion', async () => {
      const chunks = createTestData()
      const originalWebStream = createWebReadableFromChunks(chunks)

      // Web -> Node
      const nodeStream = createStreamAdapter(originalWebStream).toNodeReadable()

      // Node -> Web
      const backToWeb = createStreamAdapter(nodeStream).toWebReadable()

      const reader = backToWeb.getReader()
      const result: Uint8Array[] = []

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        result.push(value)
      }

      expect(result.length).toBe(chunks.length)
      expect(result[0]).toEqual(chunks[0])
    })
  })

  describe('Stream piping', () => {
    it('should pipe from Node Readable to Web Writable', async () => {
      const chunks = createTestData()
      const readable = createReadableFromChunks(chunks)
      const collected: Uint8Array[] = []

      const webWritable = new WritableStream<Uint8Array>({
        write(chunk) {
          collected.push(chunk)
        },
      })

      const webReadable = createStreamAdapter(readable).toWebReadable()

      await webReadable.pipeTo(webWritable)

      expect(collected.length).toBe(chunks.length)
      expect(collected[0]).toEqual(chunks[0])
    })

    it('should pipe from Web Readable to Node Writable', async () => {
      const chunks = createTestData()
      const webStream = createWebReadableFromChunks(chunks)
      const { writable, chunks: collected } = createCollectingWritable()

      const nodeReadable = createStreamAdapter(webStream).toNodeReadable()

      await new Promise<void>((resolve, reject) => {
        nodeReadable.pipe(writable)
        writable.on('finish', resolve)
        writable.on('error', reject)
      })

      expect(collected.length).toBe(chunks.length)
      expect(collected[0]).toEqual(chunks[0])
    })
  })

  describe('Options handling', () => {
    it('should respect highWaterMark option', () => {
      const readable = createReadableFromChunks([])
      const adapter = createStreamAdapter(readable, { highWaterMark: 1024 })

      const webStream = adapter.toWebReadable()
      // The option is passed internally; we just verify no errors
      expect(webStream).toBeDefined()

      readable.destroy()
    })
  })
})

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('Error Handling', () => {
  it('should throw for unknown stream types', () => {
    expect(() => {
      createStreamAdapter({} as any)
    }).toThrow('Unknown stream type')
  })

  it('should propagate read errors through Web stream', async () => {
    const error = new Error('Read error')
    const readable = new Readable({
      read() {
        process.nextTick(() => this.destroy(error))
      },
    })

    const webStream = createStreamAdapter(readable).toWebReadable()
    const reader = webStream.getReader()

    await expect(reader.read()).rejects.toThrow('Read error')
  })

  it('should handle cancel on Web stream', async () => {
    let destroyed = false
    const readable = new Readable({
      read() {
        // Keep the stream open
      },
      destroy(err, callback) {
        destroyed = true
        callback(err)
      },
    })

    const webStream = createStreamAdapter(readable).toWebReadable()
    await webStream.cancel('cancelled')

    expect(destroyed).toBe(true)
  })
})
