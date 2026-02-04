/**
 * Node.js Stream Adapter for ParqueDB
 *
 * Provides bidirectional conversion between Node.js streams and ParqueDB's
 * internal stream formats (Web Streams API and async iterators).
 *
 * ## Features
 *
 * - Convert Node.js Readable streams to Web ReadableStream
 * - Convert Node.js Writable streams to Web WritableStream
 * - Convert Web streams to Node.js streams
 * - Async iterator support for both directions
 * - Backpressure handling
 * - Error propagation
 *
 * ## Usage
 *
 * ```typescript
 * import { createStreamAdapter } from 'parquedb/streams'
 * import { createReadStream, createWriteStream } from 'node:fs'
 *
 * // Convert Node.js readable to Web ReadableStream
 * const nodeReadable = createReadStream('data.parquet')
 * const webReadable = createStreamAdapter(nodeReadable).toWebReadable()
 *
 * // Convert Node.js writable to Web WritableStream
 * const nodeWritable = createWriteStream('output.parquet')
 * const webWritable = createStreamAdapter(nodeWritable).toWebWritable()
 *
 * // Convert Web stream to Node.js stream
 * const webStream = new ReadableStream({ ... })
 * const nodeStream = createStreamAdapter(webStream).toNodeReadable()
 *
 * // Use with async iterators
 * const adapter = createStreamAdapter(nodeReadable)
 * for await (const chunk of adapter.toAsyncIterator()) {
 *   console.log('Chunk:', chunk)
 * }
 * ```
 *
 * @module streams/node-adapter
 */

import { Readable, Writable, Transform, PassThrough } from 'node:stream'
import type { TransformOptions } from 'node:stream'

// =============================================================================
// Types
// =============================================================================

/**
 * Options for creating stream adapters
 */
export interface NodeStreamAdapterOptions {
  /**
   * High water mark for buffering (bytes)
   * @default 16384 (16KB)
   */
  highWaterMark?: number | undefined

  /**
   * Object mode - when true, streams operate on objects instead of bytes
   * @default false
   */
  objectMode?: boolean | undefined

  /**
   * Encoding for string data
   * @default undefined (binary)
   */
  encoding?: BufferEncoding | undefined

  /**
   * Whether to handle errors by propagating them
   * @default true
   */
  propagateErrors?: boolean | undefined

  /**
   * Custom signal for aborting operations
   */
  signal?: AbortSignal | undefined
}

/**
 * Result of converting a Node.js Readable stream
 */
export interface ReadableAdapterResult {
  /** Original Node.js Readable stream */
  nodeStream: Readable

  /** Convert to Web ReadableStream */
  toWebReadable(): ReadableStream<Uint8Array>

  /** Convert to async iterator */
  toAsyncIterator(): AsyncIterableIterator<Uint8Array>

  /** Pipe to a Node.js Writable stream */
  pipeTo(destination: Writable): Promise<void>

  /** Pipe through a Node.js Transform stream */
  pipeThrough<T extends Transform>(transform: T): ReadableAdapterResult

  /** Destroy the underlying stream */
  destroy(error?: Error): void
}

/**
 * Result of converting a Node.js Writable stream
 */
export interface WritableAdapterResult {
  /** Original Node.js Writable stream */
  nodeStream: Writable

  /** Convert to Web WritableStream */
  toWebWritable(): WritableStream<Uint8Array>

  /** Create an async write function */
  createWriter(): AsyncWriter

  /** Destroy the underlying stream */
  destroy(error?: Error): void
}

/**
 * Result of converting a Web ReadableStream
 */
export interface WebReadableAdapterResult {
  /** Original Web ReadableStream */
  webStream: ReadableStream<Uint8Array>

  /** Convert to Node.js Readable stream */
  toNodeReadable(): Readable

  /** Convert to async iterator */
  toAsyncIterator(): AsyncIterableIterator<Uint8Array>

  /** Cancel the underlying stream */
  cancel(reason?: string): Promise<void>
}

/**
 * Result of converting a Web WritableStream
 */
export interface WebWritableAdapterResult {
  /** Original Web WritableStream */
  webStream: WritableStream<Uint8Array>

  /** Convert to Node.js Writable stream */
  toNodeWritable(): Writable

  /** Create an async write function */
  createWriter(): AsyncWriter

  /** Abort the underlying stream */
  abort(reason?: string): Promise<void>
}

/**
 * Async writer interface for writing chunks
 */
export interface AsyncWriter {
  /** Write a chunk of data */
  write(chunk: Uint8Array): Promise<void>

  /** Close the writer */
  close(): Promise<void>

  /** Abort with an error */
  abort(reason?: Error): Promise<void>

  /** Check if the writer is ready for more data */
  get ready(): Promise<void>

  /** Check if the writer is closed */
  get closed(): boolean

  /** Get the desired buffer size */
  get desiredSize(): number | null
}

// =============================================================================
// Default Configuration
// =============================================================================

const DEFAULT_OPTIONS: Required<Omit<NodeStreamAdapterOptions, 'signal' | 'encoding'>> = {
  highWaterMark: 16384,
  objectMode: false,
  propagateErrors: true,
}

// =============================================================================
// Node.js Readable Stream Adapter
// =============================================================================

/**
 * Creates an adapter for a Node.js Readable stream
 */
function createNodeReadableAdapter(
  stream: Readable,
  options: NodeStreamAdapterOptions = {}
): ReadableAdapterResult {
  const opts = { ...DEFAULT_OPTIONS, ...options }

  return {
    nodeStream: stream,

    toWebReadable(): ReadableStream<Uint8Array> {
      return new ReadableStream<Uint8Array>({
        start(controller) {
          stream.on('data', (chunk: Buffer | Uint8Array | string) => {
            const data = typeof chunk === 'string'
              ? new TextEncoder().encode(chunk)
              : chunk instanceof Buffer
                ? new Uint8Array(chunk)
                : chunk
            controller.enqueue(data)
          })

          stream.on('end', () => {
            controller.close()
          })

          stream.on('error', (err) => {
            controller.error(err)
          })
        },

        cancel(reason) {
          stream.destroy(reason ? new Error(String(reason)) : undefined)
        },
      }, opts.highWaterMark !== undefined ? { highWaterMark: opts.highWaterMark } : undefined)
    },

    async *toAsyncIterator(): AsyncIterableIterator<Uint8Array> {
      for await (const chunk of stream) {
        const data = typeof chunk === 'string'
          ? new TextEncoder().encode(chunk)
          : chunk instanceof Buffer
            ? new Uint8Array(chunk)
            : chunk as Uint8Array
        yield data
      }
    },

    async pipeTo(destination: Writable): Promise<void> {
      return new Promise((resolve, reject) => {
        stream.pipe(destination)
        stream.on('error', reject)
        destination.on('error', reject)
        destination.on('finish', resolve)
      })
    },

    pipeThrough<T extends Transform>(transform: T): ReadableAdapterResult {
      const piped = stream.pipe(transform)
      return createNodeReadableAdapter(piped, options)
    },

    destroy(error?: Error): void {
      stream.destroy(error)
    },
  }
}

// =============================================================================
// Node.js Writable Stream Adapter
// =============================================================================

/**
 * Creates an adapter for a Node.js Writable stream
 */
function createNodeWritableAdapter(
  stream: Writable,
  options: NodeStreamAdapterOptions = {}
): WritableAdapterResult {
  const opts = { ...DEFAULT_OPTIONS, ...options }

  return {
    nodeStream: stream,

    toWebWritable(): WritableStream<Uint8Array> {
      return new WritableStream<Uint8Array>({
        write(chunk) {
          return new Promise((resolve, reject) => {
            const ok = stream.write(chunk, (err) => {
              if (err) reject(err)
            })
            if (ok) {
              resolve()
            } else {
              stream.once('drain', resolve)
            }
          })
        },

        close() {
          return new Promise((resolve, reject) => {
            stream.end((err: Error | null | undefined) => {
              if (err) reject(err)
              else resolve()
            })
          })
        },

        abort(reason) {
          stream.destroy(reason ? new Error(String(reason)) : undefined)
        },
      }, {
        ...(opts.highWaterMark !== undefined ? { highWaterMark: opts.highWaterMark } : {}),
      })
    },

    createWriter(): AsyncWriter {
      let closed = false
      let pendingDrain: Promise<void> | null = null

      return {
        async write(chunk: Uint8Array): Promise<void> {
          if (closed) {
            throw new Error('Writer is closed')
          }

          return new Promise((resolve, reject) => {
            const ok = stream.write(chunk, (err) => {
              if (err) reject(err)
            })
            if (ok) {
              resolve()
            } else {
              pendingDrain = new Promise((drainResolve) => {
                stream.once('drain', drainResolve)
              })
              pendingDrain.then(resolve)
            }
          })
        },

        async close(): Promise<void> {
          if (closed) return
          closed = true

          return new Promise((resolve, reject) => {
            stream.end((err: Error | null | undefined) => {
              if (err) reject(err)
              else resolve()
            })
          })
        },

        async abort(reason?: Error): Promise<void> {
          closed = true
          stream.destroy(reason)
        },

        get ready(): Promise<void> {
          return pendingDrain ?? Promise.resolve()
        },

        get closed(): boolean {
          return closed
        },

        get desiredSize(): number | null {
          return stream.writableHighWaterMark - stream.writableLength
        },
      }
    },

    destroy(error?: Error): void {
      stream.destroy(error)
    },
  }
}

// =============================================================================
// Web ReadableStream Adapter
// =============================================================================

/**
 * Creates an adapter for a Web ReadableStream
 */
function createWebReadableAdapter(
  stream: ReadableStream<Uint8Array>,
  options: NodeStreamAdapterOptions = {}
): WebReadableAdapterResult {
  const opts = { ...DEFAULT_OPTIONS, ...options }

  return {
    webStream: stream,

    toNodeReadable(): Readable {
      const reader = stream.getReader()

      return new Readable({
        highWaterMark: opts.highWaterMark,
        objectMode: opts.objectMode,

        async read() {
          try {
            const { done, value } = await reader.read()
            if (done) {
              this.push(null)
            } else {
              this.push(Buffer.from(value))
            }
          } catch (err) {
            this.destroy(err instanceof Error ? err : new Error(String(err)))
          }
        },

        destroy(error, callback) {
          reader.cancel(error?.message).then(
            () => callback(null),
            (err) => callback(err)
          )
        },
      })
    },

    async *toAsyncIterator(): AsyncIterableIterator<Uint8Array> {
      const reader = stream.getReader()
      try {
        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          yield value
        }
      } finally {
        reader.releaseLock()
      }
    },

    async cancel(reason?: string): Promise<void> {
      await stream.cancel(reason)
    },
  }
}

// =============================================================================
// Web WritableStream Adapter
// =============================================================================

/**
 * Creates an adapter for a Web WritableStream
 */
function createWebWritableAdapter(
  stream: WritableStream<Uint8Array>,
  options: NodeStreamAdapterOptions = {}
): WebWritableAdapterResult {
  const opts = { ...DEFAULT_OPTIONS, ...options }

  return {
    webStream: stream,

    toNodeWritable(): Writable {
      const writer = stream.getWriter()

      return new Writable({
        highWaterMark: opts.highWaterMark,
        objectMode: opts.objectMode,

        async write(chunk, _encoding, callback) {
          try {
            const data = Buffer.isBuffer(chunk)
              ? new Uint8Array(chunk)
              : typeof chunk === 'string'
                ? new TextEncoder().encode(chunk)
                : chunk
            await writer.write(data)
            callback()
          } catch (err) {
            callback(err instanceof Error ? err : new Error(String(err)))
          }
        },

        async final(callback) {
          try {
            await writer.close()
            callback()
          } catch (err) {
            callback(err instanceof Error ? err : new Error(String(err)))
          }
        },

        destroy(error, callback) {
          writer.abort(error?.message).then(
            () => callback(null),
            (err) => callback(err)
          )
        },
      })
    },

    createWriter(): AsyncWriter {
      const writer = stream.getWriter()
      let closed = false

      return {
        async write(chunk: Uint8Array): Promise<void> {
          if (closed) {
            throw new Error('Writer is closed')
          }
          await writer.write(chunk)
        },

        async close(): Promise<void> {
          if (closed) return
          closed = true
          await writer.close()
        },

        async abort(reason?: Error): Promise<void> {
          closed = true
          await writer.abort(reason?.message)
        },

        get ready(): Promise<void> {
          return writer.ready
        },

        get closed(): boolean {
          return closed
        },

        get desiredSize(): number | null {
          return writer.desiredSize
        },
      }
    },

    async abort(reason?: string): Promise<void> {
      await stream.abort(reason)
    },
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Input type for createStreamAdapter
 */
export type StreamInput =
  | Readable
  | Writable
  | ReadableStream<Uint8Array>
  | WritableStream<Uint8Array>

/**
 * Output type based on input
 */
export type StreamAdapterResult<T extends StreamInput> =
  T extends Readable ? ReadableAdapterResult :
  T extends Writable ? WritableAdapterResult :
  T extends ReadableStream<Uint8Array> ? WebReadableAdapterResult :
  T extends WritableStream<Uint8Array> ? WebWritableAdapterResult :
  never

/**
 * Create a stream adapter for Node.js or Web streams
 *
 * Automatically detects the stream type and returns the appropriate adapter.
 *
 * @example
 * ```typescript
 * import { createStreamAdapter } from 'parquedb/streams'
 * import { createReadStream } from 'node:fs'
 *
 * // From Node.js Readable
 * const nodeReadable = createReadStream('data.parquet')
 * const adapter = createStreamAdapter(nodeReadable)
 * const webStream = adapter.toWebReadable()
 *
 * // From Web ReadableStream
 * const webReadable = new ReadableStream({ ... })
 * const adapter2 = createStreamAdapter(webReadable)
 * const nodeStream = adapter2.toNodeReadable()
 * ```
 */
export function createStreamAdapter<T extends StreamInput>(
  stream: T,
  options?: NodeStreamAdapterOptions
): StreamAdapterResult<T>

export function createStreamAdapter(
  stream: StreamInput,
  options: NodeStreamAdapterOptions = {}
): ReadableAdapterResult | WritableAdapterResult | WebReadableAdapterResult | WebWritableAdapterResult {
  // Check if it's a Node.js Readable stream
  if (stream instanceof Readable) {
    return createNodeReadableAdapter(stream, options)
  }

  // Check if it's a Node.js Writable stream
  if (stream instanceof Writable) {
    return createNodeWritableAdapter(stream, options)
  }

  // Check if it's a Web ReadableStream
  if (isWebReadableStream(stream)) {
    return createWebReadableAdapter(stream, options)
  }

  // Check if it's a Web WritableStream
  if (isWebWritableStream(stream)) {
    return createWebWritableAdapter(stream, options)
  }

  throw new Error('Unknown stream type: expected Node.js stream or Web stream')
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a value is a Web ReadableStream
 */
export function isWebReadableStream(value: unknown): value is ReadableStream<Uint8Array> {
  return (
    value !== null &&
    typeof value === 'object' &&
    'getReader' in value &&
    typeof (value as ReadableStream).getReader === 'function' &&
    'cancel' in value &&
    typeof (value as ReadableStream).cancel === 'function'
  )
}

/**
 * Check if a value is a Web WritableStream
 */
export function isWebWritableStream(value: unknown): value is WritableStream<Uint8Array> {
  return (
    value !== null &&
    typeof value === 'object' &&
    'getWriter' in value &&
    typeof (value as WritableStream).getWriter === 'function' &&
    'abort' in value &&
    typeof (value as WritableStream).abort === 'function'
  )
}

/**
 * Check if a value is a Node.js Readable stream
 */
export function isNodeReadable(value: unknown): value is Readable {
  return value instanceof Readable
}

/**
 * Check if a value is a Node.js Writable stream
 */
export function isNodeWritable(value: unknown): value is Writable {
  return value instanceof Writable
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Create a pass-through Transform stream for tee-ing data
 *
 * @example
 * ```typescript
 * const [stream1, stream2] = createTee(sourceStream)
 * // Both stream1 and stream2 receive the same data
 * ```
 */
export function createTee(source: Readable): [Readable, Readable] {
  const pass1 = new PassThrough()
  const pass2 = new PassThrough()

  source.on('data', (chunk) => {
    pass1.write(chunk)
    pass2.write(chunk)
  })

  source.on('end', () => {
    pass1.end()
    pass2.end()
  })

  source.on('error', (err) => {
    pass1.destroy(err)
    pass2.destroy(err)
  })

  return [pass1, pass2]
}

/**
 * Create a transform stream that applies a function to each chunk
 *
 * @example
 * ```typescript
 * const upperCase = createTransform((chunk) => chunk.toString().toUpperCase())
 * source.pipe(upperCase).pipe(dest)
 * ```
 */
export function createTransform<TInput = Uint8Array, TOutput = Uint8Array>(
  transformFn: (chunk: TInput) => TOutput | Promise<TOutput>,
  options?: TransformOptions
): Transform {
  return new Transform({
    ...options,
    async transform(chunk, _encoding, callback) {
      try {
        const result = await transformFn(chunk)
        callback(null, result)
      } catch (err) {
        callback(err instanceof Error ? err : new Error(String(err)))
      }
    },
  })
}

/**
 * Collect all chunks from a readable stream into a single Uint8Array
 *
 * @example
 * ```typescript
 * const data = await collectStream(readableStream)
 * ```
 */
export async function collectStream(stream: Readable | ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const chunks: Uint8Array[] = []

  if (stream instanceof Readable) {
    for await (const chunk of stream) {
      chunks.push(
        Buffer.isBuffer(chunk)
          ? new Uint8Array(chunk)
          : typeof chunk === 'string'
            ? new TextEncoder().encode(chunk)
            : chunk
      )
    }
  } else {
    const reader = stream.getReader()
    try {
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        chunks.push(value)
      }
    } finally {
      reader.releaseLock()
    }
  }

  // Concatenate all chunks
  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
  const result = new Uint8Array(totalLength)
  let offset = 0
  for (const chunk of chunks) {
    result.set(chunk, offset)
    offset += chunk.length
  }

  return result
}

/**
 * Create a readable stream from an async iterator
 *
 * @example
 * ```typescript
 * async function* generateData() {
 *   yield new Uint8Array([1, 2, 3])
 *   yield new Uint8Array([4, 5, 6])
 * }
 *
 * const stream = fromAsyncIterator(generateData())
 * ```
 */
export function fromAsyncIterator(
  iterator: AsyncIterable<Uint8Array>,
  options?: NodeStreamAdapterOptions
): Readable {
  const opts = { ...DEFAULT_OPTIONS, ...options }

  return Readable.from(iterator, {
    highWaterMark: opts.highWaterMark,
    objectMode: opts.objectMode,
  })
}

/**
 * Create a Web ReadableStream from an async iterator
 *
 * @example
 * ```typescript
 * async function* generateData() {
 *   yield new Uint8Array([1, 2, 3])
 *   yield new Uint8Array([4, 5, 6])
 * }
 *
 * const stream = webReadableFromAsyncIterator(generateData())
 * ```
 */
export function webReadableFromAsyncIterator(
  iterator: AsyncIterable<Uint8Array>,
  options?: NodeStreamAdapterOptions
): ReadableStream<Uint8Array> {
  const opts = { ...DEFAULT_OPTIONS, ...options }
  const asyncIterator = iterator[Symbol.asyncIterator]()

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await asyncIterator.next()
        if (done) {
          controller.close()
        } else {
          controller.enqueue(value)
        }
      } catch (err) {
        controller.error(err)
      }
    },

    async cancel(reason) {
      await asyncIterator.return?.(reason)
    },
  }, opts.highWaterMark !== undefined ? { highWaterMark: opts.highWaterMark } : undefined)
}

// =============================================================================
// Exports
// =============================================================================

export {
  createNodeReadableAdapter,
  createNodeWritableAdapter,
  createWebReadableAdapter,
  createWebWritableAdapter,
}
