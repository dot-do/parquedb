/**
 * StreamableMemoryBackend - In-memory implementation of StreamableBackend
 *
 * Extends MemoryBackend with streaming read/write support using
 * web standard ReadableStream and WritableStream APIs.
 *
 * This implementation is useful for:
 * - Testing streaming functionality
 * - Browser environments
 * - Scenarios where file data needs to be processed incrementally
 */

import type {
  StreamableBackend,
  StreamOptions,
  WriteOptions,
  FileStat,
  ListOptions,
  ListResult,
  WriteResult,
  RmdirOptions,
} from '../types/storage'
import { MemoryBackend } from './MemoryBackend'

/**
 * Default chunk size for streaming reads (64KB)
 */
const DEFAULT_HIGH_WATER_MARK = 64 * 1024

/**
 * In-memory storage backend with streaming support
 *
 * Provides all MemoryBackend functionality plus:
 * - createReadStream: Creates a ReadableStream for reading file contents
 * - createWriteStream: Creates a WritableStream for writing file contents
 *
 * @example
 * ```typescript
 * const backend = new StreamableMemoryBackend()
 *
 * // Write using stream
 * const writeStream = backend.createWriteStream('data.txt')
 * const writer = writeStream.getWriter()
 * await writer.write(new TextEncoder().encode('Hello'))
 * await writer.close()
 *
 * // Read using stream
 * const readStream = backend.createReadStream('data.txt')
 * const reader = readStream.getReader()
 * const { value } = await reader.read()
 * console.log(new TextDecoder().decode(value)) // "Hello"
 * ```
 */
export class StreamableMemoryBackend implements StreamableBackend {
  readonly type = 'memory'

  /**
   * Delegate to MemoryBackend for all non-streaming operations
   */
  private readonly delegate: MemoryBackend

  constructor() {
    this.delegate = new MemoryBackend()
  }

  // ===========================================================================
  // StreamableBackend Methods
  // ===========================================================================

  /**
   * Create a readable stream for a file
   *
   * Returns a web standard ReadableStream that yields Uint8Array chunks.
   * The stream can be consumed using a reader or piped to a WritableStream.
   *
   * @param path - Path to the file to read
   * @param options - Optional streaming options
   * @param options.start - Byte offset to start reading from (inclusive)
   * @param options.end - Byte offset to stop reading at (exclusive)
   * @param options.highWaterMark - Chunk size in bytes (default: 64KB)
   * @returns A ReadableStream of Uint8Array chunks
   *
   * @example
   * ```typescript
   * // Read entire file
   * const stream = backend.createReadStream('file.txt')
   *
   * // Read byte range
   * const rangeStream = backend.createReadStream('file.txt', { start: 100, end: 200 })
   *
   * // Control chunk size
   * const chunkedStream = backend.createReadStream('file.txt', { highWaterMark: 1024 })
   * ```
   */
  createReadStream(path: string, options?: StreamOptions): ReadableStream<Uint8Array> {
    const start = options?.start ?? 0
    const end = options?.end
    const highWaterMark = options?.highWaterMark ?? DEFAULT_HIGH_WATER_MARK

    let data: Uint8Array | null = null
    let offset = start
    let fileSize = 0

    return new ReadableStream<Uint8Array>(
      {
        start: async (controller) => {
          try {
            // Read the file data
            const fullData = await this.delegate.read(path)
            fileSize = fullData.length

            // Apply start/end range
            const effectiveEnd = end !== undefined ? Math.min(end, fileSize) : fileSize
            if (start >= fileSize) {
              // Start is beyond file size, return empty
              data = new Uint8Array(0)
            } else {
              data = fullData.slice(start, effectiveEnd)
            }

            // Reset offset to 0 since we've already sliced
            offset = 0
          } catch (error) {
            controller.error(error)
          }
        },

        pull: (controller) => {
          if (!data) {
            controller.close()
            return
          }

          if (offset >= data.length) {
            controller.close()
            return
          }

          // Calculate chunk size
          const chunkEnd = Math.min(offset + highWaterMark, data.length)
          const chunk = data.slice(offset, chunkEnd)
          offset = chunkEnd

          controller.enqueue(chunk)
        },
      },
      {
        highWaterMark: 1, // Number of chunks to buffer
      }
    )
  }

  /**
   * Create a writable stream for a file
   *
   * Returns a web standard WritableStream that accepts Uint8Array chunks.
   * The file is created when the stream is closed successfully.
   *
   * @param path - Path where the file will be written
   * @param options - Optional write options (contentType, metadata, etc.)
   * @returns A WritableStream accepting Uint8Array chunks
   *
   * @example
   * ```typescript
   * // Write to file
   * const stream = backend.createWriteStream('output.txt')
   * const writer = stream.getWriter()
   * await writer.write(new TextEncoder().encode('chunk 1'))
   * await writer.write(new TextEncoder().encode('chunk 2'))
   * await writer.close()
   *
   * // Write with options
   * const jsonStream = backend.createWriteStream('data.json', {
   *   contentType: 'application/json'
   * })
   * ```
   */
  createWriteStream(path: string, options?: WriteOptions): WritableStream<Uint8Array> {
    const chunks: Uint8Array[] = []

    return new WritableStream<Uint8Array>(
      {
        write: (chunk) => {
          chunks.push(chunk)
        },

        close: async () => {
          // Concatenate all chunks
          const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
          const data = new Uint8Array(totalLength)
          let offset = 0
          for (const chunk of chunks) {
            data.set(chunk, offset)
            offset += chunk.length
          }

          // Write the complete file
          await this.delegate.write(path, data, options)
        },

        abort: (_reason) => {
          // Clear collected chunks on abort
          chunks.length = 0
        },
      },
      {
        highWaterMark: 1, // Number of chunks to buffer
      }
    )
  }

  // ===========================================================================
  // Delegated StorageBackend Methods
  // ===========================================================================

  async read(path: string): Promise<Uint8Array> {
    return this.delegate.read(path)
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    return this.delegate.readRange(path, start, end)
  }

  async exists(path: string): Promise<boolean> {
    return this.delegate.exists(path)
  }

  async stat(path: string): Promise<FileStat | null> {
    return this.delegate.stat(path)
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    return this.delegate.list(prefix, options)
  }

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    return this.delegate.write(path, data, options)
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    return this.delegate.writeAtomic(path, data, options)
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    return this.delegate.append(path, data)
  }

  async delete(path: string): Promise<boolean> {
    return this.delegate.delete(path)
  }

  async deletePrefix(prefix: string): Promise<number> {
    return this.delegate.deletePrefix(prefix)
  }

  async mkdir(path: string): Promise<void> {
    return this.delegate.mkdir(path)
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    return this.delegate.rmdir(path, options)
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    return this.delegate.writeConditional(path, data, expectedVersion, options)
  }

  async copy(source: string, dest: string): Promise<void> {
    return this.delegate.copy(source, dest)
  }

  async move(source: string, dest: string): Promise<void> {
    return this.delegate.move(source, dest)
  }
}
