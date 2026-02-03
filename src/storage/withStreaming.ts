/**
 * withStreaming - Factory function to add streaming capabilities to any StorageBackend
 *
 * This wrapper adds web standard ReadableStream/WritableStream support to any
 * existing StorageBackend implementation. It's useful when you have an existing
 * backend and want to add streaming without creating a new class.
 *
 * The streaming implementation uses the underlying backend's read/write methods
 * with chunking for efficient memory usage.
 */

import type {
  StorageBackend,
  StreamableBackend,
  StreamOptions,
  WriteOptions,
  FileStat,
  ListOptions,
  ListResult,
  WriteResult,
  RmdirOptions,
} from '../types/storage'

/**
 * Default chunk size for streaming reads (64KB)
 */
const DEFAULT_HIGH_WATER_MARK = 64 * 1024

/**
 * Options for the withStreaming wrapper
 */
export interface WithStreamingOptions {
  /**
   * Default chunk size for streaming reads
   * @default 65536 (64KB)
   */
  defaultHighWaterMark?: number | undefined
}

/**
 * Wrap any StorageBackend with streaming capabilities
 *
 * Creates a new backend that implements StreamableBackend by adding
 * createReadStream and createWriteStream methods to the base backend.
 *
 * @param backend - The underlying StorageBackend to wrap
 * @param options - Optional configuration for streaming behavior
 * @returns A StreamableBackend that wraps the original backend
 *
 * @example
 * ```typescript
 * import { MemoryBackend, withStreaming, isStreamable } from '@parquedb/storage'
 *
 * // Create a streamable version of any backend
 * const memoryBackend = new MemoryBackend()
 * const streamableBackend = withStreaming(memoryBackend)
 *
 * // Now it's streamable
 * console.log(isStreamable(streamableBackend)) // true
 *
 * // Use streaming APIs
 * const readStream = streamableBackend.createReadStream('file.txt')
 * const writeStream = streamableBackend.createWriteStream('output.txt')
 * await readStream.pipeTo(writeStream)
 * ```
 *
 * @example
 * ```typescript
 * // Custom chunk size
 * const backend = withStreaming(new MemoryBackend(), {
 *   defaultHighWaterMark: 1024 * 1024 // 1MB chunks
 * })
 * ```
 */
export function withStreaming(
  backend: StorageBackend,
  options?: WithStreamingOptions
): StreamableBackend {
  const defaultHighWaterMark = options?.defaultHighWaterMark ?? DEFAULT_HIGH_WATER_MARK

  return new StreamingWrapper(backend, defaultHighWaterMark)
}

/**
 * Internal wrapper class that adds streaming to any StorageBackend
 */
class StreamingWrapper implements StreamableBackend {
  readonly type: string

  constructor(
    private readonly backend: StorageBackend,
    private readonly defaultHighWaterMark: number
  ) {
    this.type = backend.type
  }

  // ===========================================================================
  // StreamableBackend Methods
  // ===========================================================================

  /**
   * Create a readable stream for a file
   *
   * Uses the underlying backend's read or readRange method to fetch data,
   * then streams it out in chunks.
   */
  createReadStream(path: string, options?: StreamOptions): ReadableStream<Uint8Array> {
    const start = options?.start ?? 0
    const end = options?.end
    const highWaterMark = options?.highWaterMark ?? this.defaultHighWaterMark

    let data: Uint8Array | null = null
    let offset = 0

    return new ReadableStream<Uint8Array>(
      {
        start: async (controller) => {
          try {
            // Read the data (with optional range)
            if (start === 0 && end === undefined) {
              data = await this.backend.read(path)
            } else if (end !== undefined) {
              data = await this.backend.readRange(path, start, end)
            } else {
              // start > 0, no end - need to read full then slice
              const fullData = await this.backend.read(path)
              if (start >= fullData.length) {
                data = new Uint8Array(0)
              } else {
                data = fullData.slice(start)
              }
            }
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
   * Collects all written chunks and writes them to the underlying backend
   * when the stream is closed.
   */
  createWriteStream(path: string, options?: WriteOptions): WritableStream<Uint8Array> {
    const chunks: Uint8Array[] = []
    const backend = this.backend

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
          await backend.write(path, data, options)
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
    return this.backend.read(path)
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    return this.backend.readRange(path, start, end)
  }

  async exists(path: string): Promise<boolean> {
    return this.backend.exists(path)
  }

  async stat(path: string): Promise<FileStat | null> {
    return this.backend.stat(path)
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    return this.backend.list(prefix, options)
  }

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    return this.backend.write(path, data, options)
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    return this.backend.writeAtomic(path, data, options)
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    return this.backend.append(path, data)
  }

  async delete(path: string): Promise<boolean> {
    return this.backend.delete(path)
  }

  async deletePrefix(prefix: string): Promise<number> {
    return this.backend.deletePrefix(prefix)
  }

  async mkdir(path: string): Promise<void> {
    return this.backend.mkdir(path)
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    return this.backend.rmdir(path, options)
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    return this.backend.writeConditional(path, data, expectedVersion, options)
  }

  async copy(source: string, dest: string): Promise<void> {
    return this.backend.copy(source, dest)
  }

  async move(source: string, dest: string): Promise<void> {
    return this.backend.move(source, dest)
  }
}
