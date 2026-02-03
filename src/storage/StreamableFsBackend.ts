/**
 * StreamableFsBackend - Node.js filesystem implementation of StreamableBackend
 *
 * Extends FsBackend with streaming read/write support using Node.js native streams
 * wrapped in web standard ReadableStream and WritableStream APIs.
 *
 * This implementation provides:
 * - Efficient memory usage for large files
 * - Native filesystem streaming performance
 * - Web standard stream API for portability
 */

import { createReadStream, createWriteStream as fsCreateWriteStream } from 'node:fs'
import { promises as fs } from 'node:fs'
import { Readable, Writable } from 'node:stream'
import { dirname } from 'node:path'
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
import { FsBackend } from './FsBackend'
import { NotFoundError } from './errors'

/**
 * Default chunk size for streaming reads (64KB)
 */
const DEFAULT_HIGH_WATER_MARK = 64 * 1024

/**
 * Node.js filesystem storage backend with streaming support
 *
 * Uses Node.js native streams for efficient I/O, wrapped in web standard
 * ReadableStream/WritableStream APIs for portability.
 *
 * @example
 * ```typescript
 * const backend = new StreamableFsBackend('/data')
 *
 * // Stream a large file
 * const readStream = backend.createReadStream('large-file.parquet')
 * const writeStream = backend.createWriteStream('output.parquet')
 * await readStream.pipeTo(writeStream)
 *
 * // Read a specific byte range
 * const footerStream = backend.createReadStream('file.parquet', {
 *   start: fileSize - 8,
 *   end: fileSize
 * })
 * ```
 */
export class StreamableFsBackend implements StreamableBackend {
  readonly type = 'fs'

  /**
   * Delegate to FsBackend for all non-streaming operations
   */
  private readonly delegate: FsBackend

  /**
   * Root path for file resolution
   */
  private readonly rootPath: string

  /**
   * Create a new StreamableFsBackend
   * @param rootPath - The root directory for all operations
   */
  constructor(rootPath: string) {
    this.rootPath = rootPath
    this.delegate = new FsBackend(rootPath)
  }

  /**
   * Resolve a path relative to the root
   */
  private resolvePath(path: string): string {
    // Use the same path resolution as FsBackend
    return `${this.rootPath}/${path}`
  }

  // ===========================================================================
  // StreamableBackend Methods
  // ===========================================================================

  /**
   * Create a readable stream for a file
   *
   * Uses Node.js native filesystem streams for efficient reading,
   * converted to web standard ReadableStream.
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
   * // Read Parquet footer (last 8 bytes)
   * const stat = await backend.stat('file.parquet')
   * const footerStream = backend.createReadStream('file.parquet', {
   *   start: stat.size - 8
   * })
   * ```
   */
  createReadStream(path: string, options?: StreamOptions): ReadableStream<Uint8Array> {
    const fullPath = this.resolvePath(path)
    const start = options?.start ?? 0
    const end = options?.end // undefined means read to end
    const highWaterMark = options?.highWaterMark ?? DEFAULT_HIGH_WATER_MARK

    // Create Node.js read stream options
    const nodeOptions: { start?: number | undefined; end?: number | undefined; highWaterMark: number } = {
      highWaterMark,
    }

    if (start > 0) {
      nodeOptions.start = start
    }

    // Node.js createReadStream uses inclusive end, but our API uses exclusive
    if (end !== undefined) {
      nodeOptions.end = end - 1
    }

    // Create the Node.js readable stream
    const nodeStream = createReadStream(fullPath, nodeOptions)

    // Convert Node.js stream to web ReadableStream
    return new ReadableStream<Uint8Array>(
      {
        start(controller) {
          // Handle errors from the underlying stream
          nodeStream.on('error', (error: NodeJS.ErrnoException) => {
            if (error.code === 'ENOENT') {
              controller.error(new NotFoundError(path))
            } else {
              controller.error(error)
            }
          })
        },

        pull(controller) {
          return new Promise<void>((resolve, reject) => {
            const onData = (chunk: Buffer) => {
              // Convert Buffer to Uint8Array
              controller.enqueue(new Uint8Array(chunk))
              nodeStream.pause()
              nodeStream.off('data', onData)
              nodeStream.off('end', onEnd)
              nodeStream.off('error', onError)
              resolve()
            }

            const onEnd = () => {
              controller.close()
              nodeStream.off('data', onData)
              nodeStream.off('error', onError)
              resolve()
            }

            const onError = (error: NodeJS.ErrnoException) => {
              nodeStream.off('data', onData)
              nodeStream.off('end', onEnd)
              if (error.code === 'ENOENT') {
                reject(new NotFoundError(path))
              } else {
                reject(error)
              }
            }

            nodeStream.on('data', onData)
            nodeStream.on('end', onEnd)
            nodeStream.on('error', onError)
            nodeStream.resume()
          })
        },

        cancel() {
          nodeStream.destroy()
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
   * Uses Node.js native filesystem streams for efficient writing,
   * wrapped in web standard WritableStream API.
   *
   * @param path - Path where the file will be written
   * @param options - Optional write options (contentType, metadata ignored for fs)
   * @returns A WritableStream accepting Uint8Array chunks
   *
   * @example
   * ```typescript
   * const stream = backend.createWriteStream('output.txt')
   * const writer = stream.getWriter()
   * await writer.write(new TextEncoder().encode('Hello'))
   * await writer.close()
   * ```
   */
  createWriteStream(path: string, options?: WriteOptions): WritableStream<Uint8Array> {
    const fullPath = this.resolvePath(path)
    let nodeStream: ReturnType<typeof fsCreateWriteStream> | null = null
    let streamError: Error | null = null
    let isInitialized = false

    return new WritableStream<Uint8Array>(
      {
        start: async () => {
          try {
            // Create parent directories
            await fs.mkdir(dirname(fullPath), { recursive: true })

            // Create the Node.js write stream
            nodeStream = fsCreateWriteStream(fullPath)

            // Handle errors from the underlying stream
            nodeStream.on('error', (error) => {
              streamError = error
            })

            isInitialized = true
          } catch (error) {
            streamError = error as Error
          }
        },

        write: (chunk) => {
          if (!isInitialized) {
            return Promise.reject(new Error('Stream not initialized'))
          }

          if (streamError) {
            return Promise.reject(streamError)
          }

          if (!nodeStream) {
            return Promise.reject(new Error('Stream not created'))
          }

          return new Promise<void>((resolve, reject) => {
            // Convert Uint8Array to Buffer for Node.js
            const buffer = Buffer.from(chunk)
            const canContinue = nodeStream!.write(buffer)

            if (canContinue) {
              resolve()
            } else {
              // Wait for drain event if buffer is full
              nodeStream!.once('drain', resolve)
              nodeStream!.once('error', reject)
            }
          })
        },

        close: () => {
          if (!nodeStream) {
            return Promise.resolve()
          }

          return new Promise<void>((resolve, reject) => {
            nodeStream!.end(() => {
              if (streamError) {
                reject(streamError)
              } else {
                resolve()
              }
            })
          })
        },

        abort: (reason) => {
          if (nodeStream) {
            nodeStream.destroy(reason instanceof Error ? reason : new Error(String(reason)))
          }
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
