/**
 * Bun-optimized JSONL Writer for MergeTree Engine
 *
 * Uses Bun.file().writer() (FileSink) when running on Bun runtime for
 * significantly faster buffered writes. Falls back to Node.js fs.appendFile
 * when running on Node.js.
 *
 * FileSink advantages:
 * - Kernel-level buffered I/O (vs userspace buffering)
 * - Configurable highWaterMark for batch flushing
 * - 10x+ faster than fs.appendFile for sequential writes
 */

import { appendFile } from 'node:fs/promises'

/** Whether we're running on Bun runtime */
const isBun = typeof globalThis.Bun !== 'undefined'

export interface BunJsonlWriterOptions {
  /** High water mark for Bun FileSink (bytes). Default: 64KB */
  highWaterMark?: number
}

export class BunJsonlWriter {
  private path: string
  private writer: { write(data: string | Uint8Array): number; flush(): Promise<void>; end(): void } | null = null
  private highWaterMark: number
  private lineCount = 0
  private closed = false

  constructor(path: string, options?: BunJsonlWriterOptions) {
    this.path = path
    this.highWaterMark = options?.highWaterMark ?? 65536
  }

  /** Initialize the writer. Must be called before append. */
  async init(): Promise<void> {
    if (isBun) {
      // @ts-expect-error - Bun global not typed in Node.js
      const file = Bun.file(this.path)
      this.writer = file.writer({ highWaterMark: this.highWaterMark })
    }
    // Node.js: no-op, appendFile creates file on first write
  }

  /** Append a single line to the JSONL file */
  async append(line: Record<string, unknown>): Promise<void> {
    this.assertNotClosed()
    const serialized = JSON.stringify(line) + '\n'

    if (this.writer) {
      // Bun FileSink path
      this.writer.write(serialized)
    } else {
      // Node.js fallback
      await appendFile(this.path, serialized)
    }
    this.lineCount++
  }

  /** Append multiple lines as a batch */
  async appendBatch(lines: Record<string, unknown>[]): Promise<void> {
    this.assertNotClosed()
    if (lines.length === 0) return

    const data = lines.map(line => JSON.stringify(line) + '\n').join('')

    if (this.writer) {
      // Bun: write all lines, FileSink handles buffering
      this.writer.write(data)
    } else {
      // Node.js: batch into single appendFile call
      await appendFile(this.path, data)
    }
    this.lineCount += lines.length
  }

  /** Flush any buffered data to disk */
  async flush(): Promise<void> {
    if (this.writer) {
      await this.writer.flush()
    }
    // Node.js: appendFile is already unbuffered
  }

  /** Close the writer and release resources */
  async close(): Promise<void> {
    if (this.closed) {
      return
    }
    if (this.writer) {
      await this.writer.flush()
      this.writer.end()
      this.writer = null
    }
    this.closed = true
  }

  /** Get the number of lines written */
  get count(): number {
    return this.lineCount
  }

  /** Whether this writer uses Bun FileSink */
  get usesBunFileSink(): boolean {
    return this.writer !== null
  }

  /**
   * Throws if the writer has been closed.
   */
  private assertNotClosed(): void {
    if (this.closed) {
      throw new Error(`BunJsonlWriter is closed: ${this.path}`)
    }
  }
}
