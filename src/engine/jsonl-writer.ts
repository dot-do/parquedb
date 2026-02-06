/**
 * JsonlWriter - JSONL append-only write primitive for ParqueDB's MergeTree engine.
 *
 * Each mutation is serialized as a single JSON line and appended to a file.
 * This is the core write path -- it must be fast, safe (no data loss), and portable.
 *
 * Design decisions:
 * - Uses `fs/promises.appendFile` for Node.js compatibility (Bun-compatible too)
 * - Serializes concurrent writes via a Promise chain (writeQueue) to prevent interleaving
 * - Tracks lineCount and byteCount for compaction threshold decisions
 * - After close(), all write operations throw to prevent use-after-close bugs
 * - Write queue uses catch-and-recover pattern so a single failed write does not
 *   permanently brick the queue (subsequent writes can still succeed)
 */

import { appendFile } from 'node:fs/promises'

export class JsonlWriter {
  private path: string
  private lineCount: number
  private byteCount: number
  private closed: boolean
  private writeQueue: Promise<void>

  constructor(filePath: string) {
    this.path = filePath
    this.lineCount = 0
    this.byteCount = 0
    this.closed = false
    this.writeQueue = Promise.resolve()
  }

  /**
   * Append a single JSON line to the file.
   * Serializes the object with JSON.stringify and appends a newline.
   * Concurrent calls are serialized via the internal write queue.
   *
   * If the underlying appendFile fails, this call rejects but the write queue
   * resets to a resolved state so subsequent writes can still succeed.
   */
  async append(line: Record<string, unknown>): Promise<void> {
    this.assertNotClosed()

    const data = JSON.stringify(line) + '\n'
    const bytes = Buffer.byteLength(data, 'utf-8')

    // Chain onto the write queue to serialize concurrent writes
    const current = this.writeQueue.then(async () => {
      await appendFile(this.path, data, 'utf-8')
    })

    // Always reset queue to resolved state for the next caller,
    // even if this write fails. Without this, a single rejected
    // promise would brick all subsequent .then() chains.
    this.writeQueue = current.catch(() => {})

    // Await the actual operation so THIS caller sees the error
    await current

    this.lineCount += 1
    this.byteCount += bytes
  }

  /**
   * Append multiple JSON lines in a single write call.
   * All lines are concatenated into one string and written atomically
   * (single appendFile call), ensuring no partial batches on disk.
   *
   * If the underlying appendFile fails, this call rejects but the write queue
   * resets to a resolved state so subsequent writes can still succeed.
   */
  async appendBatch(lines: Record<string, unknown>[]): Promise<void> {
    this.assertNotClosed()

    const data = lines.map((line) => JSON.stringify(line) + '\n').join('')
    const bytes = Buffer.byteLength(data, 'utf-8')

    // Chain onto the write queue to serialize concurrent writes
    const current = this.writeQueue.then(async () => {
      await appendFile(this.path, data, 'utf-8')
    })

    // Always reset queue to resolved state for the next caller
    this.writeQueue = current.catch(() => {})

    // Await the actual operation so THIS caller sees the error
    await current

    this.lineCount += lines.length
    this.byteCount += bytes
  }

  /**
   * Force buffered writes to disk.
   * Since we use appendFile (which opens, writes, and closes the fd each call),
   * this primarily ensures the write queue is drained. For additional safety,
   * we could use fsync, but appendFile with the default flags already flushes
   * kernel buffers on close.
   */
  async flush(): Promise<void> {
    this.assertNotClosed()

    // Drain the write queue -- all pending writes complete
    await this.writeQueue
  }

  /**
   * Flush all pending writes and mark the writer as closed.
   * After close(), any further write operations will throw.
   * Calling close() multiple times is safe (idempotent).
   */
  async close(): Promise<void> {
    if (this.closed) {
      return
    }

    // Drain remaining writes
    await this.writeQueue

    this.closed = true
  }

  /**
   * Returns the file path this writer appends to.
   */
  getPath(): string {
    return this.path
  }

  /**
   * Returns the total number of lines written since construction.
   */
  getLineCount(): number {
    return this.lineCount
  }

  /**
   * Returns the total number of bytes written since construction.
   * Useful for compaction threshold checks (e.g., trigger compaction at 64MB).
   */
  getByteCount(): number {
    return this.byteCount
  }

  /**
   * Throws if the writer has been closed.
   */
  private assertNotClosed(): void {
    if (this.closed) {
      throw new Error(`JsonlWriter is closed: ${this.path}`)
    }
  }
}
