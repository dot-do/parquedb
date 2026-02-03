/**
 * StreamableBackend Interface Tests
 *
 * Tests for the StreamableBackend interface which extends StorageBackend
 * with streaming read/write support using web standard ReadableStream/WritableStream.
 *
 * Following TDD: Write tests first, then implement.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { StreamableMemoryBackend } from '../../../src/storage/StreamableMemoryBackend'
import { isStreamable } from '../../../src/types/storage'
import type { StreamableBackend, StreamOptions, WriteOptions } from '../../../src/types/storage'

// =============================================================================
// Helper Functions
// =============================================================================

function textToBytes(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

function bytesToText(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes)
}

function generateTestData(size: number): Uint8Array {
  const data = new Uint8Array(size)
  for (let i = 0; i < size; i++) {
    data[i] = i % 256
  }
  return data
}

/**
 * Collect all chunks from a ReadableStream into a single Uint8Array
 */
async function collectStream(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader()
  const chunks: Uint8Array[] = []

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      if (value) chunks.push(value)
    }
  } finally {
    reader.releaseLock()
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
 * Write data to a WritableStream
 */
async function writeToStream(stream: WritableStream<Uint8Array>, data: Uint8Array): Promise<void> {
  const writer = stream.getWriter()
  try {
    await writer.write(data)
    await writer.close()
  } catch (error) {
    await writer.abort(error)
    throw error
  }
}

/**
 * Write data to a WritableStream in chunks
 */
async function writeToStreamChunked(
  stream: WritableStream<Uint8Array>,
  data: Uint8Array,
  chunkSize: number
): Promise<void> {
  const writer = stream.getWriter()
  try {
    for (let i = 0; i < data.length; i += chunkSize) {
      const chunk = data.slice(i, Math.min(i + chunkSize, data.length))
      await writer.write(chunk)
    }
    await writer.close()
  } catch (error) {
    await writer.abort(error)
    throw error
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('StreamableBackend Interface', () => {
  let backend: StreamableBackend

  beforeEach(() => {
    backend = new StreamableMemoryBackend()
  })

  // ===========================================================================
  // Type Guards
  // ===========================================================================

  describe('isStreamable type guard', () => {
    it('should return true for StreamableMemoryBackend', () => {
      expect(isStreamable(backend)).toBe(true)
    })

    it('should return false for regular MemoryBackend', () => {
      const regularBackend = new MemoryBackend()
      expect(isStreamable(regularBackend)).toBe(false)
    })

    it('should detect createReadStream method', () => {
      expect(typeof backend.createReadStream).toBe('function')
    })

    it('should detect createWriteStream method', () => {
      expect(typeof backend.createWriteStream).toBe('function')
    })
  })

  // ===========================================================================
  // createReadStream Tests
  // ===========================================================================

  describe('createReadStream(path, options?)', () => {
    describe('happy path', () => {
      it('should create a readable stream for an existing file', async () => {
        const path = 'test/stream-read.txt'
        const content = textToBytes('Hello, Streaming World!')
        await backend.write(path, content)

        const stream = backend.createReadStream(path)

        expect(stream).toBeInstanceOf(ReadableStream)
        const result = await collectStream(stream)
        expect(bytesToText(result)).toBe('Hello, Streaming World!')
      })

      it('should read file contents correctly', async () => {
        const path = 'test/stream-content.txt'
        const content = textToBytes('Stream content test')
        await backend.write(path, content)

        const stream = backend.createReadStream(path)
        const result = await collectStream(stream)

        expect(result).toEqual(content)
      })

      it('should handle empty files', async () => {
        const path = 'test/stream-empty.txt'
        await backend.write(path, new Uint8Array(0))

        const stream = backend.createReadStream(path)
        const result = await collectStream(stream)

        expect(result.length).toBe(0)
      })

      it('should handle binary data', async () => {
        const path = 'test/stream-binary.bin'
        const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
        await backend.write(path, binaryData)

        const stream = backend.createReadStream(path)
        const result = await collectStream(stream)

        expect(result).toEqual(binaryData)
      })

      it('should handle large files', async () => {
        const path = 'test/stream-large.bin'
        const largeData = generateTestData(1024 * 1024) // 1MB
        await backend.write(path, largeData)

        const stream = backend.createReadStream(path)
        const result = await collectStream(stream)

        expect(result.length).toBe(largeData.length)
        expect(result).toEqual(largeData)
      })
    })

    describe('range options', () => {
      it('should support start option for byte offset', async () => {
        const path = 'test/stream-start.txt'
        const content = textToBytes('ABCDEFGHIJ')
        await backend.write(path, content)

        const stream = backend.createReadStream(path, { start: 3 })
        const result = await collectStream(stream)

        expect(bytesToText(result)).toBe('DEFGHIJ')
      })

      it('should support end option for byte limit', async () => {
        const path = 'test/stream-end.txt'
        const content = textToBytes('ABCDEFGHIJ')
        await backend.write(path, content)

        const stream = backend.createReadStream(path, { end: 5 })
        const result = await collectStream(stream)

        expect(bytesToText(result)).toBe('ABCDE')
      })

      it('should support both start and end options', async () => {
        const path = 'test/stream-range.txt'
        const content = textToBytes('ABCDEFGHIJ')
        await backend.write(path, content)

        const stream = backend.createReadStream(path, { start: 2, end: 7 })
        const result = await collectStream(stream)

        expect(bytesToText(result)).toBe('CDEFG')
      })

      it('should handle Parquet footer read pattern (last N bytes)', async () => {
        const path = 'test/stream-footer.bin'
        const data = generateTestData(1024)
        await backend.write(path, data)

        // Read last 8 bytes
        const stream = backend.createReadStream(path, { start: 1016 })
        const result = await collectStream(stream)

        expect(result.length).toBe(8)
        expect(result).toEqual(data.slice(1016))
      })
    })

    describe('highWaterMark option', () => {
      it('should accept highWaterMark option', async () => {
        const path = 'test/stream-hwm.txt'
        const content = textToBytes('High water mark test')
        await backend.write(path, content)

        const stream = backend.createReadStream(path, { highWaterMark: 4 })
        const result = await collectStream(stream)

        expect(result).toEqual(content)
      })

      it('should produce smaller chunks with smaller highWaterMark', async () => {
        const path = 'test/stream-chunks.txt'
        const content = generateTestData(100)
        await backend.write(path, content)

        const stream = backend.createReadStream(path, { highWaterMark: 10 })
        const reader = stream.getReader()
        const chunks: Uint8Array[] = []

        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          if (value) chunks.push(value)
        }
        reader.releaseLock()

        // With highWaterMark of 10 on 100 bytes, we expect ~10 chunks
        expect(chunks.length).toBeGreaterThanOrEqual(1)
        // Most chunks should be at or below highWaterMark
        const smallChunks = chunks.filter(c => c.length <= 10)
        expect(smallChunks.length).toBeGreaterThan(0)
      })
    })

    describe('error handling', () => {
      it('should error when reading non-existent file', async () => {
        const stream = backend.createReadStream('test/nonexistent.txt')

        await expect(collectStream(stream)).rejects.toThrow()
      })

      it('should error with descriptive message for non-existent file', async () => {
        const stream = backend.createReadStream('test/missing/file.txt')

        try {
          await collectStream(stream)
          expect.fail('Should have thrown an error')
        } catch (error) {
          expect((error as Error).message).toMatch(/not found|missing|does not exist/i)
        }
      })
    })

    describe('edge cases', () => {
      it('should handle Unicode content', async () => {
        const path = 'test/stream-unicode.txt'
        const unicodeContent = '\u4e2d\u6587\u5185\u5bb9 - Japanese: \u65e5\u672c\u8a9e'
        const content = textToBytes(unicodeContent)
        await backend.write(path, content)

        const stream = backend.createReadStream(path)
        const result = await collectStream(stream)

        expect(bytesToText(result)).toBe(unicodeContent)
      })

      it('should allow multiple concurrent reads of same file', async () => {
        const path = 'test/stream-concurrent.txt'
        const content = textToBytes('concurrent content')
        await backend.write(path, content)

        const stream1 = backend.createReadStream(path)
        const stream2 = backend.createReadStream(path)

        const [result1, result2] = await Promise.all([
          collectStream(stream1),
          collectStream(stream2),
        ])

        expect(result1).toEqual(content)
        expect(result2).toEqual(content)
      })

      it('should handle start beyond file size', async () => {
        const path = 'test/stream-beyond.txt'
        const content = textToBytes('short')
        await backend.write(path, content)

        const stream = backend.createReadStream(path, { start: 100 })
        const result = await collectStream(stream)

        expect(result.length).toBe(0)
      })

      it('should handle end beyond file size', async () => {
        const path = 'test/stream-end-beyond.txt'
        const content = textToBytes('short')
        await backend.write(path, content)

        const stream = backend.createReadStream(path, { end: 1000 })
        const result = await collectStream(stream)

        expect(result).toEqual(content)
      })
    })
  })

  // ===========================================================================
  // createWriteStream Tests
  // ===========================================================================

  describe('createWriteStream(path, options?)', () => {
    describe('happy path', () => {
      it('should create a writable stream', async () => {
        const path = 'test/stream-write.txt'
        const content = textToBytes('Written via stream')

        const stream = backend.createWriteStream(path)

        expect(stream).toBeInstanceOf(WritableStream)
        await writeToStream(stream, content)

        const result = await backend.read(path)
        expect(bytesToText(result)).toBe('Written via stream')
      })

      it('should write content correctly', async () => {
        const path = 'test/stream-write-content.txt'
        const content = textToBytes('Stream write test')

        const stream = backend.createWriteStream(path)
        await writeToStream(stream, content)

        const result = await backend.read(path)
        expect(result).toEqual(content)
      })

      it('should handle empty writes', async () => {
        const path = 'test/stream-write-empty.txt'

        const stream = backend.createWriteStream(path)
        await writeToStream(stream, new Uint8Array(0))

        const result = await backend.read(path)
        expect(result.length).toBe(0)
      })

      it('should handle binary data', async () => {
        const path = 'test/stream-write-binary.bin'
        const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])

        const stream = backend.createWriteStream(path)
        await writeToStream(stream, binaryData)

        const result = await backend.read(path)
        expect(result).toEqual(binaryData)
      })

      it('should handle large file writes', async () => {
        const path = 'test/stream-write-large.bin'
        const largeData = generateTestData(1024 * 1024) // 1MB

        const stream = backend.createWriteStream(path)
        await writeToStream(stream, largeData)

        const result = await backend.read(path)
        expect(result.length).toBe(largeData.length)
        expect(result).toEqual(largeData)
      })
    })

    describe('chunked writes', () => {
      it('should handle multiple chunks', async () => {
        const path = 'test/stream-chunks.txt'
        const content = textToBytes('Chunk1Chunk2Chunk3')

        const stream = backend.createWriteStream(path)
        await writeToStreamChunked(stream, content, 6)

        const result = await backend.read(path)
        expect(result).toEqual(content)
      })

      it('should handle many small chunks', async () => {
        const path = 'test/stream-small-chunks.bin'
        const data = generateTestData(1000)

        const stream = backend.createWriteStream(path)
        await writeToStreamChunked(stream, data, 10)

        const result = await backend.read(path)
        expect(result).toEqual(data)
      })

      it('should handle single byte chunks', async () => {
        const path = 'test/stream-byte-chunks.txt'
        const content = textToBytes('ABCDE')

        const stream = backend.createWriteStream(path)
        await writeToStreamChunked(stream, content, 1)

        const result = await backend.read(path)
        expect(result).toEqual(content)
      })
    })

    describe('write options', () => {
      it('should accept contentType option', async () => {
        const path = 'test/stream-content-type.json'
        const content = textToBytes('{"key": "value"}')

        const stream = backend.createWriteStream(path, { contentType: 'application/json' })
        await writeToStream(stream, content)

        const stat = await backend.stat(path)
        expect(stat?.contentType).toBe('application/json')
      })

      it('should accept metadata option', async () => {
        const path = 'test/stream-metadata.txt'
        const content = textToBytes('metadata test')

        const stream = backend.createWriteStream(path, {
          metadata: { 'x-custom': 'value' },
        })
        await writeToStream(stream, content)

        const stat = await backend.stat(path)
        expect(stat?.metadata?.['x-custom']).toBe('value')
      })
    })

    describe('overwrite behavior', () => {
      it('should overwrite existing file', async () => {
        const path = 'test/stream-overwrite.txt'
        await backend.write(path, textToBytes('original'))

        const stream = backend.createWriteStream(path)
        await writeToStream(stream, textToBytes('overwritten'))

        const result = await backend.read(path)
        expect(bytesToText(result)).toBe('overwritten')
      })

      it('should create parent directories', async () => {
        const path = 'test/new/nested/dir/stream-file.txt'
        const content = textToBytes('nested content')

        const stream = backend.createWriteStream(path)
        await writeToStream(stream, content)

        expect(await backend.exists(path)).toBe(true)
        const result = await backend.read(path)
        expect(result).toEqual(content)
      })
    })

    describe('error handling', () => {
      it('should handle abort correctly', async () => {
        const path = 'test/stream-abort.txt'
        await backend.write(path, textToBytes('original'))

        const stream = backend.createWriteStream(path)
        const writer = stream.getWriter()

        await writer.write(textToBytes('partial'))
        await writer.abort(new Error('Aborted'))

        // File should either be unchanged or not exist
        // depending on implementation
        const exists = await backend.exists(path)
        if (exists) {
          const result = await backend.read(path)
          // Either original content or partial content is acceptable
          expect(result.length).toBeGreaterThan(0)
        }
      })
    })

    describe('edge cases', () => {
      it('should handle Unicode content', async () => {
        const path = 'test/stream-write-unicode.txt'
        const unicodeContent = '\u4e2d\u6587\u5185\u5bb9 - Japanese: \u65e5\u672c\u8a9e'
        const content = textToBytes(unicodeContent)

        const stream = backend.createWriteStream(path)
        await writeToStream(stream, content)

        const result = await backend.read(path)
        expect(bytesToText(result)).toBe(unicodeContent)
      })

      it('should handle concurrent writes to different files', async () => {
        const path1 = 'test/stream-concurrent-1.txt'
        const path2 = 'test/stream-concurrent-2.txt'
        const content1 = textToBytes('content1')
        const content2 = textToBytes('content2')

        const stream1 = backend.createWriteStream(path1)
        const stream2 = backend.createWriteStream(path2)

        await Promise.all([
          writeToStream(stream1, content1),
          writeToStream(stream2, content2),
        ])

        const [result1, result2] = await Promise.all([
          backend.read(path1),
          backend.read(path2),
        ])

        expect(result1).toEqual(content1)
        expect(result2).toEqual(content2)
      })
    })
  })

  // ===========================================================================
  // Integration Tests
  // ===========================================================================

  describe('Integration', () => {
    it('should pipe from read stream to write stream', async () => {
      const sourcePath = 'test/stream-source.txt'
      const destPath = 'test/stream-dest.txt'
      const content = textToBytes('Piped content')

      await backend.write(sourcePath, content)

      const readStream = backend.createReadStream(sourcePath)
      const writeStream = backend.createWriteStream(destPath)

      await readStream.pipeTo(writeStream)

      const result = await backend.read(destPath)
      expect(result).toEqual(content)
    })

    it('should handle large file streaming copy', async () => {
      const sourcePath = 'test/stream-copy-source.bin'
      const destPath = 'test/stream-copy-dest.bin'
      const largeData = generateTestData(512 * 1024) // 512KB

      await backend.write(sourcePath, largeData)

      const readStream = backend.createReadStream(sourcePath)
      const writeStream = backend.createWriteStream(destPath)

      await readStream.pipeTo(writeStream)

      const result = await backend.read(destPath)
      expect(result).toEqual(largeData)
    })

    it('should maintain consistency with regular read/write methods', async () => {
      const path = 'test/stream-consistency.txt'
      const content = textToBytes('Stream and regular consistency')

      // Write via stream
      const writeStream = backend.createWriteStream(path)
      await writeToStream(writeStream, content)

      // Read via regular method
      const regularRead = await backend.read(path)
      expect(regularRead).toEqual(content)

      // Overwrite via regular method
      const newContent = textToBytes('New content')
      await backend.write(path, newContent)

      // Read via stream
      const readStream = backend.createReadStream(path)
      const streamRead = await collectStream(readStream)
      expect(streamRead).toEqual(newContent)
    })

    it('should work with exists() after stream write', async () => {
      const path = 'test/stream-exists.txt'

      expect(await backend.exists(path)).toBe(false)

      const stream = backend.createWriteStream(path)
      await writeToStream(stream, textToBytes('exists now'))

      expect(await backend.exists(path)).toBe(true)
    })

    it('should work with stat() after stream write', async () => {
      const path = 'test/stream-stat.txt'
      const content = textToBytes('stat test content')

      const stream = backend.createWriteStream(path)
      await writeToStream(stream, content)

      const stat = await backend.stat(path)
      expect(stat).not.toBeNull()
      expect(stat?.size).toBe(content.length)
    })

    it('should work with delete() after stream operations', async () => {
      const path = 'test/stream-delete.txt'
      const content = textToBytes('to be deleted')

      const stream = backend.createWriteStream(path)
      await writeToStream(stream, content)

      expect(await backend.exists(path)).toBe(true)

      await backend.delete(path)

      expect(await backend.exists(path)).toBe(false)
    })
  })

  // ===========================================================================
  // Interface Contract Tests
  // ===========================================================================

  describe('Interface Contract', () => {
    it('should extend StorageBackend interface', () => {
      // Verify all StorageBackend methods exist
      expect(typeof backend.read).toBe('function')
      expect(typeof backend.write).toBe('function')
      expect(typeof backend.exists).toBe('function')
      expect(typeof backend.list).toBe('function')
      expect(typeof backend.readRange).toBe('function')
      expect(typeof backend.writeAtomic).toBe('function')
      expect(typeof backend.stat).toBe('function')
      expect(typeof backend.delete).toBe('function')
      expect(typeof backend.deletePrefix).toBe('function')
      expect(typeof backend.mkdir).toBe('function')
      expect(typeof backend.rmdir).toBe('function')
      expect(typeof backend.writeConditional).toBe('function')
      expect(typeof backend.copy).toBe('function')
      expect(typeof backend.move).toBe('function')
      expect(typeof backend.append).toBe('function')
    })

    it('should have streaming methods', () => {
      expect(typeof backend.createReadStream).toBe('function')
      expect(typeof backend.createWriteStream).toBe('function')
    })

    it('should return ReadableStream from createReadStream', async () => {
      const path = 'test/contract-read.txt'
      await backend.write(path, textToBytes('test'))

      const stream = backend.createReadStream(path)
      expect(stream).toBeInstanceOf(ReadableStream)
    })

    it('should return WritableStream from createWriteStream', () => {
      const stream = backend.createWriteStream('test/contract-write.txt')
      expect(stream).toBeInstanceOf(WritableStream)
    })
  })
})
