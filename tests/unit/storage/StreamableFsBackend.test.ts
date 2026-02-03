/**
 * StreamableFsBackend Tests
 *
 * Tests for the Node.js filesystem backend with streaming support.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { StreamableFsBackend } from '../../../src/storage/StreamableFsBackend'
import { isStreamable } from '../../../src/types/storage'

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

  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
  const result = new Uint8Array(totalLength)
  let offset = 0
  for (const chunk of chunks) {
    result.set(chunk, offset)
    offset += chunk.length
  }

  return result
}

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

// =============================================================================
// Test Suite
// =============================================================================

describe('StreamableFsBackend', () => {
  let testDir: string
  let backend: StreamableFsBackend

  beforeEach(async () => {
    // Create a unique temp directory for each test
    testDir = join(tmpdir(), `parquedb-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    await fs.mkdir(testDir, { recursive: true })
    backend = new StreamableFsBackend(testDir)
  })

  afterEach(async () => {
    // Clean up test directory
    try {
      await fs.rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  // ===========================================================================
  // Basic Tests
  // ===========================================================================

  describe('basic functionality', () => {
    it('should be recognized as streamable', () => {
      expect(isStreamable(backend)).toBe(true)
    })

    it('should have type "fs"', () => {
      expect(backend.type).toBe('fs')
    })

    it('should have createReadStream method', () => {
      expect(typeof backend.createReadStream).toBe('function')
    })

    it('should have createWriteStream method', () => {
      expect(typeof backend.createWriteStream).toBe('function')
    })
  })

  // ===========================================================================
  // Streaming Read Tests
  // ===========================================================================

  describe('createReadStream', () => {
    it('should read file content via stream', async () => {
      const content = textToBytes('Hello from filesystem stream!')
      await backend.write('test.txt', content)

      const stream = backend.createReadStream('test.txt')
      expect(stream).toBeInstanceOf(ReadableStream)

      const result = await collectStream(stream)
      expect(result).toEqual(content)
    })

    it('should support start option', async () => {
      const content = textToBytes('ABCDEFGHIJ')
      await backend.write('range.txt', content)

      const stream = backend.createReadStream('range.txt', { start: 3 })
      const result = await collectStream(stream)

      expect(bytesToText(result)).toBe('DEFGHIJ')
    })

    it('should support end option', async () => {
      const content = textToBytes('ABCDEFGHIJ')
      await backend.write('range.txt', content)

      const stream = backend.createReadStream('range.txt', { end: 5 })
      const result = await collectStream(stream)

      expect(bytesToText(result)).toBe('ABCDE')
    })

    it('should support start and end options together', async () => {
      const content = textToBytes('ABCDEFGHIJ')
      await backend.write('range.txt', content)

      const stream = backend.createReadStream('range.txt', { start: 2, end: 7 })
      const result = await collectStream(stream)

      expect(bytesToText(result)).toBe('CDEFG')
    })

    it('should handle non-existent file', async () => {
      const stream = backend.createReadStream('nonexistent.txt')
      await expect(collectStream(stream)).rejects.toThrow()
    })

    it('should handle empty file', async () => {
      await backend.write('empty.txt', new Uint8Array(0))

      const stream = backend.createReadStream('empty.txt')
      const result = await collectStream(stream)

      expect(result.length).toBe(0)
    })

    it('should handle binary data', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      await backend.write('binary.bin', binaryData)

      const stream = backend.createReadStream('binary.bin')
      const result = await collectStream(stream)

      expect(result).toEqual(binaryData)
    })

    it('should handle large files', async () => {
      const largeData = generateTestData(256 * 1024) // 256KB
      await backend.write('large.bin', largeData)

      const stream = backend.createReadStream('large.bin')
      const result = await collectStream(stream)

      expect(result).toEqual(largeData)
    })

    it('should support highWaterMark option', async () => {
      const content = generateTestData(100)
      await backend.write('chunks.bin', content)

      const stream = backend.createReadStream('chunks.bin', { highWaterMark: 10 })
      const reader = stream.getReader()
      const chunks: Uint8Array[] = []

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        if (value) chunks.push(value)
      }
      reader.releaseLock()

      // Concatenate should match original
      const totalLength = chunks.reduce((sum, c) => sum + c.length, 0)
      expect(totalLength).toBe(content.length)
    })

    it('should handle reading last bytes (Parquet footer pattern)', async () => {
      const data = generateTestData(1024)
      await backend.write('parquet.bin', data)

      // Read last 8 bytes
      const stream = backend.createReadStream('parquet.bin', { start: 1016 })
      const result = await collectStream(stream)

      expect(result.length).toBe(8)
      expect(result).toEqual(data.slice(1016))
    })
  })

  // ===========================================================================
  // Streaming Write Tests
  // ===========================================================================

  describe('createWriteStream', () => {
    it('should write content via stream', async () => {
      const content = textToBytes('Written via stream')

      const stream = backend.createWriteStream('output.txt')
      expect(stream).toBeInstanceOf(WritableStream)
      await writeToStream(stream, content)

      const result = await backend.read('output.txt')
      expect(result).toEqual(content)
    })

    it('should handle multiple chunks', async () => {
      const stream = backend.createWriteStream('chunks.txt')
      const writer = stream.getWriter()

      await writer.write(textToBytes('Hello '))
      await writer.write(textToBytes('World'))
      await writer.write(textToBytes('!'))
      await writer.close()

      const result = await backend.read('chunks.txt')
      expect(bytesToText(result)).toBe('Hello World!')
    })

    it('should create parent directories', async () => {
      const content = textToBytes('nested content')

      const stream = backend.createWriteStream('deep/nested/dir/file.txt')
      await writeToStream(stream, content)

      expect(await backend.exists('deep/nested/dir/file.txt')).toBe(true)
      const result = await backend.read('deep/nested/dir/file.txt')
      expect(result).toEqual(content)
    })

    it('should overwrite existing file', async () => {
      await backend.write('existing.txt', textToBytes('original'))

      const stream = backend.createWriteStream('existing.txt')
      await writeToStream(stream, textToBytes('overwritten'))

      const result = await backend.read('existing.txt')
      expect(bytesToText(result)).toBe('overwritten')
    })

    it('should handle empty write', async () => {
      const stream = backend.createWriteStream('empty.txt')
      await writeToStream(stream, new Uint8Array(0))

      const result = await backend.read('empty.txt')
      expect(result.length).toBe(0)
    })

    it('should handle binary data', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])

      const stream = backend.createWriteStream('binary.bin')
      await writeToStream(stream, binaryData)

      const result = await backend.read('binary.bin')
      expect(result).toEqual(binaryData)
    })

    it('should handle large writes', async () => {
      const largeData = generateTestData(256 * 1024) // 256KB

      const stream = backend.createWriteStream('large.bin')
      await writeToStream(stream, largeData)

      const result = await backend.read('large.bin')
      expect(result).toEqual(largeData)
    })
  })

  // ===========================================================================
  // Integration Tests
  // ===========================================================================

  describe('integration', () => {
    it('should support pipeTo for stream copying', async () => {
      const content = textToBytes('pipe content')
      await backend.write('source.txt', content)

      const readStream = backend.createReadStream('source.txt')
      const writeStream = backend.createWriteStream('dest.txt')

      await readStream.pipeTo(writeStream)

      const result = await backend.read('dest.txt')
      expect(result).toEqual(content)
    })

    it('should handle large file copy via streams', async () => {
      const largeData = generateTestData(128 * 1024) // 128KB
      await backend.write('source-large.bin', largeData)

      const readStream = backend.createReadStream('source-large.bin')
      const writeStream = backend.createWriteStream('dest-large.bin')

      await readStream.pipeTo(writeStream)

      const result = await backend.read('dest-large.bin')
      expect(result).toEqual(largeData)
    })

    it('should maintain consistency with regular read/write', async () => {
      // Write via stream
      const writeStream = backend.createWriteStream('consistency.txt')
      await writeToStream(writeStream, textToBytes('via stream'))

      // Read via regular method
      const regularRead = await backend.read('consistency.txt')
      expect(bytesToText(regularRead)).toBe('via stream')

      // Overwrite via regular method
      await backend.write('consistency.txt', textToBytes('via regular'))

      // Read via stream
      const readStream = backend.createReadStream('consistency.txt')
      const streamRead = await collectStream(readStream)
      expect(bytesToText(streamRead)).toBe('via regular')
    })

    it('should work with concurrent read streams', async () => {
      const content = textToBytes('concurrent content')
      await backend.write('concurrent.txt', content)

      const stream1 = backend.createReadStream('concurrent.txt')
      const stream2 = backend.createReadStream('concurrent.txt')

      const [result1, result2] = await Promise.all([
        collectStream(stream1),
        collectStream(stream2),
      ])

      expect(result1).toEqual(content)
      expect(result2).toEqual(content)
    })

    it('should work with concurrent write streams to different files', async () => {
      const content1 = textToBytes('content1')
      const content2 = textToBytes('content2')

      const stream1 = backend.createWriteStream('file1.txt')
      const stream2 = backend.createWriteStream('file2.txt')

      await Promise.all([
        writeToStream(stream1, content1),
        writeToStream(stream2, content2),
      ])

      const [result1, result2] = await Promise.all([
        backend.read('file1.txt'),
        backend.read('file2.txt'),
      ])

      expect(result1).toEqual(content1)
      expect(result2).toEqual(content2)
    })
  })

  // ===========================================================================
  // Delegated Method Tests
  // ===========================================================================

  describe('delegated methods', () => {
    it('should delegate read correctly', async () => {
      const content = textToBytes('read test')
      await backend.write('read.txt', content)

      const result = await backend.read('read.txt')
      expect(result).toEqual(content)
    })

    it('should delegate exists correctly', async () => {
      await backend.write('exists.txt', textToBytes('data'))

      expect(await backend.exists('exists.txt')).toBe(true)
      expect(await backend.exists('not-exists.txt')).toBe(false)
    })

    it('should delegate stat correctly', async () => {
      const content = textToBytes('stat test')
      await backend.write('stat.txt', content)

      const stat = await backend.stat('stat.txt')
      expect(stat).not.toBeNull()
      expect(stat?.size).toBe(content.length)
    })

    it('should delegate list correctly', async () => {
      await backend.write('list/a.txt', textToBytes('a'))
      await backend.write('list/b.txt', textToBytes('b'))

      const result = await backend.list('list/')
      expect(result.files.length).toBe(2)
    })

    it('should delegate delete correctly', async () => {
      await backend.write('to-delete.txt', textToBytes('data'))

      const deleted = await backend.delete('to-delete.txt')
      expect(deleted).toBe(true)
      expect(await backend.exists('to-delete.txt')).toBe(false)
    })

    it('should delegate copy correctly', async () => {
      await backend.write('copy-source.txt', textToBytes('copy me'))

      await backend.copy('copy-source.txt', 'copy-dest.txt')

      const result = await backend.read('copy-dest.txt')
      expect(bytesToText(result)).toBe('copy me')
    })

    it('should delegate move correctly', async () => {
      await backend.write('move-source.txt', textToBytes('move me'))

      await backend.move('move-source.txt', 'move-dest.txt')

      expect(await backend.exists('move-source.txt')).toBe(false)
      const result = await backend.read('move-dest.txt')
      expect(bytesToText(result)).toBe('move me')
    })
  })
})
