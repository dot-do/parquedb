/**
 * withStreaming Factory Tests
 *
 * Tests for the withStreaming factory function that adds streaming capabilities
 * to any existing StorageBackend.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { withStreaming } from '../../../src/storage/withStreaming'
import { isStreamable } from '../../../src/types/storage'
import type { StreamableBackend, StorageBackend } from '../../../src/types/storage'

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

describe('withStreaming Factory', () => {
  let baseBackend: StorageBackend
  let streamableBackend: StreamableBackend

  beforeEach(() => {
    baseBackend = new MemoryBackend()
    streamableBackend = withStreaming(baseBackend)
  })

  // ===========================================================================
  // Factory Function Tests
  // ===========================================================================

  describe('factory function', () => {
    it('should return a StreamableBackend', () => {
      expect(streamableBackend).toBeDefined()
      expect(isStreamable(streamableBackend)).toBe(true)
    })

    it('should preserve the underlying backend type', () => {
      expect(streamableBackend.type).toBe('memory')
    })

    it('should add createReadStream method', () => {
      expect(typeof streamableBackend.createReadStream).toBe('function')
    })

    it('should add createWriteStream method', () => {
      expect(typeof streamableBackend.createWriteStream).toBe('function')
    })

    it('should preserve all StorageBackend methods', () => {
      expect(typeof streamableBackend.read).toBe('function')
      expect(typeof streamableBackend.write).toBe('function')
      expect(typeof streamableBackend.exists).toBe('function')
      expect(typeof streamableBackend.list).toBe('function')
      expect(typeof streamableBackend.readRange).toBe('function')
      expect(typeof streamableBackend.writeAtomic).toBe('function')
      expect(typeof streamableBackend.stat).toBe('function')
      expect(typeof streamableBackend.delete).toBe('function')
      expect(typeof streamableBackend.deletePrefix).toBe('function')
      expect(typeof streamableBackend.mkdir).toBe('function')
      expect(typeof streamableBackend.rmdir).toBe('function')
      expect(typeof streamableBackend.writeConditional).toBe('function')
      expect(typeof streamableBackend.copy).toBe('function')
      expect(typeof streamableBackend.move).toBe('function')
      expect(typeof streamableBackend.append).toBe('function')
    })
  })

  // ===========================================================================
  // Options Tests
  // ===========================================================================

  describe('options', () => {
    it('should accept custom defaultHighWaterMark', async () => {
      const backend = withStreaming(baseBackend, { defaultHighWaterMark: 10 })
      const content = generateTestData(50)
      await backend.write('test.bin', content)

      const stream = backend.createReadStream('test.bin')
      const reader = stream.getReader()
      const chunks: Uint8Array[] = []

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        if (value) chunks.push(value)
      }
      reader.releaseLock()

      // With highWaterMark of 10 on 50 bytes, expect at least 5 chunks
      expect(chunks.length).toBeGreaterThanOrEqual(5)
    })
  })

  // ===========================================================================
  // Streaming Read Tests
  // ===========================================================================

  describe('createReadStream', () => {
    it('should read file content via stream', async () => {
      const content = textToBytes('Stream content')
      await streamableBackend.write('test.txt', content)

      const stream = streamableBackend.createReadStream('test.txt')
      const result = await collectStream(stream)

      expect(result).toEqual(content)
    })

    it('should support start option', async () => {
      const content = textToBytes('ABCDEFGHIJ')
      await streamableBackend.write('test.txt', content)

      const stream = streamableBackend.createReadStream('test.txt', { start: 5 })
      const result = await collectStream(stream)

      expect(bytesToText(result)).toBe('FGHIJ')
    })

    it('should support end option', async () => {
      const content = textToBytes('ABCDEFGHIJ')
      await streamableBackend.write('test.txt', content)

      const stream = streamableBackend.createReadStream('test.txt', { end: 5 })
      const result = await collectStream(stream)

      expect(bytesToText(result)).toBe('ABCDE')
    })

    it('should support start and end options together', async () => {
      const content = textToBytes('ABCDEFGHIJ')
      await streamableBackend.write('test.txt', content)

      const stream = streamableBackend.createReadStream('test.txt', { start: 2, end: 7 })
      const result = await collectStream(stream)

      expect(bytesToText(result)).toBe('CDEFG')
    })

    it('should support highWaterMark option', async () => {
      const content = generateTestData(100)
      await streamableBackend.write('test.bin', content)

      const stream = streamableBackend.createReadStream('test.bin', { highWaterMark: 20 })
      const result = await collectStream(stream)

      expect(result).toEqual(content)
    })

    it('should handle non-existent file', async () => {
      const stream = streamableBackend.createReadStream('nonexistent.txt')
      await expect(collectStream(stream)).rejects.toThrow()
    })

    it('should handle empty file', async () => {
      await streamableBackend.write('empty.txt', new Uint8Array(0))

      const stream = streamableBackend.createReadStream('empty.txt')
      const result = await collectStream(stream)

      expect(result.length).toBe(0)
    })

    it('should handle large files', async () => {
      const largeData = generateTestData(512 * 1024) // 512KB
      await streamableBackend.write('large.bin', largeData)

      const stream = streamableBackend.createReadStream('large.bin')
      const result = await collectStream(stream)

      expect(result).toEqual(largeData)
    })
  })

  // ===========================================================================
  // Streaming Write Tests
  // ===========================================================================

  describe('createWriteStream', () => {
    it('should write content via stream', async () => {
      const content = textToBytes('Written via stream')

      const stream = streamableBackend.createWriteStream('output.txt')
      await writeToStream(stream, content)

      const result = await streamableBackend.read('output.txt')
      expect(result).toEqual(content)
    })

    it('should handle multiple chunks', async () => {
      const stream = streamableBackend.createWriteStream('chunks.txt')
      const writer = stream.getWriter()

      await writer.write(textToBytes('chunk1'))
      await writer.write(textToBytes('chunk2'))
      await writer.write(textToBytes('chunk3'))
      await writer.close()

      const result = await streamableBackend.read('chunks.txt')
      expect(bytesToText(result)).toBe('chunk1chunk2chunk3')
    })

    it('should overwrite existing file', async () => {
      await streamableBackend.write('existing.txt', textToBytes('original'))

      const stream = streamableBackend.createWriteStream('existing.txt')
      await writeToStream(stream, textToBytes('replaced'))

      const result = await streamableBackend.read('existing.txt')
      expect(bytesToText(result)).toBe('replaced')
    })

    it('should accept write options', async () => {
      const stream = streamableBackend.createWriteStream('with-options.txt', {
        contentType: 'text/plain',
        metadata: { 'custom-key': 'custom-value' },
      })
      await writeToStream(stream, textToBytes('content'))

      const stat = await streamableBackend.stat('with-options.txt')
      expect(stat?.contentType).toBe('text/plain')
      expect(stat?.metadata?.['custom-key']).toBe('custom-value')
    })

    it('should handle empty write', async () => {
      const stream = streamableBackend.createWriteStream('empty-write.txt')
      await writeToStream(stream, new Uint8Array(0))

      const result = await streamableBackend.read('empty-write.txt')
      expect(result.length).toBe(0)
    })

    it('should handle large writes', async () => {
      const largeData = generateTestData(512 * 1024) // 512KB

      const stream = streamableBackend.createWriteStream('large-write.bin')
      await writeToStream(stream, largeData)

      const result = await streamableBackend.read('large-write.bin')
      expect(result).toEqual(largeData)
    })
  })

  // ===========================================================================
  // Delegation Tests
  // ===========================================================================

  describe('method delegation', () => {
    it('should delegate read to underlying backend', async () => {
      const content = textToBytes('read test')
      await baseBackend.write('test.txt', content)

      const result = await streamableBackend.read('test.txt')
      expect(result).toEqual(content)
    })

    it('should delegate write to underlying backend', async () => {
      const content = textToBytes('write test')
      await streamableBackend.write('test.txt', content)

      const result = await baseBackend.read('test.txt')
      expect(result).toEqual(content)
    })

    it('should delegate exists to underlying backend', async () => {
      await baseBackend.write('exists.txt', textToBytes('data'))

      expect(await streamableBackend.exists('exists.txt')).toBe(true)
      expect(await streamableBackend.exists('not-exists.txt')).toBe(false)
    })

    it('should delegate list to underlying backend', async () => {
      await baseBackend.write('list/a.txt', textToBytes('a'))
      await baseBackend.write('list/b.txt', textToBytes('b'))

      const result = await streamableBackend.list('list/')
      expect(result.files).toContain('list/a.txt')
      expect(result.files).toContain('list/b.txt')
    })

    it('should delegate readRange to underlying backend', async () => {
      const content = textToBytes('ABCDEFGHIJ')
      await baseBackend.write('range.txt', content)

      const result = await streamableBackend.readRange('range.txt', 2, 6)
      expect(bytesToText(result)).toBe('CDEF')
    })

    it('should delegate delete to underlying backend', async () => {
      await baseBackend.write('to-delete.txt', textToBytes('data'))

      const deleted = await streamableBackend.delete('to-delete.txt')
      expect(deleted).toBe(true)
      expect(await baseBackend.exists('to-delete.txt')).toBe(false)
    })

    it('should delegate stat to underlying backend', async () => {
      const content = textToBytes('stat test')
      await baseBackend.write('stat.txt', content)

      const stat = await streamableBackend.stat('stat.txt')
      expect(stat).not.toBeNull()
      expect(stat?.size).toBe(content.length)
    })

    it('should delegate copy to underlying backend', async () => {
      await baseBackend.write('source.txt', textToBytes('source'))

      await streamableBackend.copy('source.txt', 'dest.txt')

      const result = await baseBackend.read('dest.txt')
      expect(bytesToText(result)).toBe('source')
    })

    it('should delegate move to underlying backend', async () => {
      await baseBackend.write('move-source.txt', textToBytes('move'))

      await streamableBackend.move('move-source.txt', 'move-dest.txt')

      expect(await baseBackend.exists('move-source.txt')).toBe(false)
      const result = await baseBackend.read('move-dest.txt')
      expect(bytesToText(result)).toBe('move')
    })
  })

  // ===========================================================================
  // Integration Tests
  // ===========================================================================

  describe('integration', () => {
    it('should work with pipeTo for stream copying', async () => {
      const content = textToBytes('pipe content')
      await streamableBackend.write('source.txt', content)

      const readStream = streamableBackend.createReadStream('source.txt')
      const writeStream = streamableBackend.createWriteStream('dest.txt')

      await readStream.pipeTo(writeStream)

      const result = await streamableBackend.read('dest.txt')
      expect(result).toEqual(content)
    })

    it('should share data between wrapped and original backend', async () => {
      // Write via original
      await baseBackend.write('shared.txt', textToBytes('from base'))

      // Read via wrapped stream
      const stream = streamableBackend.createReadStream('shared.txt')
      const result = await collectStream(stream)
      expect(bytesToText(result)).toBe('from base')

      // Write via wrapped stream
      const writeStream = streamableBackend.createWriteStream('shared2.txt')
      await writeToStream(writeStream, textToBytes('from wrapped'))

      // Read via original
      const result2 = await baseBackend.read('shared2.txt')
      expect(bytesToText(result2)).toBe('from wrapped')
    })
  })
})
