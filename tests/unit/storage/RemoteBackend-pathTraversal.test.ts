/**
 * RemoteBackend Path Traversal Protection Tests
 *
 * Tests to ensure the RemoteBackend properly validates paths and prevents
 * path traversal attacks that could access files outside the intended directory.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { RemoteBackend } from '../../../src/storage/RemoteBackend'
import { PathTraversalError } from '../../../src/storage/errors'

describe('RemoteBackend path traversal protection', () => {
  let mockFetch: ReturnType<typeof vi.fn>
  let backend: RemoteBackend

  beforeEach(() => {
    mockFetch = vi.fn()
    backend = new RemoteBackend({
      baseUrl: 'https://example.com/db/test',
      fetch: mockFetch,
    })
  })

  describe('parent directory traversal (..)', () => {
    it('read() should reject paths containing ..', async () => {
      await expect(backend.read('../../../etc/passwd'))
        .rejects.toThrow(PathTraversalError)
    })

    it('readRange() should reject paths containing ..', async () => {
      await expect(backend.readRange('../secret/data.parquet', 0, 100))
        .rejects.toThrow(PathTraversalError)
    })

    it('stat() should reject paths containing ..', async () => {
      await expect(backend.stat('data/../../../etc/passwd'))
        .rejects.toThrow(PathTraversalError)
    })

    it('exists() should reject paths containing ..', async () => {
      await expect(backend.exists('foo/bar/../../baz/../../../secret'))
        .rejects.toThrow(PathTraversalError)
    })

    it('write() should reject paths containing ..', async () => {
      await expect(backend.write('../malicious.txt', new Uint8Array([1, 2, 3])))
        .rejects.toThrow(PathTraversalError)
    })

    it('should reject paths with .. embedded in directory names', async () => {
      await expect(backend.read('data/foo..bar/file.txt'))
        .rejects.toThrow(PathTraversalError)
    })
  })

  describe('double slash (//) bypass attempts', () => {
    it('read() should reject paths containing //', async () => {
      await expect(backend.read('data//secret/file.txt'))
        .rejects.toThrow(PathTraversalError)
    })

    it('readRange() should reject paths containing //', async () => {
      await expect(backend.readRange('foo//bar.parquet', 0, 100))
        .rejects.toThrow(PathTraversalError)
    })

    it('stat() should reject paths containing //', async () => {
      await expect(backend.stat('data//hidden.parquet'))
        .rejects.toThrow(PathTraversalError)
    })

    it('exists() should reject paths containing //', async () => {
      await expect(backend.exists('test//file.txt'))
        .rejects.toThrow(PathTraversalError)
    })
  })

  describe('absolute paths starting with /', () => {
    it('read() should reject absolute paths', async () => {
      await expect(backend.read('/etc/passwd'))
        .rejects.toThrow(PathTraversalError)
    })

    it('readRange() should reject absolute paths', async () => {
      await expect(backend.readRange('/var/log/system.log', 0, 100))
        .rejects.toThrow(PathTraversalError)
    })

    it('stat() should reject absolute paths', async () => {
      await expect(backend.stat('/root/.ssh/id_rsa'))
        .rejects.toThrow(PathTraversalError)
    })

    it('exists() should reject absolute paths', async () => {
      await expect(backend.exists('/tmp/secret'))
        .rejects.toThrow(PathTraversalError)
    })
  })

  describe('valid paths should work', () => {
    it('read() should accept valid relative paths', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: async () => new ArrayBuffer(0),
      })

      await expect(backend.read('data/posts/data.parquet')).resolves.toBeDefined()
      expect(mockFetch).toHaveBeenCalledWith(
        'https://example.com/db/test/data/posts/data.parquet',
        expect.any(Object)
      )
    })

    it('readRange() should accept valid relative paths', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 206,
        arrayBuffer: async () => new ArrayBuffer(5),
      })

      await expect(backend.readRange('data/posts/data.parquet', 0, 5)).resolves.toBeDefined()
    })

    it('stat() should accept valid relative paths', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        headers: new Map([
          ['Content-Length', '1000'],
          ['Last-Modified', new Date().toISOString()],
        ]) as unknown as Headers,
      })

      await expect(backend.stat('data/posts/data.parquet')).resolves.toBeDefined()
    })

    it('exists() should accept valid relative paths', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        headers: new Map([
          ['Content-Length', '1000'],
          ['Last-Modified', new Date().toISOString()],
        ]) as unknown as Headers,
      })

      await expect(backend.exists('data/posts/data.parquet')).resolves.toBeDefined()
    })

    it('should accept paths with single dots (current directory)', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: async () => new ArrayBuffer(0),
      })

      // Single dot is OK (it's not a traversal)
      await expect(backend.read('data/./posts/data.parquet')).resolves.toBeDefined()
    })

    it('should accept paths with dots in filenames', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: async () => new ArrayBuffer(0),
      })

      await expect(backend.read('data/my.file.with.dots.parquet')).resolves.toBeDefined()
    })
  })
})
