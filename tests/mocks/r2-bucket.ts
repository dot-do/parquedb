/**
 * R2Bucket Mock Factory
 *
 * Provides mock implementations of Cloudflare R2Bucket for testing.
 * Supports both simple spy-based mocks and functional in-memory implementations.
 */

import { vi, type Mock } from 'vitest'
import type {
  R2Bucket,
  R2Object,
  R2ObjectBody,
  R2Objects,
  R2GetOptions,
  R2PutOptions,
  R2ListOptions,
  R2MultipartUpload,
  R2MultipartOptions,
  R2UploadedPart,
} from '../../src/storage/types/r2'

// =============================================================================
// Types
// =============================================================================

/**
 * Mock R2Bucket with vi.fn() methods for assertions
 */
export interface MockR2Bucket extends R2Bucket {
  get: Mock<Parameters<R2Bucket['get']>, ReturnType<R2Bucket['get']>>
  head: Mock<Parameters<R2Bucket['head']>, ReturnType<R2Bucket['head']>>
  put: Mock<Parameters<R2Bucket['put']>, ReturnType<R2Bucket['put']>>
  delete: Mock<Parameters<R2Bucket['delete']>, ReturnType<R2Bucket['delete']>>
  list: Mock<Parameters<R2Bucket['list']>, ReturnType<R2Bucket['list']>>
  createMultipartUpload: Mock<Parameters<R2Bucket['createMultipartUpload']>, ReturnType<R2Bucket['createMultipartUpload']>>
  resumeMultipartUpload: Mock<[string, string], R2MultipartUpload>

  // Test helpers
  _store: Map<string, { data: Uint8Array; metadata: R2Object }>
  _clear: () => void
}

/**
 * Options for creating mock R2Bucket
 */
export interface MockR2BucketOptions {
  /**
   * If true, returns a functional in-memory implementation.
   * If false (default), returns spy-only mocks that return sensible defaults.
   */
  functional?: boolean | undefined

  /**
   * Initial data to populate the bucket with
   */
  initialData?: Map<string, Uint8Array> | undefined
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create a mock R2Object metadata
 */
export function createMockR2Object(key: string, size: number, options?: Partial<R2Object>): R2Object {
  const etag = options?.etag ?? `"${Math.random().toString(36).slice(2)}"`
  return {
    key,
    version: options?.version ?? '1',
    size,
    etag: etag.replace(/"/g, ''),
    httpEtag: etag,
    uploaded: options?.uploaded ?? new Date(),
    storageClass: 'Standard',
    checksums: options?.checksums ?? {},
    httpMetadata: options?.httpMetadata,
    customMetadata: options?.customMetadata,
    writeHttpMetadata: vi.fn(),
    ...options,
  }
}

/**
 * Create a mock R2ObjectBody (object with data)
 */
export function createMockR2ObjectBody(
  key: string,
  data: Uint8Array,
  options?: Partial<R2Object>
): R2ObjectBody {
  const metadata = createMockR2Object(key, data.length, options)
  let bodyUsed = false

  return {
    ...metadata,
    body: new ReadableStream({
      start(controller) {
        controller.enqueue(data)
        controller.close()
      },
    }),
    get bodyUsed() {
      return bodyUsed
    },
    arrayBuffer: async () => {
      bodyUsed = true
      return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
    },
    text: async () => {
      bodyUsed = true
      return new TextDecoder().decode(data)
    },
    json: async () => {
      bodyUsed = true
      return JSON.parse(new TextDecoder().decode(data))
    },
    blob: async () => {
      bodyUsed = true
      return new Blob([data])
    },
  }
}

/**
 * Create a mock R2Objects list result
 */
export function createMockR2Objects(
  objects: R2Object[],
  options?: { truncated?: boolean; cursor?: string; delimitedPrefixes?: string[] }
): R2Objects {
  return {
    objects,
    truncated: options?.truncated ?? false,
    cursor: options?.cursor,
    delimitedPrefixes: options?.delimitedPrefixes ?? [],
  }
}

/**
 * Create a mock R2MultipartUpload
 */
export function createMockMultipartUpload(key: string, uploadId?: string): R2MultipartUpload {
  const id = uploadId ?? `upload-${Math.random().toString(36).slice(2)}`
  const parts: R2UploadedPart[] = []

  return {
    key,
    uploadId: id,
    uploadPart: vi.fn(async (partNumber: number, _value: unknown): Promise<R2UploadedPart> => {
      const part: R2UploadedPart = {
        partNumber,
        etag: `"part-${partNumber}-${Math.random().toString(36).slice(2)}"`,
      }
      parts.push(part)
      return part
    }),
    abort: vi.fn(async () => {
      parts.length = 0
    }),
    complete: vi.fn(async (_uploadedParts: R2UploadedPart[]): Promise<R2Object> => {
      return createMockR2Object(key, 0)
    }),
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a mock R2Bucket
 *
 * @param options - Configuration options
 * @returns Mock R2Bucket instance
 *
 * @example
 * ```typescript
 * // Simple spy-based mock (default)
 * const bucket = createMockR2Bucket()
 * bucket.get.mockResolvedValue(createMockR2ObjectBody('test.txt', data))
 *
 * // Functional in-memory implementation
 * const bucket = createMockR2Bucket({ functional: true })
 * await bucket.put('test.txt', data)
 * const result = await bucket.get('test.txt')
 * ```
 */
export function createMockR2Bucket(options?: MockR2BucketOptions): MockR2Bucket {
  const store = new Map<string, { data: Uint8Array; metadata: R2Object }>()

  // Initialize with any provided data
  if (options?.initialData) {
    for (const [key, data] of options.initialData) {
      store.set(key, {
        data,
        metadata: createMockR2Object(key, data.length),
      })
    }
  }

  if (options?.functional) {
    // Functional implementation that actually stores/retrieves data
    return {
      _store: store,
      _clear: () => store.clear(),

      get: vi.fn(async (key: string, getOptions?: R2GetOptions): Promise<R2ObjectBody | null> => {
        const item = store.get(key)
        if (!item) return null

        let data = item.data

        // Handle range requests
        if (getOptions?.range && 'offset' in getOptions.range) {
          const { offset = 0, length } = getOptions.range
          const end = length !== undefined ? offset + length : undefined
          data = data.slice(offset, end)
        }

        return createMockR2ObjectBody(key, data, item.metadata)
      }),

      head: vi.fn(async (key: string): Promise<R2Object | null> => {
        const item = store.get(key)
        return item ? item.metadata : null
      }),

      put: vi.fn(async (key: string, value: unknown, putOptions?: R2PutOptions): Promise<R2Object | null> => {
        let data: Uint8Array
        if (value instanceof Uint8Array) {
          data = value
        } else if (value instanceof ArrayBuffer) {
          data = new Uint8Array(value)
        } else if (typeof value === 'string') {
          data = new TextEncoder().encode(value)
        } else if (value === null) {
          data = new Uint8Array(0)
        } else if (value instanceof ReadableStream) {
          const chunks: Uint8Array[] = []
          const reader = value.getReader()
          let result = await reader.read()
          while (!result.done) {
            chunks.push(result.value)
            result = await reader.read()
          }
          data = new Uint8Array(
            chunks.reduce((acc, chunk) => acc + chunk.length, 0)
          )
          let offset = 0
          for (const chunk of chunks) {
            data.set(chunk, offset)
            offset += chunk.length
          }
        } else {
          data = new Uint8Array(0)
        }

        const metadata = createMockR2Object(key, data.length, {
          httpMetadata: putOptions?.httpMetadata as R2Object['httpMetadata'],
          customMetadata: putOptions?.customMetadata,
        })

        store.set(key, { data, metadata })
        return metadata
      }),

      delete: vi.fn(async (keys: string | string[]): Promise<void> => {
        const keyArray = Array.isArray(keys) ? keys : [keys]
        for (const key of keyArray) {
          store.delete(key)
        }
      }),

      list: vi.fn(async (listOptions?: R2ListOptions): Promise<R2Objects> => {
        const objects: R2Object[] = []
        const prefixes = new Set<string>()

        for (const [key, item] of store) {
          if (listOptions?.prefix && !key.startsWith(listOptions.prefix)) {
            continue
          }

          if (listOptions?.delimiter) {
            const prefixLen = listOptions.prefix?.length ?? 0
            const rest = key.slice(prefixLen)
            const delimiterIndex = rest.indexOf(listOptions.delimiter)
            if (delimiterIndex >= 0) {
              prefixes.add(key.slice(0, prefixLen + delimiterIndex + 1))
              continue
            }
          }

          objects.push(item.metadata)

          if (listOptions?.limit && objects.length >= listOptions.limit) {
            break
          }
        }

        return createMockR2Objects(objects, {
          truncated: listOptions?.limit ? store.size > objects.length : false,
          delimitedPrefixes: Array.from(prefixes),
        })
      }),

      createMultipartUpload: vi.fn(async (key: string, _options?: R2MultipartOptions): Promise<R2MultipartUpload> => {
        return createMockMultipartUpload(key)
      }),

      resumeMultipartUpload: vi.fn((key: string, uploadId: string): R2MultipartUpload => {
        return createMockMultipartUpload(key, uploadId)
      }),
    }
  }

  // Spy-based mock with sensible defaults
  return {
    _store: store,
    _clear: () => store.clear(),

    get: vi.fn().mockResolvedValue(null),
    head: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockImplementation(async (key: string, _value: unknown) => {
      return createMockR2Object(key, 0)
    }),
    delete: vi.fn().mockResolvedValue(undefined),
    list: vi.fn().mockResolvedValue(createMockR2Objects([])),
    createMultipartUpload: vi.fn().mockImplementation(async (key: string) => {
      return createMockMultipartUpload(key)
    }),
    resumeMultipartUpload: vi.fn().mockImplementation((key: string, uploadId: string) => {
      return createMockMultipartUpload(key, uploadId)
    }),
  }
}

/**
 * Create a mock R2Bucket that simulates errors
 *
 * @param errorType - Type of error to simulate
 * @returns Mock R2Bucket that throws errors
 */
export function createErrorR2Bucket(errorType: 'notFound' | 'accessDenied' | 'network' | 'quota'): MockR2Bucket {
  const bucket = createMockR2Bucket()

  const createError = () => {
    switch (errorType) {
      case 'notFound':
        return Object.assign(new Error('NoSuchKey'), { name: 'NoSuchKey' })
      case 'accessDenied':
        return Object.assign(new Error('AccessDenied'), { name: 'AccessDenied' })
      case 'network':
        return new Error('Network error: connection refused')
      case 'quota':
        return Object.assign(new Error('QuotaExceeded'), { name: 'QuotaExceeded' })
    }
  }

  bucket.get.mockRejectedValue(createError())
  bucket.head.mockRejectedValue(createError())
  bucket.put.mockRejectedValue(createError())
  bucket.delete.mockRejectedValue(createError())
  bucket.list.mockRejectedValue(createError())

  return bucket
}
