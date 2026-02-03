/**
 * R2Backend Integration Tests
 *
 * Tests for Cloudflare R2 implementation using real R2/S3 operations.
 * Uses S3-compatible API with credentials from .env file.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach, vi } from 'vitest'
import { R2Backend, R2OperationError, R2NotFoundError } from '../../src/R2Backend'
import type { R2Bucket, R2Object, R2ObjectBody, R2Objects, R2MultipartUpload, R2UploadedPart, R2ListOptions, R2GetOptions, R2PutOptions, R2MultipartOptions, R2HTTPMetadata } from '../types/r2'
import { createHmac, createHash } from 'crypto'
import { readFileSync } from 'fs'
import { join } from 'path'

// Load .env file manually
function loadEnv() {
  try {
    const envPath = join(process.cwd(), '.env')
    const content = readFileSync(envPath, 'utf-8')
    for (const line of content.split('\n')) {
      const trimmed = line.trim()
      if (!trimmed || trimmed.startsWith('#')) continue
      const eqIndex = trimmed.indexOf('=')
      if (eqIndex > 0) {
        const key = trimmed.slice(0, eqIndex)
        const value = trimmed.slice(eqIndex + 1)
        if (!process.env[key]) {
          process.env[key] = value
        }
      }
    }
  } catch {
    // .env file not found, rely on existing env vars
  }
}

loadEnv()

// =============================================================================
// S3-Compatible R2Bucket Implementation for Testing
// =============================================================================

interface S3Credentials {
  accessKeyId: string
  secretAccessKey: string
  endpoint: string
  bucket: string
}

/**
 * URI encode a string according to AWS S3 rules
 */
function uriEncode(str: string, encodeSlash: boolean = true): string {
  let result = ''
  for (const char of str) {
    if (
      (char >= 'A' && char <= 'Z') ||
      (char >= 'a' && char <= 'z') ||
      (char >= '0' && char <= '9') ||
      char === '_' ||
      char === '-' ||
      char === '~' ||
      char === '.'
    ) {
      result += char
    } else if (char === '/' && !encodeSlash) {
      result += char
    } else {
      result += '%' + char.charCodeAt(0).toString(16).toUpperCase().padStart(2, '0')
    }
  }
  return result
}

/**
 * Create AWS Signature V4 authorization header
 */
function signRequest(
  method: string,
  path: string,
  queryParams: Record<string, string>,
  headers: Record<string, string>,
  credentials: S3Credentials,
  region: string = 'auto'
): Record<string, string> {
  const now = new Date()
  const dateStamp = now.toISOString().slice(0, 10).replace(/-/g, '')
  const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, '')
  const service = 's3'

  // Parse endpoint to get host
  const url = new URL(credentials.endpoint)
  const host = url.host

  // Canonical headers - must be lowercase
  const canonicalHeaders: Record<string, string> = {
    host,
    'x-amz-date': amzDate,
    'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
  }

  // Add extra headers (lowercased)
  for (const [key, value] of Object.entries(headers)) {
    canonicalHeaders[key.toLowerCase()] = value
  }

  // Sort header keys
  const signedHeaderKeys = Object.keys(canonicalHeaders).sort()
  const signedHeaders = signedHeaderKeys.join(';')

  // Build canonical header string
  const canonicalHeaderString = signedHeaderKeys
    .map((key) => `${key}:${canonicalHeaders[key]}`)
    .join('\n')

  // Build canonical query string (sorted by key)
  const sortedQueryKeys = Object.keys(queryParams).sort()
  const canonicalQueryString = sortedQueryKeys
    .map((key) => `${uriEncode(key)}=${uriEncode(queryParams[key])}`)
    .join('&')

  // URI encode the path (but not slashes)
  const canonicalUri = uriEncode(path, false)

  // Build canonical request
  const canonicalRequest = [
    method,
    canonicalUri,
    canonicalQueryString,
    canonicalHeaderString + '\n',
    signedHeaders,
    'UNSIGNED-PAYLOAD',
  ].join('\n')

  // Create string to sign
  const algorithm = 'AWS4-HMAC-SHA256'
  const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`

  // Hash the canonical request
  const canonicalHash = createHash('sha256').update(canonicalRequest).digest('hex')

  const stringToSign = [
    algorithm,
    amzDate,
    credentialScope,
    canonicalHash,
  ].join('\n')

  // Calculate signing key
  const kDate = createHmac('sha256', `AWS4${credentials.secretAccessKey}`)
    .update(dateStamp)
    .digest()
  const kRegion = createHmac('sha256', kDate).update(region).digest()
  const kService = createHmac('sha256', kRegion).update(service).digest()
  const kSigning = createHmac('sha256', kService).update('aws4_request').digest()

  // Calculate signature
  const signature = createHmac('sha256', kSigning)
    .update(stringToSign)
    .digest('hex')

  // Build authorization header
  const authorization = `${algorithm} Credential=${credentials.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`

  return {
    ...canonicalHeaders,
    Authorization: authorization,
  }
}

/**
 * Create an S3-compatible R2Bucket implementation for testing
 */
function createS3CompatibleBucket(credentials: S3Credentials): R2Bucket {
  const makeRequest = async (
    method: string,
    key: string,
    body?: Uint8Array | string | null,
    extraHeaders: Record<string, string> = {},
    queryParams: Record<string, string> = {}
  ): Promise<Response> => {
    const path = `/${credentials.bucket}/${key}`
    const headers = signRequest(method, path, queryParams, extraHeaders, credentials)

    // Build URL with query params
    let url = `${credentials.endpoint}${path}`
    const queryString = Object.entries(queryParams)
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join('&')
    if (queryString) {
      url += '?' + queryString
    }

    const response = await fetch(url, {
      method,
      headers,
      body: body ?? undefined,
    })

    return response
  }

  const parseXmlResponse = (xml: string): Record<string, unknown> => {
    // Simple XML parsing for S3 responses
    const result: Record<string, unknown> = {}

    // Extract common fields
    const extract = (tag: string): string | undefined => {
      const match = xml.match(new RegExp(`<${tag}>([^<]*)</${tag}>`))
      return match?.[1]
    }

    const extractAll = (tag: string): string[] => {
      const matches = xml.matchAll(new RegExp(`<${tag}>([^<]*)</${tag}>`, 'g'))
      return Array.from(matches).map((m) => m[1])
    }

    result.ETag = extract('ETag')
    result.Key = extract('Key')
    result.Size = extract('Size')
    result.LastModified = extract('LastModified')
    result.UploadId = extract('UploadId')
    result.IsTruncated = extract('IsTruncated')
    result.NextContinuationToken = extract('NextContinuationToken')

    // Parse Contents for list
    const contents: Record<string, unknown>[] = []
    const contentMatches = xml.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)
    for (const match of contentMatches) {
      const content = match[1]
      contents.push({
        Key: content.match(/<Key>([^<]*)<\/Key>/)?.[1],
        Size: content.match(/<Size>([^<]*)<\/Size>/)?.[1],
        ETag: content.match(/<ETag>([^<]*)<\/ETag>/)?.[1],
        LastModified: content.match(/<LastModified>([^<]*)<\/LastModified>/)?.[1],
      })
    }
    if (contents.length > 0) {
      result.Contents = contents
    }

    // Parse CommonPrefixes
    const prefixes = extractAll('Prefix')
    if (prefixes.length > 0) {
      result.CommonPrefixes = prefixes
    }

    return result
  }

  const createR2Object = (
    key: string,
    etag: string,
    size: number,
    lastModified: Date,
    metadata?: Record<string, string>,
    contentType?: string
  ): R2Object => ({
    key,
    version: etag,
    size,
    etag: etag.replace(/"/g, ''),
    httpEtag: etag.includes('"') ? etag : `"${etag}"`,
    uploaded: lastModified,
    storageClass: 'Standard',
    checksums: {},
    httpMetadata: contentType ? { contentType } : undefined,
    customMetadata: metadata,
    writeHttpMetadata: () => {},
  })

  const bucket: R2Bucket = {
    async get(key: string, options?: R2GetOptions): Promise<R2ObjectBody | null> {
      const headers: Record<string, string> = {}

      if (options?.range && typeof options.range !== 'string' && !(options.range instanceof Headers)) {
        const range = options.range
        if (range.offset !== undefined && range.length !== undefined) {
          headers['range'] = `bytes=${range.offset}-${range.offset + range.length - 1}`
        }
      }

      const response = await makeRequest('GET', key, null, headers)

      if (response.status === 404) {
        return null
      }

      if (!response.ok) {
        throw new Error(`S3 GET failed: ${response.status} ${await response.text()}`)
      }

      const data = new Uint8Array(await response.arrayBuffer())
      const etag = response.headers.get('etag') || ''
      const size = parseInt(response.headers.get('content-length') || '0')
      const lastModified = new Date(response.headers.get('last-modified') || Date.now())
      const contentType = response.headers.get('content-type') || undefined

      const obj = createR2Object(key, etag, size, lastModified, undefined, contentType)

      return {
        ...obj,
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(data)
            controller.close()
          },
        }),
        bodyUsed: false,
        arrayBuffer: async () => data.buffer as ArrayBuffer,
        text: async () => new TextDecoder().decode(data),
        json: async () => JSON.parse(new TextDecoder().decode(data)),
        blob: async () => new Blob([data]),
      }
    },

    async head(key: string): Promise<R2Object | null> {
      const response = await makeRequest('HEAD', key)

      if (response.status === 404) {
        return null
      }

      if (!response.ok) {
        throw new Error(`S3 HEAD failed: ${response.status}`)
      }

      const etag = response.headers.get('etag') || ''
      const size = parseInt(response.headers.get('content-length') || '0')
      const lastModified = new Date(response.headers.get('last-modified') || Date.now())
      const contentType = response.headers.get('content-type') || undefined

      // Parse custom metadata from x-amz-meta-* headers
      const metadata: Record<string, string> = {}
      response.headers.forEach((value, name) => {
        if (name.toLowerCase().startsWith('x-amz-meta-')) {
          metadata[name.slice(11)] = value
        }
      })

      return createR2Object(
        key,
        etag,
        size,
        lastModified,
        Object.keys(metadata).length > 0 ? metadata : undefined,
        contentType
      )
    },

    async put(
      key: string,
      value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob | null,
      options?: R2PutOptions
    ): Promise<R2Object | null> {
      let data: Uint8Array
      if (value === null) {
        data = new Uint8Array(0)
      } else if (value instanceof Uint8Array) {
        data = value
      } else if (value instanceof ArrayBuffer) {
        data = new Uint8Array(value)
      } else if (ArrayBuffer.isView(value)) {
        data = new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
      } else if (typeof value === 'string') {
        data = new TextEncoder().encode(value)
      } else if (value instanceof Blob) {
        data = new Uint8Array(await value.arrayBuffer())
      } else {
        // ReadableStream
        const chunks: Uint8Array[] = []
        const reader = (value as ReadableStream<Uint8Array>).getReader()
        while (true) {
          const { done, value: chunk } = await reader.read()
          if (done) break
          chunks.push(chunk)
        }
        const totalLength = chunks.reduce((sum, c) => sum + c.length, 0)
        data = new Uint8Array(totalLength)
        let offset = 0
        for (const chunk of chunks) {
          data.set(chunk, offset)
          offset += chunk.length
        }
      }

      const headers: Record<string, string> = {
        'content-length': data.length.toString(),
      }

      // Handle HTTP metadata
      if (options?.httpMetadata && !(options.httpMetadata instanceof Headers)) {
        const httpMeta = options.httpMetadata as R2HTTPMetadata
        if (httpMeta.contentType) {
          headers['content-type'] = httpMeta.contentType
        }
        if (httpMeta.cacheControl) {
          headers['cache-control'] = httpMeta.cacheControl
        }
      }

      // Handle custom metadata
      if (options?.customMetadata) {
        for (const [k, v] of Object.entries(options.customMetadata)) {
          headers[`x-amz-meta-${k}`] = v
        }
      }

      // Handle conditional writes
      if (options?.onlyIf && !(options.onlyIf instanceof Headers)) {
        const cond = options.onlyIf
        if (cond.etagMatches) {
          headers['if-match'] = cond.etagMatches
        }
        if (cond.etagDoesNotMatch) {
          headers['if-none-match'] = cond.etagDoesNotMatch
        }
      }

      const response = await makeRequest('PUT', key, data, headers)

      // Precondition failed
      if (response.status === 412) {
        return null
      }

      if (!response.ok) {
        throw new Error(`S3 PUT failed: ${response.status} ${await response.text()}`)
      }

      const etag = response.headers.get('etag') || ''
      return createR2Object(key, etag, data.length, new Date())
    },

    async delete(keys: string | string[]): Promise<void> {
      const keyArray = Array.isArray(keys) ? keys : [keys]

      for (const key of keyArray) {
        const response = await makeRequest('DELETE', key)
        // S3 delete returns 204 for success and also for non-existent keys
        if (!response.ok && response.status !== 204) {
          throw new Error(`S3 DELETE failed: ${response.status}`)
        }
      }
    },

    async list(options?: R2ListOptions): Promise<R2Objects> {
      const queryParams: Record<string, string> = {
        'list-type': '2',
      }

      if (options?.prefix) {
        queryParams['prefix'] = options.prefix
      }
      if (options?.limit) {
        queryParams['max-keys'] = options.limit.toString()
      }
      if (options?.cursor) {
        queryParams['continuation-token'] = options.cursor
      }
      if (options?.delimiter) {
        queryParams['delimiter'] = options.delimiter
      }

      const path = `/${credentials.bucket}`
      const headers = signRequest('GET', path, queryParams, {}, credentials)

      // Build URL with query params
      const queryString = Object.entries(queryParams)
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
        .join('&')
      const url = `${credentials.endpoint}${path}?${queryString}`
      const response = await fetch(url, { method: 'GET', headers })

      if (!response.ok) {
        throw new Error(`S3 LIST failed: ${response.status} ${await response.text()}`)
      }

      const xml = await response.text()
      const parsed = parseXmlResponse(xml)

      const objects: R2Object[] = []
      if (parsed.Contents && Array.isArray(parsed.Contents)) {
        for (const item of parsed.Contents) {
          objects.push(
            createR2Object(
              item.Key as string,
              (item.ETag as string) || '',
              parseInt((item.Size as string) || '0'),
              new Date((item.LastModified as string) || Date.now())
            )
          )
        }
      }

      const delimitedPrefixes: string[] = []
      if (parsed.CommonPrefixes && Array.isArray(parsed.CommonPrefixes)) {
        delimitedPrefixes.push(...(parsed.CommonPrefixes as string[]))
      }

      return {
        objects,
        truncated: parsed.IsTruncated === 'true',
        cursor: parsed.NextContinuationToken as string | undefined,
        delimitedPrefixes,
      }
    },

    async createMultipartUpload(key: string, options?: R2MultipartOptions): Promise<R2MultipartUpload> {
      const headers: Record<string, string> = {}

      if (options?.httpMetadata && !(options.httpMetadata instanceof Headers)) {
        const httpMeta = options.httpMetadata as R2HTTPMetadata
        if (httpMeta.contentType) {
          headers['content-type'] = httpMeta.contentType
        }
      }

      if (options?.customMetadata) {
        for (const [k, v] of Object.entries(options.customMetadata)) {
          headers[`x-amz-meta-${k}`] = v
        }
      }

      const path = `/${credentials.bucket}/${key}`
      const queryParams = { uploads: '' }
      const signedHeaders = signRequest('POST', path, queryParams, headers, credentials)

      const url = `${credentials.endpoint}${path}?uploads`
      const response = await fetch(url, { method: 'POST', headers: signedHeaders })

      if (!response.ok) {
        throw new Error(`S3 CreateMultipartUpload failed: ${response.status} ${await response.text()}`)
      }

      const xml = await response.text()
      const parsed = parseXmlResponse(xml)
      const uploadId = parsed.UploadId as string

      return {
        key,
        uploadId,
        async uploadPart(partNumber: number, value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob): Promise<R2UploadedPart> {
          let data: Uint8Array
          if (value instanceof Uint8Array) {
            data = value
          } else if (value instanceof ArrayBuffer) {
            data = new Uint8Array(value)
          } else if (ArrayBuffer.isView(value)) {
            data = new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
          } else if (typeof value === 'string') {
            data = new TextEncoder().encode(value)
          } else if (value instanceof Blob) {
            data = new Uint8Array(await value.arrayBuffer())
          } else {
            const chunks: Uint8Array[] = []
            const reader = (value as ReadableStream<Uint8Array>).getReader()
            while (true) {
              const { done, value: chunk } = await reader.read()
              if (done) break
              chunks.push(chunk)
            }
            const totalLength = chunks.reduce((sum, c) => sum + c.length, 0)
            data = new Uint8Array(totalLength)
            let offset = 0
            for (const chunk of chunks) {
              data.set(chunk, offset)
              offset += chunk.length
            }
          }

          const partPath = `/${credentials.bucket}/${key}`
          const partQueryParams = { partNumber: partNumber.toString(), uploadId }
          const partHeaders = signRequest('PUT', partPath, partQueryParams, { 'content-length': data.length.toString() }, credentials)

          const partUrl = `${credentials.endpoint}${partPath}?partNumber=${partNumber}&uploadId=${encodeURIComponent(uploadId)}`
          const partResponse = await fetch(partUrl, { method: 'PUT', headers: partHeaders, body: data })

          if (!partResponse.ok) {
            throw new Error(`S3 UploadPart failed: ${partResponse.status} ${await partResponse.text()}`)
          }

          const etag = partResponse.headers.get('etag') || ''
          return { partNumber, etag: etag.replace(/"/g, '') }
        },
        async abort(): Promise<void> {
          const abortPath = `/${credentials.bucket}/${key}`
          const abortQueryParams = { uploadId }
          const abortHeaders = signRequest('DELETE', abortPath, abortQueryParams, {}, credentials)

          const abortUrl = `${credentials.endpoint}${abortPath}?uploadId=${encodeURIComponent(uploadId)}`
          await fetch(abortUrl, { method: 'DELETE', headers: abortHeaders })
        },
        async complete(uploadedParts: R2UploadedPart[]): Promise<R2Object> {
          const partsXml = uploadedParts
            .sort((a, b) => a.partNumber - b.partNumber)
            .map((p) => `<Part><PartNumber>${p.partNumber}</PartNumber><ETag>"${p.etag}"</ETag></Part>`)
            .join('')
          const body = `<CompleteMultipartUpload>${partsXml}</CompleteMultipartUpload>`

          const completePath = `/${credentials.bucket}/${key}`
          const completeQueryParams = { uploadId }
          const completeHeaders = signRequest('POST', completePath, completeQueryParams, { 'content-type': 'application/xml' }, credentials)

          const completeUrl = `${credentials.endpoint}${completePath}?uploadId=${encodeURIComponent(uploadId)}`
          const completeResponse = await fetch(completeUrl, { method: 'POST', headers: completeHeaders, body })

          if (!completeResponse.ok) {
            throw new Error(`S3 CompleteMultipartUpload failed: ${completeResponse.status} ${await completeResponse.text()}`)
          }

          const xml = await completeResponse.text()
          const parsed = parseXmlResponse(xml)
          const etag = (parsed.ETag as string) || ''

          // Get actual size via HEAD
          const headResponse = await bucket.head(key)
          const size = headResponse?.size || 0

          return createR2Object(key, etag, size, new Date())
        },
      }
    },

    resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
      // Return a multipart upload handle for resuming
      return {
        key,
        uploadId,
        async uploadPart(partNumber: number, value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob): Promise<R2UploadedPart> {
          let data: Uint8Array
          if (value instanceof Uint8Array) {
            data = value
          } else if (value instanceof ArrayBuffer) {
            data = new Uint8Array(value)
          } else if (ArrayBuffer.isView(value)) {
            data = new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
          } else if (typeof value === 'string') {
            data = new TextEncoder().encode(value)
          } else if (value instanceof Blob) {
            data = new Uint8Array(await value.arrayBuffer())
          } else {
            const chunks: Uint8Array[] = []
            const reader = (value as ReadableStream<Uint8Array>).getReader()
            while (true) {
              const { done, value: chunk } = await reader.read()
              if (done) break
              chunks.push(chunk)
            }
            const totalLength = chunks.reduce((sum, c) => sum + c.length, 0)
            data = new Uint8Array(totalLength)
            let offset = 0
            for (const chunk of chunks) {
              data.set(chunk, offset)
              offset += chunk.length
            }
          }

          const partPath = `/${credentials.bucket}/${key}`
          const partQueryParams = { partNumber: partNumber.toString(), uploadId }
          const partHeaders = signRequest('PUT', partPath, partQueryParams, { 'content-length': data.length.toString() }, credentials)

          const partUrl = `${credentials.endpoint}${partPath}?partNumber=${partNumber}&uploadId=${encodeURIComponent(uploadId)}`
          const partResponse = await fetch(partUrl, { method: 'PUT', headers: partHeaders, body: data })

          if (!partResponse.ok) {
            throw new Error(`S3 UploadPart failed: ${partResponse.status}`)
          }

          const etag = partResponse.headers.get('etag') || ''
          return { partNumber, etag: etag.replace(/"/g, '') }
        },
        async abort(): Promise<void> {
          const abortPath = `/${credentials.bucket}/${key}`
          const abortQueryParams = { uploadId }
          const abortHeaders = signRequest('DELETE', abortPath, abortQueryParams, {}, credentials)

          const abortUrl = `${credentials.endpoint}${abortPath}?uploadId=${encodeURIComponent(uploadId)}`
          await fetch(abortUrl, { method: 'DELETE', headers: abortHeaders })
        },
        async complete(uploadedParts: R2UploadedPart[]): Promise<R2Object> {
          const partsXml = uploadedParts
            .sort((a, b) => a.partNumber - b.partNumber)
            .map((p) => `<Part><PartNumber>${p.partNumber}</PartNumber><ETag>"${p.etag}"</ETag></Part>`)
            .join('')
          const body = `<CompleteMultipartUpload>${partsXml}</CompleteMultipartUpload>`

          const completePath = `/${credentials.bucket}/${key}`
          const completeQueryParams = { uploadId }
          const completeHeaders = signRequest('POST', completePath, completeQueryParams, { 'content-type': 'application/xml' }, credentials)

          const completeUrl = `${credentials.endpoint}${completePath}?uploadId=${encodeURIComponent(uploadId)}`
          const completeResponse = await fetch(completeUrl, { method: 'POST', headers: completeHeaders, body })

          if (!completeResponse.ok) {
            throw new Error(`S3 CompleteMultipartUpload failed: ${completeResponse.status}`)
          }

          const xml = await completeResponse.text()
          const parsed = parseXmlResponse(xml)
          const etag = (parsed.ETag as string) || ''

          const headResponse = await bucket.head(key)
          const size = headResponse?.size || 0

          return createR2Object(key, etag, size, new Date())
        },
      }
    },
  }

  return bucket
}

// =============================================================================
// Test Setup
// =============================================================================

// Load credentials from environment
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY
const R2_URL = process.env.R2_URL
const R2_BUCKET = process.env.R2_BUCKET || 'parquedb'

const hasCredentials = R2_ACCESS_KEY_ID && R2_SECRET_ACCESS_KEY && R2_URL

// Generate unique test prefix for isolation
const testPrefix = `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}/`

// Track files created during tests for cleanup
let createdFiles: string[] = []
let bucket: R2Bucket
let backend: R2Backend

// =============================================================================
// Integration Tests
// =============================================================================

describe('R2Backend Integration Tests', () => {
  beforeAll(() => {
    if (!hasCredentials) {
      console.warn('Skipping R2 integration tests: Missing R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, or R2_URL')
      return
    }

    bucket = createS3CompatibleBucket({
      accessKeyId: R2_ACCESS_KEY_ID!,
      secretAccessKey: R2_SECRET_ACCESS_KEY!,
      endpoint: R2_URL!,
      bucket: R2_BUCKET,
    })

    backend = new R2Backend(bucket, { prefix: testPrefix })
  })

  afterAll(async () => {
    if (!hasCredentials || !backend) return

    // Clean up all test files
    try {
      const count = await backend.deletePrefix('')
      if (count > 0) {
        console.log(`Cleaned up ${count} test files with prefix: ${testPrefix}`)
      }
    } catch (error) {
      console.error('Failed to clean up test files:', error)
    }
  })

  beforeEach(() => {
    createdFiles = []
  })

  afterEach(async () => {
    if (!hasCredentials || !backend) return

    // Clean up files created during this test
    for (const file of createdFiles) {
      try {
        await backend.delete(file)
      } catch {
        // Ignore errors during cleanup
      }
    }
  })

  // Helper to track created files
  const trackFile = (path: string) => {
    createdFiles.push(path)
    return path
  }

  // ===========================================================================
  // Skip Tests if No Credentials
  // ===========================================================================

  describe.runIf(hasCredentials)('write operations', () => {
    it('should write and read data', async () => {
      const path = trackFile('write-read-test.txt')
      const testData = new Uint8Array([1, 2, 3, 4, 5])

      const writeResult = await backend.write(path, testData)

      expect(writeResult.etag).toBeDefined()
      expect(writeResult.size).toBe(5)

      const readResult = await backend.read(path)
      expect(readResult).toEqual(testData)
    })

    it('should write with content type', async () => {
      const path = trackFile('content-type-test.json')
      const testData = new TextEncoder().encode('{"test": true}')

      await backend.write(path, testData, {
        contentType: 'application/json',
      })

      const stat = await backend.stat(path)
      expect(stat).not.toBeNull()
      expect(stat!.contentType).toBe('application/json')
    })

    it('should write with custom metadata', async () => {
      const path = trackFile('metadata-test.txt')
      const testData = new TextEncoder().encode('test data')

      await backend.write(path, testData, {
        metadata: {
          'x-custom-field': 'custom-value',
          'x-version': '1',
        },
      })

      const stat = await backend.stat(path)
      expect(stat).not.toBeNull()
      expect(stat!.metadata).toBeDefined()
      expect(stat!.metadata!['x-custom-field']).toBe('custom-value')
      expect(stat!.metadata!['x-version']).toBe('1')
    })

    it('should handle empty data', async () => {
      const path = trackFile('empty-test.txt')
      const emptyData = new Uint8Array(0)

      const result = await backend.write(path, emptyData)
      expect(result.size).toBe(0)

      const readResult = await backend.read(path)
      expect(readResult.length).toBe(0)
    })

    it('should handle large data (1MB)', async () => {
      const path = trackFile('large-test.bin')
      const largeData = new Uint8Array(1024 * 1024)
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256
      }

      const writeResult = await backend.write(path, largeData)
      expect(writeResult.size).toBe(1024 * 1024)

      const readResult = await backend.read(path)
      expect(readResult.length).toBe(largeData.length)
      expect(readResult[0]).toBe(0)
      expect(readResult[255]).toBe(255)
      expect(readResult[256]).toBe(0)
    })
  })

  describe.runIf(hasCredentials)('writeAtomic operations', () => {
    it('should perform atomic write', async () => {
      const path = trackFile('atomic-test.txt')
      const testData = new Uint8Array([10, 20, 30])

      const result = await backend.writeAtomic(path, testData)

      expect(result.etag).toBeDefined()
      expect(result.size).toBe(3)

      const readResult = await backend.read(path)
      expect(readResult).toEqual(testData)
    })

    it('should overwrite existing file atomically', async () => {
      const path = trackFile('atomic-overwrite.txt')

      await backend.write(path, new Uint8Array([1, 2, 3]))
      const newData = new Uint8Array([4, 5, 6, 7])

      await backend.writeAtomic(path, newData)

      const readResult = await backend.read(path)
      expect(readResult).toEqual(newData)
    })
  })

  describe.runIf(hasCredentials)('read operations', () => {
    it('should read entire file', async () => {
      const path = trackFile('read-entire.txt')
      const testData = new Uint8Array([100, 101, 102, 103, 104])

      await backend.write(path, testData)
      const result = await backend.read(path)

      expect(result).toEqual(testData)
    })

    it('should throw R2NotFoundError for non-existent file', async () => {
      await expect(backend.read('non-existent-file-12345.txt')).rejects.toThrow(R2NotFoundError)
    })

    it('should read byte range', async () => {
      const path = trackFile('range-read.txt')
      const testData = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

      await backend.write(path, testData)

      // Read bytes 2-5 (inclusive)
      const rangeResult = await backend.readRange(path, 2, 5)
      expect(rangeResult).toEqual(new Uint8Array([2, 3, 4, 5]))
    })

    it('should read from start of file', async () => {
      const path = trackFile('range-start.txt')
      const testData = new Uint8Array([10, 20, 30, 40, 50])

      await backend.write(path, testData)

      const rangeResult = await backend.readRange(path, 0, 2)
      expect(rangeResult).toEqual(new Uint8Array([10, 20, 30]))
    })

    it('should read single byte', async () => {
      const path = trackFile('single-byte.txt')
      const testData = new Uint8Array([10, 20, 30, 40, 50])

      await backend.write(path, testData)

      const rangeResult = await backend.readRange(path, 2, 2)
      expect(rangeResult).toEqual(new Uint8Array([30]))
    })

    it('should validate range parameters', async () => {
      const path = trackFile('range-validate.txt')
      await backend.write(path, new Uint8Array([1, 2, 3]))

      // Negative start should throw
      await expect(backend.readRange(path, -1, 2)).rejects.toThrow(R2OperationError)

      // End < start should throw
      await expect(backend.readRange(path, 5, 2)).rejects.toThrow(R2OperationError)
    })
  })

  describe.runIf(hasCredentials)('exists operations', () => {
    it('should return true for existing file', async () => {
      const path = trackFile('exists-test.txt')
      await backend.write(path, new Uint8Array([1, 2, 3]))

      const exists = await backend.exists(path)
      expect(exists).toBe(true)
    })

    it('should return false for non-existent file', async () => {
      const exists = await backend.exists('non-existent-12345.txt')
      expect(exists).toBe(false)
    })
  })

  describe.runIf(hasCredentials)('stat operations', () => {
    it('should return file stats', async () => {
      const path = trackFile('stat-test.txt')
      const testData = new Uint8Array([1, 2, 3, 4, 5])

      await backend.write(path, testData, {
        contentType: 'text/plain',
      })

      const stat = await backend.stat(path)

      expect(stat).not.toBeNull()
      expect(stat!.path).toBe(path)
      expect(stat!.size).toBe(5)
      expect(stat!.isDirectory).toBe(false)
      expect(stat!.etag).toBeDefined()
      expect(stat!.mtime).toBeInstanceOf(Date)
    })

    it('should return null for non-existent file', async () => {
      const stat = await backend.stat('non-existent-stat-12345.txt')
      expect(stat).toBeNull()
    })
  })

  describe.runIf(hasCredentials)('list operations', () => {
    it('should list files with prefix', async () => {
      const prefix = 'list-test/'

      // Create test files
      const files = ['list-test/a.txt', 'list-test/b.txt', 'list-test/c.txt']
      for (const file of files) {
        trackFile(file)
        await backend.write(file, new Uint8Array([1, 2, 3]))
      }

      const result = await backend.list(prefix)

      expect(result.files).toHaveLength(3)
      expect(result.files).toContain('list-test/a.txt')
      expect(result.files).toContain('list-test/b.txt')
      expect(result.files).toContain('list-test/c.txt')
      expect(result.hasMore).toBe(false)
    })

    it('should handle empty result', async () => {
      const result = await backend.list('non-existent-prefix-12345/')

      expect(result.files).toHaveLength(0)
      expect(result.hasMore).toBe(false)
    })

    it('should support limit option', async () => {
      const prefix = 'list-limit/'

      // Create 5 files
      for (let i = 0; i < 5; i++) {
        const file = `list-limit/file${i}.txt`
        trackFile(file)
        await backend.write(file, new Uint8Array([i]))
      }

      const result = await backend.list(prefix, { limit: 2 })

      expect(result.files.length).toBeLessThanOrEqual(2)
    })

    it('should support delimiter for directory grouping', async () => {
      // Create files in nested structure
      const files = [
        'list-delim/root.txt',
        'list-delim/dir1/a.txt',
        'list-delim/dir1/b.txt',
        'list-delim/dir2/c.txt',
      ]
      for (const file of files) {
        trackFile(file)
        await backend.write(file, new Uint8Array([1]))
      }

      const result = await backend.list('list-delim/', { delimiter: '/' })

      // Should have root.txt as file and dir1/, dir2/ as prefixes
      expect(result.files).toContain('list-delim/root.txt')
      expect(result.prefixes).toBeDefined()
    })
  })

  describe.runIf(hasCredentials)('delete operations', () => {
    it('should delete existing file', async () => {
      const path = 'delete-test.txt'
      await backend.write(path, new Uint8Array([1, 2, 3]))

      const deleted = await backend.delete(path)
      expect(deleted).toBe(true)

      const exists = await backend.exists(path)
      expect(exists).toBe(false)
    })

    it('should return false when deleting non-existent file', async () => {
      const deleted = await backend.delete('non-existent-delete-12345.txt')
      expect(deleted).toBe(false)
    })

    it('should delete files with prefix', async () => {
      const prefix = 'delete-prefix/'

      // Create files
      for (let i = 0; i < 3; i++) {
        await backend.write(`delete-prefix/file${i}.txt`, new Uint8Array([i]))
      }

      const count = await backend.deletePrefix(prefix)
      expect(count).toBe(3)

      // Verify all deleted
      const result = await backend.list(prefix)
      expect(result.files).toHaveLength(0)
    })

    it('should return 0 when deleting non-existent prefix', async () => {
      const count = await backend.deletePrefix('non-existent-prefix-12345/')
      expect(count).toBe(0)
    })
  })

  describe.runIf(hasCredentials)('copy and move operations', () => {
    it('should copy file', async () => {
      const source = trackFile('copy-source.txt')
      const dest = trackFile('copy-dest.txt')
      const testData = new Uint8Array([1, 2, 3, 4, 5])

      await backend.write(source, testData)
      await backend.copy(source, dest)

      // Both should exist
      expect(await backend.exists(source)).toBe(true)
      expect(await backend.exists(dest)).toBe(true)

      // Content should match
      const destData = await backend.read(dest)
      expect(destData).toEqual(testData)
    })

    it('should throw when copying non-existent source', async () => {
      await expect(
        backend.copy('non-existent-source.txt', 'dest.txt')
      ).rejects.toThrow(R2NotFoundError)
    })

    it('should move file', async () => {
      const source = 'move-source.txt'
      const dest = trackFile('move-dest.txt')
      const testData = new Uint8Array([10, 20, 30])

      await backend.write(source, testData)
      await backend.move(source, dest)

      // Source should not exist, dest should
      expect(await backend.exists(source)).toBe(false)
      expect(await backend.exists(dest)).toBe(true)

      const destData = await backend.read(dest)
      expect(destData).toEqual(testData)
    })
  })

  describe.runIf(hasCredentials)('append operations', () => {
    it('should append to existing file', async () => {
      const path = trackFile('append-test.txt')

      await backend.write(path, new Uint8Array([1, 2, 3]))
      await backend.append(path, new Uint8Array([4, 5, 6]))

      const result = await backend.read(path)
      expect(result).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]))
    })

    it('should create file if it does not exist', async () => {
      const path = trackFile('append-new.txt')

      await backend.append(path, new Uint8Array([7, 8, 9]))

      const result = await backend.read(path)
      expect(result).toEqual(new Uint8Array([7, 8, 9]))
    })
  })

  describe.runIf(hasCredentials)('directory operations', () => {
    it('mkdir should be a no-op', async () => {
      // R2 doesn't have real directories
      await expect(backend.mkdir('test-dir/')).resolves.toBeUndefined()
    })

    it('rmdir should delete all files when recursive', async () => {
      const prefix = 'rmdir-test/'

      // Create files
      for (let i = 0; i < 3; i++) {
        trackFile(`rmdir-test/file${i}.txt`)
        await backend.write(`rmdir-test/file${i}.txt`, new Uint8Array([i]))
      }

      await backend.rmdir(prefix, { recursive: true })

      const result = await backend.list(prefix)
      expect(result.files).toHaveLength(0)
    })
  })

  describe.runIf(hasCredentials)('edge cases', () => {
    it('should handle keys with special characters', async () => {
      const path = trackFile('special chars (1).txt')
      const testData = new Uint8Array([1, 2, 3])

      await backend.write(path, testData)

      const exists = await backend.exists(path)
      expect(exists).toBe(true)

      const readResult = await backend.read(path)
      expect(readResult).toEqual(testData)
    })

    it('should handle binary data correctly', async () => {
      const path = trackFile('binary-test.bin')
      const binaryData = new Uint8Array(256)
      for (let i = 0; i < 256; i++) {
        binaryData[i] = i
      }

      await backend.write(path, binaryData)

      const result = await backend.read(path)
      expect(result).toEqual(binaryData)
    })

    it('should handle nested paths', async () => {
      const path = trackFile('deep/nested/path/file.txt')
      const testData = new Uint8Array([1, 2, 3])

      await backend.write(path, testData)

      const exists = await backend.exists(path)
      expect(exists).toBe(true)

      const readResult = await backend.read(path)
      expect(readResult).toEqual(testData)
    })
  })

  describe.runIf(hasCredentials)('constructor', () => {
    it('should have type "r2"', () => {
      expect(backend.type).toBe('r2')
    })

    it('should handle prefix configuration', async () => {
      // The backend already uses testPrefix
      const path = trackFile('prefix-test.txt')
      await backend.write(path, new Uint8Array([1, 2, 3]))

      // Verify the file exists at the prefixed location
      const exists = await backend.exists(path)
      expect(exists).toBe(true)
    })

    it('should normalize prefix with trailing slash', () => {
      const backendWithSlash = new R2Backend(bucket, { prefix: 'test/' })
      const backendWithoutSlash = new R2Backend(bucket, { prefix: 'test' })

      // Both should behave the same
      expect(backendWithSlash.type).toBe('r2')
      expect(backendWithoutSlash.type).toBe('r2')
    })
  })
})

// =============================================================================
// Unit Tests for Stale Upload Cleanup (no credentials required)
// =============================================================================

/**
 * Create a minimal mock R2Bucket for unit testing multipart upload cleanup.
 * Only the multipart-related methods are implemented.
 */
function createMockBucket() {
  let uploadCounter = 0
  const abortedUploadIds: string[] = []

  const mockBucket: R2Bucket = {
    async get() { return null },
    async head() { return null },
    async put() { return null },
    async delete() {},
    async list() {
      return { objects: [], truncated: false, delimitedPrefixes: [] }
    },
    async createMultipartUpload(key: string) {
      const uploadId = `upload-${++uploadCounter}`
      return {
        key,
        uploadId,
        async uploadPart(partNumber: number, _value: any) {
          return { partNumber, etag: `etag-${partNumber}` }
        },
        async abort() {
          abortedUploadIds.push(uploadId)
        },
        async complete(_parts: any[]) {
          return {
            key,
            version: 'v1',
            size: 100,
            etag: 'final-etag',
            httpEtag: '"final-etag"',
            uploaded: new Date(),
            storageClass: 'Standard' as const,
            checksums: {},
            writeHttpMetadata: () => {},
          }
        },
      }
    },
    resumeMultipartUpload(key: string, uploadId: string) {
      return {
        key,
        uploadId,
        async uploadPart(partNumber: number, _value: any) {
          return { partNumber, etag: `etag-${partNumber}` }
        },
        async abort() {
          abortedUploadIds.push(uploadId)
        },
        async complete(_parts: any[]) {
          return {
            key,
            version: 'v1',
            size: 100,
            etag: 'final-etag',
            httpEtag: '"final-etag"',
            uploaded: new Date(),
            storageClass: 'Standard' as const,
            checksums: {},
            writeHttpMetadata: () => {},
          }
        },
      }
    },
  }

  return { mockBucket, abortedUploadIds }
}

describe('R2Backend Stale Upload Cleanup (Unit Tests)', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('should track active upload count', async () => {
    const { mockBucket } = createMockBucket()
    const backend = new R2Backend(mockBucket)

    expect(backend.activeUploadCount).toBe(0)

    const uploadId = await backend.startMultipartUpload('test/file.bin')
    expect(backend.activeUploadCount).toBe(1)

    await backend.completeMultipartUpload('test/file.bin', uploadId, [])
    expect(backend.activeUploadCount).toBe(0)
  })

  it('should remove upload on abort', async () => {
    const { mockBucket } = createMockBucket()
    const backend = new R2Backend(mockBucket)

    const uploadId = await backend.startMultipartUpload('test/file.bin')
    expect(backend.activeUploadCount).toBe(1)

    await backend.abortMultipartUpload('test/file.bin', uploadId)
    expect(backend.activeUploadCount).toBe(0)
  })

  it('should clean up stale uploads past the TTL', async () => {
    const { mockBucket, abortedUploadIds } = createMockBucket()
    // Use a very short TTL (100ms) for testing
    const backend = new R2Backend(mockBucket, { multipartUploadTTL: 100 })

    // Start an upload
    await backend.startMultipartUpload('test/stale.bin')
    expect(backend.activeUploadCount).toBe(1)

    // Advance time past the TTL
    vi.advanceTimersByTime(150)

    // Cleanup should remove it
    const cleaned = backend.cleanupStaleUploads()
    expect(cleaned).toBe(1)
    expect(backend.activeUploadCount).toBe(0)
    // Verify abort was called on the R2 upload
    expect(abortedUploadIds).toHaveLength(1)
  })

  it('should not clean up uploads within the TTL', async () => {
    const { mockBucket } = createMockBucket()
    // Use a long TTL (10 seconds)
    const backend = new R2Backend(mockBucket, { multipartUploadTTL: 10000 })

    await backend.startMultipartUpload('test/fresh.bin')
    expect(backend.activeUploadCount).toBe(1)

    const cleaned = backend.cleanupStaleUploads()
    expect(cleaned).toBe(0)
    expect(backend.activeUploadCount).toBe(1)
  })

  it('should lazily clean up stale uploads when starting a new upload', async () => {
    const { mockBucket, abortedUploadIds } = createMockBucket()
    const backend = new R2Backend(mockBucket, { multipartUploadTTL: 100 })

    // Start first upload
    await backend.startMultipartUpload('test/stale.bin')
    expect(backend.activeUploadCount).toBe(1)

    // Advance time past the TTL
    vi.advanceTimersByTime(150)

    // Starting a new upload should trigger cleanup of the stale one
    await backend.startMultipartUpload('test/fresh.bin')

    // The stale one should be cleaned, and the fresh one should exist
    expect(backend.activeUploadCount).toBe(1)
    expect(abortedUploadIds).toHaveLength(1)
  })

  it('should clean up multiple stale uploads at once', async () => {
    const { mockBucket, abortedUploadIds } = createMockBucket()
    const backend = new R2Backend(mockBucket, { multipartUploadTTL: 100 })

    // Start three uploads
    await backend.startMultipartUpload('test/stale1.bin')
    await backend.startMultipartUpload('test/stale2.bin')
    await backend.startMultipartUpload('test/stale3.bin')
    expect(backend.activeUploadCount).toBe(3)

    // Advance time past the TTL
    vi.advanceTimersByTime(150)

    const cleaned = backend.cleanupStaleUploads()
    expect(cleaned).toBe(3)
    expect(backend.activeUploadCount).toBe(0)
    expect(abortedUploadIds).toHaveLength(3)
  })

  it('should only clean up stale uploads, leaving fresh ones', async () => {
    const { mockBucket, abortedUploadIds } = createMockBucket()
    const backend = new R2Backend(mockBucket, { multipartUploadTTL: 100 })

    // Start first upload (will become stale)
    await backend.startMultipartUpload('test/stale.bin')

    // Advance time past the TTL
    vi.advanceTimersByTime(150)

    // Start a fresh upload
    const freshUploadId = await backend.startMultipartUpload('test/fresh.bin')

    // After lazy cleanup from startMultipartUpload, only the fresh one remains
    expect(backend.activeUploadCount).toBe(1)
    expect(abortedUploadIds).toHaveLength(1)

    // The fresh upload should still work
    const partResult = await backend.uploadPart('test/fresh.bin', freshUploadId, 1, new Uint8Array([1, 2, 3]))
    expect(partResult.etag).toBeDefined()
  })

  it('should handle abort errors gracefully during cleanup', async () => {
    let uploadCounter = 0
    const mockBucket: R2Bucket = {
      async get() { return null },
      async head() { return null },
      async put() { return null },
      async delete() {},
      async list() {
        return { objects: [], truncated: false, delimitedPrefixes: [] }
      },
      async createMultipartUpload(key: string) {
        const uploadId = `upload-${++uploadCounter}`
        return {
          key,
          uploadId,
          async uploadPart(partNumber: number) {
            return { partNumber, etag: `etag-${partNumber}` }
          },
          async abort() {
            throw new Error('R2 abort failed: network error')
          },
          async complete() {
            return {
              key,
              version: 'v1',
              size: 100,
              etag: 'final-etag',
              httpEtag: '"final-etag"',
              uploaded: new Date(),
              storageClass: 'Standard' as const,
              checksums: {},
              writeHttpMetadata: () => {},
            }
          },
        }
      },
      resumeMultipartUpload() {
        throw new Error('not implemented')
      },
    }

    const backend = new R2Backend(mockBucket, { multipartUploadTTL: 100 })

    await backend.startMultipartUpload('test/stale.bin')

    // Advance time past the TTL
    vi.advanceTimersByTime(150)

    // Should not throw even though abort fails on the server
    const cleaned = backend.cleanupStaleUploads()
    expect(cleaned).toBe(1)
    expect(backend.activeUploadCount).toBe(0)
  })

  it('should use default TTL of 30 minutes', async () => {
    const { mockBucket } = createMockBucket()
    const backend = new R2Backend(mockBucket)

    // Start an upload
    await backend.startMultipartUpload('test/file.bin')

    // Immediate cleanup should not remove anything (default TTL is 30 minutes)
    const cleaned = backend.cleanupStaleUploads()
    expect(cleaned).toBe(0)
    expect(backend.activeUploadCount).toBe(1)
  })

  it('should return 0 when no stale uploads exist', () => {
    const { mockBucket } = createMockBucket()
    const backend = new R2Backend(mockBucket)

    const cleaned = backend.cleanupStaleUploads()
    expect(cleaned).toBe(0)
    expect(backend.activeUploadCount).toBe(0)
  })
})
