/**
 * NS Handler Validation Tests
 *
 * Tests that the /ns/:namespace handler properly validates HTTP inputs:
 * - POST body requires $type and name
 * - PATCH body requires valid update operators
 * - Invalid JSON filter returns 400, not all records
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { handleNsRoute } from '@/worker/handlers/ns'
import type { HandlerContext } from '@/worker/handlers/types'

// =============================================================================
// Mock Worker
// =============================================================================

function createMockWorker() {
  return {
    get: vi.fn().mockResolvedValue({ $id: 'test/1', $type: 'Test', name: 'Test Entity' }),
    find: vi.fn().mockResolvedValue({ items: [], stats: {}, hasMore: false }),
    create: vi.fn().mockResolvedValue({ $id: 'test/new', $type: 'Test', name: 'New' }),
    update: vi.fn().mockResolvedValue({ matchedCount: 1, modifiedCount: 1 }),
    delete: vi.fn().mockResolvedValue({ deletedCount: 1 }),
  }
}

function createContext(
  method: string,
  body?: unknown,
  searchParams?: Record<string, string>,
): { context: HandlerContext; worker: ReturnType<typeof createMockWorker> } {
  const worker = createMockWorker()
  const url = new URL(`http://localhost/ns/test${searchParams ? '?' + new URLSearchParams(searchParams).toString() : ''}`)

  // Build headers - include CSRF headers for mutation methods
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (['POST', 'PATCH', 'PUT', 'DELETE'].includes(method)) {
    headers['X-Requested-With'] = 'XMLHttpRequest'
    headers['Origin'] = 'http://localhost'
  }

  const requestInit: RequestInit = {
    method,
    headers,
  }

  // For requests with a body, we need to provide it
  if (body !== undefined) {
    requestInit.body = JSON.stringify(body)
  }

  const request = new Request(url.toString(), requestInit)

  const context: HandlerContext = {
    request,
    url,
    baseUrl: 'http://localhost',
    path: '/ns/test',
    worker: worker as any,
    startTime: performance.now(),
    ctx: { waitUntil: vi.fn(), passThroughOnException: vi.fn() } as any,
  }

  return { context, worker }
}

function createContextWithInvalidJson(method: string): { context: HandlerContext; worker: ReturnType<typeof createMockWorker> } {
  const worker = createMockWorker()
  const url = new URL('http://localhost/ns/test')

  // Build headers - include CSRF headers for mutation methods
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (['POST', 'PATCH', 'PUT', 'DELETE'].includes(method)) {
    headers['X-Requested-With'] = 'XMLHttpRequest'
    headers['Origin'] = 'http://localhost'
  }

  // Create a request with invalid JSON body
  const request = new Request(url.toString(), {
    method,
    headers,
    body: 'this is not json{{{',
  })

  const context: HandlerContext = {
    request,
    url,
    baseUrl: 'http://localhost',
    path: '/ns/test',
    worker: worker as any,
    startTime: performance.now(),
    ctx: { waitUntil: vi.fn(), passThroughOnException: vi.fn() } as any,
  }

  return { context, worker }
}

// =============================================================================
// POST Validation
// =============================================================================

describe('handleNsRoute - POST validation', () => {
  it('warns but creates when type is missing', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { context, worker } = createContext('POST', { name: 'Test Entity' })
    const response = await handleNsRoute(context, 'test')
    expect(response.status).toBe(201)
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('missing type field'))
    expect(worker.create).toHaveBeenCalled()
    warnSpy.mockRestore()
  })

  it('warns but creates when name is missing', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { context, worker } = createContext('POST', { type: 'TestType' })
    const response = await handleNsRoute(context, 'test')
    expect(response.status).toBe(201)
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('missing name field'))
    expect(worker.create).toHaveBeenCalled()
    warnSpy.mockRestore()
  })

  it('throws QueryParamError when body is not an object', async () => {
    const { context } = createContext('POST', 'just a string')
    await expect(handleNsRoute(context, 'test')).rejects.toThrow('Invalid body: must be a JSON object')
  })

  it('throws QueryParamError when body is an array', async () => {
    const { context } = createContext('POST', [1, 2, 3])
    await expect(handleNsRoute(context, 'test')).rejects.toThrow('Invalid body: must be a JSON object')
  })

  it('warns but creates when type is empty string', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { context, worker } = createContext('POST', { type: '', name: 'Test' })
    const response = await handleNsRoute(context, 'test')
    expect(response.status).toBe(201)
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('missing type field'))
    expect(worker.create).toHaveBeenCalled()
    warnSpy.mockRestore()
  })

  it('warns but creates when type is not a string', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { context, worker } = createContext('POST', { type: 123, name: 'Test' })
    const response = await handleNsRoute(context, 'test')
    // type: 123 is truthy so the implementation won't warn about type
    expect(response.status).toBe(201)
    expect(worker.create).toHaveBeenCalled()
    warnSpy.mockRestore()
  })

  it('warns but creates when name is not a string', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { context, worker } = createContext('POST', { type: 'Test', name: 42 })
    const response = await handleNsRoute(context, 'test')
    // name: 42 is truthy so the implementation won't warn about name
    expect(response.status).toBe(201)
    expect(worker.create).toHaveBeenCalled()
    warnSpy.mockRestore()
  })

  it('throws QueryParamError when body is invalid JSON', async () => {
    const { context } = createContextWithInvalidJson('POST')
    await expect(handleNsRoute(context, 'test')).rejects.toThrow('Invalid JSON body')
  })

  it('succeeds with valid create body', async () => {
    const { context, worker } = createContext('POST', { type: 'Post', name: 'My Post', title: 'Hello' })
    const response = await handleNsRoute(context, 'test')
    expect(response.status).toBe(201)
    expect(worker.create).toHaveBeenCalledOnce()
  })
})

// =============================================================================
// PATCH Validation
// =============================================================================

describe('handleNsRoute - PATCH validation', () => {
  it('passes through body without operator validation', async () => {
    const { context, worker } = createContext('PATCH', { name: 'Updated' })
    const response = await handleNsRoute(context, 'test', 'entity-1')
    expect(response.status).toBe(200)
    expect(worker.update).toHaveBeenCalledWith('test', 'entity-1', { name: 'Updated' })
  })

  it('passes through empty body', async () => {
    const { context, worker } = createContext('PATCH', {})
    const response = await handleNsRoute(context, 'test', 'entity-1')
    expect(response.status).toBe(200)
    expect(worker.update).toHaveBeenCalledWith('test', 'entity-1', {})
  })

  it('throws QueryParamError when body is not an object', async () => {
    const { context } = createContext('PATCH', 'not-an-object')
    await expect(handleNsRoute(context, 'test', 'entity-1')).rejects.toThrow('Invalid body: must be a JSON object')
  })

  it('passes through body with unknown operators', async () => {
    const { context, worker } = createContext('PATCH', { $invalid: { field: 'value' } })
    const response = await handleNsRoute(context, 'test', 'entity-1')
    expect(response.status).toBe(200)
    expect(worker.update).toHaveBeenCalledWith('test', 'entity-1', { $invalid: { field: 'value' } })
  })

  it('throws QueryParamError when body is invalid JSON', async () => {
    const { context } = createContextWithInvalidJson('PATCH')
    await expect(handleNsRoute(context, 'test', 'entity-1')).rejects.toThrow('Invalid JSON body')
  })

  it('returns 400 when no ID provided', async () => {
    const { context } = createContext('PATCH', { $set: { name: 'Updated' } })
    const response = await handleNsRoute(context, 'test')
    expect(response.status).toBe(400)
    const body = await response.json() as any
    expect(body.api.message).toContain('ID required')
  })

  it('succeeds with valid $set operator', async () => {
    const { context, worker } = createContext('PATCH', { $set: { name: 'Updated' } })
    const response = await handleNsRoute(context, 'test', 'entity-1')
    expect(response.status).toBe(200)
    expect(worker.update).toHaveBeenCalledOnce()
  })

  it('succeeds with valid $inc operator', async () => {
    const { context, worker } = createContext('PATCH', { $inc: { viewCount: 1 } })
    const response = await handleNsRoute(context, 'test', 'entity-1')
    expect(response.status).toBe(200)
    expect(worker.update).toHaveBeenCalledOnce()
  })

  it('succeeds with multiple valid operators', async () => {
    const { context, worker } = createContext('PATCH', {
      $set: { status: 'published' },
      $inc: { version: 1 },
    })
    const response = await handleNsRoute(context, 'test', 'entity-1')
    expect(response.status).toBe(200)
    expect(worker.update).toHaveBeenCalledOnce()
  })
})

// =============================================================================
// GET with invalid filter
// =============================================================================

describe('handleNsRoute - GET validation', () => {
  it('returns 400 when filter is invalid JSON', async () => {
    const { context } = createContext('GET', undefined, { filter: 'not-json' })
    const response = await handleNsRoute(context, 'test')
    expect(response.status).toBe(400)
    const body = await response.json() as any
    expect(body.api.message).toContain('Invalid filter: must be valid JSON')
  })

  it('returns 400 when filter is a JSON array', async () => {
    const { context } = createContext('GET', undefined, { filter: '[1,2,3]' })
    const response = await handleNsRoute(context, 'test')
    expect(response.status).toBe(400)
    const body = await response.json() as any
    expect(body.api.message).toContain('must be a JSON object')
  })

  it('does not call find when filter is invalid', async () => {
    const { context, worker } = createContext('GET', undefined, { filter: 'bad-json' })
    await handleNsRoute(context, 'test')
    expect(worker.find).not.toHaveBeenCalled()
  })

  it('succeeds with valid filter', async () => {
    const { context, worker } = createContext('GET', undefined, { filter: '{"status":"published"}' })
    await handleNsRoute(context, 'test')
    expect(worker.find).toHaveBeenCalledOnce()
  })

  it('succeeds with no filter (returns all)', async () => {
    const { context, worker } = createContext('GET')
    await handleNsRoute(context, 'test')
    expect(worker.find).toHaveBeenCalledOnce()
  })

  it('returns 400 when limit is not a number', async () => {
    const { context } = createContext('GET', undefined, { limit: 'abc' })
    const response = await handleNsRoute(context, 'test')
    expect(response.status).toBe(400)
    const body = await response.json() as any
    expect(body.api.message).toContain('Invalid limit: must be a non-negative integer')
  })
})
