/**
 * Fetch Mock Factory
 *
 * Provides mock implementations of the fetch API for testing.
 * Includes helpers for creating mock responses and request matchers.
 */

import { vi, type Mock } from 'vitest'

// =============================================================================
// Types
// =============================================================================

/**
 * Mock fetch function type
 */
export type MockFetch = Mock<Parameters<typeof fetch>, ReturnType<typeof fetch>>

/**
 * Options for creating mock responses
 */
export interface MockResponseOptions {
  status?: number | undefined
  statusText?: string | undefined
  headers?: Record<string, string> | Headers | undefined
  ok?: boolean | undefined
}

/**
 * Route handler for mock fetch
 */
export interface FetchRoute {
  pattern: string | RegExp | ((url: string) => boolean)
  method?: string | string[] | undefined
  handler: (request: Request) => Response | Promise<Response>
}

/**
 * Options for creating mock fetch
 */
export interface MockFetchOptions {
  /**
   * Default response when no routes match
   */
  defaultResponse?: (Response | ((request: Request) => Response | Promise<Response>)) | undefined

  /**
   * Routes to handle specific URL patterns
   */
  routes?: FetchRoute[] | undefined

  /**
   * Whether to throw on unmatched requests (default: false, returns 404)
   */
  throwOnUnmatched?: boolean | undefined
}

// =============================================================================
// Response Helpers
// =============================================================================

/**
 * Create a mock Response object
 *
 * @param body - Response body (string, object for JSON, or Uint8Array)
 * @param options - Response options
 * @returns Response object
 *
 * @example
 * ```typescript
 * const response = createMockResponse({ data: 'test' }, { status: 200 })
 * const response = createMockResponse('plain text', { headers: { 'Content-Type': 'text/plain' } })
 * ```
 */
export function createMockResponse(
  body?: string | Record<string, unknown> | Uint8Array | null,
  options?: MockResponseOptions
): Response {
  const status = options?.status ?? 200
  const statusText = options?.statusText ?? 'OK'
  const ok = options?.ok ?? (status >= 200 && status < 300)

  // Build headers
  const headers = new Headers()
  if (options?.headers instanceof Headers) {
    options.headers.forEach((value, key) => headers.set(key, value))
  } else if (options?.headers) {
    for (const [key, value] of Object.entries(options.headers)) {
      headers.set(key, value)
    }
  }

  // Determine body and content type
  let responseBody: BodyInit | null = null
  if (body === null || body === undefined) {
    responseBody = null
  } else if (body instanceof Uint8Array) {
    responseBody = body
    if (!headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/octet-stream')
    }
  } else if (typeof body === 'string') {
    responseBody = body
    if (!headers.has('Content-Type')) {
      headers.set('Content-Type', 'text/plain')
    }
  } else {
    responseBody = JSON.stringify(body)
    if (!headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json')
    }
  }

  // Create response with standard Response constructor
  const response = new Response(responseBody, {
    status,
    statusText,
    headers,
  })

  // Override ok property if explicitly set
  if (options?.ok !== undefined && options.ok !== response.ok) {
    Object.defineProperty(response, 'ok', { value: options.ok })
  }

  return response
}

/**
 * Create a JSON response
 *
 * This function JSON.stringify's the data and creates a proper JSON response.
 * Use this when you need to send JSON data (including null, arrays, strings).
 */
export function createJsonResponse<T>(data: T, options?: Omit<MockResponseOptions, 'headers'>): Response {
  const body = JSON.stringify(data)
  const status = options?.status ?? 200
  const statusText = options?.statusText ?? 'OK'

  return new Response(body, {
    status,
    statusText,
    headers: { 'Content-Type': 'application/json' },
  })
}

/**
 * Create a text response
 */
export function createTextResponse(text: string, options?: Omit<MockResponseOptions, 'headers'>): Response {
  return createMockResponse(text, {
    ...options,
    headers: { 'Content-Type': 'text/plain' },
  })
}

/**
 * Create a binary response
 */
export function createBinaryResponse(data: Uint8Array, options?: MockResponseOptions): Response {
  return createMockResponse(data, {
    ...options,
    headers: {
      'Content-Type': 'application/octet-stream',
      ...options?.headers,
    } as Record<string, string>,
  })
}

/**
 * Create an error response
 */
export function createErrorResponse(
  status: number,
  message?: string,
  options?: Omit<MockResponseOptions, 'status'>
): Response {
  const body = message ? { error: message } : null
  return createMockResponse(body as Record<string, unknown> | null, {
    ...options,
    status,
    statusText: options?.statusText ?? getStatusText(status),
    ok: false,
  })
}

/**
 * Get standard status text for HTTP status codes
 */
function getStatusText(status: number): string {
  const statusTexts: Record<number, string> = {
    200: 'OK',
    201: 'Created',
    204: 'No Content',
    301: 'Moved Permanently',
    302: 'Found',
    304: 'Not Modified',
    400: 'Bad Request',
    401: 'Unauthorized',
    403: 'Forbidden',
    404: 'Not Found',
    405: 'Method Not Allowed',
    409: 'Conflict',
    422: 'Unprocessable Entity',
    429: 'Too Many Requests',
    500: 'Internal Server Error',
    502: 'Bad Gateway',
    503: 'Service Unavailable',
    504: 'Gateway Timeout',
  }
  return statusTexts[status] ?? 'Unknown'
}

// =============================================================================
// Fetch Factory Functions
// =============================================================================

/**
 * Create a simple mock fetch function
 *
 * @returns Mock fetch function with default 404 response
 *
 * @example
 * ```typescript
 * const mockFetch = createMockFetch()
 * mockFetch.mockResolvedValue(createJsonResponse({ data: 'test' }))
 * ```
 */
export function createMockFetch(): MockFetch {
  return vi.fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>()
    .mockResolvedValue(createErrorResponse(404, 'Not Found'))
}

/**
 * Create a mock fetch with routing support
 *
 * @param options - Configuration options including routes
 * @returns Mock fetch function that routes requests
 *
 * @example
 * ```typescript
 * const mockFetch = createRoutedMockFetch({
 *   routes: [
 *     {
 *       pattern: '/api/users',
 *       method: 'GET',
 *       handler: () => createJsonResponse([{ id: 1, name: 'Alice' }])
 *     },
 *     {
 *       pattern: /\/api\/users\/\d+/,
 *       method: 'GET',
 *       handler: (req) => {
 *         const id = req.url.split('/').pop()
 *         return createJsonResponse({ id, name: 'User' })
 *       }
 *     }
 *   ]
 * })
 * ```
 */
export function createRoutedMockFetch(options?: MockFetchOptions): MockFetch {
  const routes = options?.routes ?? []
  const defaultResponse = options?.defaultResponse ?? createErrorResponse(404, 'Not Found')
  const throwOnUnmatched = options?.throwOnUnmatched ?? false

  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = input instanceof Request
      ? input
      : new Request(input instanceof URL ? input.toString() : input, init)

    const url = new URL(request.url)
    const urlPath = url.pathname

    // Find matching route
    for (const route of routes) {
      // Check method
      if (route.method) {
        const methods = Array.isArray(route.method) ? route.method : [route.method]
        if (!methods.includes(request.method)) {
          continue
        }
      }

      // Check pattern
      let matches = false
      if (typeof route.pattern === 'string') {
        matches = urlPath === route.pattern || urlPath.startsWith(route.pattern)
      } else if (route.pattern instanceof RegExp) {
        matches = route.pattern.test(urlPath) || route.pattern.test(request.url)
      } else {
        matches = route.pattern(request.url)
      }

      if (matches) {
        return route.handler(request)
      }
    }

    // No match found
    if (throwOnUnmatched) {
      throw new Error(`No route matched for ${request.method} ${request.url}`)
    }

    if (typeof defaultResponse === 'function') {
      return defaultResponse(request)
    }
    return defaultResponse.clone()
  })
}

/**
 * Create a mock fetch that returns sequential responses
 *
 * @param responses - Array of responses to return in order
 * @returns Mock fetch function
 *
 * @example
 * ```typescript
 * const mockFetch = createSequentialMockFetch([
 *   createJsonResponse({ attempt: 1 }),
 *   createErrorResponse(503),
 *   createJsonResponse({ attempt: 3 }),
 * ])
 * ```
 */
export function createSequentialMockFetch(responses: Response[]): MockFetch {
  let index = 0
  return vi.fn(async (): Promise<Response> => {
    if (index >= responses.length) {
      return createErrorResponse(500, 'No more responses configured')
    }
    return responses[index++].clone()
  })
}

/**
 * Create a mock fetch that simulates network failures
 *
 * @param errorType - Type of network error to simulate
 * @returns Mock fetch function that rejects
 */
export function createFailingMockFetch(
  errorType: 'network' | 'timeout' | 'aborted' = 'network'
): MockFetch {
  return vi.fn(async (): Promise<Response> => {
    switch (errorType) {
      case 'timeout':
        throw new DOMException('The operation timed out', 'TimeoutError')
      case 'aborted':
        throw new DOMException('The operation was aborted', 'AbortError')
      default:
        throw new TypeError('Failed to fetch')
    }
  })
}

// =============================================================================
// Global Fetch Helpers
// =============================================================================

/**
 * Store for original fetch
 */
let originalFetch: typeof fetch | undefined

/**
 * Install a mock fetch globally
 *
 * @param mockFetch - Mock fetch to install
 *
 * @example
 * ```typescript
 * beforeEach(() => {
 *   installMockFetch(createMockFetch())
 * })
 *
 * afterEach(() => {
 *   restoreGlobalFetch()
 * })
 * ```
 */
export function installMockFetch(mockFetch: MockFetch): void {
  if (!originalFetch) {
    originalFetch = globalThis.fetch
  }
  globalThis.fetch = mockFetch
}

/**
 * Restore the original global fetch
 */
export function restoreGlobalFetch(): void {
  if (originalFetch) {
    globalThis.fetch = originalFetch
    originalFetch = undefined
  }
}

/**
 * Create a scoped mock fetch that automatically restores on cleanup
 *
 * @param mockFetch - Mock fetch to use
 * @returns Cleanup function
 *
 * @example
 * ```typescript
 * const cleanup = useMockFetch(createMockFetch())
 * // ... tests ...
 * cleanup()
 * ```
 */
export function useMockFetch(mockFetch: MockFetch): () => void {
  installMockFetch(mockFetch)
  return restoreGlobalFetch
}
