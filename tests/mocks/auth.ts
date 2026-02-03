/**
 * Authentication and CSRF Mock Factories
 *
 * Provides mock implementations of authentication and CSRF validation services.
 * These factories replace inline vi.fn() mocks with reusable, typed mock objects.
 */

import { vi, type Mock } from 'vitest'

// =============================================================================
// Types
// =============================================================================

/**
 * User object structure
 */
export interface MockUser {
  id: string
  email?: string
  name?: string
  picture?: string
  provider?: string
  [key: string]: unknown
}

/**
 * CSRF validation result
 */
export interface CsrfValidationResult {
  valid: boolean
  reason?: string
}

/**
 * Mock authentication service interface
 */
export interface MockAuthService {
  getUser: Mock<[unknown], MockUser | null>
  requireUser: Mock<[unknown], MockUser>
  verifyToken: Mock<[string], Promise<MockUser | null>>

  // Test helpers
  _currentUser: MockUser | null
  _setUser: (user: MockUser | null) => void
}

/**
 * Mock CSRF service interface
 */
export interface MockCsrfService {
  validateCsrf: Mock<[unknown], CsrfValidationResult>
  generateToken: Mock<[], string>

  // Test helpers
  _isValid: boolean
  _setValid: (valid: boolean, reason?: string) => void
}

/**
 * Options for creating mock auth service
 */
export interface MockAuthServiceOptions {
  /**
   * Initial user (null for unauthenticated)
   */
  user?: MockUser | null

  /**
   * If true, returns functional implementation
   */
  functional?: boolean
}

/**
 * Options for creating mock CSRF service
 */
export interface MockCsrfServiceOptions {
  /**
   * Initial validity state (default: true)
   */
  valid?: boolean

  /**
   * Reason for invalid state
   */
  reason?: string
}

// =============================================================================
// Default Test Data
// =============================================================================

/**
 * Create a test user with sensible defaults
 */
export function createTestUser(overrides: Partial<MockUser> = {}): MockUser {
  return {
    id: overrides.id ?? `user_${Math.random().toString(36).slice(2, 10)}`,
    email: overrides.email ?? 'test@example.com',
    name: overrides.name ?? 'Test User',
    ...overrides,
  }
}

/**
 * Common test users for reuse across tests
 */
export const TEST_USERS = {
  admin: createTestUser({ id: 'user_admin', email: 'admin@example.com', name: 'Admin User' }),
  regular: createTestUser({ id: 'user_123', email: 'user@example.com', name: 'Regular User' }),
  guest: createTestUser({ id: 'user_guest', email: 'guest@example.com', name: 'Guest User' }),
} as const

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a mock authentication service
 *
 * @param options - Configuration options
 * @returns Mock auth service
 *
 * @example
 * ```typescript
 * // Unauthenticated (default)
 * const auth = createMockAuthService()
 * expect(auth.getUser(ctx)).toBeNull()
 *
 * // Authenticated user
 * const auth = createMockAuthService({ user: TEST_USERS.regular })
 * expect(auth.getUser(ctx)).toEqual(TEST_USERS.regular)
 * ```
 */
export function createMockAuthService(options?: MockAuthServiceOptions): MockAuthService {
  let currentUser: MockUser | null = options?.user ?? null

  if (options?.functional) {
    return {
      _currentUser: currentUser,
      _setUser: (user: MockUser | null) => {
        currentUser = user
      },

      getUser: vi.fn((_ctx: unknown): MockUser | null => {
        return currentUser
      }),

      requireUser: vi.fn((_ctx: unknown): MockUser => {
        if (!currentUser) {
          throw new Error('Authentication required')
        }
        return currentUser
      }),

      verifyToken: vi.fn(async (_token: string): Promise<MockUser | null> => {
        return currentUser
      }),
    }
  }

  // Spy-based mock
  const service: MockAuthService = {
    _currentUser: currentUser,
    _setUser: (user: MockUser | null) => {
      currentUser = user
      service.getUser.mockReturnValue(user)
      if (user) {
        service.requireUser.mockReturnValue(user)
        service.verifyToken.mockResolvedValue(user)
      } else {
        service.requireUser.mockImplementation(() => {
          throw new Error('Authentication required')
        })
        service.verifyToken.mockResolvedValue(null)
      }
    },

    getUser: vi.fn().mockReturnValue(currentUser),
    requireUser: currentUser
      ? vi.fn().mockReturnValue(currentUser)
      : vi.fn().mockImplementation(() => {
          throw new Error('Authentication required')
        }),
    verifyToken: vi.fn().mockResolvedValue(currentUser),
  }

  return service
}

/**
 * Create a mock CSRF validation service
 *
 * @param options - Configuration options
 * @returns Mock CSRF service
 *
 * @example
 * ```typescript
 * // Valid by default
 * const csrf = createMockCsrfService()
 * expect(csrf.validateCsrf(ctx)).toEqual({ valid: true })
 *
 * // Invalid
 * const csrf = createMockCsrfService({ valid: false, reason: 'Missing header' })
 * expect(csrf.validateCsrf(ctx)).toEqual({ valid: false, reason: 'Missing header' })
 * ```
 */
export function createMockCsrfService(options?: MockCsrfServiceOptions): MockCsrfService {
  let isValid = options?.valid ?? true
  let reason = options?.reason

  const service: MockCsrfService = {
    _isValid: isValid,
    _setValid: (valid: boolean, newReason?: string) => {
      isValid = valid
      reason = newReason
      service.validateCsrf.mockReturnValue(
        valid ? { valid: true } : { valid: false, reason: newReason }
      )
    },

    validateCsrf: vi.fn().mockReturnValue(
      isValid ? { valid: true } : { valid: false, reason }
    ),

    generateToken: vi.fn().mockReturnValue(`csrf_${Math.random().toString(36).slice(2)}`),
  }

  return service
}

/**
 * Create mock getUser function for direct use with vi.mock
 *
 * @param user - User to return (null for unauthenticated)
 * @returns Mock getUser function
 */
export function createMockGetUser(user?: MockUser | null): Mock<[unknown], MockUser | null> {
  return vi.fn().mockReturnValue(user ?? null)
}

/**
 * Create mock validateCsrf function for direct use with vi.mock
 *
 * @param valid - Whether validation should pass
 * @param reason - Reason for failure (when valid is false)
 * @returns Mock validateCsrf function
 */
export function createMockValidateCsrf(
  valid: boolean = true,
  reason?: string
): Mock<[unknown], CsrfValidationResult> {
  return vi.fn().mockReturnValue(valid ? { valid: true } : { valid: false, reason })
}

// =============================================================================
// Hono Context Helpers
// =============================================================================

/**
 * Create a mock Hono context with authentication
 *
 * @param options - Context options
 * @returns Mock context object
 */
export function createMockAuthContext(options: {
  user?: MockUser | null
  actor?: string | null
  vars?: Record<string, unknown>
} = {}): {
  var: Record<string, unknown>
  set: Mock<[string, unknown], void>
  req: {
    header: Mock<[string], string | undefined>
    param: Mock<[string], string | undefined>
    url: string
  }
} {
  const vars: Record<string, unknown> = {
    ...options.vars,
  }

  if (options.user !== undefined) {
    vars.user = options.user
  }

  if (options.actor !== undefined) {
    vars.actor = options.actor
  }

  return {
    var: vars,
    set: vi.fn((key: string, value: unknown) => {
      vars[key] = value
    }),
    req: {
      header: vi.fn().mockReturnValue(undefined),
      param: vi.fn().mockReturnValue(undefined),
      url: 'http://localhost/test',
    },
  }
}
