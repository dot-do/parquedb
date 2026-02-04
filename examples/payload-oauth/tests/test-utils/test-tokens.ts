/**
 * Test Token Generation Utilities
 *
 * Provides functions to generate signed JWTs for E2E testing.
 * Uses the private key from the local JWKS server.
 */

import { SignJWT, type KeyLike } from 'jose'

/**
 * Claims for a test JWT
 */
export interface TestTokenClaims {
  /** User ID (subject) */
  sub: string
  /** Email address */
  email?: string
  /** Display name */
  name?: string
  /** First name */
  firstName?: string
  /** Last name */
  lastName?: string
  /** User roles */
  roles?: string[]
  /** Organization ID */
  org_id?: string
  /** Custom additional claims */
  [key: string]: unknown
}

/**
 * Options for creating a test token
 */
export interface CreateTokenOptions {
  /** Key ID matching the JWKS */
  kid: string
  /** Issuer (default: 'test-issuer') */
  issuer?: string
  /** Audience (optional) */
  audience?: string
  /** Expiration time in seconds from now (default: 3600 = 1 hour) */
  expiresIn?: number
  /** Override issued-at timestamp (for testing expired tokens) */
  issuedAt?: Date
}

/**
 * Create a signed test JWT
 */
export async function createTestToken(
  privateKey: KeyLike,
  claims: TestTokenClaims,
  options: CreateTokenOptions
): Promise<string> {
  const now = options.issuedAt || new Date()
  const expiresIn = options.expiresIn ?? 3600

  let jwt = new SignJWT(claims)
    .setProtectedHeader({ alg: 'RS256', kid: options.kid })
    .setIssuer(options.issuer ?? 'test-issuer')
    .setIssuedAt(now)
    .setExpirationTime(new Date(now.getTime() + expiresIn * 1000))

  if (options.audience) {
    jwt = jwt.setAudience(options.audience)
  }

  return jwt.sign(privateKey)
}

/**
 * Create an expired test token (for testing rejection)
 */
export async function createExpiredToken(
  privateKey: KeyLike,
  claims: TestTokenClaims,
  options: Omit<CreateTokenOptions, 'expiresIn' | 'issuedAt'>
): Promise<string> {
  // Token expired 1 hour ago
  const issuedAt = new Date(Date.now() - 2 * 60 * 60 * 1000) // 2 hours ago
  return createTestToken(privateKey, claims, {
    ...options,
    issuedAt,
    expiresIn: 3600, // 1 hour from issued time (so expired 1 hour ago)
  })
}

/**
 * Create a token with wrong audience (for testing rejection)
 */
export async function createWrongAudienceToken(
  privateKey: KeyLike,
  claims: TestTokenClaims,
  options: Omit<CreateTokenOptions, 'audience'>
): Promise<string> {
  return createTestToken(privateKey, claims, {
    ...options,
    audience: 'wrong-audience',
  })
}

/**
 * Pre-defined test users with their claims
 */
export const TEST_USERS = {
  admin: {
    sub: 'user-admin-123',
    email: 'admin@example.com',
    name: 'Test Admin',
    firstName: 'Test',
    lastName: 'Admin',
    roles: ['admin'],
  } satisfies TestTokenClaims,

  editor: {
    sub: 'user-editor-456',
    email: 'editor@example.com',
    name: 'Test Editor',
    firstName: 'Test',
    lastName: 'Editor',
    roles: ['editor'],
  } satisfies TestTokenClaims,

  viewer: {
    sub: 'user-viewer-789',
    email: 'viewer@example.com',
    name: 'Test Viewer',
    firstName: 'Test',
    lastName: 'Viewer',
    roles: ['viewer'], // No admin or editor role
  } satisfies TestTokenClaims,

  noRoles: {
    sub: 'user-noroles-000',
    email: 'noroles@example.com',
    name: 'No Roles User',
  } satisfies TestTokenClaims,
} as const

/**
 * Result of generating all test tokens
 */
export interface TestTokens {
  admin: string
  editor: string
  viewer: string
  noRoles: string
  expired: string
}

/**
 * Generate all test tokens for E2E testing
 */
export async function generateAllTestTokens(
  privateKey: KeyLike,
  kid: string,
  options?: {
    issuer?: string
    audience?: string
  }
): Promise<TestTokens> {
  const baseOptions: CreateTokenOptions = {
    kid,
    issuer: options?.issuer ?? 'test-issuer',
    audience: options?.audience,
  }

  const [admin, editor, viewer, noRoles, expired] = await Promise.all([
    createTestToken(privateKey, TEST_USERS.admin, baseOptions),
    createTestToken(privateKey, TEST_USERS.editor, baseOptions),
    createTestToken(privateKey, TEST_USERS.viewer, baseOptions),
    createTestToken(privateKey, TEST_USERS.noRoles, baseOptions),
    createExpiredToken(privateKey, TEST_USERS.admin, baseOptions),
  ])

  return { admin, editor, viewer, noRoles, expired }
}

/**
 * Write tokens to a file for use by tests
 */
export async function writeTokensToFile(
  tokens: TestTokens,
  filePath: string
): Promise<void> {
  const fs = await import('fs/promises')
  await fs.writeFile(filePath, JSON.stringify(tokens, null, 2))
  console.log(`Test tokens written to ${filePath}`)
}

/**
 * Read tokens from file
 */
export async function readTokensFromFile(filePath: string): Promise<TestTokens> {
  const fs = await import('fs/promises')
  const content = await fs.readFile(filePath, 'utf-8')
  return JSON.parse(content) as TestTokens
}
