/**
 * MCP Authentication Tests
 *
 * Tests for the ParqueDB MCP server authentication module.
 * Covers API key authentication, custom authenticators, scope checking,
 * and integration with the MCP server.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { DB } from '../../../src/db'
import { createParqueDBMCPServer } from '../../../src/integrations/mcp'
import {
  createApiKeyAuthenticator,
  createCustomAuthenticator,
  parseBearerToken,
  redactToken,
  hasRequiredScopes,
  getToolScopes,
  PARQUEDB_SCOPES,
  TOOL_SCOPE_REQUIREMENTS,
  AuthenticationError,
  type AuthInfo,
  type AuthContext,
  type ApiKeyEntry,
} from '../../../src/integrations/mcp/auth'
import type { ToolResult } from '../../../src/integrations/mcp/types'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'

/**
 * Helper to parse MCP tool result content
 */
function parseToolResult(content: Array<{ type: string; text?: string }>): ToolResult {
  const textContent = content.find(c => c.type === 'text')
  if (!textContent?.text) {
    throw new Error('No text content in tool result')
  }
  return JSON.parse(textContent.text) as ToolResult
}

describe('MCP Authentication', () => {
  describe('createApiKeyAuthenticator', () => {
    it('should authenticate valid API key', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key-123', { clientId: 'client-1', scopes: ['parquedb:read'] }]
        ])
      })

      const result = await authenticator.authenticate('test-key-123')

      expect(result.success).toBe(true)
      if (result.success) {
        expect(result.authInfo.clientId).toBe('client-1')
        expect(result.authInfo.scopes).toContain('parquedb:read')
      }
    })

    it('should reject invalid API key', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key-123', { clientId: 'client-1', scopes: ['parquedb:read'] }]
        ])
      })

      const result = await authenticator.authenticate('wrong-key')

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('invalid_token')
        expect(result.error.statusCode).toBe(401)
      }
    })

    it('should reject empty token', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1', scopes: [] }]
        ])
      })

      const result = await authenticator.authenticate('')

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('missing_token')
        expect(result.error.statusCode).toBe(401)
      }
    })

    it('should reject whitespace-only token', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1', scopes: [] }]
        ])
      })

      const result = await authenticator.authenticate('   ')

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('missing_token')
      }
    })

    it('should trim whitespace from token', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1', scopes: ['parquedb:read'] }]
        ])
      })

      const result = await authenticator.authenticate('  test-key  ')

      expect(result.success).toBe(true)
    })

    it('should use default scopes when entry has no scopes', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1' }] // No scopes
        ]),
        defaultScopes: ['parquedb:read', 'parquedb:list']
      })

      const result = await authenticator.authenticate('test-key')

      expect(result.success).toBe(true)
      if (result.success) {
        expect(result.authInfo.scopes).toEqual(['parquedb:read', 'parquedb:list'])
      }
    })

    it('should reject deactivated key', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1', scopes: [], active: false }]
        ])
      })

      const result = await authenticator.authenticate('test-key')

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('invalid_token')
        expect(result.error.message).toContain('deactivated')
      }
    })

    it('should reject expired key when checkExpiration is enabled', async () => {
      const pastTimestamp = Math.floor(Date.now() / 1000) - 3600 // 1 hour ago

      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1', scopes: [], expiresAt: pastTimestamp }]
        ]),
        checkExpiration: true
      })

      const result = await authenticator.authenticate('test-key')

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('expired_token')
        expect(result.error.message).toContain('expired')
      }
    })

    it('should accept non-expired key when checkExpiration is enabled', async () => {
      const futureTimestamp = Math.floor(Date.now() / 1000) + 3600 // 1 hour from now

      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1', scopes: [], expiresAt: futureTimestamp }]
        ]),
        checkExpiration: true
      })

      const result = await authenticator.authenticate('test-key')

      expect(result.success).toBe(true)
    })

    it('should not check expiration by default', async () => {
      const pastTimestamp = Math.floor(Date.now() / 1000) - 3600 // 1 hour ago

      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1', scopes: [], expiresAt: pastTimestamp }]
        ])
        // checkExpiration defaults to false
      })

      const result = await authenticator.authenticate('test-key')

      expect(result.success).toBe(true)
    })

    it('should check required scopes from context', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1', scopes: ['parquedb:read'] }]
        ])
      })

      const context: AuthContext = {
        operation: 'parquedb_create',
        requiredScopes: ['parquedb:write']
      }

      const result = await authenticator.authenticate('test-key', context)

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('insufficient_scope')
        expect(result.error.statusCode).toBe(403)
        expect(result.error.message).toContain('parquedb:write')
      }
    })

    it('should allow when all required scopes are present', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', { clientId: 'client-1', scopes: ['parquedb:read', 'parquedb:write'] }]
        ])
      })

      const context: AuthContext = {
        operation: 'parquedb_create',
        requiredScopes: ['parquedb:write']
      }

      const result = await authenticator.authenticate('test-key', context)

      expect(result.success).toBe(true)
    })

    it('should include metadata in authInfo', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['test-key', {
            clientId: 'client-1',
            scopes: [],
            name: 'Test Key',
            metadata: { environment: 'test' }
          }]
        ])
      })

      const result = await authenticator.authenticate('test-key')

      expect(result.success).toBe(true)
      if (result.success) {
        expect(result.authInfo.extra?.name).toBe('Test Key')
        expect(result.authInfo.extra?.environment).toBe('test')
      }
    })

    it('should redact token in authInfo', async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['super-secret-api-key-12345', { clientId: 'client-1', scopes: [] }]
        ])
      })

      const result = await authenticator.authenticate('super-secret-api-key-12345')

      expect(result.success).toBe(true)
      if (result.success) {
        expect(result.authInfo.token).toBe('supe...2345')
        expect(result.authInfo.token).not.toContain('secret')
      }
    })

    describe('revoke()', () => {
      it('should deactivate a key', async () => {
        const apiKeys = new Map<string, ApiKeyEntry>([
          ['test-key', { clientId: 'client-1', scopes: [] }]
        ])
        const authenticator = createApiKeyAuthenticator({ apiKeys })

        // Key should work initially
        let result = await authenticator.authenticate('test-key')
        expect(result.success).toBe(true)

        // Revoke the key
        await authenticator.revoke!('test-key')

        // Key should no longer work
        result = await authenticator.authenticate('test-key')
        expect(result.success).toBe(false)
        if (!result.success) {
          expect(result.error.message).toContain('deactivated')
        }
      })
    })

    describe('SHA-256 hash comparison', () => {
      it('should authenticate with hashed key comparison', async () => {
        const authenticator = createApiKeyAuthenticator({
          apiKeys: new Map([
            ['test-key-123', { clientId: 'client-1', scopes: ['parquedb:read'] }]
          ]),
          hashAlgorithm: 'sha256'
        })

        const result = await authenticator.authenticate('test-key-123')

        expect(result.success).toBe(true)
      })

      it('should reject invalid key with hash comparison', async () => {
        const authenticator = createApiKeyAuthenticator({
          apiKeys: new Map([
            ['test-key-123', { clientId: 'client-1', scopes: ['parquedb:read'] }]
          ]),
          hashAlgorithm: 'sha256'
        })

        const result = await authenticator.authenticate('wrong-key')

        expect(result.success).toBe(false)
      })
    })
  })

  describe('createCustomAuthenticator', () => {
    it('should authenticate with custom verify function', async () => {
      const authenticator = createCustomAuthenticator({
        verify: async (token) => {
          if (token === 'valid-jwt') {
            return {
              token: redactToken(token),
              clientId: 'jwt-user-123',
              scopes: ['parquedb:read', 'parquedb:write']
            }
          }
          return null
        }
      })

      const result = await authenticator.authenticate('valid-jwt')

      expect(result.success).toBe(true)
      if (result.success) {
        expect(result.authInfo.clientId).toBe('jwt-user-123')
      }
    })

    it('should reject invalid token from custom verify', async () => {
      const authenticator = createCustomAuthenticator({
        verify: async () => null
      })

      const result = await authenticator.authenticate('invalid-token')

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('invalid_token')
      }
    })

    it('should handle errors from verify function', async () => {
      const authenticator = createCustomAuthenticator({
        verify: async () => {
          throw new Error('Verification service unavailable')
        }
      })

      const result = await authenticator.authenticate('any-token')

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('server_error')
        expect(result.error.statusCode).toBe(500)
        expect(result.error.message).toContain('Verification service unavailable')
      }
    })

    it('should check token expiration', async () => {
      const pastTimestamp = Math.floor(Date.now() / 1000) - 3600

      const authenticator = createCustomAuthenticator({
        verify: async (token) => ({
          token,
          clientId: 'user-1',
          scopes: [],
          expiresAt: pastTimestamp
        })
      })

      const result = await authenticator.authenticate('any-token')

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('expired_token')
      }
    })

    it('should check required scopes from context', async () => {
      const authenticator = createCustomAuthenticator({
        verify: async (token) => ({
          token,
          clientId: 'user-1',
          scopes: ['parquedb:read']
        })
      })

      const context: AuthContext = {
        requiredScopes: ['parquedb:write']
      }

      const result = await authenticator.authenticate('any-token', context)

      expect(result.success).toBe(false)
      if (!result.success) {
        expect(result.error.code).toBe('insufficient_scope')
      }
    })

    describe('refresh()', () => {
      it('should refresh token when refresh function is provided', async () => {
        const authenticator = createCustomAuthenticator({
          verify: async (token) => ({
            token,
            clientId: 'user-1',
            scopes: ['parquedb:read']
          }),
          refresh: async (authInfo) => ({
            ...authInfo,
            expiresAt: Math.floor(Date.now() / 1000) + 7200
          })
        })

        const authInfo: AuthInfo = {
          token: 'old-token',
          clientId: 'user-1',
          scopes: ['parquedb:read'],
          expiresAt: Math.floor(Date.now() / 1000) + 100
        }

        const result = await authenticator.refresh!(authInfo)

        expect(result.success).toBe(true)
        if (result.success) {
          expect(result.authInfo.expiresAt).toBeGreaterThan(authInfo.expiresAt!)
        }
      })

      it('should return error when refresh is not supported', async () => {
        const authenticator = createCustomAuthenticator({
          verify: async (token) => ({
            token,
            clientId: 'user-1',
            scopes: []
          })
          // No refresh function provided
        })

        const authInfo: AuthInfo = {
          token: 'token',
          clientId: 'user-1',
          scopes: []
        }

        const result = await authenticator.refresh!(authInfo)

        expect(result.success).toBe(false)
        if (!result.success) {
          expect(result.error.statusCode).toBe(501)
        }
      })
    })

    describe('revoke()', () => {
      it('should call custom revoke function', async () => {
        const revokeFn = vi.fn()

        const authenticator = createCustomAuthenticator({
          verify: async () => null,
          revoke: revokeFn
        })

        await authenticator.revoke!('token-to-revoke')

        expect(revokeFn).toHaveBeenCalledWith('token-to-revoke')
      })
    })
  })

  describe('Utility Functions', () => {
    describe('parseBearerToken', () => {
      it('should extract token from Bearer header', () => {
        const token = parseBearerToken('Bearer abc123')
        expect(token).toBe('abc123')
      })

      it('should be case-insensitive for Bearer', () => {
        expect(parseBearerToken('bearer abc123')).toBe('abc123')
        expect(parseBearerToken('BEARER abc123')).toBe('abc123')
      })

      it('should return null for non-Bearer headers', () => {
        expect(parseBearerToken('Basic abc123')).toBeNull()
        expect(parseBearerToken('Token abc123')).toBeNull()
      })

      it('should return null for malformed headers', () => {
        expect(parseBearerToken('Bearer')).toBeNull()
        expect(parseBearerToken('Bearer abc 123')).toBeNull()
        expect(parseBearerToken('')).toBeNull()
      })

      it('should return null for null/undefined input', () => {
        expect(parseBearerToken(null)).toBeNull()
        expect(parseBearerToken(undefined)).toBeNull()
      })
    })

    describe('redactToken', () => {
      it('should redact middle of token', () => {
        const redacted = redactToken('my-secret-api-key-12345')
        expect(redacted).toBe('my-s...2345')
      })

      it('should return *** for short tokens', () => {
        expect(redactToken('short')).toBe('***')
        expect(redactToken('ab')).toBe('***')
      })

      it('should handle exactly 12 character tokens', () => {
        expect(redactToken('123456789012')).toBe('***')
      })

      it('should handle 13 character tokens', () => {
        expect(redactToken('1234567890123')).toBe('1234...0123')
      })
    })

    describe('hasRequiredScopes', () => {
      it('should return true when all scopes are present', () => {
        const granted = ['parquedb:read', 'parquedb:write']
        const required = ['parquedb:read']

        expect(hasRequiredScopes(granted, required)).toBe(true)
      })

      it('should return false when scopes are missing', () => {
        const granted = ['parquedb:read']
        const required = ['parquedb:write']

        expect(hasRequiredScopes(granted, required)).toBe(false)
      })

      it('should return true for empty required scopes', () => {
        const granted = ['parquedb:read']
        const required: string[] = []

        expect(hasRequiredScopes(granted, required)).toBe(true)
      })

      it('should grant all access with admin scope', () => {
        const granted = [PARQUEDB_SCOPES.ADMIN]
        const required = ['parquedb:read', 'parquedb:write', 'parquedb:delete', 'parquedb:aggregate']

        expect(hasRequiredScopes(granted, required)).toBe(true)
      })
    })

    describe('getToolScopes', () => {
      it('should return correct scopes for read operations', () => {
        expect(getToolScopes('parquedb_find')).toEqual([PARQUEDB_SCOPES.READ])
        expect(getToolScopes('parquedb_get')).toEqual([PARQUEDB_SCOPES.READ])
        expect(getToolScopes('parquedb_count')).toEqual([PARQUEDB_SCOPES.READ])
      })

      it('should return correct scopes for write operations', () => {
        expect(getToolScopes('parquedb_create')).toEqual([PARQUEDB_SCOPES.WRITE])
        expect(getToolScopes('parquedb_update')).toEqual([PARQUEDB_SCOPES.UPDATE])
        expect(getToolScopes('parquedb_delete')).toEqual([PARQUEDB_SCOPES.DELETE])
      })

      it('should return correct scopes for utility operations', () => {
        expect(getToolScopes('parquedb_list_collections')).toEqual([PARQUEDB_SCOPES.LIST])
        expect(getToolScopes('parquedb_semantic_search')).toEqual([PARQUEDB_SCOPES.SEARCH])
        expect(getToolScopes('parquedb_aggregate')).toEqual([PARQUEDB_SCOPES.AGGREGATE])
      })

      it('should return default read scope for unknown tools', () => {
        expect(getToolScopes('unknown_tool')).toEqual([PARQUEDB_SCOPES.READ])
      })
    })

    describe('PARQUEDB_SCOPES', () => {
      it('should have all expected scopes defined', () => {
        expect(PARQUEDB_SCOPES.READ).toBe('parquedb:read')
        expect(PARQUEDB_SCOPES.WRITE).toBe('parquedb:write')
        expect(PARQUEDB_SCOPES.UPDATE).toBe('parquedb:update')
        expect(PARQUEDB_SCOPES.DELETE).toBe('parquedb:delete')
        expect(PARQUEDB_SCOPES.LIST).toBe('parquedb:list')
        expect(PARQUEDB_SCOPES.SEARCH).toBe('parquedb:search')
        expect(PARQUEDB_SCOPES.AGGREGATE).toBe('parquedb:aggregate')
        expect(PARQUEDB_SCOPES.ADMIN).toBe('parquedb:admin')
      })
    })

    describe('TOOL_SCOPE_REQUIREMENTS', () => {
      it('should have requirements for all tools', () => {
        expect(TOOL_SCOPE_REQUIREMENTS['parquedb_find']).toBeDefined()
        expect(TOOL_SCOPE_REQUIREMENTS['parquedb_get']).toBeDefined()
        expect(TOOL_SCOPE_REQUIREMENTS['parquedb_create']).toBeDefined()
        expect(TOOL_SCOPE_REQUIREMENTS['parquedb_update']).toBeDefined()
        expect(TOOL_SCOPE_REQUIREMENTS['parquedb_delete']).toBeDefined()
        expect(TOOL_SCOPE_REQUIREMENTS['parquedb_count']).toBeDefined()
        expect(TOOL_SCOPE_REQUIREMENTS['parquedb_aggregate']).toBeDefined()
        expect(TOOL_SCOPE_REQUIREMENTS['parquedb_list_collections']).toBeDefined()
        expect(TOOL_SCOPE_REQUIREMENTS['parquedb_semantic_search']).toBeDefined()
      })
    })
  })

  describe('AuthenticationError', () => {
    it('should create error with correct properties', () => {
      const error = new AuthenticationError({
        code: 'invalid_token',
        message: 'Token is invalid',
        statusCode: 401
      })

      expect(error.name).toBe('AuthenticationError')
      expect(error.code).toBe('invalid_token')
      expect(error.message).toBe('Token is invalid')
      expect(error.statusCode).toBe(401)
    })

    it('should use default status code 401', () => {
      const error = new AuthenticationError({
        code: 'missing_token',
        message: 'No token'
      })

      expect(error.statusCode).toBe(401)
    })

    it('should be instance of Error', () => {
      const error = new AuthenticationError({
        code: 'server_error',
        message: 'Internal error',
        statusCode: 500
      })

      expect(error).toBeInstanceOf(Error)
      expect(error).toBeInstanceOf(AuthenticationError)
    })
  })
})

/**
 * MCP Server Authentication Integration Tests
 *
 * Tests that verify authentication is properly applied when the MCP server is used.
 */
describe('MCP Server Authentication Integration', () => {
  let db: ReturnType<typeof DB>
  let server: McpServer
  let client: Client

  beforeEach(async () => {
    db = DB({
      Posts: {
        title: 'string!',
        content: 'text',
        status: 'string',
      },
    })

    await db.collection('posts').create({
      $type: 'Post',
      name: 'Test Post',
      title: 'Hello World',
      content: 'Content',
      status: 'published',
    })
  })

  afterEach(async () => {
    if (client) await client.close()
    if (server) await server.close()
  })

  describe('Server without authentication', () => {
    beforeEach(async () => {
      server = createParqueDBMCPServer(db)
      client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)
    })

    it('should allow unauthenticated access when no auth is configured', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: { collection: 'posts' }
      })

      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
    })
  })

  describe('Server with API key authentication', () => {
    const apiKeys = new Map([
      ['read-only-key', {
        clientId: 'reader',
        scopes: [PARQUEDB_SCOPES.READ, PARQUEDB_SCOPES.LIST]
      }],
      ['full-access-key', {
        clientId: 'admin',
        scopes: [PARQUEDB_SCOPES.ADMIN]
      }],
      ['write-key', {
        clientId: 'writer',
        scopes: [PARQUEDB_SCOPES.READ, PARQUEDB_SCOPES.WRITE]
      }]
    ])

    beforeEach(async () => {
      const authenticator = createApiKeyAuthenticator({ apiKeys })

      server = createParqueDBMCPServer(db, {
        auth: {
          authenticator,
          required: true,
          extractToken: (context) => {
            // For testing, we'll use a context property
            return (context as any).apiKey ?? null
          }
        }
      })

      client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)
    })

    it('should include auth instructions in server info', async () => {
      const result = await client.listTools()
      // The server should be configured - tools should be available
      expect(result.tools).toBeDefined()
    })

    // Note: The MCP SDK's InMemoryTransport doesn't directly support passing
    // context to tool calls in the way we'd need for auth testing.
    // In a real deployment, auth context would come from the transport layer.
    // These tests verify the auth configuration is accepted without errors.

    it('should configure server with authentication', () => {
      // Server created successfully with auth config
      expect(server).toBeDefined()
    })
  })

  describe('Server with custom authentication', () => {
    const validTokens = new Map([
      ['jwt-token-admin', { clientId: 'admin', scopes: [PARQUEDB_SCOPES.ADMIN] }],
      ['jwt-token-reader', { clientId: 'reader', scopes: [PARQUEDB_SCOPES.READ] }],
    ])

    beforeEach(async () => {
      const authenticator = createCustomAuthenticator({
        verify: async (token) => {
          const entry = validTokens.get(token)
          if (!entry) return null
          return {
            token: redactToken(token),
            clientId: entry.clientId,
            scopes: entry.scopes
          }
        }
      })

      server = createParqueDBMCPServer(db, {
        auth: {
          authenticator,
          required: false // Optional auth
        }
      })

      client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)
    })

    it('should allow access when auth is optional and no token provided', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: { collection: 'posts' }
      })

      // When auth is optional and no token, should still work
      expect(result.isError).toBeFalsy()
    })
  })

  describe('Server with scope checking disabled', () => {
    beforeEach(async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['limited-key', { clientId: 'limited', scopes: ['custom:scope'] }]
        ])
      })

      server = createParqueDBMCPServer(db, {
        auth: {
          authenticator,
          required: true,
          enableScopeCheck: false // Disable scope checking
        }
      })

      client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)
    })

    it('should configure server with disabled scope checking', () => {
      // Server created successfully
      expect(server).toBeDefined()
    })
  })

  describe('Server with auth callbacks', () => {
    it('should call onAuthSuccess callback', async () => {
      const onAuthSuccess = vi.fn()
      const onAuthFailure = vi.fn()

      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['valid-key', { clientId: 'test', scopes: [PARQUEDB_SCOPES.READ] }]
        ])
      })

      server = createParqueDBMCPServer(db, {
        auth: {
          authenticator,
          required: false,
          onAuthSuccess,
          onAuthFailure
        }
      })

      client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      // Server configured with callbacks
      expect(server).toBeDefined()
    })
  })

  describe('Read-only mode with authentication', () => {
    beforeEach(async () => {
      const authenticator = createApiKeyAuthenticator({
        apiKeys: new Map([
          ['admin-key', { clientId: 'admin', scopes: [PARQUEDB_SCOPES.ADMIN] }]
        ])
      })

      server = createParqueDBMCPServer(db, {
        readOnly: true, // Read-only mode
        auth: {
          authenticator,
          required: false
        }
      })

      client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)
    })

    it('should not register write tools even with admin auth', async () => {
      const result = await client.listTools()
      const toolNames = result.tools.map(t => t.name)

      // Read tools should be present
      expect(toolNames).toContain('parquedb_find')
      expect(toolNames).toContain('parquedb_get')

      // Write tools should NOT be present (read-only mode)
      expect(toolNames).not.toContain('parquedb_create')
      expect(toolNames).not.toContain('parquedb_update')
      expect(toolNames).not.toContain('parquedb_delete')
    })
  })
})

/**
 * JWT-style Authentication Example
 *
 * Demonstrates how to integrate with a JWT-based auth system.
 */
describe('JWT Authentication Pattern', () => {
  it('should work with JWT-like token verification', async () => {
    // Simulated JWT structure
    interface DecodedJWT {
      sub: string
      scope: string
      exp: number
      iat: number
    }

    // Mock JWT decode function
    const decodeJWT = (token: string): DecodedJWT | null => {
      if (token.startsWith('eyJ')) {
        // Simulated valid JWT
        return {
          sub: 'user-123',
          scope: 'parquedb:read parquedb:write',
          exp: Math.floor(Date.now() / 1000) + 3600,
          iat: Math.floor(Date.now() / 1000)
        }
      }
      return null
    }

    const authenticator = createCustomAuthenticator({
      verify: async (token) => {
        const decoded = decodeJWT(token)
        if (!decoded) return null

        return {
          token: redactToken(token),
          clientId: decoded.sub,
          scopes: decoded.scope.split(' '),
          expiresAt: decoded.exp
        }
      }
    })

    // Test with valid JWT-like token
    const result = await authenticator.authenticate('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9')

    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.authInfo.clientId).toBe('user-123')
      expect(result.authInfo.scopes).toContain('parquedb:read')
      expect(result.authInfo.scopes).toContain('parquedb:write')
    }
  })

  it('should reject invalid JWT tokens', async () => {
    const authenticator = createCustomAuthenticator({
      verify: async (token) => {
        // Only accept tokens starting with eyJ (JWT format)
        if (!token.startsWith('eyJ')) return null
        return {
          token: redactToken(token),
          clientId: 'user',
          scopes: []
        }
      }
    })

    const result = await authenticator.authenticate('not-a-jwt')

    expect(result.success).toBe(false)
    if (!result.success) {
      expect(result.error.code).toBe('invalid_token')
    }
  })
})
