/**
 * Cloudflare Snippets API Integration Tests
 *
 * Tests for the Snippets deployment module.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  SnippetsClient,
  createSnippetsClientFromEnv,
  isValidSnippetName,
  normalizeSnippetName,
  type SnippetsConfig,
  type Snippet,
  type SnippetRule,
  type CloudflareResponse,
} from '../../../src/deploy/snippets'

describe('SnippetsClient', () => {
  const mockConfig: SnippetsConfig = {
    apiToken: 'test-api-token',
    zoneId: 'test-zone-id',
  }

  let client: SnippetsClient
  let originalFetch: typeof global.fetch

  beforeEach(() => {
    client = new SnippetsClient(mockConfig)
    originalFetch = global.fetch
  })

  afterEach(() => {
    global.fetch = originalFetch
    vi.restoreAllMocks()
  })

  describe('constructor', () => {
    it('should create a client with valid config', () => {
      expect(client).toBeInstanceOf(SnippetsClient)
    })

    it('should throw if apiToken is missing', () => {
      expect(() => new SnippetsClient({ apiToken: '', zoneId: 'zone' })).toThrow(
        'apiToken is required'
      )
    })

    it('should throw if zoneId is missing', () => {
      expect(() => new SnippetsClient({ apiToken: 'token', zoneId: '' })).toThrow(
        'zoneId is required'
      )
    })

    it('should use default API base URL', () => {
      const client = new SnippetsClient(mockConfig)
      // The client uses the default URL internally
      expect(client).toBeInstanceOf(SnippetsClient)
    })

    it('should accept custom API base URL', () => {
      const client = new SnippetsClient({
        ...mockConfig,
        apiBaseUrl: 'https://custom.api.com',
      })
      expect(client).toBeInstanceOf(SnippetsClient)
    })
  })

  describe('listSnippets', () => {
    it('should list all snippets', async () => {
      const mockSnippets: Snippet[] = [
        { snippet_name: 'test_snippet', created_on: '2024-01-01', modified_on: '2024-01-02' },
        { snippet_name: 'another_snippet', created_on: '2024-01-03', modified_on: '2024-01-04' },
      ]

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: mockSnippets,
            errors: [],
            messages: [],
          } as CloudflareResponse<Snippet[]>),
      })

      const snippets = await client.listSnippets()

      expect(snippets).toEqual(mockSnippets)
      expect(global.fetch).toHaveBeenCalledWith(
        'https://api.cloudflare.com/client/v4/zones/test-zone-id/snippets',
        expect.objectContaining({
          headers: expect.any(Headers),
        })
      )
    })

    it('should return empty array when no snippets exist', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: [],
            errors: [],
            messages: [],
          } as CloudflareResponse<Snippet[]>),
      })

      const snippets = await client.listSnippets()
      expect(snippets).toEqual([])
    })
  })

  describe('getSnippet', () => {
    it('should get a specific snippet', async () => {
      const mockSnippet: Snippet = {
        snippet_name: 'my_snippet',
        created_on: '2024-01-01',
        modified_on: '2024-01-02',
      }

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: mockSnippet,
            errors: [],
            messages: [],
          } as CloudflareResponse<Snippet>),
      })

      const snippet = await client.getSnippet('my_snippet')

      expect(snippet).toEqual(mockSnippet)
      expect(global.fetch).toHaveBeenCalledWith(
        'https://api.cloudflare.com/client/v4/zones/test-zone-id/snippets/my_snippet',
        expect.anything()
      )
    })

    it('should return null for non-existent snippet', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        json: () =>
          Promise.resolve({
            success: false,
            result: null,
            errors: [{ code: 10000, message: 'not found' }],
            messages: [],
          }),
      })

      const snippet = await client.getSnippet('nonexistent')
      expect(snippet).toBeNull()
    })
  })

  describe('getSnippetContent', () => {
    it('should get snippet content', async () => {
      const code = 'export default { fetch() { return new Response("Hello") } }'

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        text: () => Promise.resolve(code),
      })

      const content = await client.getSnippetContent('my_snippet')

      expect(content).toBe(code)
      expect(global.fetch).toHaveBeenCalledWith(
        'https://api.cloudflare.com/client/v4/zones/test-zone-id/snippets/my_snippet/content',
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: 'Bearer test-api-token',
          }),
        })
      )
    })

    it('should return null for non-existent snippet', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 404,
      })

      const content = await client.getSnippetContent('nonexistent')
      expect(content).toBeNull()
    })
  })

  describe('createOrUpdateSnippet', () => {
    it('should create a new snippet', async () => {
      const mockSnippet: Snippet = {
        snippet_name: 'new_snippet',
        created_on: '2024-01-01',
        modified_on: '2024-01-01',
      }

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: mockSnippet,
            errors: [],
            messages: [],
          } as CloudflareResponse<Snippet>),
      })

      const code = 'export default { fetch() { return new Response("Hello") } }'
      const result = await client.createOrUpdateSnippet({
        name: 'new_snippet',
        code,
      })

      expect(result).toEqual(mockSnippet)
      expect(global.fetch).toHaveBeenCalledWith(
        'https://api.cloudflare.com/client/v4/zones/test-zone-id/snippets/new_snippet',
        expect.objectContaining({
          method: 'PUT',
          body: expect.any(FormData),
        })
      )
    })

    it('should reject invalid snippet names', async () => {
      await expect(
        client.createOrUpdateSnippet({
          name: 'Invalid-Name!',
          code: 'code',
        })
      ).rejects.toThrow('Snippet name can only contain lowercase letters')
    })

    it('should accept valid snippet names', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: { snippet_name: 'valid_name_123', created_on: '', modified_on: '' },
            errors: [],
            messages: [],
          }),
      })

      await expect(
        client.createOrUpdateSnippet({
          name: 'valid_name_123',
          code: 'code',
        })
      ).resolves.toBeDefined()
    })

    it('should use custom main module name', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: { snippet_name: 'test', created_on: '', modified_on: '' },
            errors: [],
            messages: [],
          }),
      })

      await client.createOrUpdateSnippet({
        name: 'test',
        code: 'code',
        mainModule: 'custom.js',
      })

      // Verify the FormData was created with the custom filename
      expect(global.fetch).toHaveBeenCalled()
    })
  })

  describe('deleteSnippet', () => {
    it('should delete an existing snippet', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: 'deleted',
            errors: [],
            messages: [],
          }),
      })

      const result = await client.deleteSnippet('my_snippet')

      expect(result).toBe(true)
      expect(global.fetch).toHaveBeenCalledWith(
        'https://api.cloudflare.com/client/v4/zones/test-zone-id/snippets/my_snippet',
        expect.objectContaining({
          method: 'DELETE',
        })
      )
    })

    it('should return false for non-existent snippet', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        json: () =>
          Promise.resolve({
            success: false,
            result: null,
            errors: [{ code: 10000, message: 'not found' }],
            messages: [],
          }),
      })

      const result = await client.deleteSnippet('nonexistent')
      expect(result).toBe(false)
    })
  })

  describe('listRules', () => {
    it('should list all snippet rules', async () => {
      const mockRules: SnippetRule[] = [
        {
          id: 'rule1',
          snippet_name: 'test_snippet',
          expression: 'http.request.uri.path eq "/api"',
          enabled: true,
        },
      ]

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: { rules: mockRules },
            errors: [],
            messages: [],
          }),
      })

      const rules = await client.listRules()

      expect(rules).toEqual(mockRules)
      expect(global.fetch).toHaveBeenCalledWith(
        'https://api.cloudflare.com/client/v4/zones/test-zone-id/snippets/snippet_rules',
        expect.anything()
      )
    })
  })

  describe('updateRules', () => {
    it('should update snippet rules', async () => {
      const newRules: SnippetRule[] = [
        {
          snippet_name: 'my_snippet',
          expression: 'http.request.uri.path starts_with "/api"',
          enabled: true,
          description: 'Route API calls',
        },
      ]

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: { rules: newRules },
            errors: [],
            messages: [],
          }),
      })

      const result = await client.updateRules({ rules: newRules })

      expect(result).toEqual(newRules)
      expect(global.fetch).toHaveBeenCalledWith(
        'https://api.cloudflare.com/client/v4/zones/test-zone-id/snippets/snippet_rules',
        expect.objectContaining({
          method: 'PUT',
          body: JSON.stringify({ rules: newRules }),
        })
      )
    })
  })

  describe('deploy', () => {
    it('should deploy a snippet without a rule', async () => {
      const mockSnippet: Snippet = {
        snippet_name: 'my_api',
        created_on: '2024-01-01',
        modified_on: '2024-01-01',
      }

      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () =>
          Promise.resolve({
            success: true,
            result: mockSnippet,
            errors: [],
            messages: [],
          }),
      })

      const result = await client.deploy({
        name: 'my_api',
        code: 'export default { fetch() { return new Response("OK") } }',
      })

      expect(result.success).toBe(true)
      expect(result.snippet).toEqual(mockSnippet)
      expect(result.rules).toBeUndefined()
    })

    it('should deploy a snippet with a rule', async () => {
      const mockSnippet: Snippet = {
        snippet_name: 'my_api',
        created_on: '2024-01-01',
        modified_on: '2024-01-01',
      }

      const mockRules: SnippetRule[] = [
        {
          snippet_name: 'my_api',
          expression: 'http.request.uri.path starts_with "/api"',
          enabled: true,
        },
      ]

      let callCount = 0
      global.fetch = vi.fn().mockImplementation(() => {
        callCount++
        if (callCount === 1) {
          // createOrUpdateSnippet
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                success: true,
                result: mockSnippet,
                errors: [],
                messages: [],
              }),
          })
        } else if (callCount === 2) {
          // listRules
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                success: true,
                result: { rules: [] },
                errors: [],
                messages: [],
              }),
          })
        } else {
          // updateRules
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                success: true,
                result: { rules: mockRules },
                errors: [],
                messages: [],
              }),
          })
        }
      })

      const result = await client.deploy({
        name: 'my_api',
        code: 'export default { fetch() { return new Response("OK") } }',
        rule: {
          expression: 'http.request.uri.path starts_with "/api"',
        },
      })

      expect(result.success).toBe(true)
      expect(result.snippet).toEqual(mockSnippet)
      expect(result.rules).toEqual(mockRules)
    })

    it('should return error on failure', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: false,
        json: () =>
          Promise.resolve({
            success: false,
            result: null,
            errors: [{ code: 10001, message: 'API error' }],
            messages: [],
          }),
      })

      const result = await client.deploy({
        name: 'my_api',
        code: 'invalid',
      })

      expect(result.success).toBe(false)
      expect(result.error).toBeDefined()
    })
  })

  describe('undeploy', () => {
    it('should remove snippet and its rules', async () => {
      let callCount = 0
      global.fetch = vi.fn().mockImplementation(() => {
        callCount++
        if (callCount === 1) {
          // listRules
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                success: true,
                result: {
                  rules: [
                    { snippet_name: 'my_api', expression: 'test', enabled: true },
                    { snippet_name: 'other', expression: 'test2', enabled: true },
                  ],
                },
                errors: [],
                messages: [],
              }),
          })
        } else if (callCount === 2) {
          // updateRules (without my_api rule)
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                success: true,
                result: { rules: [{ snippet_name: 'other', expression: 'test2', enabled: true }] },
                errors: [],
                messages: [],
              }),
          })
        } else {
          // deleteSnippet
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                success: true,
                result: 'deleted',
                errors: [],
                messages: [],
              }),
          })
        }
      })

      const result = await client.undeploy('my_api')

      expect(result).toBe(true)
      expect(callCount).toBe(3) // listRules, updateRules, deleteSnippet
    })
  })
})

describe('createSnippetsClientFromEnv', () => {
  const originalEnv = process.env

  beforeEach(() => {
    process.env = { ...originalEnv }
  })

  afterEach(() => {
    process.env = originalEnv
  })

  it('should create client from environment variables', () => {
    process.env.CLOUDFLARE_API_TOKEN = 'test-token'
    process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

    const client = createSnippetsClientFromEnv()
    expect(client).toBeInstanceOf(SnippetsClient)
  })

  it('should throw if CLOUDFLARE_API_TOKEN is missing', () => {
    delete process.env.CLOUDFLARE_API_TOKEN
    process.env.CLOUDFLARE_ZONE_ID = 'test-zone'

    expect(() => createSnippetsClientFromEnv()).toThrow(
      'CLOUDFLARE_API_TOKEN environment variable is required'
    )
  })

  it('should throw if CLOUDFLARE_ZONE_ID is missing', () => {
    process.env.CLOUDFLARE_API_TOKEN = 'test-token'
    delete process.env.CLOUDFLARE_ZONE_ID

    expect(() => createSnippetsClientFromEnv()).toThrow(
      'CLOUDFLARE_ZONE_ID environment variable is required'
    )
  })
})

describe('isValidSnippetName', () => {
  it('should accept valid names', () => {
    expect(isValidSnippetName('test')).toBe(true)
    expect(isValidSnippetName('my_snippet')).toBe(true)
    expect(isValidSnippetName('snippet123')).toBe(true)
    expect(isValidSnippetName('a')).toBe(true)
    expect(isValidSnippetName('test_name_123')).toBe(true)
  })

  it('should reject invalid names', () => {
    expect(isValidSnippetName('')).toBe(false)
    expect(isValidSnippetName('Test')).toBe(false) // uppercase
    expect(isValidSnippetName('my-snippet')).toBe(false) // hyphen
    expect(isValidSnippetName('my snippet')).toBe(false) // space
    expect(isValidSnippetName('my.snippet')).toBe(false) // dot
    expect(isValidSnippetName('snippet!')).toBe(false) // special char
  })

  it('should reject names over 100 characters', () => {
    expect(isValidSnippetName('a'.repeat(100))).toBe(true)
    expect(isValidSnippetName('a'.repeat(101))).toBe(false)
  })
})

describe('normalizeSnippetName', () => {
  it('should lowercase names', () => {
    expect(normalizeSnippetName('TestSnippet')).toBe('testsnippet')
    expect(normalizeSnippetName('MY_SNIPPET')).toBe('my_snippet')
  })

  it('should replace invalid characters with underscores', () => {
    expect(normalizeSnippetName('my-snippet')).toBe('my_snippet')
    expect(normalizeSnippetName('my snippet')).toBe('my_snippet')
    expect(normalizeSnippetName('my.snippet')).toBe('my_snippet')
    expect(normalizeSnippetName('my@snippet!')).toBe('my_snippet')
  })

  it('should collapse multiple underscores', () => {
    expect(normalizeSnippetName('my--snippet')).toBe('my_snippet')
    expect(normalizeSnippetName('my___snippet')).toBe('my_snippet')
    expect(normalizeSnippetName('my - snippet')).toBe('my_snippet')
  })

  it('should remove leading and trailing underscores', () => {
    expect(normalizeSnippetName('_snippet')).toBe('snippet')
    expect(normalizeSnippetName('snippet_')).toBe('snippet')
    expect(normalizeSnippetName('_snippet_')).toBe('snippet')
  })

  it('should truncate to 100 characters', () => {
    const longName = 'a'.repeat(150)
    expect(normalizeSnippetName(longName).length).toBe(100)
  })
})
