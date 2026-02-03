/**
 * Cloudflare Snippets API Integration
 *
 * Provides deployment capabilities for ParqueDB to Cloudflare Snippets.
 * Snippets are lightweight JavaScript code that runs at the edge,
 * enabling fast, programmable HTTP traffic control.
 *
 * @see https://developers.cloudflare.com/rules/snippets/
 * @see https://developers.cloudflare.com/api/resources/snippets/
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Cloudflare API response wrapper
 */
export interface CloudflareResponse<T> {
  success: boolean
  errors: Array<{ code: number; message: string }>
  messages: string[]
  result: T
}

/**
 * Snippet metadata from the API
 */
export interface Snippet {
  snippet_name: string
  created_on: string
  modified_on: string
}

/**
 * Snippet rule for triggering execution
 */
export interface SnippetRule {
  id?: string
  description?: string
  enabled: boolean
  expression: string
  snippet_name: string
  last_updated?: string
}

/**
 * Configuration for Cloudflare Snippets deployment
 */
export interface SnippetsConfig {
  /** Cloudflare API token with Zone > Snippets > Edit permission */
  apiToken: string
  /** Zone ID (hexadecimal string from Cloudflare dashboard) */
  zoneId: string
  /** Base URL for Cloudflare API (default: https://api.cloudflare.com/client/v4) */
  apiBaseUrl?: string
}

/**
 * Options for creating or updating a snippet
 */
export interface CreateSnippetOptions {
  /** Snippet name (a-z, 0-9, _ only, cannot be changed after creation) */
  name: string
  /** JavaScript code to deploy */
  code: string
  /** Optional filename for the main module (default: snippet.js) */
  mainModule?: string
}

/**
 * Options for creating snippet rules
 */
export interface CreateRulesOptions {
  /** Rules to create/update (replaces all existing rules) */
  rules: SnippetRule[]
}

/**
 * Result of a deployment operation
 */
export interface DeployResult {
  success: boolean
  snippet?: Snippet
  rules?: SnippetRule[]
  error?: string
}

// =============================================================================
// Client Implementation
// =============================================================================

/**
 * Cloudflare Snippets API Client
 *
 * Provides methods for managing Cloudflare Snippets via the API.
 *
 * @example
 * ```typescript
 * const client = new SnippetsClient({
 *   apiToken: process.env.CLOUDFLARE_API_TOKEN!,
 *   zoneId: process.env.CLOUDFLARE_ZONE_ID!,
 * })
 *
 * // Deploy a snippet
 * await client.createOrUpdateSnippet({
 *   name: 'my_parquedb_endpoint',
 *   code: 'export default { fetch(req) { return new Response("Hello") } }',
 * })
 *
 * // Create a rule to trigger it
 * await client.updateRules({
 *   rules: [{
 *     description: 'Route to ParqueDB',
 *     enabled: true,
 *     expression: 'http.request.uri.path starts_with "/api/db"',
 *     snippet_name: 'my_parquedb_endpoint',
 *   }],
 * })
 * ```
 */
export class SnippetsClient {
  private apiToken: string
  private zoneId: string
  private apiBaseUrl: string

  constructor(config: SnippetsConfig) {
    if (!config.apiToken) {
      throw new Error('apiToken is required')
    }
    if (!config.zoneId) {
      throw new Error('zoneId is required')
    }

    this.apiToken = config.apiToken
    this.zoneId = config.zoneId
    this.apiBaseUrl = config.apiBaseUrl || 'https://api.cloudflare.com/client/v4'
  }

  /**
   * Get the base URL for snippet operations
   */
  private getSnippetsUrl(): string {
    return `${this.apiBaseUrl}/zones/${this.zoneId}/snippets`
  }

  /**
   * Make an authenticated request to the Cloudflare API
   */
  private async request<T>(
    path: string,
    options: RequestInit = {}
  ): Promise<CloudflareResponse<T>> {
    const url = `${this.getSnippetsUrl()}${path}`
    const headers = new Headers(options.headers)
    headers.set('Authorization', `Bearer ${this.apiToken}`)

    const response = await fetch(url, {
      ...options,
      headers,
    })

    const data = await response.json() as CloudflareResponse<T>

    if (!response.ok && data.errors?.length > 0) {
      const errorMessages = data.errors.map(e => `${e.code}: ${e.message}`).join(', ')
      throw new Error(`Cloudflare API error: ${errorMessages}`)
    }

    return data
  }

  /**
   * List all snippets in the zone
   */
  async listSnippets(): Promise<Snippet[]> {
    const response = await this.request<Snippet[]>('')
    return response.result || []
  }

  /**
   * Get a specific snippet by name
   */
  async getSnippet(name: string): Promise<Snippet | null> {
    try {
      const response = await this.request<Snippet>(`/${name}`)
      return response.result
    } catch (error) {
      // Return null if snippet doesn't exist
      if (error instanceof Error && error.message.includes('not found')) {
        return null
      }
      throw error
    }
  }

  /**
   * Get the content of a snippet
   */
  async getSnippetContent(name: string): Promise<string | null> {
    try {
      const url = `${this.getSnippetsUrl()}/${name}/content`
      const response = await fetch(url, {
        headers: {
          'Authorization': `Bearer ${this.apiToken}`,
        },
      })

      if (!response.ok) {
        if (response.status === 404) {
          return null
        }
        throw new Error(`Failed to get snippet content: ${response.statusText}`)
      }

      return await response.text()
    } catch (error) {
      if (error instanceof Error && error.message.includes('not found')) {
        return null
      }
      throw error
    }
  }

  /**
   * Create or update a snippet
   *
   * @param options - Snippet creation options
   * @returns The created/updated snippet metadata
   */
  async createOrUpdateSnippet(options: CreateSnippetOptions): Promise<Snippet> {
    const { name, code, mainModule = 'snippet.js' } = options

    // Validate snippet name
    if (!/^[a-z0-9_]+$/.test(name)) {
      throw new Error(
        'Snippet name can only contain lowercase letters (a-z), numbers (0-9), and underscores (_)'
      )
    }

    // Create form data with the snippet file and metadata
    const formData = new FormData()

    // Add the JavaScript file
    const blob = new Blob([code], { type: 'application/javascript' })
    formData.append('files', blob, mainModule)

    // Add metadata
    formData.append('metadata', JSON.stringify({ main_module: mainModule }))

    const url = `${this.getSnippetsUrl()}/${name}`
    const response = await fetch(url, {
      method: 'PUT',
      headers: {
        'Authorization': `Bearer ${this.apiToken}`,
      },
      body: formData,
    })

    const data = await response.json() as CloudflareResponse<Snippet>

    if (!response.ok || !data.success) {
      const errorMessages = data.errors?.map(e => `${e.code}: ${e.message}`).join(', ') || 'Unknown error'
      throw new Error(`Failed to create/update snippet: ${errorMessages}`)
    }

    return data.result
  }

  /**
   * Delete a snippet
   */
  async deleteSnippet(name: string): Promise<boolean> {
    try {
      await this.request<string>(`/${name}`, { method: 'DELETE' })
      return true
    } catch (error) {
      if (error instanceof Error && error.message.includes('not found')) {
        return false
      }
      throw error
    }
  }

  /**
   * List all snippet rules in the zone
   */
  async listRules(): Promise<SnippetRule[]> {
    const response = await this.request<{ rules: SnippetRule[] }>('/snippet_rules')
    return response.result?.rules || []
  }

  /**
   * Update snippet rules (replaces all existing rules)
   *
   * @param options - Rules configuration
   * @returns The updated rules
   */
  async updateRules(options: CreateRulesOptions): Promise<SnippetRule[]> {
    const response = await this.request<{ rules: SnippetRule[] }>('/snippet_rules', {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ rules: options.rules }),
    })
    return response.result?.rules || []
  }

  /**
   * Delete all snippet rules
   */
  async deleteAllRules(): Promise<boolean> {
    try {
      await this.request<void>('/snippet_rules', { method: 'DELETE' })
      return true
    } catch {
      return false
    }
  }

  /**
   * Deploy a snippet with an optional rule
   *
   * This is a convenience method that creates/updates a snippet and
   * optionally adds a rule to trigger it.
   *
   * @param options - Deployment options
   * @returns Deployment result
   */
  async deploy(options: {
    name: string
    code: string
    rule?: {
      expression: string
      description?: string
      enabled?: boolean
    }
  }): Promise<DeployResult> {
    try {
      // Create or update the snippet
      const snippet = await this.createOrUpdateSnippet({
        name: options.name,
        code: options.code,
      })

      let rules: SnippetRule[] | undefined

      // If a rule is provided, add it to existing rules
      if (options.rule) {
        const existingRules = await this.listRules()

        // Remove any existing rule for this snippet
        const filteredRules = existingRules.filter(r => r.snippet_name !== options.name)

        // Add the new rule
        const newRule: SnippetRule = {
          snippet_name: options.name,
          expression: options.rule.expression,
          description: options.rule.description,
          enabled: options.rule.enabled ?? true,
        }

        rules = await this.updateRules({
          rules: [...filteredRules, newRule],
        })
      }

      return {
        success: true,
        snippet,
        rules,
      }
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  /**
   * Undeploy a snippet (delete snippet and remove its rules)
   *
   * @param name - Snippet name to undeploy
   * @returns True if successfully undeployed
   */
  async undeploy(name: string): Promise<boolean> {
    try {
      // Remove rules for this snippet
      const existingRules = await this.listRules()
      const filteredRules = existingRules.filter(r => r.snippet_name !== name)

      if (filteredRules.length !== existingRules.length) {
        await this.updateRules({ rules: filteredRules })
      }

      // Delete the snippet
      return await this.deleteSnippet(name)
    } catch {
      return false
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a Snippets client from environment variables
 *
 * Expects:
 * - CLOUDFLARE_API_TOKEN: API token with Zone > Snippets > Edit permission
 * - CLOUDFLARE_ZONE_ID: Zone ID from Cloudflare dashboard
 */
export function createSnippetsClientFromEnv(): SnippetsClient {
  const apiToken = process.env.CLOUDFLARE_API_TOKEN
  const zoneId = process.env.CLOUDFLARE_ZONE_ID

  if (!apiToken) {
    throw new Error('CLOUDFLARE_API_TOKEN environment variable is required')
  }
  if (!zoneId) {
    throw new Error('CLOUDFLARE_ZONE_ID environment variable is required')
  }

  return new SnippetsClient({ apiToken, zoneId })
}

/**
 * Validate a snippet name
 */
export function isValidSnippetName(name: string): boolean {
  return /^[a-z0-9_]+$/.test(name) && name.length > 0 && name.length <= 100
}

/**
 * Normalize a string to a valid snippet name
 */
export function normalizeSnippetName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .slice(0, 100)
}
