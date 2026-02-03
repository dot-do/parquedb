/**
 * Route Registry Pattern
 *
 * Provides a clean, type-safe way to register and match HTTP routes.
 * Routes can be static ("/health") or dynamic with parameters ("/datasets/:id").
 *
 * Features:
 * - Type-safe route handlers with context
 * - Support for static and parametric routes
 * - Method-based route matching (GET, POST, etc.)
 * - Priority-based matching (static before dynamic, more specific first)
 */

import type { Env } from '../types/worker'
import type { HandlerContext } from './handlers/types'

// =============================================================================
// Types
// =============================================================================

/**
 * Extended handler context with environment and extracted route params
 */
export interface RouteHandlerContext extends HandlerContext {
  /** Worker environment bindings */
  env: Env
  /** Extracted route parameters from dynamic paths */
  params: Record<string, string>
}

/**
 * Route handler function signature
 */
export type RouteHandler = (ctx: RouteHandlerContext) => Promise<Response> | Response

/**
 * Route definition
 */
export interface RouteDefinition {
  /** HTTP method(s) to match */
  method: string | string[]
  /** URL path pattern (e.g., "/health" or "/datasets/:id") */
  pattern: string
  /** Handler function */
  handler: RouteHandler
  /** Optional description for documentation */
  description?: string | undefined
  /** Rate limit endpoint type */
  rateLimit?: string | undefined
}

/**
 * Compiled route with regex for matching
 */
interface CompiledRoute extends RouteDefinition {
  /** Compiled regex pattern */
  regex: RegExp
  /** Parameter names extracted from pattern */
  paramNames: string[]
  /** Whether this is a static route (no params) */
  isStatic: boolean
}

// =============================================================================
// Route Registry
// =============================================================================

/**
 * Route Registry - manages route registration and matching
 */
export class RouteRegistry {
  private staticRoutes: Map<string, Map<string, CompiledRoute>> = new Map()
  private dynamicRoutes: CompiledRoute[] = []

  /**
   * Register a route
   */
  register(definition: RouteDefinition): this {
    const compiled = this.compileRoute(definition)
    const methods = Array.isArray(definition.method) ? definition.method : [definition.method]

    for (const method of methods) {
      const upperMethod = method.toUpperCase()

      if (compiled.isStatic) {
        // Static routes go into a map for O(1) lookup
        if (!this.staticRoutes.has(upperMethod)) {
          this.staticRoutes.set(upperMethod, new Map())
        }
        this.staticRoutes.get(upperMethod)!.set(definition.pattern, compiled)
      } else {
        // Dynamic routes need regex matching
        this.dynamicRoutes.push({ ...compiled, method: upperMethod })
      }
    }

    return this
  }

  /**
   * Register multiple routes
   */
  registerAll(definitions: RouteDefinition[]): this {
    for (const def of definitions) {
      this.register(def)
    }
    return this
  }

  /**
   * Match a request to a route
   *
   * @param method - HTTP method
   * @param path - URL path
   * @returns Matched route and extracted params, or null if no match
   */
  match(method: string, path: string): { route: CompiledRoute; params: Record<string, string> } | null {
    const upperMethod = method.toUpperCase()

    // Try static routes first (O(1) lookup)
    const staticMethodRoutes = this.staticRoutes.get(upperMethod)
    if (staticMethodRoutes) {
      const staticRoute = staticMethodRoutes.get(path)
      if (staticRoute) {
        return { route: staticRoute, params: {} }
      }
    }

    // Try dynamic routes
    for (const route of this.dynamicRoutes) {
      const methods = Array.isArray(route.method) ? route.method : [route.method]
      if (!methods.includes(upperMethod)) continue

      const match = path.match(route.regex)
      if (match) {
        const params: Record<string, string> = {}
        for (let i = 0; i < route.paramNames.length; i++) {
          params[route.paramNames[i]!] = decodeURIComponent(match[i + 1] ?? '')
        }
        return { route, params }
      }
    }

    return null
  }

  /**
   * Compile a route pattern into a regex
   */
  private compileRoute(definition: RouteDefinition): CompiledRoute {
    const { pattern } = definition
    const paramNames: string[] = []

    // Check if pattern has any parameters
    const hasParams = pattern.includes(':') || pattern.includes('*')

    if (!hasParams) {
      // Static route - use exact match
      return {
        ...definition,
        regex: new RegExp(`^${this.escapeRegex(pattern)}$`),
        paramNames: [],
        isStatic: true,
      }
    }

    // Convert pattern to regex
    // :param matches one path segment
    // :param* matches rest of path (greedy)
    // * matches rest of path (greedy)
    let regexPattern = pattern
      // Escape special regex chars (except : and *)
      .replace(/[.+?^${}()|[\]\\]/g, '\\$&')
      // Replace :param with capturing group for single segment
      .replace(/:([a-zA-Z][a-zA-Z0-9_]*)\*/g, (_, name) => {
        paramNames.push(name)
        return '(.+)' // Greedy capture for rest of path
      })
      .replace(/:([a-zA-Z][a-zA-Z0-9_]*)/g, (_, name) => {
        paramNames.push(name)
        return '([^/]+)' // Single segment capture
      })
      // Handle standalone *
      .replace(/\*/g, '(.*)')

    return {
      ...definition,
      regex: new RegExp(`^${regexPattern}$`),
      paramNames,
      isStatic: false,
    }
  }

  /**
   * Escape special regex characters
   */
  private escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  }

  /**
   * Get all registered routes for documentation
   */
  getRoutes(): RouteDefinition[] {
    const routes: RouteDefinition[] = []

    for (const [, methodRoutes] of this.staticRoutes) {
      for (const [, route] of methodRoutes) {
        routes.push(route)
      }
    }

    for (const route of this.dynamicRoutes) {
      routes.push(route)
    }

    return routes
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create a new route registry
 */
export function createRouteRegistry(): RouteRegistry {
  return new RouteRegistry()
}

/**
 * Route builder helper for fluent API
 */
export const route = {
  get: (pattern: string, handler: RouteHandler, opts?: Partial<RouteDefinition>): RouteDefinition => ({
    method: 'GET',
    pattern,
    handler,
    ...opts,
  }),

  post: (pattern: string, handler: RouteHandler, opts?: Partial<RouteDefinition>): RouteDefinition => ({
    method: 'POST',
    pattern,
    handler,
    ...opts,
  }),

  put: (pattern: string, handler: RouteHandler, opts?: Partial<RouteDefinition>): RouteDefinition => ({
    method: 'PUT',
    pattern,
    handler,
    ...opts,
  }),

  patch: (pattern: string, handler: RouteHandler, opts?: Partial<RouteDefinition>): RouteDefinition => ({
    method: 'PATCH',
    pattern,
    handler,
    ...opts,
  }),

  delete: (pattern: string, handler: RouteHandler, opts?: Partial<RouteDefinition>): RouteDefinition => ({
    method: 'DELETE',
    pattern,
    handler,
    ...opts,
  }),

  all: (pattern: string, handler: RouteHandler, opts?: Partial<RouteDefinition>): RouteDefinition => ({
    method: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'],
    pattern,
    handler,
    ...opts,
  }),
}
