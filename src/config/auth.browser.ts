/**
 * Browser-safe Authentication Stub
 *
 * This module provides browser-compatible stubs for auth functions.
 * The actual oauth.do integration is only available in Node.js environments.
 *
 * In browser environments:
 * - createOAuthActorResolver() returns a no-op resolver
 * - Other functions work normally (they don't require oauth.do)
 */

import type { EntityId } from '../types/entity'

/**
 * Actor resolver function type
 */
export type ActorResolver = () => Promise<EntityId | null>

// Module-scoped actor resolver
let _actorResolver: ActorResolver | null = null

/**
 * Set the global actor resolver
 * @param resolver - The actor resolver function to set
 * @returns void
 */
export function setActorResolver(resolver: ActorResolver): void {
  _actorResolver = resolver
}

/**
 * Get the current actor resolver
 */
export function getActorResolver(): ActorResolver | null {
  return _actorResolver
}

/**
 * Resolve the current actor using the configured resolver
 */
export async function resolveActor(): Promise<EntityId | null> {
  if (!_actorResolver) {
    return null
  }
  return _actorResolver()
}

/**
 * Create an actor resolver from oauth.do
 *
 * NOTE: In browser environments, this returns a no-op resolver.
 * oauth.do requires Node.js APIs (keytar) that are not available in browsers.
 *
 * For browser authentication, use a custom resolver that works with your
 * browser-based auth system (e.g., cookies, localStorage tokens).
 *
 * @example
 * ```typescript
 * // In browser, create a custom resolver instead:
 * setActorResolver(async () => {
 *   const token = localStorage.getItem('auth_token')
 *   if (!token) return null
 *   // Parse token to get user ID
 *   const payload = JSON.parse(atob(token.split('.')[1]))
 *   return `users/${payload.sub}` as EntityId
 * })
 * ```
 */
export function createOAuthActorResolver(): ActorResolver {
  // Browser stub - oauth.do not available
  return async () => {
    console.warn(
      '[ParqueDB] createOAuthActorResolver() is not available in browser environments. ' +
      'Use setActorResolver() with a custom resolver for browser auth.'
    )
    return null
  }
}

/**
 * Create an actor resolver from environment variable
 *
 * NOTE: In browser environments, this always returns null since
 * process.env is not available.
 */
export function createEnvActorResolver(_envVar: string = 'PARQUEDB_ACTOR'): ActorResolver {
  return async () => {
    // process.env not available in browser
    return null
  }
}

/**
 * Create a static actor resolver
 *
 * @example
 * ```typescript
 * setActorResolver(createStaticActorResolver('system/cli'))
 * ```
 */
export function createStaticActorResolver(actor: EntityId): ActorResolver {
  return async () => actor
}

/**
 * Create a combined actor resolver that tries multiple sources
 *
 * @example
 * ```typescript
 * setActorResolver(createCombinedActorResolver([
 *   createStaticActorResolver('system/anonymous'),
 * ]))
 * ```
 */
export function createCombinedActorResolver(resolvers: ActorResolver[]): ActorResolver {
  return async () => {
    for (const resolver of resolvers) {
      const actor = await resolver()
      if (actor) {
        return actor
      }
    }
    return null
  }
}

/**
 * Authentication context for Workers/requests
 */
export interface AuthContext {
  user: {
    id: string
    email?: string | undefined
    organizationId?: string | undefined
    roles?: string[] | undefined
    permissions?: string[] | undefined
  } | null
  token?: string | undefined
  actor: EntityId | null
}

/**
 * Create auth context from oauth.do user
 */
export function createAuthContext(user: AuthContext['user'], token?: string): AuthContext {
  return {
    user,
    token,
    actor: user?.id ? (`users/${user.id}` as EntityId) : null,
  }
}
