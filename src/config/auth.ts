/**
 * Authentication Integration
 *
 * Integrates oauth.do with ParqueDB for actor resolution
 * and authentication context.
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
 * This resolver automatically gets the current user from oauth.do
 * and returns their ID as the actor for mutations.
 *
 * @example
 * ```typescript
 * import { createOAuthActorResolver, setActorResolver } from 'parquedb/config'
 *
 * setActorResolver(createOAuthActorResolver())
 * ```
 */
export function createOAuthActorResolver(): ActorResolver {
  return async () => {
    try {
      // Dynamic import to avoid loading oauth.do unless needed
      const { getToken, getUser } = await import('oauth.do')

      const token = await getToken()
      if (!token) {
        return null
      }

      const authResult = await getUser(token)
      if (!authResult.user?.id) {
        return null
      }

      // Return user ID as EntityId (users/{id} format)
      return `users/${authResult.user.id}` as EntityId
    } catch {
      // oauth.do not available or error
      return null
    }
  }
}

/**
 * Create an actor resolver from environment variable
 *
 * @example
 * ```typescript
 * setActorResolver(createEnvActorResolver('PARQUEDB_ACTOR'))
 * ```
 */
export function createEnvActorResolver(envVar: string = 'PARQUEDB_ACTOR'): ActorResolver {
  return async () => {
    const actor = process.env[envVar]
    return actor ? (actor as EntityId) : null
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
 *   createOAuthActorResolver(),
 *   createEnvActorResolver(),
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
    email?: string
    organizationId?: string
    roles?: string[]
    permissions?: string[]
  } | null
  token?: string
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
